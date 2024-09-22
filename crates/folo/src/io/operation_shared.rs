use crate::{
    constants::{self, GENERAL_BYTES_BUCKETS, GENERAL_MILLISECONDS_BUCKETS},
    io::{self, Buffer, OperationResultShared},
    mem::{isolation::Shared, DropPolicy, PinnedSlabChain},
    metrics::{Event, EventBuilder, Magnitude},
    time::UltraLowPrecisionInstant,
};
use pin_project::pin_project;
use std::{
    cell::{RefCell, UnsafeCell},
    fmt,
    future::Future,
    mem::{self, ManuallyDrop},
    pin::Pin,
    ptr,
    sync::Mutex,
    task::Poll,
};
use tracing::{event, Level};
use windows::Win32::{
    Foundation::{ERROR_IO_PENDING, NTSTATUS, STATUS_SUCCESS},
    Networking::WinSock::{SOCKET_ERROR, WSA_IO_PENDING},
    System::IO::{OVERLAPPED, OVERLAPPED_ENTRY},
};

/// Maintains the backing storage for the metadata structures of I/O operations submitted to the
/// operating system and organizes their allocation/release.
///
/// The operation store uses interior mutability to facilitate access from different parts of a call
/// chain. For example, during the preparation of an operation, it may be abandoned, in which case
/// the operation object will use a circular reference back to the store to ask for itself to be
/// freed. This would be impossible with regular borrowing rules.
///
/// TODO: The API has simplified enough that this might now be possible? Give it a try.
///
/// # Safety
///
/// Contents of an OperationStore are exposed to the operating system and do not always follow Rust
/// borrow checking rules nor participate in Rust lifetime logic nor participate in Rust native
/// allocation/deallocation logic at all times. For safe operation, the OperationStore must be freed
/// only after all native I/O operations referencing the contents have been completed. You can check
/// whether this is the case via `is_empty()` - freeing the store is only valid when empty.
#[derive(Debug)]
pub(super) struct OperationStoreShared {
    // The operations are stored in UnsafeCell because we are doings things like taking a shared
    // reference from the slab chain and giving it to the operating system to mutate, which would
    // be invalid Rust without Unsafecell.
    items: Mutex<RefCell<PinnedSlabChain<UnsafeCell<OperationCore>>>>,
}

impl OperationStoreShared {
    pub fn new() -> Self {
        Self {
            // We use a MustNotDropItems policy because the operations are shared with the operating
            // system so it is in general not safe to drop the memory unless the OS is done with it,
            // in which case our completion methods below will remove the operation from the items
            // collection.
            items: Mutex::new(RefCell::new(PinnedSlabChain::new(
                DropPolicy::MustNotDropItems,
            ))),
        }
    }

    /// Whether the operation store is empty and it is safe to drop the instance.
    pub fn is_empty(&self) -> bool {
        self.items
            .lock()
            .expect(constants::POISONED_LOCK)
            .borrow()
            .is_empty()
    }

    /// Creates a new operation for performing I/O. You need to wrap each native I/O API call you
    /// make into a new one of these operations. The caller provides a buffer for any input/output
    /// data, which the operation takes ownership of. Once the operation has completed, the buffer
    /// is returned to the caller for reading, reuse or disposal.
    pub fn new_operation(&self, buffer: Buffer<Shared>) -> OperationShared {
        OPERATIONS_ALLOCATED.with(Event::observe_unit);

        let items_guard = self.items.lock().expect(constants::POISONED_LOCK);
        let mut items = (*items_guard).borrow_mut();

        let inserter = items.begin_insert();
        let key = inserter.index();

        let core = inserter.insert(UnsafeCell::new(OperationCore::new(key, buffer)));

        OperationShared {
            // SAFETY: The core is only referenced by either Operation or the operating system at any
            // given time, so there is no possibility of multiple exclusive references being created.
            core: unsafe { &mut *core.get() },
            control: self.control_node(),
        }
    }

    /// Delivers the result of an operation that has completed asynchronously to its originator and
    /// releases any resources held by the operation store. We consume here the OVERLAPPED_ENTRY
    /// structure that represents not only the operation core but also the status and the number of
    /// bytes transferred.
    ///
    /// If the operation was executed on a caller-provided buffer, the caller can now get the buffer
    /// back from the returned value and reuse it for another operation.
    ///
    /// # Safety
    ///
    /// The input value must be the result of delivering to the operating system a legitimate
    /// OVERLAPPED pointer obtained from the callback given to `Operation::begin()` earlier.
    /// You must also have received a completion notification from the OS, saying that the operation
    /// has completed.
    pub unsafe fn complete_operation(&self, overlapped_entry: OVERLAPPED_ENTRY) {
        let bytes_transferred = overlapped_entry.dwNumberOfBytesTransferred as usize;
        let status = NTSTATUS(overlapped_entry.Internal as i32);

        OPERATIONS_COMPLETED_ASYNC.with(Event::observe_unit);
        OPERATION_COMPLETED_BYTES.with(|x| x.observe(bytes_transferred as Magnitude));

        // SAFETY: The core is only referenced by either Operation or the operating system at any
        // given time, so there is no possibility of multiple exclusive references being created.
        let core = &mut *(overlapped_entry.lpOverlapped as *mut OperationCore);

        // The buffer is returned to the originator, carrying any data affected by the operation.
        // This also enables them to reuse the buffer if they wish to do so.
        let mut buffer = core
            .buffer
            .take()
            .expect("buffer must exist because we only remove it after completion");

        buffer.set_len(bytes_transferred);

        let duration = UltraLowPrecisionInstant::now().duration_since(
            core.started
                .take()
                .expect("must have an operation start time because the operation is completed"),
        );

        OPERATION_COMPLETED_ASYNC_OK_DURATION.with(|x| x.observe_millis(duration));

        let result_tx = core
            .result_tx
            .take()
            .expect("result tx must exist because we have not yet sent the result");

        // The operation may not have been successful, so we need to investigate the status.
        // We ignore the tx return value because the receiver may have dropped already.
        if status != STATUS_SUCCESS {
            _ = result_tx.send(Err(io::OperationErrorShared::new(
                io::Error::Windows(status.into()),
                buffer,
            )));
        } else {
            _ = result_tx.send(Ok(buffer));
        }

        // All done!
        self.release(core.key);
    }

    /// Delivers the result of an operation that has completed synchronously to its originator and
    /// releases any resources held by the operation store. We consume here the OVERLAPPED
    /// structure that represents the operation core.
    ///
    /// If the operation was executed on a caller-provided buffer, the caller can now get the buffer
    /// back from the returned value and reuse it for another operation.
    ///
    /// This is for use with synchronous I/O operations that complete immediately, without
    /// triggering a completion notification.
    ///
    /// # Safety
    ///
    /// The input value must be the OVERLAPPED pointer handed to the callback in
    /// `Operation::begin()` earlier, which received a response from the OS saying that the
    /// operation completed immediately.
    unsafe fn complete_immediately(&self, overlapped: *mut OVERLAPPED) {
        // SAFETY: The core is only referenced by either Operation or the operating system at any
        // given time, so there is no possibility of multiple exclusive references being created.
        let core = &mut *(overlapped as *mut OperationCore);

        // The buffer is returned to the originator, carrying any data affected by the operation.
        // This also enables them to reuse the buffer if they wish to do so.
        let mut buffer = core
            .buffer
            .take()
            .expect("buffer must exist because we only remove it after completion");

        let bytes_transferred = core.immediate_bytes_transferred as usize;
        assert!(bytes_transferred <= buffer.len());

        OPERATIONS_COMPLETED_SYNC.with(Event::observe_unit);
        OPERATION_COMPLETED_BYTES.with(|x| x.observe(bytes_transferred as Magnitude));

        buffer.set_len(bytes_transferred);

        _ = core
            .result_tx
            .take()
            .expect("result tx must exist because we have not yet sent the result")
            .send(Ok(buffer));

        // All done!
        self.release(core.key);
    }

    fn release(&self, key: OperationKey) {
        assert!(key != OperationKey::MAX);

        let items_guard = self.items.lock().expect(constants::POISONED_LOCK);
        (*items_guard).borrow_mut().remove(key);
    }

    fn control_node(&self) -> ControlNode {
        ControlNode {
            // SAFETY: We pretend that the store is 'static to avoid overcomplex lifetime
            // annotations. This is embedded into operations, which anyway require us to keep the
            // store alive for the duration of their life, so it does not raise expectations.
            store: unsafe { mem::transmute::<&OperationStoreShared, &OperationStoreShared>(self) },
        }
    }
}

type OperationKey = usize;

/// Constrained API surface that allows an operation to command the store that owns it. This creates
/// a circular reference between an operation and the OperationStore, so we always use
/// OperationStore via interior mutability to prevent accidents here.
#[derive(Clone, Debug)]
struct ControlNode {
    /// This is not really 'static but we pretend it is to avoid overcomplicating with annotations.
    store: &'static OperationStoreShared,
}

impl ControlNode {
    fn release(&mut self, key: OperationKey) {
        self.store.release(key);
    }

    unsafe fn complete_immediately(&mut self, overlapped: *mut OVERLAPPED) {
        self.store.complete_immediately(overlapped)
    }
}

/// The operation core contains the data structures required to communicate with the operating
/// system and obtain the result of an asynchronous I/O operation.
///
/// As values participate in FFI calls, they can be leaked to the operating system. Before being
/// handed over the the operation system, an instance is wrapped in an Operation, which is
/// responsible for proper disposal if the operation is abandoned before it arrives at the OS API.
///
/// After the operation starts, the operating system owns the OperationCore in the form of a raw
/// pointer to OVERLAPPED (wrapped in OVERLAPPED_ENTRY when handed back to us). Once the I/O driver
/// receives a completion notification (or Operation detects that immediate completion occurred),
/// we ask the operation store to notify the caller that their result is ready, after which the
/// store disposes of the OperationCore.
#[repr(C)] // Facilitates conversion to/from OVERLAPPED.
struct OperationCore {
    /// The part of the operation visible to the operating system.
    ///
    /// NB! This must be the first item in the struct because
    /// we treat `*OperationCore` and `*OVERLAPPED` as equivalent!
    overlapped: OVERLAPPED,

    /// The caller-provided buffer containing the data affected by the operation. The Buffer type
    /// guarantees that this is pinned and will not move. Once the operation is complete, we return
    /// the buffer to the caller and set this to None.
    buffer: Option<Buffer<Shared>>,

    /// Used to operate the control node, which requires us to know our own key.
    key: OperationKey,

    /// If the operation completed immediately (synchronously), this stores the number of bytes
    /// transferred. If the operation supports immediate completion, this value must be set by
    /// the caller (a `&mut` to this is handed to them in the callback of `Operation::begin()`).
    immediate_bytes_transferred: u32,

    /// This is where the I/O completion handler will deliver the result of the operation.
    /// Value is cleared when consumed, to make it obvious if any accidental reuse occurs.
    result_tx: Option<oneshot::Sender<io::OperationResultShared>>,
    result_rx: Option<oneshot::Receiver<io::OperationResultShared>>,

    /// Timestamp of when the operation is started. Used to report I/O operation durations.
    started: Option<UltraLowPrecisionInstant>,

    // Once pinned, this type cannot be unpinned.
    _phantom_pin: std::marker::PhantomPinned,
}

impl OperationCore {
    pub fn new(key: OperationKey, mut buffer: Buffer<Shared>) -> Self {
        let (result_tx, result_rx) = oneshot::channel();

        // IOCP cannot deal with bigger slices of data than u32::MAX, so limit the active range.
        if buffer.len() > u32::MAX as usize {
            buffer.set_len(u32::MAX as usize);
        }

        Self {
            overlapped: OVERLAPPED::default(),
            buffer: Some(buffer),
            key,
            immediate_bytes_transferred: 0,
            result_tx: Some(result_tx),
            result_rx: Some(result_rx),
            started: None,
            _phantom_pin: std::marker::PhantomPinned,
        }
    }
}

// SAFETY: It's OK, I promise (and hope).
unsafe impl Send for OperationCore {}
unsafe impl Sync for OperationCore {}

impl fmt::Debug for OperationCore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OperationCore")
            .field("buffer", &self.buffer)
            .field("key", &self.key)
            .field(
                "immediate_bytes_transferred",
                &self.immediate_bytes_transferred,
            )
            .field("result_tx", &self.result_tx)
            .field("result_rx", &self.result_rx)
            .field("started", &self.started)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct OperationShared {
    // We erase the lifetime because the lifetime of this extends outside the Rust universe and
    // we need to manually manage it anyway.
    core: &'static mut OperationCore,

    control: ControlNode,
}

impl OperationShared {
    /// For seekable I/O primitives (e.g. files), sets the offset in the file where the operation
    /// should be performed.
    pub fn set_offset(&mut self, offset: usize) {
        self.core.overlapped.Anonymous.Anonymous.Offset = offset as u32;
        self.core.overlapped.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
    }

    /// Executes an I/O operation, using the specified callback to pass the operation buffer and
    /// OVERLAPPED metadata structure to native OS functions.
    ///
    /// The callback must not reference any data owned by the caller because in many circumstances
    /// the I/O operation may outlive the caller. In other words, any state captured by the closure
    /// must have a 'static lifetime.
    ///
    /// This is the thread-safe operation, so any captured state must also be thread-safe,
    /// implementing at least the `Send` trait.
    ///
    /// # Callback arguments
    ///
    /// 1. The buffer to be used for the operation. For reads, just pass it along to a native API.
    ///    For writes, fill it with data and constrain the size as needed before passing it on.
    /// 2. The OVERLAPPED structure to be used for the operation. Pass it along to the native API
    ///    without modification.
    /// 3. An exclusive  reference to a variable that is to receive the number of bytes transferred
    ///    if the I/O operation completes synchronously (i.e. with `Ok(())`). This value is ignored
    ///    if the I/O operation completes asynchronously (i.e. with `Err(ERROR_IO_PENDING)`).
    ///
    /// # Safety
    ///
    /// You must call a native I/O operation with the OVERLAPPED pointer provided by the callback.
    /// If you fail to make such a call, you will leak resources and cause a panic on runtime
    /// shutdown when the leak is detected. It is fine if the call fails, but it must always happen
    /// and the callback return value must accurately represent the native API call result.
    ///
    /// All callback arguments are only valid for the duration of the callback. The 'static
    /// lifetimes on them are a lie because assigning correct lifetimes was too difficult.
    ///
    /// TODO: Replace 'static lifetimes with something that makes it clear that the values
    /// have some temporary lifetime only valid for the duration of the callback.
    pub unsafe fn begin<F>(self, f: F) -> OperationResultSharedFuture
    where
        F: FnOnce(&'static mut [u8], *mut OVERLAPPED, &mut u32) -> io::Result<()> + Send + 'static,
    {
        let result_rx = self
            .core
            .result_rx
            .take()
            .expect("operation is always expected to have result rx when beginning I/O");

        // We clone the control node because we may need to release the operation core if the
        // callback fails or even resurrect it immediately if the callback completes synchronously.
        let mut control_node = self.control.clone();
        let (buffer, overlapped, immediate_bytes_transferred) = self.into_callback_arguments();

        match f(buffer, overlapped, immediate_bytes_transferred) {
            // The operation was started asynchronously. This is what we want to see.
            Err(io::Error::Windows(e)) if e.code() == ERROR_IO_PENDING.into() => {}
            Err(io::Error::Winsock { code, detail })
                if code == SOCKET_ERROR && detail == WSA_IO_PENDING => {}

            // The operation completed synchronously. This means we will not get a completion
            // notification and must handle the result inline (because we set a flag saying this
            // when binding to the completion port).
            Ok(()) => {
                event!(
                    Level::TRACE,
                    message = "I/O operation completed immediately",
                    length = immediate_bytes_transferred
                );

                control_node.complete_immediately(overlapped);
            }

            // Something went wrong. In this case, the operation core was not consumed by the OS.
            // We need to free the operation core ourselves to avoid leaking it forever, as well
            // as resurrect the core so we can get the buffer out of it and back to the originator.
            Err(e) => {
                // SAFETY: The core is only referenced by either Operation or the operating system at any
                // given time, so there is no possibility of multiple exclusive references being created.
                let core = overlapped as *mut OperationCore;

                let buffer = (*core).buffer.take().expect(
                    "buffer must exist because we only remove it after completion or failure and right now we are doing the latter",
                );

                control_node.release((*core).key);

                return OperationResultSharedFuture {
                    receiver: result_rx,
                    error: Some(io::OperationErrorShared::new(e, buffer)),
                };
            }
        }

        OperationResultSharedFuture {
            receiver: result_rx,
            error: None,
        }
    }

    fn into_callback_arguments(self) -> (&'static mut [u8], *mut OVERLAPPED, &'static mut u32) {
        // We do not want to run Drop - this is an intentional cleanupless shattering of the type.
        // This is the reason for the "you must pass OVERLAPPED to the native API" warnings above.
        // If the values we extract are not used, we forever leak the object we got them from.
        let this = ManuallyDrop::new(self);

        // SAFETY: This is just a manual move between compatible fields - no worries.
        let operation = unsafe { ptr::read(&this.core) };

        operation.started = Some(UltraLowPrecisionInstant::now());

        (
            // SAFETY: Sets the lifetime to 'static because I cannot figure out a straightforward way to declare lifetimes here.
            // As long as the value is only used during the callback, this is fine (caller is responsible for not using it afterwards).
            unsafe {
                mem::transmute::<&mut [u8], &mut [u8]>(
                    Pin::into_inner_unchecked(operation
                        .buffer
                        .as_mut()
                        .expect("the buffer is only removed when the operation completes, so it must exist")
                        .as_mut_slice(),
                ))
            },
            &mut operation.overlapped as *mut _,
            // SAFETY: Sets the lifetime to 'static because I cannot figure out a straightforward way to declare lifetimes here.
            // As long as the value is only used during the callback, this is fine (caller is responsible for not using it afterwards).
            unsafe {
                mem::transmute::<&mut u32, &mut u32>(&mut operation.immediate_bytes_transferred)
            },
        )
    }
}

#[pin_project]
#[derive(Debug)]
pub struct OperationResultSharedFuture {
    #[pin]
    receiver: oneshot::Receiver<io::OperationResultShared>,
    error: Option<io::OperationErrorShared>,
}

impl Future for OperationResultSharedFuture {
    type Output = OperationResultShared;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if let Some(err) = this.error.take() {
            return Poll::Ready(Err(err));
        }

        match this.receiver.poll(cx) {
            Poll::Ready(v) => Poll::Ready(v.expect("")),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for OperationShared {
    fn drop(&mut self) {
        self.control.release(self.core.key);
    }
}

thread_local! {
    static OPERATIONS_ALLOCATED: Event = EventBuilder::new("io_shared_ops_allocated")
        .build();

    static OPERATIONS_COMPLETED_ASYNC: Event = EventBuilder::new("io_shared_ops_completed_async")
        .build();

    static OPERATIONS_COMPLETED_SYNC: Event = EventBuilder::new("io_shared_ops_completed_sync")
        .build();

    static OPERATION_COMPLETED_BYTES: Event = EventBuilder::new("io_shared_completed_bytes")
        .buckets(GENERAL_BYTES_BUCKETS)
        .build();

    static OPERATION_COMPLETED_ASYNC_OK_DURATION: Event = EventBuilder::new("io_shared_completed_async_ok_duration_millis")
        .buckets(GENERAL_MILLISECONDS_BUCKETS)
        .build();
}
