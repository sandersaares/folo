use super::Buffer;
use crate::{
    constants::{GENERAL_BYTES_BUCKETS, GENERAL_SECONDS_BUCKETS},
    io,
    metrics::{Event, EventBuilder},
    util::PinnedSlabChain,
};
use negative_impl::negative_impl;
use std::{
    cell::{RefCell, UnsafeCell},
    fmt,
    mem::{self, ManuallyDrop},
    ptr,
    time::Instant,
};
use tracing::{event, Level};
use windows::Win32::{
    Foundation::{ERROR_IO_PENDING, NTSTATUS, STATUS_SUCCESS},
    System::IO::{OVERLAPPED, OVERLAPPED_ENTRY},
};

// TODO: Try out using proper lifetime bounds once we kick out the data buffer from the block.
// Today, the data buffer makes the lifetime logic too complicated to be worth it.

/// Maintains the backing storage for I/O blocks and organizes their allocation/release.
///
/// The block store uses interior mutability to facilitate block operations from different parts
/// of a call chain, as blocks may need to be released when errors occur in deeper layers of
/// processing and it is very cumbersome to thread a `&mut BlockStore` through all the layers,
/// if at all possible.
///
/// # Safety
///
/// Contents of a BlockStore are exposed to native code. For safe operation, the BlockStore must
/// be freed after all native I/O operations referencing the blocks have been terminated. This
/// includes:
///
/// * All file handles must be closed.
/// * All I/O completion ports must be closed.
///
/// It is not required to wait for scheduled I/O completions to finish - closing the handles is
/// enough. TODO: Prove it.
#[derive(Debug)]
pub(super) struct BlockStore {
    // The blocks are stored in UnsafeCell because we are doings things like taking a shared
    // reference from the slab chain and giving it to the operating system to mutate, which would
    // be invalid Rust without Unsafecell.
    // We erase the lifetime of the blocks here and only specify an actual lifetime when returning
    // individual blocks to the caller because the lifetime tracking gets too convoluted otherwise.
    blocks: RefCell<PinnedSlabChain<UnsafeCell<Block<'static>>>>,
}

impl BlockStore {
    pub fn new() -> Self {
        Self {
            blocks: RefCell::new(PinnedSlabChain::new()),
        }
    }

    /// Allocates a new block for I/O operations.
    pub fn allocate<'a, 'b>(&'a self, buffer: Buffer<'b>) -> PrepareBlock<'b> {
        BLOCKS_ALLOCATED.with(Event::observe_unit);

        let mut blocks = self.blocks.borrow_mut();

        let inserter = blocks.begin_insert();
        let key = inserter.index();

        // SAFETY: We do not preserve the 'b annotation in the block store because the block store
        // does not need to know about such details. We preserve the annotation on the wrapper types
        // PrepareBlock and CompleteBlock instead, which should be enough to safeguard correctness.
        let block = inserter.insert(UnsafeCell::new(unsafe {
            mem::transmute(Block::new(key, buffer))
        }));

        PrepareBlock::<'b> {
            // SAFETY: The block is referenced by exactly one of PrepareBlock, CompleteBlock or the
            // operating system at any given time, so there is no possibility of multiple exclusive
            // references being created.
            // We reattach the 'b lifetime here, which we did not preserve in the block store.
            block: unsafe { mem::transmute(&mut *block.get()) },
            control: self.control_node(),
        }
    }

    /// Converts a block that has been handed over to the operating system into a `CompleteBlock`
    /// that represents the completed operation. We consume here the OVERLAPPED_ENTRY structure
    /// that contains not only the block but also the status and the number of bytes transferred.
    ///
    /// # Safety
    ///
    /// The input value must be the result of delivering to the operating system a legitimate
    /// OVERLAPPED pointer obtained from the callback given to `PrepareBlock::begin()` earlier.
    /// You must also have received a completion notification from the OS, saying that the block is
    /// again ready for your use.
    pub unsafe fn complete<'a, 'b>(
        &'a self,
        overlapped_entry: OVERLAPPED_ENTRY,
    ) -> CompleteBlock<'b> {
        let bytes_transferred = overlapped_entry.dwNumberOfBytesTransferred as usize;
        let status = NTSTATUS(overlapped_entry.Internal as i32);

        BLOCKS_COMPLETED_ASYNC.with(Event::observe_unit);
        BLOCK_COMPLETED_BYTES.with(|x| x.observe(bytes_transferred as f64));

        // SAFETY: The block is referenced by exactly one of PrepareBlock, CompleteBlock or the
        // operating system at any given time, so there is no possibility of multiple exclusive
        // references being created.
        let block = &mut *(overlapped_entry.lpOverlapped as *mut Block);

        block.buffer.set_length(bytes_transferred);

        let duration = Instant::now().duration_since(
            block
                .started
                .take()
                .expect("block must have an operation start time because the block is completed"),
        );

        BLOCK_COMPLETED_ASYNC_OK_DURATION.with(|x| x.observe(duration.as_secs_f64()));

        CompleteBlock {
            block,
            control: self.control_node(),
            status,
        }
    }

    /// Converts a block that was never handed over to the operating system into a `CompleteBlock`
    /// that represents the completed operation. We consume here the original OVERLAPPED that was
    /// provided to the native I/O function.
    ///
    /// This is for use with synchronous I/O operations that complete immediately, without
    /// triggering a completion notification.
    ///
    /// # Safety
    ///
    /// The input value must be the OVERLAPPED pointer handed to the callback in
    /// `PrepareBlock::begin()` earlier, which received a response from the OK saying that the
    /// operation completed immediately.
    unsafe fn complete_immediately(&self, overlapped: *mut OVERLAPPED) -> CompleteBlock {
        // SAFETY: The block is referenced by exactly one of PrepareBlock, CompleteBlock or the
        // operating system at any given time, so there is no possibility of multiple exclusive
        // references being created.
        let block = &mut *(overlapped as *mut Block);

        let bytes_transferred = block.immediate_bytes_transferred as usize;
        assert!(bytes_transferred <= block.buffer.length());

        BLOCKS_COMPLETED_SYNC.with(Event::observe_unit);
        BLOCK_COMPLETED_BYTES.with(|x| x.observe(bytes_transferred as f64));

        block.buffer.set_length(bytes_transferred);

        CompleteBlock {
            block,
            control: self.control_node(),
            status: STATUS_SUCCESS,
        }
    }

    fn release(&self, key: BlockKey) {
        assert!(key != BlockKey::MAX);

        self.blocks.borrow_mut().remove(key);
    }

    fn control_node(&self) -> ControlNode {
        ControlNode {
            // SAFETY: We pretend that the BlockStore is 'static to avoid overcomplex lifetime
            // annotations. This is embedded into Blocks, which anyway require us to keep the
            // block store alive for the duration of their life, so it does not raise expectations.
            store: unsafe { mem::transmute(self) },
        }
    }
}

type BlockKey = usize;

/// Constrained API surface that allows a block to command the BlockStore that owns it. This creates
/// a circular reference between a block and the BlockStore, so we always use BlockStore via
/// interior mutability.
#[derive(Clone, Debug)]
#[repr(transparent)]
struct ControlNode {
    /// This is not really 'static but we pretend it is to avoid overcomplex lifetime annotations.
    /// TODO: Try using real lifetimes once we get rid of the data buffer inside Block.
    store: &'static BlockStore,
}

impl ControlNode {
    fn release(&mut self, key: BlockKey) {
        self.store.release(key);
    }

    unsafe fn complete_immediately(&mut self, overlapped: *mut OVERLAPPED) -> CompleteBlock {
        self.store.complete_immediately(overlapped)
    }
}

// Just being careful here because we have a 'static reference in there which is very "loose".
#[negative_impl]
impl !Send for ControlNode {}
#[negative_impl]
impl !Sync for ControlNode {}

/// An I/O block contains the data structures required to communicate with the operating system
/// and obtain the result of an asynchronous I/O operation.
///
/// As they participate in FFI calls, they can be leaked to the operating system. There are two
/// forms in which blocks are exposed in code:
///
/// * PrepareBlock is a reference to a block that is currently owned by Folo code and is being
///   prepared as part of starting an I/O operation (e.g. a buffer is being filled or offset set).
/// * CompleteBlock is a reference to a block that carries the result of a completed operation and
///   is intended to be consumed before being reed.
///
/// Between the above steps, the operating system owns the block in the form of a raw pointer to
/// OVERLAPPED (wrapped in OVERLAPPED_ENTRY when handed back to us).
///
/// Dropping the block in any of the states releases all the resources, except when you call the
/// function to convert it to the native operating system objects (`.into_buffer_and_overlapped()`).
#[repr(C)] // Facilitates conversion to/from OVERLAPPED.
struct Block<'b> {
    /// The part of the operation block visible to the operating system.
    ///
    /// NB! This must be the first item in the struct because
    /// we treat `*Block` and `*OVERLAPPED` as equivalent!
    overlapped: OVERLAPPED,

    /// The buffer containing the data affected by the operation.
    buffer: Buffer<'b>,

    /// Used to operate the control node, which requires us to know our own key.
    key: BlockKey,

    /// If the operation completed immediately (synchronously), this stores the number of bytes
    /// transferred. Normally getting this information requires a round-trip through the IOCP but
    /// we expect that for immediate completions this value is set inline.
    immediate_bytes_transferred: u32,

    /// This is where the I/O completion handler will deliver the result of the operation.
    /// Value is cleared when consumed, to make it obvious if any accidental reuse occurs.
    result_tx: Option<oneshot::Sender<io::Result<CompleteBlock<'b>>>>,
    result_rx: Option<oneshot::Receiver<io::Result<CompleteBlock<'b>>>>,

    /// Timestamp of when the operation is started. Used to report I/O operation durations.
    started: Option<Instant>,

    // Once pinned, this type cannot be unpinned.
    _phantom_pin: std::marker::PhantomPinned,
}

impl<'b> Block<'b> {
    pub fn new(key: BlockKey, buffer: Buffer<'b>) -> Self {
        let (result_tx, result_rx) = oneshot::channel();

        Self {
            overlapped: OVERLAPPED::default(),
            buffer,
            key,
            immediate_bytes_transferred: 0,
            result_tx: Some(result_tx),
            result_rx: Some(result_rx),
            started: None,
            _phantom_pin: std::marker::PhantomPinned,
        }
    }
}

impl fmt::Debug for Block<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Block")
            .field("buffer", &self.buffer)
            .field("key", &self.key)
            .field(
                "immediate_bytes_transferred",
                &self.immediate_bytes_transferred,
            )
            .field("result_tx", &self.result_tx)
            .field("result_rx", &self.result_rx)
            .finish()
    }
}

// We need to to avoid accidents. All our I/O blocks need to stay on the same thread when they
// are in the Rust universe. The OS can do what it wants when it holds ownership but for us they
// are single-threaded.
#[negative_impl]
impl !Send for Block<'_> {}
#[negative_impl]
impl !Sync for Block<'_> {}

#[derive(Debug)]
pub(crate) struct PrepareBlock<'b> {
    // You can either have a PrepareBlock or a CompleteBlock or neither (when the OS owns it),
    // but not both, so we never have multiple exclusive references to the underlying Block.
    block: &'b mut Block<'b>,

    control: ControlNode,
}

impl<'b> PrepareBlock<'b> {
    /// For seekable I/O primitives (e.g. files), sets the offset in the file where the operation
    /// should be performed.
    pub fn set_offset(&mut self, offset: usize) {
        self.block.overlapped.Anonymous.Anonymous.Offset = offset as u32;
        self.block.overlapped.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
    }

    /// Executes an I/O operation, using the specified callback to pass the operation buffer and
    /// OVERLAPPED metadata structure to native OS functions.
    ///
    /// # Callback arguments
    ///
    /// 1. The buffer to be used for the operation. For reads, just pass it along to a native API.
    ///    For writes, fill it with data and constrain the size as needed before passing it on.
    /// 2. The OVERLAPPED structure to be used for the operation. Pass it along to the native API
    ///    without modification.
    /// 3. A mutable reference to a variable that is to receive the number of bytes transferred
    ///    if the I/O operation completes synchronously (i.e. with `Ok(())`). This value is ignored
    ///    if the I/O operation completes asynchronously (i.e. with `Err(ERROR_IO_PENDING)`).
    ///
    /// # Safety
    ///
    /// You must call a native I/O operation with the OVERLAPPED pointer provided by the callback.
    /// If you fail to make such a call, you will leak resources. It is fine if the call fails, but
    /// it must happen (and the callback return value must accurately represent the native result).
    ///
    /// All callback arguments are only valid for the duration of the callback. The 'static
    /// lifetimes on them are a lie because assigning correct lifetimes was too difficult.
    pub async unsafe fn begin<F>(self, f: F) -> io::Result<CompleteBlock<'b>>
    where
        F: FnOnce(&'static mut [u8], *mut OVERLAPPED, &mut u32) -> io::Result<()>,
    {
        let result_rx = self
            .block
            .result_rx
            .take()
            .expect("block is always expected to have result rx when beginning I/O operation");

        // We clone the control node because we may need to release the block if the callback fails
        // or resurrect it immediately if the callback completes synchronously.
        let mut control_node = self.control.clone();

        let block_key = self.block.key;

        let (buffer, overlapped, immediate_bytes_transferred) = self.into_callback_arguments();

        match f(buffer, overlapped, immediate_bytes_transferred) {
            // The operation was started asynchronously. This is what we want to see.
            Err(io::Error::External(e)) if e.code() == ERROR_IO_PENDING.into() => {}

            // The operation completed synchronously. This means we will not get a completion
            // notification and must handle the result inline (because we set a flag saying this
            // when binding to the completion port).
            Ok(()) => {
                let mut block = control_node.complete_immediately(overlapped);
                let result_tx = block.result_tx();
                // We ignore the tx return value because the receiver may have dropped already.
                _ = result_tx.send(Ok(block));

                event!(
                    Level::TRACE,
                    message = "I/O operation completed immediately",
                    key = block_key,
                    length = immediate_bytes_transferred
                );
            }

            // Something went wrong. In this case, the operation block was not consumed by the OS.
            // We need to resurrect and free the block ourselves to avoid leaking it forever.
            Err(e) => {
                control_node.release(block_key);
                return Err(e);
            }
        }

        result_rx
            .await
            .expect("no expected code path drops the I/O block without signaling completion result")
    }

    fn into_callback_arguments(self) -> (&'static mut [u8], *mut OVERLAPPED, &'static mut u32) {
        // We do not want to run Drop - this is an intentional cleanupless shattering of the type.
        let this = ManuallyDrop::new(self);

        // SAFETY: This is just a manual move between compatible fields - no worries.
        let block = unsafe { ptr::read(&this.block) };

        block.started = Some(Instant::now());

        (
            // SAFETY: Sets the lifetime to 'static because I cannot figure out a straightforward way to declare lifetimes here.
            // As long as the value is only used during the callback, this is fine (caller is responsible for not using it afterwards).
            unsafe { mem::transmute(block.buffer.as_mut_slice()) },
            &mut block.overlapped as *mut _,
            // SAFETY: Sets the lifetime to 'static because I cannot figure out a straightforward way to declare lifetimes here.
            // As long as the value is only used during the callback, this is fine (caller is responsible for not using it afterwards).
            unsafe { mem::transmute(&mut block.immediate_bytes_transferred) },
        )
    }
}

impl Drop for PrepareBlock<'_> {
    fn drop(&mut self) {
        self.control.release(self.block.key);
    }
}

#[derive(Debug)]
pub(crate) struct CompleteBlock<'b> {
    // You can either have a PrepareBlock or a CompleteBlock or neither (when the OS owns it),
    // but not both, so we never have multiple exclusive references to the underlying Block.
    block: &'b mut Block<'b>,

    control: ControlNode,

    status: NTSTATUS,
}

impl<'b> CompleteBlock<'b> {
    pub(crate) fn buffer(&self) -> &[u8] {
        self.block.buffer.as_slice()
    }

    pub(super) fn status(&self) -> NTSTATUS {
        self.status
    }

    /// # Panics
    ///
    /// If called more than once. You can only get the result_tx once.
    pub(super) fn result_tx(&mut self) -> oneshot::Sender<io::Result<CompleteBlock<'b>>> {
        self.block
            .result_tx
            .take()
            .expect("block is always expected to have result tx when completing I/O operation")
    }
}

impl Drop for CompleteBlock<'_> {
    fn drop(&mut self) {
        self.control.release(self.block.key);
    }
}

thread_local! {
    static BLOCKS_ALLOCATED: Event = EventBuilder::new()
        .name("io_blocks_allocated")
        .build()
        .unwrap();

    static BLOCKS_COMPLETED_ASYNC: Event = EventBuilder::new()
        .name("io_blocks_completed_async")
        .build()
        .unwrap();

    static BLOCKS_COMPLETED_SYNC: Event = EventBuilder::new()
        .name("io_blocks_completed_sync")
        .build()
        .unwrap();

    static BLOCK_COMPLETED_BYTES: Event = EventBuilder::new()
        .name("io_completed_bytes")
        .buckets(GENERAL_BYTES_BUCKETS)
        .build()
        .unwrap();

    static BLOCK_COMPLETED_ASYNC_OK_DURATION: Event = EventBuilder::new()
        .name("io_completed_async_ok_duration_seconds")
        .buckets(GENERAL_SECONDS_BUCKETS)
        .build()
        .unwrap();
}
