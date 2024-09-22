use crate::constants::GENERAL_MILLISECONDS_BUCKETS;
use crate::io::{
    self,
    operation_shared::{OperationShared, OperationStoreShared},
    Buffer, CompletionPortShared, IoPrimitive, IO_DEQUEUE_BATCH_SIZE,
};
use crate::mem::isolation::Shared;
use crate::metrics::{Event, EventBuilder, Magnitude};
use std::mem::{self, MaybeUninit};
use windows::Win32::{
    Foundation::WAIT_TIMEOUT,
    System::IO::{GetQueuedCompletionStatusEx, OVERLAPPED_ENTRY},
};
use windows_result::HRESULT;

/// Processes multithreaded I/O completion operations as part of the async worker loop.
///
/// Wrap an `Arc<>` around this to share it between all async workers that need to
/// collaborate in the execution of multithreaded I/O operations. Synchronization is taken care of
/// internally - no need to wrap in Mutex or similar, the public API is usable via shared reference.
///
/// # Safety
///
/// The driver must not be dropped while any I/O operation is in progress. To shut down safely, the
/// I/O driver must be polled until it signals that all I/O operations have completed (`is_inert()`
/// returns true).
#[derive(Debug)]
pub(crate) struct DriverShared {
    completion_port: CompletionPortShared,

    // These are the I/O operations that are currently in flight with the OS but for which the
    // result has not been processed yet. Items are added when operations are started and they are
    // removed when the completion notification has been fully processed and the originator of the
    // operation notified to pick up their results.
    //
    // This does not store the read/write buffers, only the operation metadata.
    operation_store: OperationStoreShared,
}

impl DriverShared {
    /// # Safety
    ///
    /// See safety requirements on the type.
    pub(crate) unsafe fn new() -> Self {
        Self {
            completion_port: CompletionPortShared::new(),
            operation_store: OperationStoreShared::new(),
        }
    }

    /// Whether the driver has entered a state where it is safe to drop it. This requires that all
    /// ongoing I/O operations be completed and the completion notification received.
    pub fn is_inert(&self) -> bool {
        self.operation_store.is_empty()
    }

    /// Binds an I/O primitive to the completion port of this driver, provided a handle to the I/O
    /// primitive in question (file handle, socket, ...). This must be called once for every I/O
    /// primitive used with this I/O driver.
    pub(crate) fn bind_io_primitive(
        &self,
        handle: &(impl Into<IoPrimitive> + Copy),
    ) -> io::Result<()> {
        self.completion_port.bind(handle)
    }

    /// Starts preparing for a new I/O operation on some primitive bound to this driver. The caller
    /// must provide the buffer to pick up the data from or to deliver the data to.
    ///
    /// The typical workflow is:
    ///
    /// 1. Call `new_operation()` and pass it a buffer to start the preparations to operate on the
    ///    buffer. You will get an `Operation` that you can configure (e.g. to set the offset).
    ///    Often, you will not need to do any preparation and can just proceed to the next step.
    /// 2. Call `Operation::begin()` to start the operation once all preparation is complete.
    ///    You will need to provide a callback through which you provider the buffer + OVERLAPPED
    ///    metadata object + immediate completion byte count to the native I/O function of an I/O
    ///    primitive bound to this driver.
    /// 3. Await the result of `begin()`. You will receive back an `io::Result` with the buffer on
    ///    success. In case of error, the buffer will be provided via `io::Error::OperationFailed`
    ///    so you can reuse it if you wish. An empty buffer on reads signals end of stream.
    pub(crate) fn new_operation(&self, buffer: Buffer<Shared>) -> OperationShared {
        self.operation_store.new_operation(buffer)
    }

    /// Process any I/O completion notifications and return their results to the callers. If there
    /// is no queued I/O, we simply return.
    pub(crate) fn process_completions(&self) {
        let mut completed: [MaybeUninit<OVERLAPPED_ENTRY>; IO_DEQUEUE_BATCH_SIZE] =
            [MaybeUninit::uninit(); IO_DEQUEUE_BATCH_SIZE];
        let mut completed_items: u32 = 0;

        // We intentionally do not loop here because we want to give the caller the opportunity to
        // process received I/O as soon as possible. Otherwise we might start taking too small
        // chunks out of the I/O completion stream. Tuning the batch size above is valuable to make
        // sure we make best use of each iteration and do not leave too much queued in the OS.

        // SAFETY: TODO
        unsafe {
            let result = GET_COMPLETED_DURATION.with(|x| {
                x.observe_duration_millis(|| {
                    GetQueuedCompletionStatusEx(
                        *self.completion_port.as_native_handle(),
                        // MaybeUninit is a ZST and binary-compatible. We use it to avoid
                        // initializing the array, which is only used for collecting output.
                        mem::transmute::<
                            &mut [std::mem::MaybeUninit<OVERLAPPED_ENTRY>],
                            &mut [OVERLAPPED_ENTRY],
                        >(completed.as_mut_slice()),
                        &mut completed_items as *mut _,
                        // No waiting, poll and get out.
                        0,
                        false,
                    )
                })
            });

            match result {
                Ok(()) => {}
                // Timeout just means there was nothing to do - no I/O operations completed.
                Err(e) if e.code() == HRESULT::from_win32(WAIT_TIMEOUT.0) => {
                    POLL_TIMEOUTS.with(Event::observe_unit);
                    return;
                }
                Err(e) => panic!("unexpected error from GetQueuedCompletionStatusEx: {:?}", e),
            }

            ASYNC_COMPLETIONS_DEQUEUED.with(|x| x.observe(completed_items as Magnitude));

            for index in 0..completed_items {
                let overlapped_entry = completed[index as usize].assume_init();
                self.operation_store.complete_operation(overlapped_entry);
            }
        }
    }
}

impl Drop for DriverShared {
    fn drop(&mut self) {
        // We must ensure that all I/O operations are completed before we drop the driver. This is
        // a safety requirement of the driver - if it is not inert, we are violating memory safety.
        assert!(
            self.is_inert(),
            "shared I/O driver dropped while I/O operations are still in progress"
        );
    }
}

const ASYNC_COMPLETIONS_DEQUEUED_BUCKETS: &[Magnitude] = &[0, 1, 16, 64, 256, 512];

thread_local! {
    static ASYNC_COMPLETIONS_DEQUEUED: Event = EventBuilder::new("io_shared_async_completions_dequeued")
        .buckets(ASYNC_COMPLETIONS_DEQUEUED_BUCKETS)
        .build();

    static POLL_TIMEOUTS: Event = EventBuilder::new("io_shared_async_completions_poll_timeouts")
        .build();

    static GET_COMPLETED_DURATION: Event = EventBuilder::new("io_shared_async_completions_get_duration_millis")
        .buckets(GENERAL_MILLISECONDS_BUCKETS)
        .build();
}
