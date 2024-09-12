use crate::constants::GENERAL_MILLISECONDS_BUCKETS;
use crate::io::block::{BlockStore, PrepareBlock};
use crate::io::{self, Buffer, CompletionPort, IoWaker, WAKE_UP_COMPLETION_KEY};
use crate::metrics::{Event, EventBuilder, Magnitude};
use std::mem::{self, MaybeUninit};
use windows::Win32::{
    Foundation::{HANDLE, STATUS_SUCCESS, WAIT_TIMEOUT},
    System::IO::{GetQueuedCompletionStatusEx, OVERLAPPED_ENTRY},
};
use windows_result::HRESULT;

/// Max number of I/O operations to dequeue in one go. Presumably getting more data from the OS with
/// a single call is desirable but the exact impact of different values on performance is not known.
///
/// Known aspects of performance impact:
/// * GetQueuedCompletionStatusEx duration seems linearly affected under non-concurrent synthetic
///   message load (e.g. 40 us for 1024 items).
pub const IO_DEQUEUE_BATCH_SIZE: usize = 1024;

/// Processes I/O completion operations for a given thread as part of the async worker loop.
///
/// # Safety
///
/// The driver must not be dropped while any I/O operation is in progress. To shut down safely, the
/// I/O driver must be polled until it signals that all I/O operations have completed (`is_inert()`
/// returns true).
#[derive(Debug)]
pub(crate) struct Driver {
    completion_port: CompletionPort,

    // These are the I/O blocks that are currently in flight for operation started with the OS but
    // result not yet received. Each such operation has one block in this store.
    block_store: BlockStore,
}

impl Driver {
    /// # Safety
    ///
    /// See safety requirements on the type.
    pub(crate) unsafe fn new() -> Self {
        Self {
            completion_port: CompletionPort::new(),
            block_store: BlockStore::new(),
        }
    }

    /// Whether the driver has entered a state where it is safe to drop it. This requires that all
    /// ongoing I/O operations be completed and the completion notification received.
    pub fn is_inert(&self) -> bool {
        self.block_store.is_empty()
    }

    /// Binds an I/O primitive to the completion port of this driver, provided a handle to the I/O
    /// primitive in question (file handle, socket, ...). This must be called once for every I/O
    /// primitive used with this I/O driver.
    pub(crate) fn bind_io_primitive(&self, handle: &HANDLE) -> io::Result<()> {
        self.completion_port.bind(handle)
    }

    /// Starts preparing for a new I/O operation on some primitive bound to this driver.
    /// The typical workflow is:
    ///
    /// 1. Call `operation()` and pass it a buffer to start the preparations to operate on the
    ///    buffer. You will get a `PrepareBlock` to configure the operation (e.g. set the offset).
    /// 1. Call `PrepareBlock::begin()` to start the operation once all preparation is complete.
    ///    You will need to provide a callback in which you provider the buffer + metadata object
    ///    to the native I/O function of an I/O primitive bound to this driver.
    /// 1. Await the result of `begin()`.
    pub(crate) fn operation<'a, 'b>(&'a mut self, buffer: Buffer<'b>) -> PrepareBlock<'b> {
        self.block_store.allocate(buffer)
    }

    /// Obtains a waker that can be used to wake up the I/O driver from another thread when it
    /// is waiting for I/O.
    pub(crate) fn waker(&self) -> IoWaker {
        IoWaker::new(self.completion_port.handle())
    }

    /// Process any I/O completion notifications and return their results to the callers. If there
    /// is no queued I/O, we wait up to `max_wait_time_ms` milliseconds for new I/O activity, after
    /// which we simply return.
    pub(crate) fn process_completions(&mut self, max_wait_time_ms: u32) {
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
                        ***self.completion_port.handle(),
                        // MaybeUninit is a ZST and binary-compatible. We use it to avoid
                        // initializing the array, which is only used for collecting output.
                        mem::transmute(completed.as_mut_slice()),
                        &mut completed_items as *mut _,
                        max_wait_time_ms,
                        false,
                    )
                })
            });

            match result {
                Ok(()) => {}
                // Timeout just means there was nothing to do - no I/O operations completed.
                Err(e) if e.code() == HRESULT::from_win32(WAIT_TIMEOUT.0) => {
                    if max_wait_time_ms == 0 {
                        POLL_TIMEOUTS.with(Event::observe_unit);
                    } else {
                        WAIT_TIMEOUTS.with(Event::observe_unit);
                    }

                    return;
                }
                Err(e) => panic!("unexpected error from GetQueuedCompletionStatusEx: {:?}", e),
            }

            ASYNC_COMPLETIONS_DEQUEUED.with(|x| x.observe(completed_items as Magnitude));

            for index in 0..completed_items {
                let entry = completed[index as usize].assume_init();

                // If the completion key matches our magic value, this is a wakeup packet and needs
                // special processing.
                if entry.lpCompletionKey == WAKE_UP_COMPLETION_KEY as usize {
                    // This is not a normal I/O block. All it did was wake us up, we do no further
                    // processing here. The OVERLAPPED pointer will be null here!
                    continue;
                }

                let mut block = self.block_store.complete(entry);

                let result_tx = block.result_tx();
                let status = block.status();

                // The operation may not have been successful, so we need to investigate the status.
                if status != STATUS_SUCCESS {
                    // We ignore the tx return value because the receiver may have dropped already.
                    _ = result_tx.send(Err(io::Error::External(status.into())));
                } else {
                    // We ignore the tx return value because the receiver may have dropped already.
                    _ = result_tx.send(Ok(block));
                }
            }
        }
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        // We must ensure that all I/O operations are completed before we drop the driver. This is
        // a safety requirement of the driver - if it is not inert, we are violating memory safety.
        assert!(
            self.is_inert(),
            "I/O driver dropped while I/O operations are still in progress"
        );
    }
}

const ASYNC_COMPLETIONS_DEQUEUED_BUCKETS: &[Magnitude] = &[0, 1, 16, 64, 256, 512];

thread_local! {
    static ASYNC_COMPLETIONS_DEQUEUED: Event = EventBuilder::new()
        .name("io_async_completions_dequeued")
        .buckets(ASYNC_COMPLETIONS_DEQUEUED_BUCKETS)
        .build()
        .unwrap();

    // With sleep time == 0.
    static POLL_TIMEOUTS: Event = EventBuilder::new()
        .name("io_async_completions_poll_timeouts")
        .build()
        .unwrap();

    // With sleep time != 0.
    static WAIT_TIMEOUTS: Event = EventBuilder::new()
        .name("io_async_completions_wait_timeouts")
        .build()
        .unwrap();

    static GET_COMPLETED_DURATION: Event = EventBuilder::new()
        .name("io_async_completions_get_duration_millis")
        .buckets(GENERAL_MILLISECONDS_BUCKETS)
        .build()
        .unwrap();
}
