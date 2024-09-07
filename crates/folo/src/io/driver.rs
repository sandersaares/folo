use super::Buffer;
use crate::io::block::{BlockStore, PrepareBlock};
use crate::io::{self, CompletionPort};
use crate::metrics::{Event, EventBuilder, Magnitude};
use windows::Win32::Foundation::ERROR_IO_PENDING;
use windows::Win32::System::IO::{PostQueuedCompletionStatus, OVERLAPPED};
use windows::{
    core::Owned,
    Win32::{
        Foundation::{HANDLE, STATUS_SUCCESS, WAIT_TIMEOUT},
        System::IO::{GetQueuedCompletionStatusEx, OVERLAPPED_ENTRY},
    },
};
use windows_result::HRESULT;

/// Max number of I/O operations to dequeue in one go. Presumably getting more data from the OS with
/// a single call is desirable but the exact impact of different values on performance is not known.
const IO_DEQUEUE_BATCH_SIZE: usize = 1024;

/// Processes I/O completion operations for a given thread as part of the async worker loop.
#[derive(Debug)]
pub(crate) struct Driver {
    completion_port: CompletionPort,
    block_store: BlockStore,
}

impl Driver {
    pub(crate) fn new() -> Self {
        Self {
            completion_port: CompletionPort::new(),
            block_store: BlockStore::new(),
        }
    }

    /// Binds an I/O primitive to the completion port of this driver, provided a handle to the I/O
    /// primitive in question (file handle, socket, ...). This must be called once for every I/O
    /// primitive used with this I/O driver.
    pub(crate) fn bind_io_primitive(&self, handle: &Owned<HANDLE>) -> io::Result<()> {
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
        let mut completed = vec![OVERLAPPED_ENTRY::default(); IO_DEQUEUE_BATCH_SIZE];
        let mut completed_items: u32 = 0;

        // We intentionally do not loop here because we want to give the caller the opportunity to
        // process received I/O as soon as possible. Otherwise we might start taking too small
        // chunks out of the I/O completion stream. Tuning the batch size above is valuable to make
        // sure we make best use of each iteration and do not leave too much queued in the OS.

        // SAFETY: TODO
        unsafe {
            // For now we just do a quick immediate poll with no timeout. We will need to make it
            // smarter in the future, to actually wait until the next important event (e.g. a timer
            // elapses or a new task is scheduled).

            match GetQueuedCompletionStatusEx(
                self.completion_port.handle(),
                completed.as_mut_slice(),
                &mut completed_items as *mut _,
                max_wait_time_ms,
                false,
            ) {
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

            ASYNC_COMPLETIONS_DEQUEUED.with(|x| x.observe(completed_items as f64));

            for index in 0..completed_items {
                // If the completion key matches our magic value, this is a wakeup packet and needs
                // special processing.
                if completed[index as usize].lpCompletionKey == WAKE_UP_COMPLETION_KEY as usize {
                    // This is not a normal I/O block. All it did was wake us up, we do no further
                    // processing here and simply clean up the memory for it.

                    drop(Box::from_raw(
                        completed[index as usize].lpOverlapped as *mut OVERLAPPED,
                    ));
                    continue;
                }

                let mut block = self.block_store.complete(completed[index as usize]);

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

// Value is meaningless, just has to be unique.
const WAKE_UP_COMPLETION_KEY: usize = 0x23546789897;

/// A cross-thread element that can be used to wake up an I/O driver from another thread.
///
/// The waker itself is a "client" of sorts that can be handed over to any thread. It has a handle
/// to the completion port of the I/O driver. When it wants to wake up the I/O driver, it must post
/// a specific completion packet to the completion port. If the remote thread is closed, it will
/// just receive an error when it tries (which it ignores) - you can think of it as holding a weak
/// reference to the I/O driver.
///
/// The completion packet is simply an empty OVERLAPPED structure posted with the completion key
/// `WAKE_UP_COMPLETION_KEY`. The OVERLAPPED is allocated/deallocated by the sender/receiver as just
/// a Rust object, without going through the usual pooling mechanism (because the block store used
/// for regular I/O is single-threaded).
#[derive(Debug)]
pub(crate) struct IoWaker {
    completion_port: HANDLE,
}

impl IoWaker {
    pub(crate) fn new(completion_port: HANDLE) -> Self {
        Self { completion_port }
    }

    /// Wakes up the I/O driver by sending a completion packet to its completion port. This is a
    /// non-blocking operation.
    pub(crate) fn wake(&self) {
        let overlapped = Box::leak(Box::new(OVERLAPPED::default()));

        unsafe {
            match PostQueuedCompletionStatus(
                self.completion_port,
                0,
                WAKE_UP_COMPLETION_KEY,
                Some(overlapped as *const _),
            ) {
                Ok(()) => {}
                Err(e) if e.code() == ERROR_IO_PENDING.into() => {}
                _ => {
                    // Something went wrong? Maybe the IOCP is not connected anymore. Clean up.
                    drop(Box::from_raw(overlapped));
                }
            }
        }
    }
}

// SAFETY: It is fine to use a completion port across threads.
unsafe impl Send for IoWaker {}

const ASYNC_COMPLETIONS_DEQUEUED_BUCKETS: &[Magnitude] = &[0.0, 1.0, 16.0, 64.0, 256.0, 512.0];

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
}
