use crate::io::block::{BlockStore, PrepareBlock};
use crate::io::{self, CompletionPort};
use crate::metrics::{Event, EventBuilder, Magnitude};
use windows::{
    core::Owned,
    Win32::{
        Foundation::{HANDLE, STATUS_SUCCESS, WAIT_TIMEOUT},
        System::IO::{GetQueuedCompletionStatusEx, OVERLAPPED_ENTRY},
    },
};
use windows_result::HRESULT;

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
    /// 1. Call `operation()` to start the preparations. You will get a `PrepareBlock` to declare
    ///    your intentions (e.g. set the offset, fill a buffer, set the buffer length)
    /// 1. Call `PrepareBlock::begin()` to start the operation once all preparation is complete.
    ///    You will need to provide a callback in which you provider the buffer + metadata object
    ///    to the native I/O function of an I/O primitive bound to this driver.
    /// 1. Await the result of `begin()`.
    ///
    /// Resources (including buffers) are automatically managed by the driver.
    pub(crate) fn operation(&mut self) -> PrepareBlock {
        self.block_store.allocate()
    }

    /// Process any I/O completion notifications and return their results to the callers.
    pub(crate) fn process_completions(&mut self) {
        let mut completed = vec![OVERLAPPED_ENTRY::default(); 64];
        let mut completed_items: u32 = 0;

        // TODO: should we loop here if there might be more completions to pick up?
        // Alternatively, should we just take the time to let existing ones be processed first?

        // SAFETY: TODO
        unsafe {
            // For now we just do a quick immediate poll with no timeout. We will need to make it
            // smarter in the future, to actually wait until the next important event (e.g. a timer
            // elapses or a new task is scheduled).

            match GetQueuedCompletionStatusEx(
                self.completion_port.handle(),
                completed.as_mut_slice(),
                &mut completed_items as *mut _,
                0,
                false,
            ) {
                Ok(()) => {}
                // Timeout just means there was nothing to do - no I/O operations completed.
                Err(e) if e.code() == HRESULT::from_win32(WAIT_TIMEOUT.0) => return,
                Err(e) => panic!("unexpected error from GetQueuedCompletionStatusEx: {:?}", e),
            }

            ASYNC_COMPLETIONS_DEQUEUED.with(|x| x.observe(completed_items as f64));

            for index in 0..completed_items {
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

const ASYNC_COMPLETIONS_DEQUEUED_BUCKETS: &[Magnitude] = &[0.0, 1.0, 16.0, 64.0, 512.0];

thread_local! {
    static ASYNC_COMPLETIONS_DEQUEUED: Event = EventBuilder::new()
        .name("io_async_completions_dequeued")
        .buckets(ASYNC_COMPLETIONS_DEQUEUED_BUCKETS)
        .build()
        .unwrap();
}
