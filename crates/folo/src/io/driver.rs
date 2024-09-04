use crate::io::{self, CompletionPort};
use std::pin::Pin;
use windows::{
    core::Owned,
    Win32::{
        Foundation::{ERROR_IO_PENDING, HANDLE, NTSTATUS, STATUS_SUCCESS, WAIT_TIMEOUT},
        System::IO::{GetQueuedCompletionStatusEx, OVERLAPPED, OVERLAPPED_ENTRY},
    },
};
use windows_result::HRESULT;

/// Processes I/O completion operations for a given thread as part of the async worker loop.
#[derive(Debug)]
pub(crate) struct Driver {
    completion_port: CompletionPort,
}

impl Driver {
    pub(crate) fn new() -> Self {
        Self {
            completion_port: CompletionPort::new(),
        }
    }

    /// Binds an I/O primitive to the completion port of this driver, provided a handle to the I/O
    /// primitive in question (file handle, socket, ...). This must be called once for every I/O
    /// primitive used with this I/O driver.
    pub(crate) fn bind_io_primitive(&self, handle: &Owned<HANDLE>) -> io::Result<()> {
        self.completion_port.bind(handle)
    }

    /// Creates a new I/O operation on some primitive bound to this driver. The typical workflow is:
    ///
    /// 1. Call `operation()` to start the I/O operation.
    /// 1. For seekable I/O primitives, use `.from_offset(1234)` to specify the offset in the file.
    /// 1. Call `begin()` to obtain the memory buffer to use and an OVERLAPPED block that must be
    ///    passed to the native I/O function.
    /// 1. Await the result.
    ///
    /// Resources are automatically managed by the driver.
    ///
    /// # Safety
    ///
    /// You must ensure that you call `begin()` and pass the `overlapped` pointer to a native I/O
    /// function. This is necessary to avoid resource leaks. It is too late to change your mind
    /// in `begin()` - when you call `operation()`, you are committed to the I/O operation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // SAFETY: We must make sure we pass `overlapped` to a native I/O function to avoid leaks.
    /// unsafe {
    ///     driver
    ///         .operation()
    ///         .from_offset(1234)
    ///         .begin(|buffer, overlapped| {
    ///             // If we were doing a write operation, we would fill the buffer here and shrink to fit.
    ///             ReadFile(file_handle, buffer, overlapped)
    ///         })?.await?;
    /// }
    /// ```
    pub(crate) unsafe fn operation(&mut self) -> Operation {
        Operation::new()
    }

    /// Process any I/O completion notifications and return their results to the callers.
    pub(crate) fn process_completions(&mut self) {
        let mut completed = vec![OVERLAPPED_ENTRY::default(); 64];
        let mut completed_items: u32 = 0;

        // SAFETY: Wololo.
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

            for index in 0..completed_items {
                let index = index as usize;

                let bytes_transferred = completed[index].dwNumberOfBytesTransferred as usize;

                // We resurrect the original block from the pointer. This now causes the block to be
                // cleaned up once it is finally dropped.
                let mut block = Block::from_overlapped(completed[index].lpOverlapped);

                let result_tx = block.result_tx.take().expect(
                    "block is always expected to have result tx when completing I/O operation",
                );

                // The operation may not have been successful, so we need to investigate the status.
                if NTSTATUS(block.overlapped.Internal as i32) != STATUS_SUCCESS {
                    // We ignore the return value because the receiver may have dropped already.
                    _ = result_tx.send(Err(io::Error::External(
                        NTSTATUS(block.overlapped.Internal as i32).into(),
                    )));
                } else {
                    // We ignore the return value because the receiver may have dropped already.
                    _ = result_tx.send(Ok(OperationResult::new(block, bytes_transferred)));
                }
            }
        }
    }
}

pub(crate) struct Operation {
    from_offset: Option<usize>,
}

// TODO: Can we make the IO block handling more fail-safe, so even if apply() fails we release the
// block. We could do this by making the actual "usage" of the OVERLAPPED require consuming the
// instance? Although we still need to split it into buffer + OVERLAPPED, which leaves the
// possibility of... actually maybe that's fine? Think about it.

impl Operation {
    pub(crate) fn new() -> Self {
        Self { from_offset: None }
    }

    pub(crate) fn from_offset(self, offset: usize) -> Self {
        Self {
            from_offset: Some(offset),
        }
    }

    /// Executes an I/O operation via the specified callback. The input for the callback is a buffer
    /// to pass to the native I/O function (filling it with data first, if necessary) and a
    /// pre-filled OVERLAPPED object to pass to the native I/O function without modification.
    pub(crate) async fn begin<F>(self, f: F) -> io::Result<OperationResult>
    where
        F: FnOnce(&mut [u8], *mut OVERLAPPED) -> io::Result<()>,
    {
        // For now, we do not have any block pool and just make a fresh boxed block every time.

        // This "pin" here is just for extra safety to avoid accidents and be ready for a future
        // where some pool will be handing out pinned references.
        let mut block = Box::pin(Block::new());

        if let Some(from_offset) = self.from_offset {
            block.as_mut().overlapped.Anonymous.Anonymous.Offset = from_offset as u32;
            block.as_mut().overlapped.Anonymous.Anonymous.OffsetHigh = (from_offset >> 32) as u32;
        }

        // TODO: Is this efficient?
        let result_rx = block
            .result_rx
            .take()
            .expect("block must have result rx when starting I/O operation");

        // SAFETY: Thoughts and prayers guarantee proper cleanup in the completion handler.
        // We clean up explicitly if the callback returns an unexpected error.
        let (buffer, overlapped) = unsafe { block.into_buffer_and_overlapped() };

        match f(buffer, overlapped) {
            // The operation was started asynchronously. This is what we want to see.
            Err(io::Error::External(e)) if e.code() == ERROR_IO_PENDING.into() => {}

            // The operation completed synchronously. That's also fine, we just pretend it is async.
            // The IOCP model guarantees that completion callbacks are called even for synchronous
            // completions.
            Ok(()) => {}

            // Something went wrong. In this case, the operation block was not consumed by the OS.
            // We need to resurrect and free the block ourselves to avoid leaking it forever.
            Err(e) => {
                // SAFETY: We are the only ones who have a reference to the block, so it is safe to
                // resurrect it and free it because the operating system makes no claim over it now.
                unsafe { Block::from_overlapped(overlapped) };
                return Err(e);
            }
        }

        result_rx
            .await
            .expect("no expected code path drops the I/O block without signaling completion result")
    }
}

pub(crate) struct OperationResult {
    block: Pin<Box<Block>>,
    bytes_transferred: usize,
}

impl OperationResult {
    fn new(block: Pin<Box<Block>>, bytes_transferred: usize) -> Self {
        Self {
            block,
            bytes_transferred,
        }
    }

    pub(crate) fn payload(&self) -> &[u8] {
        &self.block.buffer[..self.bytes_transferred]
    }
}

#[repr(C)] // Facilitates conversion to/from OVERLAPPED.
struct Block {
    /// The part of the operation block visible to the operating system.
    ///
    /// NB! This must be the first item in the struct because
    /// we treat `*Block` and `*OVERLAPPED` as equivalent!
    overlapped: OVERLAPPED,

    /// The buffer containing the data affected by the operation.
    buffer: [u8; 64 * 1024],

    /// This is where the I/O completion handler will deliver the result of the operation.
    /// Value is cleared when consumed, to make it obvious if any accidental reuse occurs.
    result_tx: Option<oneshot::Sender<io::Result<OperationResult>>>,
    result_rx: Option<oneshot::Receiver<io::Result<OperationResult>>>,
}

impl Block {
    pub fn new() -> Self {
        let (result_tx, result_rx) = oneshot::channel();

        Self {
            overlapped: OVERLAPPED::default(),
            buffer: [0; 64 * 1024],
            result_tx: Some(result_tx),
            result_rx: Some(result_rx),
        }
    }

    /// # Safety
    ///
    /// The input value must be a legitimate OVERLAPPED pointer obtained from a `Block` instance
    /// via `into_buffer_and_overlapped()`.
    pub unsafe fn from_overlapped(overlapped: *mut OVERLAPPED) -> Pin<Box<Block>> {
        Pin::new(Box::from_raw(overlapped as *mut Block))
    }

    /// # Safety
    ///
    /// You must eventually convert the destructured value back into a `Block` to release resources
    /// when the `Block` is dropped. This is done using `from_overlapped` on the returned pointer.
    pub unsafe fn into_buffer_and_overlapped(
        self: Pin<Box<Self>>,
    ) -> (&'static mut [u8], *mut OVERLAPPED) {
        let boxed = Pin::into_inner_unchecked(self);
        let leaked: &'static mut Block = Box::leak(boxed);
        (&mut leaked.buffer, &mut leaked.overlapped as *mut _)
    }
}
