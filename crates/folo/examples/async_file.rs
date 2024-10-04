use std::fs::File;
use std::future::Future;
use std::io::{self, Result};
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::AsRawHandle;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use windows::core::Error;
use windows::Win32::Foundation::{
    CloseHandle, ERROR_IO_PENDING, HANDLE, STATUS_END_OF_FILE, WIN32_ERROR,
};
use windows::Win32::Storage::FileSystem::{ReadFile, FILE_FLAG_OVERLAPPED};
use windows::Win32::System::IO::{BindIoCompletionCallback, OVERLAPPED};

// Asynchronous file I/O wrapper for Windows
struct AsyncFile {
    file: File,
}

#[repr(C)]
pub struct OverlappedWrap {
    o: OVERLAPPED,
    len: u32,
    err: u32,
    waker: Option<Waker>,
}

impl Default for OverlappedWrap {
    fn default() -> Self {
        OverlappedWrap {
            o: OVERLAPPED::default(),
            waker: None,
            err: 0,
            len: 0,
        }
    }
}

unsafe extern "system" fn waker_callback(
    dwerrorcode: u32,
    dwnumberofbytestransfered: u32,
    lpoverlapped: *mut OVERLAPPED,
) {
    let wrap_ptr: *mut OverlappedWrap = lpoverlapped as *mut OverlappedWrap;
    let wrap: &mut OverlappedWrap = &mut *wrap_ptr;
    wrap.err = dwerrorcode;
    wrap.len = dwnumberofbytestransfered;
    // Use take() to avoid potential double-wake panics
    if let Some(waker) = wrap.waker.take() { 
        waker.wake();
    }
}

impl AsyncFile {
    async fn open_for_read(path: &str) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_OVERLAPPED.0)
            .open(path)?;

        // BindIoCompletionCallback is used to have a callback trigger the waker.
        unsafe {
            BindIoCompletionCallback(HANDLE(file.as_raw_handle()), Some(waker_callback), 0)
        }?;

        Ok(Self { file })
    }

    async fn read_all<F>(&self, buf: &mut [u8], callback: F) -> Result<usize>
    where
        F: FnMut(&[u8]),
    {
        AsyncFileReadFuture {
            file: &self.file,
            buf,
            overlapped: OverlappedWrap::default(),
            offset: 0,
            callback,
        }
        .await
    }

    fn close(self) -> Result<()> {
        unsafe {
            if CloseHandle(HANDLE(self.file.as_raw_handle())).is_err() {
                return Err(std::io::Error::last_os_error().into());
            }
        }
        Ok(())
    }
}

struct AsyncFileReadFuture<'a, F> {
    file: &'a File,
    buf: &'a mut [u8],
    overlapped: OverlappedWrap,
    offset: u64,
    callback: F,
}

impl<'a, F> Future for AsyncFileReadFuture<'a, F>
where
    F: FnMut(&[u8]) + 'a,
{
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };        

        if this.overlapped.err == STATUS_END_OF_FILE.0 as u32 {
            // End of file
            return Poll::Ready(Ok(this.offset as usize));
        }

        let e = Error::from(WIN32_ERROR(this.overlapped.err));
        if e.code().is_err() {
            println!("Error {:x}", e.code().0);
            return Poll::Ready(Err(io::Error::from_raw_os_error(e.code().0 as i32)));
        }

        if this.overlapped.len != 0 {
            // Some data has been read
            let bytes_transferred = this.overlapped.len;

            (this.callback)(&this.buf[..bytes_transferred as usize]);
            this.offset += bytes_transferred as u64;
            this.overlapped.o.Anonymous.Anonymous.Offset = this.offset as u32;
            this.overlapped.o.Anonymous.Anonymous.OffsetHigh = (this.offset >> 32) as u32;
            this.overlapped.len = 0;
        }

        let mut bytes_read = 0;
        let result = unsafe {
            ReadFile(
                HANDLE(this.file.as_raw_handle()),
                Some(this.buf),
                Some(&mut bytes_read),
                Some(&mut this.overlapped.o),
            )
        };

        if result.is_ok() {
            // Data was read synchronously
            (this.callback)(&this.buf[..bytes_read as usize]);
            Poll::Ready(Ok(bytes_read as usize))
        } else {
            let error = result.err().expect("Expect error code");
            if error == Error::from(ERROR_IO_PENDING) {
                this.overlapped.waker = Some(cx.waker().clone());
                Poll::Pending
            } else {
                // Read operation failed
                println!("Error {:?}", error);
                Poll::Ready(Err(io::Error::from_raw_os_error(error.code().0 as i32)))
            }
        }
    }
}

#[folo::main]
async fn main() -> Result<()> {

    // 64K buffer on the stack but could also be on the heap via box.
    // No heap allocations or buffer copying in this example.
    let mut buf = [0u8; 1024 * 64];

    // Callback allowing processing of data in buf.
    let callback = |bytes_read: &[u8]| {
        println!("transferred {} bytes", bytes_read.len());
    };

    const PATH: &str = "C:/windows/explorer.exe";
    let file = AsyncFile::open_for_read(PATH).await?;

    // Reads the entire file in chunks based on the buffer size.
    // Only the supplied buffer is used, meaning you must process the data in the callback. 
    // The buffer is subsequently overwritten with the next chunk.
    let bytes_read = file.read_all(&mut buf, callback).await?;

    println!("Complete {} bytes", bytes_read);

    file.close()?;
    Ok(())
}