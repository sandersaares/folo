use crate::{
    io::{Buffer, OperationResultFuture},
    mem::isolation::Isolated,
    net::{ShutdownFuture, TcpConnection},
    rt,
    time::{Clock, Delay},
};
use hyper::rt::{Executor, Read, ReadBufCursor, Sleep, Timer, Write};
use pin_project::pin_project;
use std::{
    future::Future,
    mem::{self, MaybeUninit},
    pin::{pin, Pin},
    task::{Context, Poll},
};

// Executor
#[non_exhaustive]
#[derive(Default, Debug, Clone)]
pub struct FoloExecutor {}

impl FoloExecutor {
    pub fn new() -> Self {
        Self {}
    }
}

impl<F> Executor<F> for FoloExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        rt::spawn(fut);
    }
}

// IO
#[derive(Debug)]
#[pin_project::pin_project]
pub struct FoloIo {
    connection: TcpConnection,

    #[pin]
    active_read: Option<OperationResultFuture>,

    #[pin]
    active_write: Option<OperationResultFuture>,

    #[pin]
    active_shutdown: Option<ShutdownFuture>,
}

impl FoloIo {
    pub fn new(connection: TcpConnection) -> Self {
        Self {
            connection,
            active_read: None,
            active_write: None,
            active_shutdown: None,
        }
    }
}

impl Read for FoloIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: ReadBufCursor<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        loop {
            let mut this = self.as_mut().project();

            if let Some(active_read) = this.active_read.as_mut().as_pin_mut() {
                match active_read.poll(cx) {
                    Poll::Ready(result) => {
                        // This future is finished, set it to none.
                        this.active_read.set(None);

                        return Poll::Ready(match result {
                            Ok(buffer) => {
                                // SAFETY: We are responsible for not uninitializing bytes.
                                // We are also responsible for ensuring the right count is advanced.
                                unsafe {
                                    // They are just bytes, we are initializing them now, it's fine.
                                    let buf_slice = &mut buf.as_mut()[..buffer.len()];
                                    // MaybeUninit is layout-compatible, so it's cool.
                                    let buf_slice_as_bytes =
                                        mem::transmute::<&mut [MaybeUninit<u8>], &mut [u8]>(
                                            buf_slice,
                                        );

                                    // Obviously, this copy is horribly inefficient to have in our I/O stack.
                                    // We should work with Hyper authors to eliminate the need for this by
                                    // enabling Hyper to use runtime-owned buffers.
                                    buf_slice_as_bytes.copy_from_slice(&buffer.as_slice());
                                    buf.advance(buffer.len());
                                }
                                Ok(())
                            }
                            Err(e) => Err(e.into_inner().into()),
                        });
                    }

                    Poll::Pending => return Poll::Pending,
                }
            }

            // There's no active read, so start a new one.

            // Hyper buffers are not memory-safe to use directly because they may be dropped while
            // we are still using them (if something drops the parent future polling us). Therefore,
            // we first read into Folo buffers (which are always safe) and only if the future is
            // still alive after the I/O operation do we copy the data into Hyper buffers.
            let mut buffer = Buffer::<Isolated>::from_pool();

            // Make sure we do not try to read more than Hyper can accommodate.
            // SAFETY: We are reponsible for not uninitializing bytes. Yeah okay.
            buffer.set_len(buffer.len().min(unsafe { buf.as_mut().len() }));

            // The I/O driver takes ownership of the buffer and will keep it alive until it is safe
            // to release it. Even if this future is dropped, memory is not released too early.
            this.active_read.set(Some(this.connection.receive(buffer)));
        }
    }
}

impl Write for FoloIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        loop {
            let mut this = self.as_mut().project();

            if let Some(active_write) = this.active_write.as_mut().as_pin_mut() {
                match active_write.poll(cx) {
                    Poll::Ready(result) => {
                        // This future is finished, set it to none.
                        this.active_write.set(None);

                        return Poll::Ready(match result {
                            Ok(r) => Ok(r.len()),
                            Err(e) => Err(e.into_inner().into()),
                        });
                    }

                    Poll::Pending => return Poll::Pending,
                }
            }

            // There's no active write, so start a new one.

            // Hyper buffers are not memory-safe to use directly because they may be dropped while
            // we are still using them (if something drops the parent future polling us). Therefore,
            // we copy the data into Folo buffers here (which are always safe)
            let mut buffer = Buffer::<Isolated>::from_pool();

            let len_to_copy = buffer.len().min(buf.len());
            buffer.set_len(len_to_copy);

            // Obviously, this copy is horribly inefficient to have in our I/O stack.
            // We should work with Hyper authors to eliminate the need for this by
            // enabling Hyper to use runtime-owned buffers.
            buffer.as_mut_slice().copy_from_slice(buf);

            // The I/O driver takes ownership of the buffer and will keep it alive until it is safe
            // to release it. Even if this future is dropped, memory is not released too early.
            this.active_write.set(Some(this.connection.send(buffer)));
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        // Flushing is not relevant yet.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        loop {
            let mut this = self.as_mut().project();

            if let Some(active_shutdown) = this.active_shutdown.as_mut().as_pin_mut() {
                match active_shutdown.poll(cx) {
                    Poll::Ready(result) => {
                        // This future is finished, set it to none.
                        this.active_shutdown.set(None);

                        match result {
                            Ok(()) => return Poll::Ready(Ok(())),
                            Err(e) => return Poll::Ready(Err(e.into())),
                        }
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            // There's no active shutdown, so start a new one.
            this.active_shutdown.set(Some(this.connection.shutdown()));
        }
    }
}

// Timer
pub struct FoloTimer {
    clock: Clock,
}

impl FoloTimer {
    pub fn new(clock: &Clock) -> Self {
        Self {
            clock: clock.clone(),
        }
    }
}

impl Timer for FoloTimer {
    fn sleep(&self, duration: std::time::Duration) -> std::pin::Pin<Box<dyn hyper::rt::Sleep>> {
        let wrapper = DelayWrapper {
            delay: Delay::with_clock(&self.clock, duration),
            thread_id: std::thread::current().id(),
        };

        Box::pin(wrapper)
    }

    fn sleep_until(
        &self,
        deadline: std::time::Instant,
    ) -> std::pin::Pin<Box<dyn hyper::rt::Sleep>> {
        self.sleep(deadline.duration_since(self.clock.instant_now()))
    }
}

/// The wrapper around the [`Delay`] future to implement [`Sleep`].
///
/// This wrappers also forces the Send and Sync traits, albeit these are not supported
/// by the `Delay` future.
///
/// This is ok, because the sleep will always be used on the same thread through using the
/// single-threaded [`FoloExecutor`] executor.
#[pin_project]
struct DelayWrapper {
    #[pin]
    delay: Delay,
    thread_id: std::thread::ThreadId,
}

impl Sleep for DelayWrapper {}

impl Future for DelayWrapper {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert_eq!(std::thread::current().id(), self.thread_id);
        self.project().delay.poll(cx)
    }
}

unsafe impl Send for DelayWrapper {}
unsafe impl Sync for DelayWrapper {}
