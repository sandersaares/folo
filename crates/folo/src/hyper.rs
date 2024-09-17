#![allow(unused_variables)] // Spammy WIP code

use std::{
    future::Future,
    pin::{pin, Pin},
    task::{Context, Poll},
};

use hyper::rt::{Executor, Read, ReadBufCursor, Timer, Write};

use crate::{
    io::{OperationResultFuture, PinnedBuffer},
    net::TcpConnection,
    rt,
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
}

impl FoloIo {
    pub fn new(connection: TcpConnection) -> Self {
        Self {
            connection,
            active_read: None,
            active_write: None,
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
                            Ok(r) => {
                                unsafe {
                                    buf.advance(r.len());
                                }
                                Ok(())
                            }
                            Err(e) => Err(e.into_inner().into()),
                        });
                    }

                    Poll::Pending => return Poll::Pending,
                }
            }

            // If there's no active read, start a new one
            let buffer = unsafe {
                PinnedBuffer::from_ptr(buf.as_mut().as_ptr() as *mut u8, buf.as_mut().len())
            };

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
                            Ok(r) => {
                                Ok(r.len())
                            }
                            Err(e) => Err(e.into_inner().into()),
                        });
                    }

                    Poll::Pending => return Poll::Pending,
                }
            }

            // If there's no active write, start a new one.
            let buffer = unsafe { PinnedBuffer::from_ptr(buf.as_ptr() as *mut u8, buf.len()) };
            this.active_write.set(Some(this.connection.send(buffer)));
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        // Flushing is not relevant yet.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        // TODO: Do a proper shutdown.
        Poll::Ready(Ok(()))
    }
}

// Timer
pub struct FoloTimer {}

impl FoloTimer {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for FoloTimer {
    fn default() -> Self {
        Self::new()
    }
}

impl Timer for FoloTimer {
    fn sleep(&self, duration: std::time::Duration) -> std::pin::Pin<Box<dyn hyper::rt::Sleep>> {
        todo!()
    }

    fn sleep_until(
        &self,
        deadline: std::time::Instant,
    ) -> std::pin::Pin<Box<dyn hyper::rt::Sleep>> {
        todo!()
    }
}
