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
pub struct FoloIo {
    connection: TcpConnection,
    active_read: Option<OperationResultFuture>,
}

impl FoloIo {
    pub fn new(connection: TcpConnection) -> Self {
        Self {
            connection,
            active_read: None,
        }
    }
}

impl Read for FoloIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: ReadBufCursor<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let io = self.get_mut();

        dbg!(&io);

        if let Some(active_read) = io.active_read.as_mut() {
            println!("has value");

            // We know that while there is something in the active_read, it's location in th memory won't change.
            let pinned = pin!(active_read);

            match pinned.poll(cx) {
                Poll::Ready(result) => {
                    // This future is finished, set it to none.
                    io.active_read = None;

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

        println!("poll_read activated");

        // If there's no active read, start a new one
        let buffer =
            unsafe { PinnedBuffer::from_ptr(buf.as_mut().as_ptr() as *mut u8, buf.as_mut().len()) };

        io.active_read = Some(io.connection.receive(buffer));
        
        Poll::Pending

    }
}

impl Write for FoloIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        todo!()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        todo!()
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        todo!()
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
