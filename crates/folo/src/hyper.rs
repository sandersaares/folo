#![allow(unused_variables)] // Spammy WIP code

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use hyper::rt::{Executor, Read, ReadBufCursor, Timer, Write};

use crate::{net::TcpConnection, rt};

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
pub struct FoloIo {
    connection: TcpConnection,
}

impl FoloIo {
    pub fn new(connection: TcpConnection) -> Self {
        Self { connection }
    }
}

impl Read for FoloIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: ReadBufCursor<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        todo!()
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
