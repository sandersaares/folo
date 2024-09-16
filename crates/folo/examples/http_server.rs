#![allow(dead_code)] // WIP

use folo::{
    io::{self, OperationResultExt, PinnedBuffer},
    net::{TcpConnection, TcpServerBuilder},
};
use std::{error::Error, thread, time::Duration};
use tracing::{event, Level};

/// This is a fake HTTP server that streams data at every caller in an infinite series of sends.
/// It runs for 5 minutes and then stops so the metrics can be emitted and results analyzed.
#[folo::main(print_metrics)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt::init();

    let mut server = TcpServerBuilder::new()
        .port(1234.try_into().unwrap())
        .on_accept(accept_connection)
        .build()
        .await?;

    let (stop_tx, stop_rx) = oneshot::channel::<()>();

    // Start a new thread that will send a shutdown signal in 5 minutes.
    _ = thread::spawn(move || {
        thread::sleep(Duration::from_secs(300));
        let _ = stop_tx.send(());
    });

    _ = stop_rx.await.unwrap();

    // Calling this is optional - just validating that it works if called early.
    // If we do not call this, it will happen automatically when the runtime shuts down workers.
    server.stop();

    event!(Level::INFO, "exiting main()");

    Ok(())
}

const HTTP_RESPONSE_HEADERS: &[u8] = b"HTTP/1.1 200 OK\r\nConnection: Clone\r\nContent-Type: application/octet-stream\r\nTransfer-Encoding: Chunked\r\n\r\n";
const NEWLINE: &[u8] = b"\r\n";
const LAST_CHUNK: &[u8] = b"0\r\n\r\n";
const ONE_MEGABYTE_CHUNK_HEADER: &[u8] = b"100000\r\n";

async fn accept_connection(connection: TcpConnection) -> io::Result<()> {
    event!(Level::INFO, "Connection received; reading HTTP request");

    // We do not yet care about the HTTP request - we do not even read it. Instead, we directly
    // start transmitting the HTTP response headers and the response data.

    send_infinite_response(connection).await
}

async fn send_infinite_response(mut connection: TcpConnection) -> io::Result<()> {
    connection
        .send(PinnedBuffer::from_boxed_slice(HTTP_RESPONSE_HEADERS.into()))
        .await
        .into_inner()?;

    // We prepare a PinnedBuffer here for each connection, filled with the data we want to send.
    // We reuse the same PinnedBuffer for each send, so we do not constantly have to fill it with
    // the same data over and over again (this is equivalent to just sending a &buffer with Tokio).
    let buffer_len = ONE_MEGABYTE_CHUNK_HEADER.len() + 1024 * 1024 + NEWLINE.len();
    let mut buffer = Vec::with_capacity(buffer_len);
    buffer.extend_from_slice(ONE_MEGABYTE_CHUNK_HEADER);

    // Let's make sure we do not blow the stack by stack-allocating some giant 1MB temporary!
    for _ in 0..1024 {
        buffer.extend_from_slice(&[b'x'; 1024]);
    }

    buffer.extend_from_slice(NEWLINE);

    // That should be all.
    assert_eq!(buffer.len(), buffer_len);

    // This is the actual buffer that we will be reusing, now full of the final data.
    let mut buffer = PinnedBuffer::from_boxed_slice(buffer.into_boxed_slice());

    loop {
        match connection.send(buffer).await {
            Ok(b) => {
                // Send successful. Reuse the buffer.
                buffer = b;
            }
            Err(e) => {
                // We have encountered an error. We will log it and return it.
                event!(Level::ERROR, "Error sending HTTP response data: {:?}", e);
                return Err(e.into_inner());
            }
        }
    }
}
