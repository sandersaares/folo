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

const GET_INFINITE_STREAM_HEADER_LINE: &[u8] = b"GET /infinite HTTP/1.1\r\n";
const GET_20KB_HEADER_LINE: &[u8] = b"GET /20kb HTTP/1.1\r\n";

const INFINITE_STREAM_RESPONSE_HEADERS: &[u8] = b"HTTP/1.1 200 OK\r\nConnection: Close\r\nContent-Type: application/octet-stream\r\nTransfer-Encoding: Chunked\r\n\r\n";
const NEWLINE: &[u8] = b"\r\n";
const ONE_MEGABYTE_CHUNK_HEADER: &[u8] = b"100000\r\n";

const TWENTY_KB_RESPONSE_HEADERS: &[u8] = b"HTTP/1.1 200 OK\r\nConnection: Close\r\nContent-Type: application/octet-stream\r\nContent-Length: 20480\r\n\r\n";
const TWENTY_KB_RESPONSE_BODY: &[u8] = &[b'x'; 20480];

const ERROR_RESPONSE_HEADERS: &[u8] = b"HTTP/1.1 500 Internal Server Error\r\nConnection: Close\r\nContent-Type: text/plain\r\nContent-Length: 0\r\n\r\n";
const NOT_FOUND_RESPONSE_HEADERS: &[u8] = b"HTTP/1.1 404 Not Found\r\nConnection: Close\r\nContent-Type: text/plain\r\nContent-Length: 0\r\n\r\n";

async fn accept_connection(mut connection: TcpConnection) -> io::Result<()> {
    event!(Level::INFO, "Connection received; reading HTTP request");

    let request_buffer = PinnedBuffer::from_pool();

    // The operating system is not required to give us any specific number of bytes here. This could
    // be only part of the HTTP request. It is highly likely to be the entire thing, however, so we
    // just assume it is - if there is another chunk coming after this, we will simply never see it.
    let request_buffer = connection.receive(request_buffer).await.into_inner()?;

    // We just care about the first line in the request to know what the caller wants from us.
    if request_buffer
        .as_slice()
        .starts_with(GET_INFINITE_STREAM_HEADER_LINE)
    {
        event!(Level::INFO, "Received GET /infinite");
        return send_infinite_response(connection).await;
    } else if request_buffer.as_slice().starts_with(GET_20KB_HEADER_LINE) {
        event!(Level::INFO, "Received GET /20kb");
        return send_20kb_response(connection).await;
    } else {
        event!(Level::INFO, "Received unknown request");
        return send_not_found_response(connection).await;
    }
}

async fn send_infinite_response(mut connection: TcpConnection) -> io::Result<()> {
    connection
        .send(PinnedBuffer::from_boxed_slice(
            INFINITE_STREAM_RESPONSE_HEADERS.into(),
        ))
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

async fn send_20kb_response(mut connection: TcpConnection) -> io::Result<()> {
    connection
        .send(PinnedBuffer::from_boxed_slice(
            TWENTY_KB_RESPONSE_HEADERS.into(),
        ))
        .await
        .into_inner()?;

    connection
        .send(PinnedBuffer::from_boxed_slice(
            TWENTY_KB_RESPONSE_BODY.into(),
        ))
        .await
        .into_inner()?;

    Ok(())
}

async fn send_not_found_response(mut connection: TcpConnection) -> io::Result<()> {
    connection
        .send(PinnedBuffer::from_boxed_slice(
            NOT_FOUND_RESPONSE_HEADERS.into(),
        ))
        .await
        .into_inner()?;

    Ok(())
}
