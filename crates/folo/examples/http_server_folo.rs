use folo::{
    io::{self, Buffer, OperationResultExt},
    mem::isolation::Isolated,
    net::{TcpConnection, TcpServerBuilder},
    time::{Clock, Delay},
};
use std::{error::Error, time::Duration};
use tracing::{event, Level};

/// This is a fake HTTP server that streams data at every caller in an infinite series of sends.
/// It runs for 5 minutes and then stops so the metrics can be emitted and results analyzed.
#[folo::main(print_metrics)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    // Logging to stdout will happen on background thread to avoid synchronous slowdowns.
    let (non_blocking_stdout, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        //.with_max_level(tracing::Level::TRACE)
        .with_writer(non_blocking_stdout)
        .init();

    // Can the clock be provided by the FOLO runtime?
    // For example, using scoped API or even the main could optionally accept a "FoloRuntime"?
    // main(runtime: &FoloRuntime)
    // let clock = runtime.clock();
    let clock = Clock::new();

    let mut server = TcpServerBuilder::new()
        .port(1234.try_into().unwrap())
        .on_accept(accept_connection)
        .build()
        .await?;

    // Stop the server after N minutes
    Delay::with_clock(&clock, Duration::from_secs(300)).await;

    // Calling this is optional - just validating that it works if called early.
    // If we do not call this, it will happen automatically when the runtime shuts down workers.
    server.stop();

    event!(Level::INFO, "exiting main()");

    Ok(())
}

const GET_INFINITE_STREAM_HEADER_LINE: &[u8] = b"GET /infinite HTTP/1.1\r\n";
const GET_20KB_HEADER_LINE: &[u8] = b"GET /20kb HTTP/1.1\r\n";
const GET_64MB_HEADER_LINE: &[u8] = b"GET /64mb HTTP/1.1\r\n";

const INFINITE_STREAM_RESPONSE_HEADERS: &[u8] = b"HTTP/1.1 200 OK\r\nConnection: Close\r\nContent-Type: application/octet-stream\r\nTransfer-Encoding: Chunked\r\n\r\n";
const NEWLINE: &[u8] = b"\r\n";
const ONE_MEGABYTE_CHUNK_HEADER: &[u8] = b"100000\r\n";

const TWENTY_KB_RESPONSE_HEADERS: &[u8] = b"HTTP/1.1 200 OK\r\nConnection: Close\r\nContent-Type: application/octet-stream\r\nContent-Length: 20480\r\n\r\n";
const TWENTY_KB_RESPONSE_BODY: &[u8] = &[b'x'; 20480];

// 64 MB file, simulating large chunks in blob storage.
const BIG_FILE_RESPONSE_HEADERS: &[u8] = b"HTTP/1.1 200 OK\r\nConnection: Close\r\nContent-Type: application/octet-stream\r\nContent-Length: 67108864\r\n\r\n";
static BIG_FILE_BODY_CHUNK: &[u8] = &[b'y'; 64 * 1024];

const NOT_FOUND_RESPONSE_HEADERS: &[u8] = b"HTTP/1.1 404 Not Found\r\nConnection: Close\r\nContent-Type: text/plain\r\nContent-Length: 0\r\n\r\n";

async fn accept_connection(mut connection: TcpConnection) -> io::Result<()> {
    event!(Level::DEBUG, "Connection received; reading HTTP request");

    let request_buffer = Buffer::<Isolated>::from_pool();

    // The operating system is not required to give us any specific number of bytes here. This could
    // be only part of the HTTP request. It is highly likely to be the entire thing, however, so we
    // just assume it is - if there is another chunk coming after this, we will simply never see it.
    let request_buffer = connection.receive(request_buffer).await.into_inner()?;

    // We just care about the first line in the request to know what the caller wants from us.
    if request_buffer
        .as_slice()
        .starts_with(GET_INFINITE_STREAM_HEADER_LINE)
    {
        event!(Level::DEBUG, "Received GET /infinite");
        send_infinite_response(&mut connection).await?;
    } else if request_buffer.as_slice().starts_with(GET_20KB_HEADER_LINE) {
        event!(Level::DEBUG, "Received GET /20kb");
        send_20kb_response(&mut connection).await?;
    } else if request_buffer.as_slice().starts_with(GET_64MB_HEADER_LINE) {
        event!(Level::DEBUG, "Received GET /64mb");
        send_64mb_response(&mut connection).await?;
    } else {
        event!(Level::DEBUG, "Received unknown request");
        send_not_found_response(&mut connection).await?;
    }

    connection.shutdown().await
}

async fn send_infinite_response(connection: &mut TcpConnection) -> io::Result<()> {
    connection
        .send(Buffer::<Isolated>::from_boxed_slice(
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
    let mut buffer = Buffer::<Isolated>::from_boxed_slice(buffer.into_boxed_slice());

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

// Below, we copy all the slices into boxed slices to simulate a "copy from somewhere", for a more
// realistic benchmark (after all, real data will not all immediately be in output buffers).

async fn send_20kb_response(connection: &mut TcpConnection) -> io::Result<()> {
    connection
        .send(Buffer::<Isolated>::from_boxed_slice(
            TWENTY_KB_RESPONSE_HEADERS.into(),
        ))
        .await
        .into_inner()?;

    connection
        .send(Buffer::<Isolated>::from_boxed_slice(
            TWENTY_KB_RESPONSE_BODY.into(),
        ))
        .await
        .into_inner()?;

    Ok(())
}

async fn send_64mb_response(connection: &mut TcpConnection) -> io::Result<()> {
    connection
        .send(Buffer::<Isolated>::from_boxed_slice(
            BIG_FILE_RESPONSE_HEADERS.into(),
        ))
        .await
        .into_inner()?;

    // In general, we would be stream-copying this in chunks of 64 KB or so, not all at once,
    // so for a realistic benchmark we also emit it here in chunks (total 1024 x 64 KB == 64 MB).
    for _ in 0..1024 {
        connection
            .send(Buffer::<Isolated>::from_boxed_slice(
                BIG_FILE_BODY_CHUNK.into(),
            ))
            .await
            .into_inner()?;
    }

    Ok(())
}

async fn send_not_found_response(connection: &mut TcpConnection) -> io::Result<()> {
    connection
        .send(Buffer::<Isolated>::from_boxed_slice(
            NOT_FOUND_RESPONSE_HEADERS.into(),
        ))
        .await
        .into_inner()?;

    Ok(())
}
