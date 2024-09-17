use std::error::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{event, Level};

/// This is a fake HTTP server that streams data at every caller in an infinite series of sends.
/// It runs for 5 minutes and then stops so the metrics can be emitted and results analyzed.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    // Logging to stdout will happen on background thread to avoid synchronous slowdowns.
    let (non_blocking_stdout, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        //.with_max_level(tracing::Level::TRACE)
        .with_writer(non_blocking_stdout)
        .init();

    // We use the entrypoint task as the TCP dispatcher in Tokio, spawning handlers for each
    // received connection.
    let server = TcpListener::bind("0.0.0.0:1234").await?;

    loop {
        let (connection, _) = server.accept().await?;
        _ = tokio::spawn(accept_connection(connection));
    }
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

async fn accept_connection(mut connection: TcpStream) -> std::io::Result<()> {
    event!(Level::DEBUG, "Connection received; reading HTTP request");

    let mut buffer = [0u8; 64 * 1024];
    let bytes_read = connection.read_buf(&mut buffer.as_mut_slice()).await?;
    let request = &buffer[..bytes_read];

    // We just care about the first line in the request to know what the caller wants from us.
    if request.starts_with(GET_INFINITE_STREAM_HEADER_LINE) {
        event!(Level::DEBUG, "Received GET /infinite");
        send_infinite_response(&mut connection).await?;
    } else if request.starts_with(GET_20KB_HEADER_LINE) {
        event!(Level::DEBUG, "Received GET /20kb");
        send_20kb_response(&mut connection).await?;
    } else if request.starts_with(GET_64MB_HEADER_LINE) {
        event!(Level::DEBUG, "Received GET /64mb");
        send_64mb_response(&mut connection).await?;
    } else {
        event!(Level::DEBUG, "Received unknown request");
        send_not_found_response(&mut connection).await?;
    }

    connection.shutdown().await?;
    Ok(())
}

async fn send_infinite_response(connection: &mut TcpStream) -> std::io::Result<()> {
    connection
        .write_all(INFINITE_STREAM_RESPONSE_HEADERS)
        .await?;

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

    loop {
        match connection.write_all(buffer.as_slice()).await {
            Ok(_) => {}
            Err(e) => {
                // We have encountered an error. We will log it and return it.
                event!(Level::ERROR, "Error sending HTTP response data: {:?}", e);
                return Err(e);
            }
        }
    }
}

// Below, we copy all the slices into boxed slices to simulate a "copy from somewhere", for a more
// realistic benchmark (after all, real data will not all immediately be in output buffers).

async fn send_20kb_response(connection: &mut TcpStream) -> std::io::Result<()> {
    let headers = Box::from(TWENTY_KB_RESPONSE_HEADERS);
    let body = Box::from(TWENTY_KB_RESPONSE_BODY);

    connection.write_all(&headers).await?;
    connection.write_all(&body).await?;

    Ok(())
}

async fn send_64mb_response(connection: &mut TcpStream) -> std::io::Result<()> {
    let headers = Box::from(BIG_FILE_RESPONSE_HEADERS);
    connection.write_all(&headers).await?;

    for _ in 0..1024 {
        let chunk = Box::from(BIG_FILE_BODY_CHUNK);
        connection.write_all(&chunk).await?;
    }

    Ok(())
}

async fn send_not_found_response(connection: &mut TcpStream) -> std::io::Result<()> {
    connection.write_all(NOT_FOUND_RESPONSE_HEADERS).await?;

    Ok(())
}
