use folo::{
    io::{self, OperationResultExt},
    net::{TcpConnection, TcpServerBuilder},
};
use std::error::Error;
use tracing::{event, Level};

/// This is a TCP server that accepts connections and echoes back whatever is sent to it.
/// While functional, it is rather useless for benchmarking because no existing tooling is designed
/// to validate TCP echo scenarios and building our own echo client would add too many variables for
/// certainty in the results.
#[folo::main(print_metrics)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt::init();

    let _server = TcpServerBuilder::new()
        .port(1234.try_into().unwrap())
        .on_accept(accept_connection)
        .build()
        .await?;

    // We wait forever here because there is no good thing to wait for in this example.
    // Really, we just want to wait for Control+C without spending any energy.
    let (_tx, rx) = oneshot::channel::<()>();
    rx.await.expect_err("this await should never complete");

    unreachable!()
}

async fn accept_connection(mut connection: TcpConnection) -> io::Result<()> {
    event!(Level::INFO, "connection received, echo starting",);

    loop {
        let receive_result = connection
            .receive(io::PinnedBuffer::from_pool())
            .await
            .into_inner();

        match receive_result {
            Ok(buffer) => {
                if buffer.is_empty() {
                    break;
                }

                // Echo back whatever was received, terminating if anything goes wrong.
                connection.send(buffer).await.into_inner()?;
            }
            Err(e) => {
                event!(
                    Level::ERROR,
                    message = "error receiving payload - terminating echo",
                    error = e.to_string()
                );
                break;
            }
        }
    }

    Ok(())
}
