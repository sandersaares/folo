use folo::{
    io::{self, OperationResultExt},
    net::{TcpConnection, TcpServerBuilder},
};
use std::error::Error;
use tracing::{event, Level};

#[folo::main(print_metrics, max_processors = 1)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt::init();

    let mut server = TcpServerBuilder::new()
        .port(1234.try_into().unwrap())
        .on_accept(accept_connection)
        .build()
        .await?;

    server.wait().await;

    Ok(())
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
                if buffer.len() == 0 {
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
