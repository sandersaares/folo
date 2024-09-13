use folo::{
    io::{self, OperationResultExt},
    net::{TcpConnection, TcpServerBuilder},
};
use std::{error::Error, thread, time::Duration};
use tokio::task::yield_now;
use tracing::{event, Level};

#[folo::main(print_metrics, max_processors = 1)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt::init();

    let server = TcpServerBuilder::new()
        .port(1234)
        .on_accept(accept_connection)
        .build()?;

    // Errr... well, we have nothing else to do here!
    loop {
        thread::sleep(Duration::from_millis(1));
        yield_now().await;
    }

    Ok(())
}

async fn accept_connection(mut connection: TcpConnection) -> io::Result<()> {
    loop {
        let result = connection
            .receive(io::PinnedBuffer::from_pool())
            .await
            .into_inner();

        match result {
            Ok(buffer) => {
                event!(
                    Level::INFO,
                    message = "received payload",
                    len = buffer.len()
                );

                if buffer.len() == 0 {
                    break;
                }
            }
            Err(e) => {
                event!(
                    Level::ERROR,
                    message = "error receiving payload",
                    error = e.to_string()
                );
                break;
            }
        }
    }

    Ok(())
}
