use folo::{
    io,
    net::{TcpConnection, TcpServerBuilder},
};
use std::{error::Error, thread, time::Duration};
use tokio::task::yield_now;

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

async fn accept_connection(_connection: TcpConnection) -> io::Result<()> {
    // Do nothing
    Ok(())
}
