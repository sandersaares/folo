use folo::{
    io::{self, Buffer, OperationResultSharedExt},
    mem::isolation::Shared,
    net::{HttpContext, HttpServerBuilder},
    time::{Clock, Delay},
};
use std::{error::Error, time::Duration};
use tracing::{event, Level};

/// It runs for 5 minutes and then stops so the metrics can be emitted and results analyzed.
#[folo::main(print_metrics)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    // Logging to stdout will happen on background thread to avoid synchronous slowdowns.
    let (non_blocking_stdout, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        //.with_max_level(tracing::Level::TRACE)
        .with_writer(non_blocking_stdout)
        .init();

    let clock = Clock::new();

    // Access denied? Run this as admin:
    // netsh http add urlacl url=http://*:1234/ user=europe\sasaares
    let mut server = HttpServerBuilder::new()
        .port(1234.try_into().unwrap())
        .on_request(handle_request)
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

const TWENTY_KB_RESPONSE_BODY: &[u8] = &[b'x'; 20480];

async fn handle_request(mut context: HttpContext) -> io::Result<()> {
    context
        .send_response_headers("application/octet-stream", TWENTY_KB_RESPONSE_BODY.len())
        .await?;

    let buffer = Buffer::<Shared>::from_boxed_slice(TWENTY_KB_RESPONSE_BODY.into());
    context
        .send_response_body(buffer, true)
        .await
        .into_inner()?;

    Ok(())
}
