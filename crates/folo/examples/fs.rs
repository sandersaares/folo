use std::error::Error;
use tracing::{event, level_filters::LevelFilter, Level};

#[folo::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let stdout_subscriber = tracing_subscriber::fmt()
        // NOTE: Enabling trace level logging slows everything way down because tracing is
        // synchronous (application code is paused while it is slowly written to stdout).
        .with_max_level(LevelFilter::TRACE)
        .finish();

    tracing::subscriber::set_global_default(stdout_subscriber)?;

    match folo::fs::read("c:\\Users\\sasaares\\Downloads\\PathOfBuildingCommunity-Setup.exe").await {
        Ok(contents) => {
            event!(
                Level::INFO,
                key = "file read completed",
                length = contents.len()
            );
        }
        Err(err) => {
            return Err(Box::new(err));
        }
    }

    Ok(())
}
