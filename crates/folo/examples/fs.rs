use std::error::Error;
use tracing::{event, Level};

//const FILE_PATH: &str = "Cargo.lock";
const FILE_PATH: &str = "c:\\Games\\X4 - Foundations\\01.dat";
//const FILE_PATH: &str = "c:\\Games\\X4 - Foundations\\extensions\\ego_dlc_pirate\\ext_01.dat";

#[folo::main(print_metrics)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let stdout_subscriber = tracing_subscriber::fmt()
        // NOTE: Enabling trace level logging slows everything way down because tracing is
        // synchronous (application code is paused while it is slowly written to stdout).
        //.with_max_level(level_filters::LevelFilter::TRACE)
        .finish();

    tracing::subscriber::set_global_default(stdout_subscriber)?;

    for _ in 0..1 {
        match folo::fs::read(FILE_PATH).await {
            Ok(contents) => {
                event!(
                    Level::INFO,
                    message = "file read completed",
                    length = contents.len()
                );
            }
            Err(err) => {
                return Err(Box::new(err));
            }
        }
    }

    Ok(())
}
