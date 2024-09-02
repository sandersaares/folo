use std::error::Error;
use tracing::{event, Level};

#[folo::main]
async fn main() -> Result<(), Box<dyn Error + Send + 'static>> {
    tracing_subscriber::fmt::init();

    match folo::fs::read("Readme.md").await {
        Ok(contents) => {
            event!(
                Level::INFO,
                key = "readme contents",
                length = contents.len()
            );
        }
        Err(err) => {
            return Err(Box::new(err) as Box<dyn Error + Send + 'static>);
        }
    }

    Ok(())
}
