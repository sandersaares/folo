use std::{error::Error, fs::File};
use tracing::{event, Level};

const FILE_SIZE: usize = 10 * 1024 * 1024 * 1024;
const FILE_PATH: &str = "testdata.bin";

#[folo::main(print_metrics)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt::init();

    File::create(FILE_PATH)
        .unwrap()
        .set_len(FILE_SIZE as u64)
        .unwrap();

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

    std::fs::remove_file(FILE_PATH).unwrap();

    Ok(())
}
