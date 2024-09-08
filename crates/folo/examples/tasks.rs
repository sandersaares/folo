use folo::rt::{spawn, yield_now};
use std::error::Error;

const BATCH_SIZE: usize = 1000;

#[folo::main(print_metrics)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    for _ in 0..10000 {
        let tasks = (0..BATCH_SIZE)
            .into_iter()
            .map(|_| spawn(yield_now()))
            .collect::<Vec<_>>();

        for task in tasks {
            task.await;
        }
    }

    Ok(())
}
