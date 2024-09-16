use folo::rt::{spawn, yield_now};
use std::error::Error;

const BATCH_SIZE: usize = 1000;
const SUB_BATCH_SIZE: usize = 100;

#[folo::main(print_metrics, max_processors = 1)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    for _ in 0..10000 {
        let tasks = (0..BATCH_SIZE)
            .map(|_| {
                spawn(async {
                    for _ in 0..SUB_BATCH_SIZE {
                        yield_now().await
                    }
                })
            })
            .collect::<Vec<_>>();

        for task in tasks {
            task.await;
        }
    }

    Ok(())
}
