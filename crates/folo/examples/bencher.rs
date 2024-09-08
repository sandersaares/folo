use folo::criterion::ComparativeAdapter;
use std::error::Error;

#[folo::main(print_metrics)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let adapter =
        ComparativeAdapter::new(|| tokio::runtime::Builder::new_multi_thread().build().unwrap());
    with_folo(&adapter).await;
    with_tokio(&adapter).await;

    Ok(())
}

async fn with_folo(adapter: &ComparativeAdapter) {
    let prepared = adapter.begin_folo(Box::new(|| {
        Box::pin(async move {
            let mut tasks = Vec::with_capacity(1);

            for _ in 0..1 {
                tasks.push(folo::rt::spawn_on_any(|| async {
                    for _ in 0..25 {
                        folo::rt::yield_now().await;
                    }
                }));
            }

            for task in tasks {
                _ = task.await;
            }

            println!("Folo done");
        })
    }));

    prepared.run();
}

async fn with_tokio(adapter: &ComparativeAdapter) {
    let prepared = adapter.begin_competitor(Box::pin(async move {
        let mut tasks = Vec::with_capacity(1);

        for _ in 0..1 {
            tasks.push(tokio::task::spawn(async {
                for _ in 0..25 {
                    tokio::task::yield_now().await;
                }
            }));
        }

        for task in tasks {
            _ = task.await;
        }

        println!("Tokio done");
    }));

    prepared.run();
}
