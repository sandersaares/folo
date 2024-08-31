use folo::{yield_now, ExecutorBuilder};
use std::sync::Arc;

fn main() {
    let executor = ExecutorBuilder::new().build().unwrap();

    let yield1 = executor.spawn_on_any(yield_now());
    let yield2 = executor.spawn_on_any(yield_now());

    let mathematics = async {
        yield_now().await;
        2 + 2
    };

    let executor_clone = Arc::clone(&executor);
    executor.spawn_on_any(async move {
        yield1.await;
        yield2.await;

        println!("Both yields awaited.");

        let result = mathematics.await;

        println!("2 + 2 = {}", result);

        executor_clone.stop();
    });

    executor.wait();
}
