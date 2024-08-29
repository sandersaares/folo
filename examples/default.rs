use folf::{yield_now, Engine};

fn main() {
    let mut engine = Engine::new();

    // Each of these should get polled twice - first time returning Pending, next time Ready.
    // We move these into a heap-allocated box and pin the box before adding to the active set.
    let yield1 = engine.enqueue(yield_now());
    let yield2 = engine.enqueue(yield_now());

    let mathematics = async {
        yield_now().await;
        2 + 2
    };

    engine.enqueue(async {
        yield1.await;
        yield2.await;

        println!("Both yields awaited.");

        let result = mathematics.await;

        println!("2 + 2 = {}", result);
    });

    engine.run();
}
