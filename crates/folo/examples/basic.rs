use folo::rt::{spawn_on_any, yield_now};
use std::error::Error;
use tracing::{event, Level};

// The main() entry point can be asynchronous, in which case its entire body becomes a task
// scheduled on an arbitrary worker thread. Note that any return value must be `Send + 'static`
// because the code will execute on a different thread from the one that called main().
#[folo::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt::init();

    let yield1 = spawn_on_any(yield_now);
    let yield2 = spawn_on_any(yield_now);

    let mathematics = async {
        yield_now().await;
        2 + 2
    };

    yield1.await;
    yield2.await;

    let result = mathematics.await;

    event!(Level::INFO, message = "calculated 2 + 2", result);

    if result != 4 {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Math is broken",
        )));
    }

    Ok(())
}

// Test entry points can also be asynchronous. Each test uses its own runtime to ensure a high
// level of isolation between tests.
#[folo::test]
async fn test_some_math() {
    let mathematics = async {
        yield_now().await;
        2 + 2
    };

    let result = mathematics.await;

    assert_eq!(result, 4);
}
