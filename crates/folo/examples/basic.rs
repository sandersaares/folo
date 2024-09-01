use folo::rt::{spawn_on_any, yield_now};
use std::{cell::Cell, error::Error};
use tracing::{event, level_filters::LevelFilter, Level};

// The main() entry point can be asynchronous, in which case its entire body becomes a task
// scheduled on an arbitrary worker thread. Note that any return value must be `Send + 'static`
// because the code will execute on a different thread from the one that called main().
#[folo::main(worker_init_fn = init_worker)]
async fn main() -> Result<(), Box<dyn Error + Send + 'static>> {
    let yield1 = spawn_on_any(yield_now());
    let yield2 = spawn_on_any(yield_now());

    let mathematics = async {
        yield_now().await;
        2 + 2
    };

    yield1.await;
    yield2.await;

    let result = mathematics.await;

    event!(Level::INFO, key = "calculated 2 + 2", result);

    if result != 4 {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Math is broken",
        )) as Box<dyn Error + Send + 'static>);
    }

    Ok(())
}

// Test entry points can also be asynchronous. Each test uses its own runtime to ensure a high
// level of isolation between tests.
#[folo::test(worker_init_fn = init_worker)]
async fn test_some_math() {
    let mathematics = async {
        yield_now().await;
        2 + 2
    };

    let result = mathematics.await;

    assert_eq!(result, 4);
}

thread_local! {
    static TRACING_CONFIG_GUARD: Cell<Option<tracing::subscriber::DefaultGuard>> = const { Cell::new(None) };
}

// This function is called once per worker thread to configure each thread for our use.
fn init_worker() {
    // We configure per-thread tracing logic here to ensure that the log pipeline of each test
    // is isolated from each other. Not strictly necessary but a useful demonstration of how to
    // initialize worker threads with custom code.

    let stdout_subscriber = tracing_subscriber::fmt()
        // By default, TRACE is suppressed. We enable it here to see detailed diagnostic info.
        // Without this setting, we could use RUST_LOG to control the log level at runtime but
        // that has less stellar IDE integration so we hardcode it for now.
        .with_max_level(LevelFilter::TRACE)
        .finish();

    let tracing_config = tracing::subscriber::set_default(stdout_subscriber);

    // We shove the guard into a thread-local to preserve our config until the end of the thread.
    TRACING_CONFIG_GUARD.set(Some(tracing_config));
}
