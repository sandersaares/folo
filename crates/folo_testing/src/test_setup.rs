use std::cell::Cell;

use tracing::level_filters::LevelFilter;

thread_local! {
    static TRACING_CONFIG_GUARD: Cell<Option<tracing::subscriber::DefaultGuard>> = const { Cell::new(None) };
}

// This function is called once per worker thread to configure each thread for our use.
pub fn init_test_worker() {
    // We configure per-thread tracing logic here to ensure that the log pipeline of each test
    // is isolated from each other. Not strictly necessary but a useful demonstration of how to
    // initialize worker threads with custom code.

    let stdout_subscriber = tracing_subscriber::fmt()
        // By default, TRACE is suppressed. We enable it here to see detailed diagnostic info.
        // Without this setting, we could use RUST_LOG to control the log level at runtime but
        // that has less stellar IDE integration so we hardcode it for now.
        //
        // NOTE: Enabling trace level logging slows everything way down because tracing is
        // synchronous (application code is paused while it is slowly written to stdout).
        .with_max_level(LevelFilter::TRACE)
        .finish();

    let tracing_config = tracing::subscriber::set_default(stdout_subscriber);

    // We shove the guard into a thread-local to preserve our config until the end of the thread.
    TRACING_CONFIG_GUARD.set(Some(tracing_config));
}
