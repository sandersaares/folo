#![allow(dead_code)] // Under active development, dead code is fine.

#[doc(hidden)]
pub mod __private;
pub mod collections;
mod constants;
#[cfg(feature = "criterion")]
pub mod criterion;
pub mod fs;
pub mod io;
pub mod linked;
pub mod mem;
pub mod metrics;
pub mod net;
pub mod rt;
pub mod sync;
pub mod time;
pub mod util;
pub mod windows;

#[cfg(feature = "hyper")]
pub mod hyper;

/// Marks a `main()` function as the async entry point of an app based on the Folo runtime.
///
/// # Arguments
///
/// * `global_init_fn` - name of a function that will be called once before the main function
///    starts.
/// * `worker_init_fn` - name of a function that will be called once on each worker thread before
///    it starts.
/// * `print_metrics` - if present, the runtime will print metrics to stdout on shutdown.
/// * `max_processors` - maximum number of processors to execute on. Potentially useful to test
///    single-threaded versus multithreaded performance of something.
///
/// # Examples
///
/// Basic usage to activate the Folo runtime for the entry point:
///
/// ```
/// use folo::rt::yield_now;
///
/// #[folo::main]
/// async fn main() {
///     yield_now().await;
///     println!("Hello, world!");
/// }
/// ```
///
/// Usage with custom initialization functions:
///
/// ```
/// use folo::rt::yield_now;
///
/// fn global_init() {
///     // TODO: Set up global state here
/// }
///
/// fn worker_init() {
///    // TODO: Set up worker-specific state here
/// }
///
/// #[folo::main(global_init_fn = global_init, worker_init_fn = worker_init)]
/// async fn main() {
///     yield_now().await;
///     println!("Hello, world!");
/// }
pub use folo_proc_macros::__macro_main as main;

/// Same as [`#[folo::main]`][main] but also marks the entrypoint as a test.
pub use folo_proc_macros::__macro_test as test;

// This is so macros can produce code which refers to
// ::folo::* which will work both in the crate and in the
// service code.
#[doc(hidden)]
extern crate self as folo;