mod constants;
#[cfg(feature = "criterion")]
pub mod criterion;
pub mod fs;
mod io;
pub mod rt;
mod util;

/// Marks a `main()` function as the async entry point of an app based on the Folo runtime.
///
/// # Arguments
///
/// * `global_init_fn` - name of a function that will be called once before the main function
///    starts.
/// * `worker_init_fn` - name of a function that will be called once on each worker thread before
///    it starts.
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
