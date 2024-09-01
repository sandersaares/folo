mod agent;
mod async_task_engine;
mod constants;
mod current_agent;
mod current_executor;
mod executor;
mod executor_builder;
mod executor_client;
mod functions;
mod local_join;
mod local_result_box;
mod local_task;
mod ready_after_poll;
mod remote_join;
mod remote_result_box;
mod remote_task;
mod types;

pub use executor_builder::*;
pub use executor_client::*;
pub use functions::*;
pub use local_join::*;
pub use remote_join::*;
pub(crate) use types::*;

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
/// #[folo::main]
/// async fn main() {
///     folo::yield_now().await;
///     println!("Hello, world!");
/// }
/// ```
/// 
/// Usage with custom initialization functions:
/// 
/// ```
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
///     folo::yield_now().await;
///     println!("Hello, world!");
/// }
pub use folo_proc_macros::__macro_main as main;

/// Same as [`#[folo::main]`][main] but also marks the entrypoint as a test.
pub use folo_proc_macros::__macro_test as test;
