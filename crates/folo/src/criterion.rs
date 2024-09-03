use criterion::async_executor::AsyncExecutor;

/// Enables usage of the Folo runtime in Criterion benchmarks.
///
/// # Special considerations
///
/// Folo does not allow the entrypoint thread to be used to execute async tasks. You MUST call
/// `folo::rt::spawn_on_any()` as the first thing in your benchmark function. This will move the
/// logic to a worker thread, where you can operate normally.
///
/// The Folo runtime is reused between benchmarks - the adapter automatically creates the runtime
/// and reuses it on each benchmark execution.
///
/// # Example
///
/// ```ignore
/// use folo::criterion::FoloAdapter;
///
/// let mut group = c.benchmark_group("file_io");
///
/// group.bench_function("folo_read_file_to_vec", |b| {
///     b.to_async(FoloAdapter::default()).iter(|| {
///         folo::rt::spawn_on_any(|| async {
///             folo::fs::read(FILE_PATH).await.unwrap();
///         })
///     });
/// });
///
/// group.finish();
/// ```
#[derive(Debug, Default)]
pub struct FoloAdapter {}

impl AsyncExecutor for FoloAdapter {
    fn block_on<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        _ = crate::rt::RuntimeBuilder::new()
            // This allows `spawn_on_any()` to work correctly and shovel work to Folo.
            // It also causes the runtime to be reused - it is only created on the first build.
            .ad_hoc_entrypoint()
            .build()
            .unwrap();

        // We use this lightweight local executor to bootstrap the whole process, using it to poll
        // the tasks running on the Folo runtime. Folo does not support execution on caller-provided
        // threads, nor any `block_on` style API.
        futures::executor::block_on(future)

        // Note that we leave the Folo runtime running, so it can be reused.
    }
}
