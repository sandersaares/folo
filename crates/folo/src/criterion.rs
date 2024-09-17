use criterion::async_executor::AsyncExecutor;
use futures::future::{BoxFuture, LocalBoxFuture};
use std::{
    sync::mpsc,
    thread::{self, JoinHandle},
};

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

/// Allows you to compare async code execution on the Folo runtime and an arbitrary competing
/// runtime. This adapter is useful because typical Rust runtimes take over the entrypoint thread
/// and use it as one of their async worker threads. Folo does not! Instead, Folo always uses only
/// dedicated worker threads. This means if the competing runtime chooses to schedule tasks on the
/// entrypoint thread, it will skip the phase where it first needs to move the benchmark to a
/// different thread. That makes the results non-comparable.
///
/// This adapter always executes logic on a worker thread of the indicated runtime, only including
/// in the timed part of the benchmark the actual duration of activity on the target thread, not the
/// overhead of moving the task to that thread.
///
/// The adapter is designed to be reused across benchmarks - it keeps the runtimes running, so they
/// do not need to be started anew for each benchmark function or iteration. After startup, the
/// process is as follows:
///
/// 1. In the "prepare payload" function, call `.begin()` to set up the benchmark payload. This
///    function will return once the worker thread is ready to start acting on the payload.
/// 2. In the benchmark function, call `.run()` to execute the payload. This function will return
///    when the payload has completed.
/// 3. Within the payload, execute logic on the current thread - it will be either a Folo worker
///    thread or the entrypoint thread of the competing runtime.
/// 4. Tell Criterion that we are dealing with a BatchSize::PerIteration - otherwise it will hang.
///
/// This ensures that only the real execution duration is measured.
pub struct ComparativeAdapter {
    folo_client: crate::rt::RuntimeClient,

    folo_jobs_tx: Option<mpsc::Sender<FoloWorkOrder>>,
    competitor_jobs_tx: Option<mpsc::Sender<CompetitorWorkOrder>>,

    competing_runtime_thread: Option<JoinHandle<()>>,
}

impl ComparativeAdapter {
    /// Starts the adapter, comparing Folo and an arbitrary competing runtime. The adapter will
    /// start Folo automatically but you need to provide the definition of the competing runtime.
    pub fn new<E>(create_competing_runtime: impl FnOnce() -> E + Send + 'static) -> Self
    where
        E: AsyncExecutor + Send + 'static,
    {
        let (folo_jobs_tx, folo_jobs_rx) = mpsc::channel();
        let (competitor_jobs_tx, competitor_jobs_rx) = mpsc::channel();

        let folo_client = crate::rt::RuntimeBuilder::new().build().unwrap();

        let competing_runtime_thread = thread::spawn(move || {
            let runtime = (create_competing_runtime)();

            while let Ok(work_order) = competitor_jobs_rx.recv() {
                let CompetitorWorkOrder {
                    payload,
                    ready_tx,
                    start_rx,
                    completed_tx,
                } = work_order;

                _ = ready_tx.send(());

                match start_rx.recv() {
                    Ok(_) => {}
                    Err(_) => continue,
                }

                runtime.block_on(payload);

                _ = completed_tx.send(());
            }
        });

        _ = folo_client.spawn_on_any(move || async move {
            while let Ok(work_order) = folo_jobs_rx.recv() {
                let FoloWorkOrder {
                    payload,
                    ready_tx,
                    start_rx,
                    completed_tx,
                } = work_order;

                _ = ready_tx.send(());

                match start_rx.recv() {
                    Ok(_) => {}
                    Err(_) => continue,
                }

                payload().await;

                _ = completed_tx.send(());
            }
        });

        Self {
            folo_client,
            folo_jobs_tx: Some(folo_jobs_tx),
            competitor_jobs_tx: Some(competitor_jobs_tx),
            competing_runtime_thread: Some(competing_runtime_thread),
        }
    }

    pub fn begin_folo(
        &self,
        payload_fn: Box<dyn FnOnce() -> LocalBoxFuture<'static, ()> + Send>,
    ) -> PreparedBenchmark {
        let (start_tx, start_rx) = oneshot::channel();
        let (completed_tx, completed_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = oneshot::channel();

        let work_order = FoloWorkOrder {
            payload: Box::new(payload_fn),
            ready_tx,
            start_rx,
            completed_tx,
        };

        self.folo_jobs_tx
            .as_ref()
            .unwrap()
            .send(work_order)
            .unwrap();

        ready_rx.recv().unwrap();

        PreparedBenchmark {
            start_tx,
            completed_rx,
        }
    }

    pub fn begin_competitor(&self, payload: BoxFuture<'static, ()>) -> PreparedBenchmark {
        let (start_tx, start_rx) = oneshot::channel();
        let (completed_tx, completed_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = oneshot::channel();

        let work_order = CompetitorWorkOrder {
            payload,
            ready_tx,
            start_rx,
            completed_tx,
        };

        self.competitor_jobs_tx
            .as_ref()
            .unwrap()
            .send(work_order)
            .unwrap();

        ready_rx.recv().unwrap();

        PreparedBenchmark {
            start_tx,
            completed_rx,
        }
    }
}

impl Drop for ComparativeAdapter {
    fn drop(&mut self) {
        drop(self.folo_jobs_tx.take().unwrap());
        drop(self.competitor_jobs_tx.take().unwrap());

        self.folo_client.stop();
        self.folo_client.wait();
        self.competing_runtime_thread
            .take()
            .unwrap()
            .join()
            .unwrap();
    }
}

struct FoloWorkOrder {
    payload: Box<dyn FnOnce() -> LocalBoxFuture<'static, ()> + Send>,

    ready_tx: oneshot::Sender<()>,
    start_rx: oneshot::Receiver<()>,
    completed_tx: oneshot::Sender<()>,
}

struct CompetitorWorkOrder {
    payload: BoxFuture<'static, ()>,

    ready_tx: oneshot::Sender<()>,
    start_rx: oneshot::Receiver<()>,
    completed_tx: oneshot::Sender<()>,
}

pub struct PreparedBenchmark {
    start_tx: oneshot::Sender<()>,
    completed_rx: oneshot::Receiver<()>,
}

impl PreparedBenchmark {
    pub fn run(self) {
        self.start_tx.send(()).unwrap();
        self.completed_rx.recv().unwrap();
    }
}
