use criterion::{async_executor::AsyncExecutor, criterion_group, criterion_main, Criterion};
use folo::RemoteJoinHandle;

criterion_group!(benches, spawn_and_await_many);
criterion_main!(benches);

const MANY_TASK_COUNT: usize = 1000;

fn spawn_and_await_many(c: &mut Criterion) {
    let tokio = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    let tokio_local = tokio::runtime::Builder::new_current_thread().build().unwrap();

    let mut group = c.benchmark_group("spawn_and_await_many");

    group.bench_function("folo_remote", |b| {
        b.to_async(FoloExecutor).iter(|| {
            folo::spawn_on_any(async {
                let mut tasks = Vec::with_capacity(MANY_TASK_COUNT);

                for _ in 0..MANY_TASK_COUNT {
                    tasks.push(folo::spawn_on_any(async { folo::yield_now().await }));
                }

                for task in tasks {
                    _ = task.await;
                }
            })
        });
    });

    group.bench_function("tokio_remote", |b| {
        b.to_async(&tokio).iter(|| {
            tokio::task::spawn(async {
                let mut tasks = Vec::with_capacity(MANY_TASK_COUNT);

                for _ in 0..MANY_TASK_COUNT {
                    tasks.push(tokio::task::spawn(async { tokio::task::yield_now().await }));
                }

                for task in tasks {
                    _ = task.await;
                }
            })
        });
    });

    group.bench_function("tokio_local", |b| {
        b.to_async(&tokio_local).iter(|| {
            tokio::task::spawn(async {
                let mut tasks = Vec::with_capacity(MANY_TASK_COUNT);

                for _ in 0..MANY_TASK_COUNT {
                    tasks.push(tokio::task::spawn(async { tokio::task::yield_now().await }));
                }

                for task in tasks {
                    _ = task.await;
                }
            })
        });
    });

    group.bench_function("folo_local_as_remote", |b| {
        b.to_async(FoloExecutor).iter(|| {
            folo::spawn_on_any(async {
                let mut tasks = Vec::<RemoteJoinHandle<_>>::with_capacity(MANY_TASK_COUNT);

                for _ in 0..MANY_TASK_COUNT {
                    tasks.push(folo::spawn(async { folo::yield_now().await }).into());
                }

                for task in tasks {
                    _ = task.await;
                }
            })
        });
    });

    group.bench_function("folo_local", |b| {
        b.to_async(FoloExecutor).iter(|| {
            folo::spawn_on_any(async {
                let root_handle: RemoteJoinHandle<_> = folo::spawn(async {
                    let mut tasks = Vec::with_capacity(MANY_TASK_COUNT);

                    for _ in 0..MANY_TASK_COUNT {
                        tasks.push(folo::spawn(async { folo::yield_now().await }));
                    }

                    for task in tasks {
                        _ = task.await;
                    }
                })
                .into();

                root_handle.await;
            })
        });
    });

    group.finish();
}

/// We use a two-fold strategy:
///
/// * For the most part, the executor works "normally" during the benchmark.
/// * However, the entrypoint from Criterion is a `block_on` which is a bit of a common antipattern
///   in Rust that it is assumed the executor wants to execute stuff on the current thread. This is
///   not the case with Folo - we deliberately use worker threads *only*. So we use the `futures`
///   lightweight local executor to bootstrap the whole process, using it to poll the first future.
///   This is only legal because we immediately `spawn_on_any()` to bring the work to a Folo worker.
struct FoloExecutor;

impl AsyncExecutor for FoloExecutor {
    fn block_on<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        _ = folo::ExecutorBuilder::new()
            // This allows `spawn_on_any()` to work correctly and shovel work to Folo.
            // It also causes the executor to be reused - it is only created on the first build.
            .ad_hoc_entrypoint()
            .build()
            .unwrap();

        // Note that we leave the Folo executor running, so it can be reused.
        futures::executor::block_on(future)
    }
}
