use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use folo::{criterion::ComparativeAdapter, rt::RemoteJoinHandle};
use std::thread;

criterion_group!(benches, spawn_and_await);
criterion_main!(benches);

// We spawn this many top level tasks.
const SPAWN_TASK_COUNT: usize = 1;
// Each polls, in sequence, this many await tasks.
const SPAWN_SUBFUTURE_COUNT: usize = 25;

fn spawn_and_await(c: &mut Criterion) {
    let comparison_adapter =
        ComparativeAdapter::new(|| tokio::runtime::Builder::new_multi_thread().build().unwrap());

    let comparison_adapter_local = ComparativeAdapter::new(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
    });

    let mut group = c.benchmark_group("spawn_and_await");

    group.bench_function("folo_remote", |b| {
        b.iter_batched(
            || {
                comparison_adapter.begin_folo(Box::new(|| {
                    Box::pin(async move {
                        let mut tasks = Vec::with_capacity(SPAWN_TASK_COUNT);

                        for _ in 0..SPAWN_TASK_COUNT {
                            tasks.push(folo::rt::spawn_on_any(|| async {
                                for _ in 0..SPAWN_SUBFUTURE_COUNT {
                                    folo::rt::yield_now().await;
                                }
                            }));
                        }

                        for task in tasks {
                            _ = task.await;
                        }
                    })
                }))
            },
            |prepared| prepared.run(),
            BatchSize::PerIteration,
        );
    });

    group.bench_function("tokio_remote", |b| {
        b.iter_batched(
            || {
                comparison_adapter.begin_competitor(Box::pin(async move {
                    let mut tasks = Vec::with_capacity(SPAWN_TASK_COUNT);

                    for _ in 0..SPAWN_TASK_COUNT {
                        tasks.push(tokio::task::spawn(async {
                            for _ in 0..SPAWN_SUBFUTURE_COUNT {
                                tokio::task::yield_now().await;
                            }
                        }));
                    }

                    for task in tasks {
                        _ = task.await;
                    }
                }))
            },
            |prepared| prepared.run(),
            BatchSize::PerIteration,
        );
    });

    group.bench_function("tokio_local", |b| {
        b.iter_batched(
            || {
                comparison_adapter_local.begin_competitor(Box::pin(async move {
                    let mut tasks = Vec::with_capacity(SPAWN_TASK_COUNT);

                    for _ in 0..SPAWN_TASK_COUNT {
                        tasks.push(tokio::task::spawn(async {
                            for _ in 0..SPAWN_SUBFUTURE_COUNT {
                                tokio::task::yield_now().await;
                            }
                        }));
                    }

                    for task in tasks {
                        _ = task.await;
                    }
                }))
            },
            |prepared| prepared.run(),
            BatchSize::PerIteration,
        );
    });

    group.bench_function("folo_local_as_remote", |b| {
        b.iter_batched(
            || {
                comparison_adapter.begin_folo(Box::new(|| {
                    Box::pin(async move {
                        let mut tasks: Vec<RemoteJoinHandle<_>> =
                            Vec::with_capacity(SPAWN_TASK_COUNT);

                        for _ in 0..SPAWN_TASK_COUNT {
                            tasks.push(
                                folo::rt::spawn(async {
                                    for _ in 0..SPAWN_SUBFUTURE_COUNT {
                                        folo::rt::yield_now().await;
                                    }
                                })
                                .into(),
                            );
                        }

                        for task in tasks {
                            _ = task.await;
                        }
                    })
                }))
            },
            |prepared| prepared.run(),
            BatchSize::PerIteration,
        );
    });

    group.bench_function("folo_local", |b| {
        b.iter_batched(
            || {
                async fn inner() {
                    let mut tasks = Vec::with_capacity(SPAWN_TASK_COUNT);

                    for _ in 0..SPAWN_TASK_COUNT {
                        tasks.push(folo::rt::spawn(async {
                            for _ in 0..SPAWN_SUBFUTURE_COUNT {
                                folo::rt::yield_now().await;
                            }
                        }));
                    }

                    for task in tasks {
                        _ = task.await;
                    }
                }

                comparison_adapter.begin_folo(Box::new(|| Box::pin(async move { inner().await })))
            },
            |prepared| prepared.run(),
            BatchSize::PerIteration,
        );
    });

    group.bench_function("tokio_blocking", |b| {
        b.iter_batched(
            || {
                comparison_adapter.begin_competitor(Box::pin(async move {
                    let mut tasks = Vec::with_capacity(SPAWN_TASK_COUNT);

                    for _ in 0..SPAWN_TASK_COUNT {
                        tasks.push(tokio::task::spawn_blocking(thread::yield_now));
                    }

                    for task in tasks {
                        _ = task.await;
                    }
                }))
            },
            |prepared| prepared.run(),
            BatchSize::PerIteration,
        );
    });

    group.bench_function("folo_sync", |b| {
        b.iter_batched(
            || {
                comparison_adapter.begin_folo(Box::new(|| {
                    Box::pin(async move {
                        let mut tasks = Vec::with_capacity(SPAWN_TASK_COUNT);

                        for _ in 0..SPAWN_TASK_COUNT {
                            tasks.push(folo::rt::spawn_sync(
                                folo::rt::SynchronousTaskType::Syscall,
                                thread::yield_now,
                            ));
                        }

                        for task in tasks {
                            _ = task.await;
                        }
                    })
                }))
            },
            |prepared| prepared.run(),
            BatchSize::PerIteration,
        );
    });

    group.finish();
}
