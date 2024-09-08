use criterion::{criterion_group, criterion_main, Criterion};
use folo::{criterion::FoloAdapter, rt::RemoteJoinHandle};
use std::thread;

criterion_group!(benches, spawn_and_await_many);
criterion_main!(benches);

// There is a fundamental problem when comparing Tokio and Folo in this benchmark. Namely, Tokio
// uses the entrypoint thread as one of its worker threads. Folo does not! Folo always uses only
// dedicated worker threads. This means if Tokio chooses to schedule tasks on the entrypoint thread,
// it will skip the phase where it first needs to move the benchmark to a different thread.
// We work around this by using large batch sizes, so the context switch at the start is a minor
// part of the overhead and disappears in the noise.

// TODO: Is there a proper fix available? This is such a hack.

// We spawn this many top level tasks.
const SPAWN_TASK_COUNT: usize = 100;
// Each polls, in sequence, this many await tasks.
const SPAWN_SUBFUTURE_COUNT: usize = 10;

fn spawn_and_await_many(c: &mut Criterion) {
    let tokio = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    let tokio_local = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("spawn_and_await");

    group.bench_function("folo_remote", |b| {
        b.to_async(FoloAdapter::default()).iter(|| {
            folo::rt::spawn_on_any(|| async {
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
        });
    });

    group.bench_function("tokio_remote", |b| {
        b.to_async(&tokio).iter(|| {
            tokio::task::spawn(async {
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
            })
        });
    });

    group.bench_function("tokio_local", |b| {
        b.to_async(&tokio_local).iter(|| {
            tokio::task::spawn(async {
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
            })
        });
    });

    group.bench_function("folo_local_as_remote", |b| {
        b.to_async(FoloAdapter::default()).iter(|| {
            folo::rt::spawn_on_any(|| async {
                let mut tasks = Vec::<RemoteJoinHandle<_>>::with_capacity(SPAWN_TASK_COUNT);

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
        });
    });

    group.bench_function("folo_local", |b| {
        b.to_async(FoloAdapter::default()).iter(|| {
            folo::rt::spawn_on_any(|| async {
                let root_handle: RemoteJoinHandle<_> = folo::rt::spawn(async {
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
                })
                .into();

                root_handle.await;
            })
        });
    });

    group.bench_function("tokio_blocking", |b| {
        b.to_async(&tokio_local).iter(|| {
            tokio::task::spawn(async {
                let mut tasks = Vec::with_capacity(SPAWN_TASK_COUNT);

                for _ in 0..SPAWN_TASK_COUNT {
                    tasks.push(tokio::task::spawn_blocking(|| thread::yield_now()));
                }

                for task in tasks {
                    _ = task.await;
                }
            })
        });
    });

    group.bench_function("folo_blocking", |b| {
        b.to_async(FoloAdapter::default()).iter(|| {
            folo::rt::spawn_on_any(|| async {
                let root_handle: RemoteJoinHandle<_> = folo::rt::spawn(async {
                    let mut tasks = Vec::with_capacity(SPAWN_TASK_COUNT);

                    for _ in 0..SPAWN_TASK_COUNT {
                        tasks.push(folo::rt::spawn_blocking(|| thread::yield_now()));
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
