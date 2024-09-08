use criterion::{criterion_group, criterion_main, Criterion};
use folo::{criterion::FoloAdapter, rt::RemoteJoinHandle};
use std::thread;

criterion_group!(benches, spawn_and_await_many);
criterion_main!(benches);

const MANY_TASK_COUNT: usize = 1000;

fn spawn_and_await_many(c: &mut Criterion) {
    let tokio = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    let tokio_local = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("spawn_and_await_many");

    group.bench_function("folo_remote", |b| {
        b.to_async(FoloAdapter::default()).iter(|| {
            folo::rt::spawn_on_any(|| async {
                let mut tasks = Vec::with_capacity(MANY_TASK_COUNT);

                for _ in 0..MANY_TASK_COUNT {
                    tasks.push(folo::rt::spawn_on_any(|| async {
                        folo::rt::yield_now().await
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
        b.to_async(FoloAdapter::default()).iter(|| {
            folo::rt::spawn_on_any(|| async {
                let mut tasks = Vec::<RemoteJoinHandle<_>>::with_capacity(MANY_TASK_COUNT);

                for _ in 0..MANY_TASK_COUNT {
                    tasks.push(folo::rt::spawn(async { folo::rt::yield_now().await }).into());
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
                    let mut tasks = Vec::with_capacity(MANY_TASK_COUNT);

                    for _ in 0..MANY_TASK_COUNT {
                        tasks.push(folo::rt::spawn(async { folo::rt::yield_now().await }));
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
                let mut tasks = Vec::with_capacity(MANY_TASK_COUNT);

                for _ in 0..MANY_TASK_COUNT {
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
                    let mut tasks = Vec::with_capacity(MANY_TASK_COUNT);

                    for _ in 0..MANY_TASK_COUNT {
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
