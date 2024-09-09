use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use std::thread;

criterion_group!(benches, default_mpsc, crossbeam_channel, default_oneshot);
criterion_main!(benches);

fn default_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("default_mpsc");

    group.bench_function("unbounded_send_only", |b| {
        b.iter_batched_ref(
            || {
                // In this scenario we only send, never receive, but we keep the rx open for the run.
                std::sync::mpsc::channel::<()>()
            },
            |input| {
                input.0.send(()).unwrap();
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("unbounded_send_with_receiver", |b| {
        b.iter_batched_ref(
            || {
                let (tx, rx) = std::sync::mpsc::channel::<()>();

                let (ready_tx, ready_rx) = oneshot::channel::<()>();

                // We start a new thread to receive the messages (and discard them).
                // The thread will exit once it sees no tx is connected.
                thread::spawn(move || {
                    ready_tx.send(()).unwrap();
                    while rx.recv().is_ok() {}
                });

                // Wait for reader thread to be ready before starting iteration.
                ready_rx.recv().unwrap();

                tx
            },
            |input| {
                input.send(()).unwrap();
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn crossbeam_channel(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossbeam_channel");

    group.bench_function("unbounded_send_only", |b| {
        b.iter_batched_ref(
            || {
                // In this scenario we only send, never receive, but we keep the rx open for the run.
                crossbeam::channel::unbounded::<()>()
            },
            |input| {
                input.0.send(()).unwrap();
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("unbounded_send_with_receiver", |b| {
        b.iter_batched_ref(
            || {
                let (tx, rx) = crossbeam::channel::unbounded::<()>();

                let (ready_tx, ready_rx) = oneshot::channel::<()>();

                // We start a new thread to receive the messages (and discard them).
                // The thread will exit once it sees no tx is connected.
                thread::spawn(move || {
                    ready_tx.send(()).unwrap();
                    while rx.recv().is_ok() {}
                });

                // Wait for reader thread to be ready before starting iteration.
                ready_rx.recv().unwrap();

                tx
            },
            |input| {
                input.send(()).unwrap();
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn default_oneshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("default_oneshot");

    group.bench_function("send_only", |b| {
        b.iter_batched(
            || {
                // In this scenario we only send, never receive, but we keep the rx open for the run.
                oneshot::channel::<()>()
            },
            |input| {
                input.0.send(()).unwrap();
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("send_with_receiver", |b| {
        b.iter_batched(
            || {
                let (tx, rx) = oneshot::channel::<()>();

                let (ready_tx, ready_rx) = oneshot::channel::<()>();

                // We start a new thread to receive the messages (and discard them).
                // The thread will exit once it sees no tx is connected.
                thread::spawn(move || {
                    ready_tx.send(()).unwrap();
                    _ = rx.recv();
                });

                // Wait for reader thread to be ready before starting iteration.
                ready_rx.recv().unwrap();

                tx
            },
            |input| {
                input.send(()).unwrap();
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}
