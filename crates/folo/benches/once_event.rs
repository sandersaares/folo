use std::{rc::Rc, task};

use criterion::{criterion_group, criterion_main, Criterion};
use folo::sync::once_event::OnceEvent;
use futures::{task::noop_waker_ref, FutureExt};

criterion_group!(benches, once_event);
criterion_main!(benches);

const CANARY: usize = 0x356789111aaaa;

fn once_event(c: &mut Criterion) {
    let mut group = c.benchmark_group("once_event");

    let ref_storage = OnceEvent::<usize>::new_slab_storage();
    let rc_storage = Rc::new(OnceEvent::<usize>::new_slab_storage());
    let unsafe_storage = Box::pin(OnceEvent::<usize>::new_slab_storage());
    let embedded_storage = Box::pin(OnceEvent::new_embedded_storage_single());

    let cx = &mut task::Context::from_waker(noop_waker_ref());

    group.bench_function("ref_getsetget", |b| {
        b.iter(|| {
            let (sender, mut receiver) = OnceEvent::new_in_ref(&ref_storage);

            let result = receiver.poll_unpin(cx);
            assert_eq!(result, task::Poll::Pending);

            sender.set(CANARY);

            let result = receiver.poll_unpin(cx);
            assert_eq!(result, task::Poll::Ready(CANARY));
        });
    });

    group.bench_function("rc_getsetget", |b| {
        b.iter(|| {
            let (sender, mut receiver) = OnceEvent::new_in_rc(Rc::clone(&rc_storage));

            let result = receiver.poll_unpin(cx);
            assert_eq!(result, task::Poll::Pending);

            sender.set(CANARY);

            let result = receiver.poll_unpin(cx);
            assert_eq!(result, task::Poll::Ready(CANARY));
        });
    });

    group.bench_function("unsafe_getsetget", |b| {
        b.iter(|| {
            let (sender, mut receiver) =
                unsafe { OnceEvent::new_in_unsafe(unsafe_storage.as_ref()) };

            let result = receiver.poll_unpin(cx);
            assert_eq!(result, task::Poll::Pending);

            sender.set(CANARY);

            let result = receiver.poll_unpin(cx);
            assert_eq!(result, task::Poll::Ready(CANARY));
        });
    });

    group.bench_function("embedded_getsetget", |b| {
        b.iter(|| {
            let (sender, mut receiver) =
                unsafe { OnceEvent::new_embedded(embedded_storage.as_ref()) };

            let result = receiver.poll_unpin(cx);
            assert_eq!(result, task::Poll::Pending);

            sender.set(CANARY);

            let result = receiver.poll_unpin(cx);
            assert_eq!(result, task::Poll::Ready(CANARY));
        });
    });

    group.bench_function("ref_setget", |b| {
        b.iter(|| {
            let (sender, mut receiver) = OnceEvent::new_in_ref(&ref_storage);

            sender.set(CANARY);

            let result = receiver.poll_unpin(cx);
            assert_eq!(result, task::Poll::Ready(CANARY));
        });
    });

    group.bench_function("rc_setget", |b| {
        b.iter(|| {
            let (sender, mut receiver) = OnceEvent::new_in_rc(Rc::clone(&rc_storage));

            sender.set(CANARY);

            let result = receiver.poll_unpin(cx);
            assert_eq!(result, task::Poll::Ready(CANARY));
        });
    });

    group.bench_function("unsafe_setget", |b| {
        b.iter(|| {
            let (sender, mut receiver) =
                unsafe { OnceEvent::new_in_unsafe(unsafe_storage.as_ref()) };

            sender.set(CANARY);

            let result = receiver.poll_unpin(cx);
            assert_eq!(result, task::Poll::Ready(CANARY));
        });
    });

    group.bench_function("embedded_setget", |b| {
        b.iter(|| {
            let (sender, mut receiver) =
                unsafe { OnceEvent::new_embedded(embedded_storage.as_ref()) };

            sender.set(CANARY);

            let result = receiver.poll_unpin(cx);
            assert_eq!(result, task::Poll::Ready(CANARY));
        });
    });

    group.finish();
}
