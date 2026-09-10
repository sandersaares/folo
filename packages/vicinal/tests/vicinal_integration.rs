//! Integration tests for the vicinal worker pool.
//!
//! These tests verify full pool functionality with real threads.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use events_once::EventPool;
use futures::executor::block_on;
use many_cpus::SystemHardware;
use many_cpus::fake::HardwareBuilder;
use new_zealand::nz;
use testing::with_watchdog;
use vicinal::Pool;

#[test]
fn spawn_and_await_value() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();

        let handle = scheduler.spawn(|| 42);
        let result = block_on(handle);

        assert_eq!(result, 42);
    });
}

#[test]
fn spawn_and_await_unit() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();

        let handle = scheduler.spawn(|| {});
        block_on(handle);
    });
}

#[test]
#[should_panic]
fn spawn_and_await_panic() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();

        let handle = scheduler.spawn(|| panic!("intentional panic"));
        block_on(handle);
    });
}

#[test]
fn clone_scheduler_spawn_from_both() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler1 = pool.scheduler();
        let scheduler2 = scheduler1.clone();

        let handle1 = scheduler1.spawn(|| 1);
        let handle2 = scheduler2.spawn(|| 2);

        assert_eq!(block_on(handle1), 1);
        assert_eq!(block_on(handle2), 2);
    });
}

#[test]
fn nested_spawn_via_captured_scheduler() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();

        let scheduler_clone = scheduler.clone();
        let outer_handle = scheduler.spawn(move || scheduler_clone.spawn(|| 99));

        let inner_handle = block_on(outer_handle);
        let result = block_on(inner_handle);

        assert_eq!(result, 99);
    });
}

#[test]
fn drop_pool_with_no_tasks() {
    with_watchdog(|| {
        let pool = Pool::new();
        drop(pool);
    });
}

#[test]
fn drop_pool_after_awaiting_task() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();

        let handle = scheduler.spawn(|| 42);
        assert_eq!(block_on(handle), 42);

        drop(pool);
    });
}

#[test]
fn spawn_many_tasks_await_all() {
    // Repeated submissions exercise task/result slot reuse; native runs retain the larger batch.
    const TASK_COUNT: usize = if cfg!(miri) { 4 } else { 100 };

    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();

        for (i, handle) in (0..TASK_COUNT)
            .map(|i| scheduler.spawn(move || i * 2))
            .enumerate()
        {
            assert_eq!(block_on(handle), i * 2);
        }
    });
}

#[test]
fn scheduler_sent_to_another_thread() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();

        let handle = thread::spawn(move || {
            let task_handle = scheduler.spawn(|| 123);
            block_on(task_handle)
        });

        let result = handle.join().unwrap();
        assert_eq!(result, 123);
    });
}

#[test]
fn concurrent_spawns_from_multiple_threads() {
    // Keep multiple submissions from every producer under Miri, with native stress coverage.
    const TASKS_PER_PRODUCER: usize = if cfg!(miri) { 2 } else { 25 };
    // Retain competing producers even when interpreting only a small batch from each.
    const PRODUCER_COUNT: usize = 4;

    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();
        let completed = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(PRODUCER_COUNT));

        let thread_handles: Vec<_> = std::iter::repeat_with(|| {
            let scheduler = scheduler.clone();
            let completed = Arc::clone(&completed);
            let start = Arc::clone(&start);

            thread::spawn(move || {
                start.wait();
                #[expect(
                    clippy::needless_collect,
                    reason = "submit the whole batch before awaiting any task to exercise queue contention"
                )]
                let handles: Vec<_> = (0..TASKS_PER_PRODUCER)
                    .map(|i| {
                        let completed = Arc::clone(&completed);
                        scheduler.spawn(move || {
                            completed.fetch_add(1, Ordering::Relaxed);
                            i
                        })
                    })
                    .collect();

                for (i, handle) in handles.into_iter().enumerate() {
                    assert_eq!(block_on(handle), i);
                }
            })
        })
        .take(PRODUCER_COUNT)
        .collect();

        for handle in thread_handles {
            handle.join().unwrap();
        }

        assert_eq!(
            completed.load(Ordering::Relaxed),
            PRODUCER_COUNT * TASKS_PER_PRODUCER
        );
    });
}

#[test]
fn spawn_and_forget_completes() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();
        let event_pool = EventPool::<()>::new();

        let (tx, rx) = event_pool.rent();
        scheduler.spawn_and_forget(move || {
            // Signal completion via the event.
            tx.send(());
        });

        // Wait for the task to complete.
        block_on(rx).unwrap();
    });
}

#[test]
fn spawn_urgent_and_forget_completes() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();
        let event_pool = EventPool::<()>::new();

        let (tx, rx) = event_pool.rent();
        scheduler.spawn_urgent_and_forget(move || {
            // Signal completion via the event.
            tx.send(());
        });

        // Wait for the task to complete.
        block_on(rx).unwrap();
    });
}

#[test]
fn spawn_and_forget_multiple_tasks() {
    // A small batch still checks independent completion signals; native runs retain the burst.
    const TASK_COUNT: usize = if cfg!(miri) { 4 } else { 50 };

    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();
        let event_pool = EventPool::<()>::new();
        let completed = Arc::new(AtomicUsize::new(0));

        let mut receivers = Vec::new();

        for _ in 0..TASK_COUNT {
            let (tx, rx) = event_pool.rent();
            let completed = Arc::clone(&completed);
            receivers.push(rx);

            scheduler.spawn_and_forget(move || {
                completed.fetch_add(1, Ordering::Relaxed);
                tx.send(());
            });
        }

        // Wait for all tasks to complete.
        for rx in receivers {
            block_on(rx).unwrap();
        }

        assert_eq!(completed.load(Ordering::Relaxed), TASK_COUNT);
    });
}

#[test]
fn spawn_and_await_with_fake_hardware() {
    with_watchdog(|| {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(2_usize), nz!(1_usize)));
        let pool = Pool::builder().hardware(hardware).build();
        let scheduler = pool.scheduler();

        let handle = scheduler.spawn(|| 42);
        assert_eq!(block_on(handle), 42);
    });
}

#[test]
fn spawn_many_tasks_with_single_processor_hardware() {
    // Repeated submissions reuse a single processor's state without a large interpreted batch.
    const TASK_COUNT: usize = if cfg!(miri) { 4 } else { 50 };

    with_watchdog(|| {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(1_usize), nz!(1_usize)));
        let pool = Pool::builder().hardware(hardware).build();
        let scheduler = pool.scheduler();

        for (i, handle) in (0..TASK_COUNT)
            .map(|i| scheduler.spawn(move || i))
            .enumerate()
        {
            assert_eq!(block_on(handle), i);
        }
    });
}

#[test]
fn fire_and_forget_with_fake_hardware() {
    with_watchdog(|| {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(2_usize), nz!(1_usize)));
        let pool = Pool::builder().hardware(hardware).build();
        let scheduler = pool.scheduler();
        let event_pool = EventPool::<()>::new();

        let (tx, rx) = event_pool.rent();
        scheduler.spawn_and_forget(move || {
            tx.send(());
        });

        block_on(rx).unwrap();
    });
}

#[test]
fn concurrent_spawns_with_fake_hardware() {
    // Keep multiple submissions from every producer under Miri, with native stress coverage.
    const TASKS_PER_PRODUCER: usize = if cfg!(miri) { 2 } else { 10 };
    // Exercise the same competing producers and hardware topology at either batch size.
    const PRODUCER_COUNT: usize = 4;

    with_watchdog(|| {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(4_usize), nz!(2_usize)));
        let pool = Pool::builder().hardware(hardware).build();
        let scheduler = pool.scheduler();
        let completed = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(PRODUCER_COUNT));

        let thread_handles: Vec<_> = std::iter::repeat_with(|| {
            let scheduler = scheduler.clone();
            let completed = Arc::clone(&completed);
            let start = Arc::clone(&start);

            thread::spawn(move || {
                start.wait();
                #[expect(
                    clippy::needless_collect,
                    reason = "submit the whole batch before awaiting any task to exercise queue contention"
                )]
                let handles: Vec<_> = (0..TASKS_PER_PRODUCER)
                    .map(|i| {
                        let completed = Arc::clone(&completed);
                        scheduler.spawn(move || {
                            completed.fetch_add(1, Ordering::Relaxed);
                            i
                        })
                    })
                    .collect();

                for (i, handle) in handles.into_iter().enumerate() {
                    assert_eq!(block_on(handle), i);
                }
            })
        })
        .take(PRODUCER_COUNT)
        .collect();

        for handle in thread_handles {
            handle.join().unwrap();
        }

        assert_eq!(
            completed.load(Ordering::Relaxed),
            PRODUCER_COUNT * TASKS_PER_PRODUCER
        );
    });
}

#[test]
#[should_panic]
fn spawn_and_await_panic_with_fake_hardware() {
    with_watchdog(|| {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(2_usize), nz!(1_usize)));
        let pool = Pool::builder().hardware(hardware).build();
        let scheduler = pool.scheduler();

        let handle = scheduler.spawn(|| panic!("intentional panic"));
        block_on(handle);
    });
}

#[test]
fn spawn_urgent_with_fake_hardware() {
    with_watchdog(|| {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(3_usize), nz!(1_usize)));
        let pool = Pool::builder().hardware(hardware).build();
        let scheduler = pool.scheduler();

        let handle = scheduler.spawn_urgent(|| "urgent result");
        assert_eq!(block_on(handle), "urgent result");
    });
}

#[test]
fn nested_spawn_with_fake_hardware() {
    with_watchdog(|| {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(2_usize), nz!(1_usize)));
        let pool = Pool::builder().hardware(hardware).build();
        let scheduler = pool.scheduler();

        let scheduler_clone = scheduler.clone();
        let outer_handle = scheduler.spawn(move || scheduler_clone.spawn(|| 99));

        let inner_handle = block_on(outer_handle);
        assert_eq!(block_on(inner_handle), 99);
    });
}
