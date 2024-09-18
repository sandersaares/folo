// Copyright (c) Microsoft Corporation.

//! Demonstrates basic usage of the linked object pattern (see `folo::linked`).

#![allow(clippy::new_without_default)] // Not relevant for example.

use std::thread;

use folo::linked::link;

mod counters {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use folo::linked;

    /// An event counter that keeps both a local count and a global count across all linked instances.
    #[linked::object] // Activates the linked object pattern on this type.
    pub struct EventCounter {
        // Each instance has its own local count.
        local_count: usize,

        // Each instance also increments a global count shared between all instances.
        global_count: Arc<AtomicUsize>,
    }

    impl EventCounter {
        pub fn new() -> Self {
            // The global count is shared between all instances by cloning this Arc into each one.
            let global_count = Arc::new(AtomicUsize::new(0));

            // Instead of just creating a new instance, we must use the `linked::new!` macro.
            // The body of the macro must be a `Self` struct-expression. This
            // struct-expression will be reused to create each linked instance. It may capture any
            // necessary variables as long as they are thread-safe (`Send`+`Sync`+`'static`).
            linked::new!(Self {
                local_count: 0,
                global_count: Arc::clone(&global_count),
            })
        }

        pub fn increment(&mut self) {
            self.local_count = self.local_count.saturating_add(1);
            self.global_count.fetch_add(1, Ordering::Relaxed);
        }

        pub fn local_count(&self) -> usize {
            self.local_count
        }

        pub fn global_count(&self) -> usize {
            self.global_count.load(Ordering::Relaxed)
        }
    }
}

use counters::*;

// A static variable provides linked instances of the event counter on any thread.
// The `link!` macro gives all necessary superpowers to this static variable.
// This is the simplest way to create instances that are linked across threads.
link!(static RECORDS_PROCESSED: EventCounter = EventCounter::new());

fn main() {
    const THREAD_COUNT: usize = 4;
    const RECORDS_PER_THREAD: usize = 1_000;

    let mut threads = Vec::with_capacity(THREAD_COUNT);

    for _ in 0..THREAD_COUNT {
        threads.push(thread::spawn(move || {
            let mut counter = RECORDS_PROCESSED.get();

            for _ in 0..RECORDS_PER_THREAD {
                counter.increment();
            }

            println!(
                "Thread completed work; local count: {}, global count: {}",
                counter.local_count(),
                counter.global_count()
            );
        }));
    }

    for thread in threads {
        thread.join().unwrap();
    }

    let final_count = RECORDS_PROCESSED.get().global_count();

    println!("All threads completed work; final global count: {final_count}");
}
