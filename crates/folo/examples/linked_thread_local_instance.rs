// Copyright (c) Microsoft Corporation.

//! This is a variation of `linked_basic.rs` - familiarize yourself with that example first.
//!
//! Demonstrates how to share thread-local instances of linked objects, so all callers on a single
//! thread access the same instance of the linked object.
//!
//! Whether this is appropriate or not in a given scenario depends on the design of the linked
//! object type.

#![allow(clippy::new_without_default)] // Not relevant for example.

use std::thread;

use folo::linked::link_ref;

mod counters {
    use std::cell::Cell;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use folo::linked;

    #[linked::object]
    pub struct EventCounter {
        // This now acts as a thread-local count because we only access a single instance of the
        // linked object on every thread.
        //
        // Multiple callers on a single thread using the same instance means they cannot use `&mut`
        // references, so we cannot have any function in our `impl` block that takes `&mut self`!
        // That requires interior mutability to be used for any local state changes, which is why
        // we use a Cell here to facilitate incrementing the local count.
        local_count: Cell<usize>,

        global_count: Arc<AtomicUsize>,
    }

    impl EventCounter {
        pub fn new() -> Self {
            let global_count = Arc::new(AtomicUsize::new(0));

            linked::new!(Self {
                local_count: Cell::new(0),
                global_count: Arc::clone(&global_count),
            })
        }

        // Note how this went from `&mut self` to `&self` - we cannot use `&mut self` or have any
        // variables typed `mut EventCounter` or `&mut EventCounter` if we are reusing the same
        // instance for all operations on a single thread.
        pub fn increment(&self) {
            self.local_count
                .set(self.local_count.get().saturating_add(1));
            self.global_count.fetch_add(1, Ordering::Relaxed);
        }

        pub fn local_count(&self) -> usize {
            self.local_count.get()
        }

        pub fn global_count(&self) -> usize {
            self.global_count.load(Ordering::Relaxed)
        }
    }
}

use counters::*;

link_ref!(static RECORDS_PROCESSED: EventCounter = EventCounter::new());

fn main() {
    const THREAD_COUNT: usize = 4;
    const RECORDS_PER_THREAD: usize = 1_000;

    let mut threads = Vec::with_capacity(THREAD_COUNT);

    for _ in 0..THREAD_COUNT {
        threads.push(thread::spawn(move || {
            // These are the exact same instance, just accessed via different references.
            let counter = RECORDS_PROCESSED.get();
            let counter2 = RECORDS_PROCESSED.get();

            for _ in 0..RECORDS_PER_THREAD {
                counter.increment();
                counter2.increment();
            }

            // Again, the exact same instance as above!
            let counter = RECORDS_PROCESSED.get();

            println!(
                "Thread completed work; thread local count: {}, global count: {}",
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
