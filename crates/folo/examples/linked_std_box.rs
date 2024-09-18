// Copyright (c) Microsoft Corporation.

//! This is a variation of `linked_basic.rs` - familiarize yourself with that example first.
//!
//! Demonstrates how to expose linked objects via abstractions (traits) on demand while
//! still using the linked objects via the concrete type itself.
//!
//! In this form, there exist two categories of instances for a type T:
//!
//! 1. The regular instances of type T, which are ordinary linked objects.
//! 2. Instances of `std::boxed::Box<dyn Xyz>` where `T: Xyz`. These remain linked internally but
//!    cannot be used to create additional linked instances (there is no `.clone()` and
//!    no `.handle()` on these objects).
//!
//! If you want to be able to create additional linked instances of `dyn Xyz` from an existing
//! instance of `dyn Xyz`, you must create **all** instances (starting from the constructor) as
//! `linked::Box<dyn Xyz>` instead of `std::boxed::Box<T>`. See `linked_box.rs` for an example.

#![allow(clippy::new_without_default)] // Not relevant for example.

use std::thread;

use folo::linked::link;

mod counters {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use folo::linked;

    /// A trait that defines functions for reporting the results of some counting that happened.
    pub trait CountResult {
        fn local_count(&self) -> usize;
        fn global_count(&self) -> usize;
    }

    // Note how this is a regular linked object type, just like in `linked_basic.rs`.
    #[linked::object]
    pub struct EventCounter {
        local_count: usize,
        global_count: Arc<AtomicUsize>,
    }

    impl EventCounter {
        pub fn new() -> Self {
            let global_count = Arc::new(AtomicUsize::new(0));

            linked::new!(Self {
                local_count: 0,
                global_count: Arc::clone(&global_count),
            })
        }

        pub fn increment(&mut self) {
            self.local_count = self.local_count.saturating_add(1);
            self.global_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl CountResult for EventCounter {
        fn local_count(&self) -> usize {
            self.local_count
        }

        fn global_count(&self) -> usize {
            self.global_count.load(Ordering::Relaxed)
        }
    }
}

use counters::*;

link!(static RECORDS_PROCESSED: EventCounter = EventCounter::new());

// Here we have some code that takes ownership of abstract count results. In this simple example
// there is of course no real "need" for us to use an abstraction but let's pretend we have a
// reason to do so.
fn finalize_counter_processing(result: Box<dyn CountResult>) {
    println!(
        "Counter finished counting: local count: {}, global count: {}",
        result.local_count(),
        result.global_count()
    );
}

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

            // You can take a regular instance of a linked object and stuff it into a Box any time.
            // Note, however, that you cannot use this instance anymore to create additional linked
            // instances because now it lacks the `.clone()` and `.handle()` required for that.
            let boxed_count_result = Box::new(counter);
            finalize_counter_processing(boxed_count_result);
        }));
    }

    for thread in threads {
        thread.join().unwrap();
    }

    let final_count = RECORDS_PROCESSED.get().global_count();

    println!("All threads completed work; final global count: {final_count}");
}
