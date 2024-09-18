// Copyright (c) Microsoft Corporation.

//! This is a variation of `linked_basic.rs` - familiarize yourself with that example first.
//!
//! Demonstrates how to apply the linked object pattern to types exposed via abstractions (traits).
//! This aims to preserve all the functionality of the linked objects pattern while allowing you
//! to expose the instances themselves as `dyn SomeTrait` instead of the concrete type.
//!
//! This is enabled by `folo::linked::Box` which works like `std::boxed::Box` but with the
//! necessary extra machinery for linked objects.
//!
//! Under this model, **all** instances of a type T must be created as `linked::Box<dyn SomeTrait>`,
//! starting right from the constructor. If you want to have some instances exist as `T` and only
//! some as `dyn SomeTrait`, refer to the example `linked_std_box.rs`.

use std::thread;

use folo::linked::{self, link};

mod counters {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use folo::linked;

    pub trait Counter {
        fn increment(&mut self);
        fn local_count(&self) -> usize;
        fn global_count(&self) -> usize;
    }

    // Note the difference from `linked_basic.rs`: there is no `#[linked::object]` attribute
    // This is because the `linked::Box` wrapper we use provides the necessary machinery.
    pub struct EventCounter {
        local_count: usize,
        global_count: Arc<AtomicUsize>,
    }

    impl EventCounter {
        // The desired pattern is to suffix the constructor with "as_<trait_name>" to indicate that
        // it returns the result as a trait object instead of the concrete type.
        pub fn new_as_counter() -> linked::Box<dyn Counter> {
            let global_count = Arc::new(AtomicUsize::new(0));

            // Instead of `linked::new!` as we did in `linked_basic.rs`, we use `linked::new_box!`.
            // The first argument is the trait object that our linked object will be used through.
            // The second argument is the instance template as a `Self` struct-expression. This
            // struct-expression will be reused to create each linked instance. It may capture any
            // necessary variables as long as they are thread-safe (`Send`+`Sync`+`'static`).
            linked::new_box!(
                dyn Counter,
                Self {
                    local_count: 0,
                    global_count: Arc::clone(&global_count),
                }
            )
        }
    }

    impl Counter for EventCounter {
        fn increment(&mut self) {
            self.local_count = self.local_count.saturating_add(1);
            self.global_count.fetch_add(1, Ordering::Relaxed);
        }

        fn local_count(&self) -> usize {
            self.local_count
        }

        fn global_count(&self) -> usize {
            self.global_count.load(Ordering::Relaxed)
        }
    }
}

use counters::*;

link!(static RECORDS_PROCESSED: linked::Box<dyn Counter> = EventCounter::new_as_counter());

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
