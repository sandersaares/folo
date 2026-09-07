// Copyright (c) Microsoft Corporation.
// Copyright (c) Folo authors.

use std::rc::Rc;
use std::thread::LocalKey;

/// This is the real type of variables wrapped in the [`linked::thread_local_rc!` macro][1].
/// See macro documentation for more details.
///
/// Instances of this type are created by the [`linked::thread_local_rc!` macro][1],
/// never directly by user code, which can call `.with()` or `.to_rc()`
/// to work with or obtain a linked instance of `T`.
///
/// [1]: [crate::thread_local_rc]
#[derive(Debug)]
pub struct StaticInstancePerThread<T>
where
    T: linked::Object,
{
    get_storage: fn() -> &'static LocalKey<Rc<T>>,
}

impl<T> StaticInstancePerThread<T>
where
    T: linked::Object,
{
    /// Note: this function exists to serve the inner workings of the
    /// `linked::thread_local_rc!` macro and should not be used directly.
    /// It is not part of the public API and may be removed or changed at any time.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(get_storage: fn() -> &'static LocalKey<Rc<T>>) -> Self {
        Self { get_storage }
    }

    /// Executes a closure with the current thread's linked instance from
    /// the object family referenced by the static variable.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::cell::Cell;
    /// # use std::sync::{Arc, Mutex};
    /// #
    /// # #[linked::object]
    /// # struct EventCounter {
    /// #     local_count: Cell<usize>,
    /// #     global_count: Arc<Mutex<usize>>,
    /// # }
    /// #
    /// # impl EventCounter {
    /// #     pub fn new() -> Self {
    /// #         let global_count = Arc::new(Mutex::new(0));
    /// #         linked::new!(Self {
    /// #             local_count: Cell::new(0),
    /// #             global_count: Arc::clone(&global_count),
    /// #         })
    /// #     }
    /// #     
    /// #     pub fn record_event(&self) {
    /// #         self.local_count.set(self.local_count.get() + 1);
    /// #         *self.global_count.lock().unwrap() += 1;
    /// #     }
    /// #     
    /// #     pub fn local_count(&self) -> usize {
    /// #         self.local_count.get()
    /// #     }
    /// #     
    /// #     pub fn global_count(&self) -> usize {
    /// #         *self.global_count.lock().unwrap()
    /// #     }
    /// # }
    /// use std::thread;
    ///
    /// linked::thread_local_rc!(static EVENTS: EventCounter = EventCounter::new());
    ///
    /// // Use .with() for efficient access when you do not need to store the Rc
    /// EVENTS.with(|counter| {
    ///     counter.record_event();
    ///     assert_eq!(counter.local_count(), 1);
    ///     assert_eq!(counter.global_count(), 1);
    /// });
    ///
    /// // Multiple calls to .with() access the same thread-local instance
    /// EVENTS.with(|counter| {
    ///     assert_eq!(counter.local_count(), 1); // Still 1 from previous event
    ///     assert_eq!(counter.global_count(), 1); // Still 1 globally
    /// });
    ///
    /// // Each thread gets its own instance with fresh local state but shared global state
    /// thread::spawn(|| {
    ///     EVENTS.with(|counter| {
    ///         assert_eq!(counter.local_count(), 0); // Fresh local count for this thread
    ///         assert_eq!(counter.global_count(), 1); // But sees global count from main thread
    ///         
    ///         counter.record_event();
    ///         assert_eq!(counter.local_count(), 1); // Local count incremented
    ///         assert_eq!(counter.global_count(), 2); // Global count now 2
    ///     });
    /// }).join().unwrap();
    ///
    /// // Back on main thread: local state unchanged, global state updated
    /// EVENTS.with(|counter| {
    ///     assert_eq!(counter.local_count(), 1); // Still 1 locally
    ///     assert_eq!(counter.global_count(), 2); // But sees update from other thread
    /// });
    /// ```
    ///
    /// # Performance
    ///
    /// For repeated access to the current thread's linked instance, prefer reusing an `Rc<T>`
    /// obtained from `.to_rc()`.
    ///
    /// If your code is not in a situation where it can reuse an existing `Rc<T>`, this method is
    /// the optimal way to access the current thread's linked instance of `T`.
    #[inline]
    pub fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Rc<T>) -> R,
    {
        (self.get_storage)().with(f)
    }

    /// Gets an `Rc<T>` to the current thread's linked instance from
    /// the object family referenced by the static variable.
    ///
    /// The instance behind this `Rc` is the same one accessed by all other calls through the static
    /// variable on this thread. Note that it is still possible to create multiple instances on a
    /// single thread, e.g. by cloning the `T` within. The "one instance per thread" logic only
    /// applies when the instances are accessed through the static variable.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::cell::Cell;
    /// # use std::sync::{Arc, Mutex};
    /// #
    /// # #[linked::object]
    /// # struct WorkerStats {
    /// #     tasks_processed: Cell<usize>,
    /// #     total_work_done: Arc<Mutex<usize>>,
    /// # }
    /// #
    /// # impl WorkerStats {
    /// #     pub fn new() -> Self {
    /// #         let total_work = Arc::new(Mutex::new(0));
    /// #         linked::new!(Self {
    /// #             tasks_processed: Cell::new(0),
    /// #             total_work_done: Arc::clone(&total_work),
    /// #         })
    /// #     }
    /// #     
    /// #     pub fn process_task(&self) {
    /// #         self.tasks_processed.set(self.tasks_processed.get() + 1);
    /// #         *self.total_work_done.lock().unwrap() += 1;
    /// #     }
    /// #     
    /// #     pub fn local_tasks(&self) -> usize {
    /// #         self.tasks_processed.get()
    /// #     }
    /// #     
    /// #     pub fn total_work(&self) -> usize {
    /// #         *self.total_work_done.lock().unwrap()
    /// #     }
    /// # }
    /// use std::rc::Rc;
    /// use std::thread;
    ///
    /// linked::thread_local_rc!(static WORKER: WorkerStats = WorkerStats::new());
    ///
    /// // Get an Rc to reuse across multiple operations
    /// let worker = WORKER.to_rc();
    /// worker.process_task();
    /// worker.process_task();
    /// assert_eq!(worker.local_tasks(), 2);
    /// assert_eq!(worker.total_work(), 2);
    ///
    /// // Multiple calls to to_rc() return Rc to the same instance
    /// let worker2 = WORKER.to_rc();
    /// assert_eq!(worker2.local_tasks(), 2); // Same instance as worker
    /// assert_eq!(worker2.total_work(), 2);
    ///
    /// // Clone the Rc for efficiency when passing around
    /// let worker_clone = Rc::clone(&worker);
    /// worker_clone.process_task();
    /// assert_eq!(worker.local_tasks(), 3);
    /// assert_eq!(worker.total_work(), 3);
    ///
    /// // Each thread gets its own instance with fresh local state but shared global state
    /// thread::spawn(|| {
    ///     let thread_worker = WORKER.to_rc();
    ///     assert_eq!(thread_worker.local_tasks(), 0); // Fresh local state
    ///     assert_eq!(thread_worker.total_work(), 3);   // But sees shared global state
    ///     
    ///     thread_worker.process_task();
    ///     assert_eq!(thread_worker.local_tasks(), 1); // Local tasks: 1
    ///     assert_eq!(thread_worker.total_work(), 4);   // Global total: 4
    /// }).join().unwrap();
    ///
    /// // Back on main thread: local state unchanged, global state updated
    /// assert_eq!(worker.local_tasks(), 3); // Still 3 locally
    /// assert_eq!(worker.total_work(), 4);   // But sees update from other thread
    /// ```
    ///
    /// # Performance
    ///
    /// This function merely clones an `Rc`, which is relatively fast but still more work than
    /// doing nothing. The most efficient way to access the current thread's linked instance is
    /// to reuse the `Rc<T>` returned from this method.
    ///
    /// If you are not in a situation where you can reuse the `Rc<T>` and a shared reference is
    /// satisfactory, prefer calling [`.with()`][Self::with] instead, which does not create an
    /// `Rc` and thereby saves a few nanoseconds.
    #[must_use]
    #[inline]
    pub fn to_rc(&self) -> Rc<T> {
        (self.get_storage)().with(Rc::clone)
    }
}

/// Declares that all static variables within the macro body contain thread-local
/// [linked objects][crate].
///
/// A single instance from the same family is maintained per-thread, represented
/// as an `Rc<T>`.
///
/// Call [`.with()`][2] to execute a closure with a shared reference to the current thread's
/// instance of the linked object. This is the most efficient way to use the static variable,
/// although the closure style can sometimes be cumbersome.
///
/// Call [`.to_rc()`][1] on the static variable to obtain a thread-specific linked instance
/// of the object, wrapped in an `Rc`. This does not limit you to a closure. Every [`.to_rc()`][1]
/// call returns an `Rc` for the same instance per thread (though you may clone the `T` inside to
/// create additional instances not governed by the mechanics of this macro).
///
/// # Accessing linked instances
///
/// If you are making multiple calls from the same thread, prefer calling [`.to_rc()`][1] and
/// reusing the returned `Rc<T>` for optimal performance.
///
/// If you are not making multiple calls or are not in a situation where you can store an `Rc<T>`,
/// you may yield optimal performance by calling [`.with()`][2] to execute a closure with a shared
/// reference to the current thread's linked instance.
///
/// # Example
///
/// ```
/// # #[linked::object]
/// # struct TokenCache { }
/// # impl TokenCache { fn with_capacity(capacity: usize) -> Self { linked::new!(Self { } ) } fn get_token(&self) -> usize { 42 } }
/// linked::thread_local_rc!(static TOKEN_CACHE: TokenCache = TokenCache::with_capacity(1000));
///
/// fn do_something() {
///     // `.with()` is the most efficient way to access the instance of the current thread.
///     let token = TOKEN_CACHE.with(|cache| cache.get_token());
/// }
/// ```
///
/// # Dynamic family relationships
///
/// If you need fully `Rc`-style dynamic storage (i.e. not a single static variable) then consider
/// either passing instances of [`InstancePerThread<T>`][4] between threads or using
/// [`Family`][3] to manually control instance creation.
///
/// [1]: StaticInstancePerThread::to_rc
/// [2]: StaticInstancePerThread::with
/// [3]: crate::Family
/// [4]: crate::InstancePerThread
#[macro_export]
macro_rules! thread_local_rc {
    () => {};

    ($(#[$attr:meta])* $vis:vis static $NAME:ident: $t:ty = $e:expr; $($rest:tt)*) => (
        $crate::thread_local_rc!($(#[$attr])* $vis static $NAME: $t = $e);
        $crate::thread_local_rc!($($rest)*);
    );

    ($(#[$attr:meta])* $vis:vis static $NAME:ident: $t:ty = $e:expr) => {
        $crate::__private::paste! {
            $crate::instances!(#[doc(hidden)] static [< $NAME _INITIALIZER >]: $t = $e;);

            ::std::thread_local!(#[doc(hidden)] static [< $NAME _RC >]: ::std::rc::Rc<$t> = ::std::rc::Rc::new([< $NAME _INITIALIZER >].get()));

            $(#[$attr])* $vis const $NAME: $crate::StaticInstancePerThread<$t> =
                $crate::StaticInstancePerThread::new(move || &[< $NAME _RC >]);
        }
    };
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::cell::Cell;
    use std::hint::black_box;
    use std::panic::{RefUnwindSafe, UnwindSafe};
    use std::rc::Rc;
    use std::thread;

    use static_assertions::assert_impl_all;

    use crate::StaticInstancePerThread;

    assert_impl_all!(
        StaticInstancePerThread<TokenCache>: UnwindSafe, RefUnwindSafe
    );

    #[linked::object]
    struct TokenCache {
        local_value: Cell<usize>,
    }

    impl TokenCache {
        fn new(value: usize) -> Self {
            linked::new!(Self {
                local_value: Cell::new(value)
            })
        }

        fn value(&self) -> usize {
            self.local_value.get()
        }

        fn increment(&self) {
            self.local_value
                .set(self.local_value.get().saturating_add(1));
        }
    }

    #[test]
    fn constructor_executes_at_runtime() {
        thread_local! {
            static CACHE: Rc<TokenCache> = Rc::new(TokenCache::new(1000));
        }

        let instance = black_box(StaticInstancePerThread::new(|| &CACHE));

        instance.with(|cache| assert_eq!(cache.value(), 1000));
    }

    #[test]
    fn smoke_test() {
        linked::thread_local_rc! {
            static BLUE_TOKEN_CACHE: TokenCache = TokenCache::new(1000);
            static YELLOW_TOKEN_CACHE: TokenCache = TokenCache::new(2000);
        }

        assert_eq!(BLUE_TOKEN_CACHE.to_rc().value(), 1000);
        assert_eq!(YELLOW_TOKEN_CACHE.to_rc().value(), 2000);

        BLUE_TOKEN_CACHE.with(|cache| {
            assert_eq!(cache.value(), 1000);
        });
        YELLOW_TOKEN_CACHE.with(|cache| {
            assert_eq!(cache.value(), 2000);
        });

        BLUE_TOKEN_CACHE.to_rc().increment();
        YELLOW_TOKEN_CACHE.to_rc().increment();

        assert_eq!(BLUE_TOKEN_CACHE.to_rc().value(), 1001);
        assert_eq!(YELLOW_TOKEN_CACHE.to_rc().value(), 2001);

        thread::spawn(move || {
            assert_eq!(BLUE_TOKEN_CACHE.to_rc().value(), 1000);
            assert_eq!(YELLOW_TOKEN_CACHE.to_rc().value(), 2000);

            BLUE_TOKEN_CACHE.to_rc().increment();
            YELLOW_TOKEN_CACHE.to_rc().increment();

            assert_eq!(BLUE_TOKEN_CACHE.to_rc().value(), 1001);
            assert_eq!(YELLOW_TOKEN_CACHE.to_rc().value(), 2001);
        })
        .join()
        .unwrap();

        assert_eq!(BLUE_TOKEN_CACHE.to_rc().value(), 1001);
        assert_eq!(YELLOW_TOKEN_CACHE.to_rc().value(), 2001);
    }
}
