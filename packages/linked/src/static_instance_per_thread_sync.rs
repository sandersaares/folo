// Copyright (c) Microsoft Corporation.
// Copyright (c) Folo authors.

use std::sync::Arc;
use std::thread::LocalKey;

/// This is the real type of variables wrapped in the [`linked::thread_local_arc!` macro][1].
/// See macro documentation for more details.
///
/// Instances of this type are created by the [`linked::thread_local_arc!` macro][1],
/// never directly by user code, which can call `.with()` or `.to_arc()`
/// to work with or obtain a linked instance of `T`.
///
/// [1]: [crate::thread_local_arc]
#[derive(Debug)]
pub struct StaticInstancePerThreadSync<T>
where
    T: linked::Object + Send + Sync,
{
    get_storage: fn() -> &'static LocalKey<Arc<T>>,
}

impl<T> StaticInstancePerThreadSync<T>
where
    T: linked::Object + Send + Sync,
{
    /// Note: this function exists to serve the inner workings of the
    /// `linked::thread_local_arc!` macro and should not be used directly.
    /// It is not part of the public API and may be removed or changed at any time.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(get_storage: fn() -> &'static LocalKey<Arc<T>>) -> Self {
        Self { get_storage }
    }

    /// Executes a closure with the current thread's linked instance from
    /// the object family referenced by the static variable.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::atomic::{AtomicUsize, Ordering};
    /// # use std::sync::{Arc, Mutex};
    /// #
    /// # #[linked::object]
    /// # struct MetricsCollector {
    /// #     local_requests: AtomicUsize,
    /// #     global_total: Arc<Mutex<usize>>,
    /// # }
    /// #
    /// # impl MetricsCollector {
    /// #     pub fn new() -> Self {
    /// #         let global_total = Arc::new(Mutex::new(0));
    /// #         linked::new!(Self {
    /// #             local_requests: AtomicUsize::new(0),
    /// #             global_total: Arc::clone(&global_total),
    /// #         })
    /// #     }
    /// #     
    /// #     pub fn record_request(&self) {
    /// #         self.local_requests.fetch_add(1, Ordering::Relaxed);
    /// #         *self.global_total.lock().unwrap() += 1;
    /// #     }
    /// #     
    /// #     pub fn local_count(&self) -> usize {
    /// #         self.local_requests.load(Ordering::Relaxed)
    /// #     }
    /// #     
    /// #     pub fn global_count(&self) -> usize {
    /// #         *self.global_total.lock().unwrap()
    /// #     }
    /// # }
    /// use std::thread;
    ///
    /// linked::thread_local_arc!(static METRICS: MetricsCollector = MetricsCollector::new());
    ///
    /// // Use .with() for efficient access when you do not need to store the Arc
    /// METRICS.with(|metrics| {
    ///     metrics.record_request();
    ///     assert_eq!(metrics.local_count(), 1);
    ///     assert_eq!(metrics.global_count(), 1);
    /// });
    ///
    /// // Multiple calls to .with() access the same thread-local instance
    /// METRICS.with(|metrics| {
    ///     assert_eq!(metrics.local_count(), 1); // Still 1 from previous call
    ///     assert_eq!(metrics.global_count(), 1); // Still 1 globally
    /// });
    ///
    /// // Each thread gets its own instance with fresh local state but shared global state
    /// thread::spawn(|| {
    ///     METRICS.with(|metrics| {
    ///         assert_eq!(metrics.local_count(), 0); // Fresh local count for this thread
    ///         assert_eq!(metrics.global_count(), 1); // But sees global count from main thread
    ///         
    ///         metrics.record_request();
    ///         assert_eq!(metrics.local_count(), 1); // Local count incremented
    ///         assert_eq!(metrics.global_count(), 2); // Global count now 2
    ///     });
    /// }).join().unwrap();
    ///
    /// // Back on main thread: local state unchanged, global state updated
    /// METRICS.with(|metrics| {
    ///     assert_eq!(metrics.local_count(), 1); // Still 1 locally
    ///     assert_eq!(metrics.global_count(), 2); // But sees update from other thread
    /// });
    /// ```
    ///
    /// # Performance
    ///
    /// For repeated access to the current thread's linked instance, prefer reusing an `Arc<T>`
    /// obtained from `.to_arc()`.
    ///
    /// If your code is not in a situation where it can reuse an existing `Arc<T>`, this method is
    /// the optimal way to access the current thread's linked instance of `T`.
    #[inline]
    pub fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Arc<T>) -> R,
    {
        (self.get_storage)().with(f)
    }

    /// Gets an `Arc<T>` to the current thread's linked instance from
    /// the object family referenced by the static variable.
    ///
    /// The instance behind this `Arc` is the same one accessed by all other calls through the static
    /// variable on this thread. Note that it is still possible to create multiple instances on a
    /// single thread, e.g. by cloning the `T` within. The "one instance per thread" logic only
    /// applies when the instances are accessed through the static variable.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::atomic::{AtomicUsize, Ordering};
    /// # use std::sync::{Arc, Mutex};
    /// #
    /// # #[linked::object]
    /// # struct ServiceMonitor {
    /// #     local_checks: AtomicUsize,
    /// #     global_failures: Arc<Mutex<usize>>,
    /// # }
    /// #
    /// # impl ServiceMonitor {
    /// #     pub fn new() -> Self {
    /// #         let global_failures = Arc::new(Mutex::new(0));
    /// #         linked::new!(Self {
    /// #             local_checks: AtomicUsize::new(0),
    /// #             global_failures: Arc::clone(&global_failures),
    /// #         })
    /// #     }
    /// #     
    /// #     pub fn check_service(&self, success: bool) {
    /// #         self.local_checks.fetch_add(1, Ordering::Relaxed);
    /// #         if !success {
    /// #             *self.global_failures.lock().unwrap() += 1;
    /// #         }
    /// #     }
    /// #     
    /// #     pub fn local_checks(&self) -> usize {
    /// #         self.local_checks.load(Ordering::Relaxed)
    /// #     }
    /// #     
    /// #     pub fn global_failures(&self) -> usize {
    /// #         *self.global_failures.lock().unwrap()
    /// #     }
    /// # }
    /// use std::thread;
    ///
    /// linked::thread_local_arc!(static MONITOR: ServiceMonitor = ServiceMonitor::new());
    ///
    /// // Get an Arc to reuse across multiple operations
    /// let monitor = MONITOR.to_arc();
    /// monitor.check_service(true);
    /// monitor.check_service(false); // This will increment global failures
    /// assert_eq!(monitor.local_checks(), 2);
    /// assert_eq!(monitor.global_failures(), 1);
    ///
    /// // Multiple calls to to_arc() return Arc to the same instance
    /// let monitor2 = MONITOR.to_arc();
    /// assert_eq!(monitor2.local_checks(), 2); // Same instance as monitor
    /// assert_eq!(monitor2.global_failures(), 1);
    ///
    /// // Clone the Arc for efficiency when passing around
    /// let monitor_clone = Arc::clone(&monitor);
    /// monitor_clone.check_service(true);
    /// assert_eq!(monitor.local_checks(), 3);
    /// assert_eq!(monitor.global_failures(), 1);
    ///
    /// // You can send the Arc to other threads (since T: Send + Sync)
    /// let monitor_for_thread = Arc::clone(&monitor);
    /// thread::spawn(move || {
    ///     // This Arc still refers to the original thread's instance
    ///     monitor_for_thread.check_service(false);
    /// }).join().unwrap();
    /// assert_eq!(monitor.local_checks(), 4); // Local checks on main thread: 4
    /// assert_eq!(monitor.global_failures(), 2); // Global failures from both threads: 2
    ///
    /// // But each thread gets its own instance when accessing through the static
    /// thread::spawn(|| {
    ///     let thread_monitor = MONITOR.to_arc();
    ///     assert_eq!(thread_monitor.local_checks(), 0); // Fresh local state
    ///     assert_eq!(thread_monitor.global_failures(), 2); // But sees shared global state
    ///     
    ///     thread_monitor.check_service(false);
    ///     assert_eq!(thread_monitor.local_checks(), 1); // Local: 1
    ///     assert_eq!(thread_monitor.global_failures(), 3); // Global: 3
    /// }).join().unwrap();
    ///
    /// // Back on main thread: local state unchanged, global state updated
    /// assert_eq!(monitor.local_checks(), 4); // Still 4 locally
    /// assert_eq!(monitor.global_failures(), 3); // But sees update from other thread
    /// ```
    ///
    /// # Performance
    ///
    /// This function merely clones an `Arc`, which is relatively fast but still more work than
    /// doing nothing. The most efficient way to access the current thread's linked instance is
    /// to reuse the `Arc<T>` returned from this method.
    ///
    /// If you are not in a situation where you can reuse the `Arc<T>` and a shared reference is
    /// satisfactory, prefer calling [`.with()`][Self::with] instead, which does not create an
    /// `Arc` and thereby saves a few nanoseconds.
    #[must_use]
    #[inline]
    pub fn to_arc(&self) -> Arc<T> {
        (self.get_storage)().with(Arc::clone)
    }
}

/// Declares that all static variables within the macro body
/// contain thread-local [linked objects][crate].
///
/// A single instance from the same family is maintained per-thread, represented
/// as an `Arc<T>`. This implies `T: Send + Sync`.
///
/// Call [`.with()`][2] to execute a closure with a shared reference to the current thread's
/// instance of the linked object. This is the most efficient way to use the static variable,
/// although the closure style can sometimes be cumbersome.
///
/// Call [`.to_arc()`][1] on the static variable to obtain a thread-specific linked instance
/// of the object, wrapped in an `Arc`. This does not limit you to a closure. Every [`.to_arc()`][1]
/// call returns an `Arc` for the same instance per thread (though you may clone the `T` inside to
/// create additional instances not governed by the mechanics of this macro).
///
/// # Accessing linked instances
///
/// If you are making multiple calls from the same thread, prefer calling [`.to_arc()`][1] and
/// reusing the returned `Arc<T>` for optimal performance.
///
/// If you are not making multiple calls, you may yield optimal performance by calling
/// [`.with()`][2] to execute a closure with a shared reference to the current thread's
/// linked instance.
///
/// # Example
///
/// ```
/// # #[linked::object]
/// # struct TokenCache { }
/// # impl TokenCache { fn with_capacity(capacity: usize) -> Self { linked::new!(Self { } ) } fn get_token(&self) -> usize { 42 } }
/// linked::thread_local_arc!(static TOKEN_CACHE: TokenCache = TokenCache::with_capacity(1000));
///
/// fn do_something() {
///     // `.with()` is the most efficient way to access the instance of the current thread.
///     let token = TOKEN_CACHE.with(|cache| cache.get_token());
/// }
/// ```
///
/// # Dynamic family relationships
///
/// If you need fully `Arc`-style dynamic storage (i.e. not a single static variable) then consider
/// either passing instances of [`InstancePerThreadSync<T>`][6] between threads or using
/// [`Family`][3] to manually control instance creation.
///
/// # Cross-thread usage
///
/// While you can pass the `Arc` returned by [`.to_arc()`][1] between threads, the object within
/// will typically (depending on implementation choices) remain aligned to the original thread it
/// was created on, which may lead to suboptimal performance if you try to use it on a different
/// thread. For optimal multithreaded behavior, call `.with()` or `.to_arc()` on the thread the
/// instance will be used on.
///
/// [1]: StaticInstancePerThreadSync::to_arc
/// [2]: StaticInstancePerThreadSync::with
/// [3]: crate::Family
/// [5]: crate::Object
/// [6]: crate::InstancePerThreadSync
#[macro_export]
macro_rules! thread_local_arc {
    () => {};

    ($(#[$attr:meta])* $vis:vis static $NAME:ident: $t:ty = $e:expr; $($rest:tt)*) => (
        $crate::thread_local_arc!($(#[$attr])* $vis static $NAME: $t = $e);
        $crate::thread_local_arc!($($rest)*);
    );

    ($(#[$attr:meta])* $vis:vis static $NAME:ident: $t:ty = $e:expr) => {
        $crate::__private::paste! {
            $crate::instances!(#[doc(hidden)] static [< $NAME _INITIALIZER >]: $t = $e;);

            ::std::thread_local!(#[doc(hidden)] static [< $NAME _ARC >]: ::std::sync::Arc<$t> = ::std::sync::Arc::new([< $NAME _INITIALIZER >].get()));

            $(#[$attr])* $vis const $NAME: $crate::StaticInstancePerThreadSync<$t> =
                $crate::StaticInstancePerThreadSync::new(move || &[< $NAME _ARC >]);
        }
    };
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::hint::black_box;
    use std::panic::{RefUnwindSafe, UnwindSafe};
    use std::sync::Arc;
    use std::sync::atomic::{self, AtomicUsize};
    use std::thread;

    use static_assertions::assert_impl_all;

    use crate::StaticInstancePerThreadSync;

    assert_impl_all!(
        StaticInstancePerThreadSync<TokenCache>: UnwindSafe,
        RefUnwindSafe
    );

    #[linked::object]
    struct TokenCache {
        local_value: AtomicUsize,
    }

    impl TokenCache {
        fn new(value: usize) -> Self {
            linked::new!(Self {
                local_value: AtomicUsize::new(value)
            })
        }

        fn value(&self) -> usize {
            self.local_value.load(atomic::Ordering::Relaxed)
        }

        fn increment(&self) {
            self.local_value.fetch_add(1, atomic::Ordering::Relaxed);
        }
    }

    #[test]
    fn constructor_executes_at_runtime() {
        thread_local! {
            static CACHE: Arc<TokenCache> = Arc::new(TokenCache::new(1000));
        }

        let instance = black_box(StaticInstancePerThreadSync::new(|| &CACHE));

        instance.with(|cache| assert_eq!(cache.value(), 1000));
    }

    #[test]
    fn smoke_test() {
        linked::thread_local_arc! {
            static BLUE_TOKEN_CACHE: TokenCache = TokenCache::new(1000);
            static YELLOW_TOKEN_CACHE: TokenCache = TokenCache::new(2000);
        }

        assert_eq!(BLUE_TOKEN_CACHE.to_arc().value(), 1000);
        assert_eq!(YELLOW_TOKEN_CACHE.to_arc().value(), 2000);

        BLUE_TOKEN_CACHE.with(|cache| {
            assert_eq!(cache.value(), 1000);
        });
        YELLOW_TOKEN_CACHE.with(|cache| {
            assert_eq!(cache.value(), 2000);
        });

        BLUE_TOKEN_CACHE.to_arc().increment();
        YELLOW_TOKEN_CACHE.to_arc().increment();

        assert_eq!(BLUE_TOKEN_CACHE.to_arc().value(), 1001);
        assert_eq!(YELLOW_TOKEN_CACHE.to_arc().value(), 2001);

        // Another thread gets instances aligned to the other thread.
        thread::spawn(move || {
            assert_eq!(BLUE_TOKEN_CACHE.to_arc().value(), 1000);
            assert_eq!(YELLOW_TOKEN_CACHE.to_arc().value(), 2000);

            BLUE_TOKEN_CACHE.to_arc().increment();
            YELLOW_TOKEN_CACHE.to_arc().increment();

            assert_eq!(BLUE_TOKEN_CACHE.to_arc().value(), 1001);
            assert_eq!(YELLOW_TOKEN_CACHE.to_arc().value(), 2001);
        })
        .join()
        .unwrap();

        assert_eq!(BLUE_TOKEN_CACHE.to_arc().value(), 1001);
        assert_eq!(YELLOW_TOKEN_CACHE.to_arc().value(), 2001);

        // We can move instances to other threads and they stay aligned to the original thread.
        let blue_cache = BLUE_TOKEN_CACHE.to_arc();
        let yellow_cache = YELLOW_TOKEN_CACHE.to_arc();

        thread::spawn(move || {
            assert_eq!(blue_cache.value(), 1001);
            assert_eq!(yellow_cache.value(), 2001);

            blue_cache.increment();
            yellow_cache.increment();

            assert_eq!(blue_cache.value(), 1002);
            assert_eq!(yellow_cache.value(), 2002);
        })
        .join()
        .unwrap();
    }
}
