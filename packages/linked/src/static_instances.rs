// Copyright (c) Microsoft Corporation.
// Copyright (c) Folo authors.

use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::collections::hash_map;
use std::sync::{LazyLock, RwLock};

use hash_hasher::HashedMap;

use crate::{ERR_POISONED_LOCK, Family};

/// This is the real type of variables wrapped in the [`linked::instances!` macro][1].
/// See macro documentation for more details.
///
/// Instances of this type are created by the [`linked::instances!` macro][1],
/// never directly by user code, which can use `.get()` to obtain a linked instance of `T`.
///
/// [1]: [crate::instances]
#[derive(Debug)]
pub struct StaticInstances<T>
where
    T: linked::Object,
{
    /// A function we can call to obtain the lookup key for the family of linked objects.
    ///
    /// The family key is a `TypeId` because the expectation is that a unique empty type is
    /// generated for each static variable in a `linked::instances!` block, to be used
    /// as the family key for all instances linked to each other via that static variable.
    family_key_provider: fn() -> TypeId,

    /// Used to create the first instance of the linked object family. May be called
    /// concurrently multiple times due to optimistic concurrency control, so it must
    /// be idempotent and returned instances must be functionally equivalent. Even though
    /// this may be called multiple times, only one return value will ever be exposed to
    /// user code, with the others being dropped shortly after creation.
    first_instance_provider: fn() -> T,
}

impl<T> StaticInstances<T>
where
    T: linked::Object,
{
    /// This function exists to serve the inner workings of the
    /// `linked::instances!` macro and should not be used directly.
    /// It is not part of the public API and may be removed or changed at any time.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(
        family_key_provider: fn() -> TypeId,
        first_instance_provider: fn() -> T,
    ) -> Self {
        Self {
            family_key_provider,
            first_instance_provider,
        }
    }

    /// Creates a new linked instance of `T` from the same family of linked objects as other
    /// instances created via the same static variable.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::{Arc, Mutex};
    /// #
    /// # #[linked::object]
    /// # struct Counter {
    /// #     local_count: usize,
    /// #     global_count: Arc<Mutex<usize>>,
    /// # }
    /// #
    /// # impl Counter {
    /// #     pub fn new() -> Self {
    /// #         let global_count = Arc::new(Mutex::new(0));
    /// #         linked::new!(Self {
    /// #             local_count: 0,
    /// #             global_count: Arc::clone(&global_count),
    /// #         })
    /// #     }
    /// #     
    /// #     pub fn increment(&mut self) {
    /// #         self.local_count += 1;
    /// #         *self.global_count.lock().unwrap() += 1;
    /// #     }
    /// #     
    /// #     pub fn local_count(&self) -> usize {
    /// #         self.local_count
    /// #     }
    /// #     
    /// #     pub fn global_count(&self) -> usize {
    /// #         *self.global_count.lock().unwrap()
    /// #     }
    /// # }
    /// use std::thread;
    ///
    /// linked::instances!(static COUNTER: Counter = Counter::new());
    ///
    /// // Each call to get() creates a new linked instance
    /// let mut counter1 = COUNTER.get();
    /// let mut counter2 = COUNTER.get();
    ///
    /// counter1.increment();
    /// counter2.increment();
    ///
    /// // Local counts are independent per instance
    /// assert_eq!(counter1.local_count(), 1);
    /// assert_eq!(counter2.local_count(), 1);
    ///
    /// // But global state is shared across the family
    /// assert_eq!(counter1.global_count(), 2);
    /// assert_eq!(counter2.global_count(), 2);
    ///
    /// // Works across threads too
    /// thread::spawn(|| {
    ///     let mut counter3 = COUNTER.get();
    ///     counter3.increment();
    ///     assert_eq!(counter3.global_count(), 3);
    /// }).join().unwrap();
    /// ```
    ///
    /// # Performance
    ///
    /// This creates a new instance of `T` on every call so caching the return value is
    /// performance-critical.
    ///
    /// If you only need one instance per thread, consider using the [`linked::thread_local_rc!`][1]
    /// or [`linked::thread_local_arc!`][2] macro for a more efficient solution.
    ///
    /// [1]: [crate::thread_local_rc]
    /// [2]: [crate::thread_local_arc]
    #[must_use]
    pub fn get(&self) -> T {
        if let Some(instance) = self.new_from_local_registry() {
            return instance;
        }

        // TODO: This global registry step feels too smeared out.
        // Can we draw it together into one step under one lock?
        self.try_initialize_global_registry(self.first_instance_provider);

        let family = self
            .get_family_global()
            .expect("we just initialized it, the family must exist");

        self.set_local(family);

        // We can now be certain the local registry has the value.
        self.new_from_local_registry()
            .expect("we just set the value, it must be there")
    }

    fn set_local(&self, value: Family<T>) {
        LOCAL_REGISTRY.with(|local_registry| {
            local_registry
                .borrow_mut()
                .insert((self.family_key_provider)(), Box::new(value));
        });
    }

    fn get_family_global(&self) -> Option<Family<T>> {
        GLOBAL_REGISTRY
            .read()
            .expect(ERR_POISONED_LOCK)
            .get(&(self.family_key_provider)())
            .and_then(|w| w.downcast_ref::<Family<T>>())
            .cloned()
    }

    /// Attempts to register the object family in the global registry (if not already registered).
    ///
    /// This may use the provided provider to create a "first" instance of `T` even if another
    /// "first" instance has been created already - optimistic concurrency is used to throw
    /// away the extra instance if this proves necessary. Type-level documented requirements
    /// give us the right to do this - the "first" instances must be functionally equivalent.
    ///
    /// # Performance
    ///
    /// This is a slightly expensive operation, so should be avoided on the hot path. We only do
    /// this once per family per thread.
    fn try_initialize_global_registry<FIP>(&self, first_instance_provider: FIP)
    where
        FIP: FnOnce() -> T,
    {
        // We do not today make use of our right to create a "first" instance of `T` even when
        // we do not need it. This is a potential future optimization if it proves valuable.

        let mut global_registry = GLOBAL_REGISTRY.write().expect(ERR_POISONED_LOCK);

        // TODO: We are repeatedly acquiring the family key here and in sibling functions.
        // Perhaps a trivial cost but explore the value of eliminating the duplicate access.
        let family_key = (self.family_key_provider)();
        let entry = global_registry.entry(family_key);

        match entry {
            hash_map::Entry::Occupied(_) => (),
            hash_map::Entry::Vacant(entry) => {
                // TODO: We create an instance here, only to immediately transform it back to
                // a family. Can we skip the middle step and just create a family directly?
                let first_instance = first_instance_provider();
                entry.insert(Box::new(first_instance.family()));
            }
        }
    }

    // Attempts to obtain a new instance of `T` using the current thread's family registry,
    // returning `None` if the linked variable has not yet been seen by this thread and is
    // therefore not present in the local registry.
    fn new_from_local_registry(&self) -> Option<T> {
        LOCAL_REGISTRY.with_borrow(|registry| {
            let family_key = (self.family_key_provider)();

            registry
                .get(&family_key)
                .and_then(|w| w.downcast_ref::<Family<T>>())
                // TODO: We clone the family here, only to immediately transform it to a
                // T instance. Can we skip the middle step and just create an instance directly?
                .map(|family| family.clone().into())
        })
    }
}

/// Declares that all static variables within the macro body define unique
/// families of [linked objects][crate].
///
/// Each [`.get()`][1] on an included static variable returns a new linked object instance,
/// with all instances obtained from the same static variable being part of the same family.
///
/// # Example
///
/// ```
/// # #[linked::object]
/// # struct TokenCache { }
/// # impl TokenCache { fn with_capacity(capacity: usize) -> Self { linked::new!(Self { } ) } fn get_token(&self) -> usize { 42 } }
/// linked::instances!(static TOKEN_CACHE: TokenCache = TokenCache::with_capacity(1000));
///
/// fn do_something() {
///     // `.get()` returns a unique linked instance on every call,
///     // so you are expected to reuse the instance for efficiency.
///     let token_cache = TOKEN_CACHE.get();
///
///     let token1 = token_cache.get_token();
///     let token2 = token_cache.get_token();
/// }
/// ```
///
/// # Dynamic family definition
///
/// If you do not know in advance how many object families are necessary and need to define them
/// at runtime, consider using these alternatives:
///
/// * [`linked::InstancePerThread<T>`][6]
/// * [`linked::InstancePerThreadSync<T>`][7] (if `T: Sync`)
/// * [`linked::Family<T>`][3]
///
/// [1]: StaticInstances::get
/// [3]: crate::Family
/// [5]: crate::Object
/// [6]: crate::InstancePerThread
/// [7]: crate::InstancePerThreadSync
#[macro_export]
macro_rules! instances {
    () => {};

    ($(#[$attr:meta])* $vis:vis static $NAME:ident: $t:ty = $e:expr; $($rest:tt)*) => (
        $crate::instances!($(#[$attr])* $vis static $NAME: $t = $e);
        $crate::instances!($($rest)*);
    );

    ($(#[$attr:meta])* $vis:vis static $NAME:ident: $t:ty = $e:expr) => {
        $crate::__private::paste! {
            #[doc(hidden)]
            #[expect(non_camel_case_types, reason = "intentionally uglified macro generated code")]
            struct [<__lookup_key_ $NAME>];

            $(#[$attr])* $vis const $NAME: $crate::StaticInstances<$t> =
            $crate::StaticInstances::new(
                ::std::any::TypeId::of::<[<__lookup_key_ $NAME>]>,
                move || $e);
        }
    };
}

// We use HashedMap which takes the raw value from Hash::hash() and uses it directly as the key.
// This is OK because TypeId already returns a hashed value as its raw value, no need to hash more.
// We also do not care about any hash manipulation because none of this is untrusted user input.
type FamilyRegistry = HashedMap<TypeId, Box<dyn Any + Send + Sync>>;

// Global registry that is the ultimate authority where all static variable based linked object
// families are registered. Values inside are type-occluded `Family<T>` where T may be different
// for each entry.
static GLOBAL_REGISTRY: LazyLock<RwLock<FamilyRegistry>> =
    LazyLock::new(|| RwLock::new(FamilyRegistry::default()));

thread_local! {
    // Thread-local registry where we cache any families that have been seen by the current
    // thread. Values inside are type-occluded `Family<T>` where T may be different for each entry.
    static LOCAL_REGISTRY: RefCell<FamilyRegistry> = RefCell::new(FamilyRegistry::default());
}

/// Clears all data stored in the static variable based linked object family system
/// from the global data set.
///
/// This is intended for use in tests only. It is publicly exposed because it may need to be called
/// from integration tests and benchmarks, which cannot access private functions.
#[cfg_attr(test, mutants::skip)] // Test/bench logic, do not waste time mutating.
#[doc(hidden)]
pub fn __private_clear_linked_variables_global() {
    let mut global_registry = GLOBAL_REGISTRY.write().expect(ERR_POISONED_LOCK);
    global_registry.clear();
}

/// Clears all data stored in the static variable based linked object family system
/// from the current thread's data set.
///
/// This is intended for use in tests only. It is publicly exposed because it may need to be called
/// from integration tests and benchmarks, which cannot access private functions.
#[cfg_attr(test, mutants::skip)] // Test/bench logic, do not waste time mutating.
#[doc(hidden)]
pub fn __private_clear_linked_variables_local() {
    LOCAL_REGISTRY.with(|local_registry| {
        local_registry.borrow_mut().clear();
    });
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::any::TypeId;
    use std::hint::black_box;
    use std::panic::{RefUnwindSafe, UnwindSafe};
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    use static_assertions::assert_impl_all;

    use crate::StaticInstances;

    assert_impl_all!(
        StaticInstances<TokenCache>: UnwindSafe, RefUnwindSafe
    );

    #[linked::object]
    struct TokenCache {
        value: Arc<Mutex<usize>>,
    }

    impl TokenCache {
        fn new(value: usize) -> Self {
            let value = Arc::new(Mutex::new(value));

            linked::new!(Self {
                value: Arc::clone(&value),
            })
        }

        fn value(&self) -> usize {
            *self.value.lock().unwrap()
        }

        fn increment(&self) {
            let mut writer = self.value.lock().unwrap();
            *writer = writer.saturating_add(1);
        }
    }

    #[test]
    fn constructor_executes_at_runtime() {
        struct Key;

        let instances = black_box(StaticInstances::new(TypeId::of::<Key>, || {
            TokenCache::new(42)
        }));

        assert_eq!(instances.get().value(), 42);
    }

    #[test]
    fn instances_non_macro() {
        // Here we test the inner logic of the instances! macro without applying the macro.
        // There is a separate test for executing the same logic via the macro itself.
        struct RedKey;
        struct GreenKey;

        const RED_TOKEN_CACHE: StaticInstances<TokenCache> =
            StaticInstances::new(TypeId::of::<RedKey>, || TokenCache::new(42));
        const GREEN_TOKEN_CACHE: StaticInstances<TokenCache> =
            StaticInstances::new(TypeId::of::<GreenKey>, || TokenCache::new(99));

        let red = RED_TOKEN_CACHE.get();
        let green = GREEN_TOKEN_CACHE.get();

        assert_eq!(red.value(), 42);
        assert_eq!(green.value(), 99);

        red.increment();
        green.increment();

        thread::spawn(move || {
            let red = RED_TOKEN_CACHE.get();
            let green = GREEN_TOKEN_CACHE.get();

            assert_eq!(red.value(), 43);
            assert_eq!(green.value(), 100);

            red.increment();
            green.increment();
        })
        .join()
        .unwrap();

        assert_eq!(red.value(), 44);
        assert_eq!(green.value(), 101);

        assert_eq!(RED_TOKEN_CACHE.get().value(), 44);
        assert_eq!(GREEN_TOKEN_CACHE.get().value(), 101);
    }

    #[test]
    fn instances_macro() {
        linked::instances! {
            static BLUE_TOKEN_CACHE: TokenCache = TokenCache::new(1000);
            static YELLOW_TOKEN_CACHE: TokenCache = TokenCache::new(2000);
        }

        let blue = BLUE_TOKEN_CACHE.get();
        let yellow = YELLOW_TOKEN_CACHE.get();

        assert_eq!(blue.value(), 1000);
        assert_eq!(yellow.value(), 2000);

        blue.increment();
        yellow.increment();

        thread::spawn(move || {
            let blue = BLUE_TOKEN_CACHE.get();
            let yellow = YELLOW_TOKEN_CACHE.get();

            assert_eq!(blue.value(), 1001);
            assert_eq!(yellow.value(), 2001);

            blue.increment();
            yellow.increment();
        })
        .join()
        .unwrap();

        assert_eq!(blue.value(), 1002);
        assert_eq!(yellow.value(), 2002);

        assert_eq!(BLUE_TOKEN_CACHE.get().value(), 1002);
        assert_eq!(YELLOW_TOKEN_CACHE.get().value(), 2002);
    }

    #[test]
    fn stored_in_thread_local() {
        // We demonstrate that what we get from `instances!` can be stored in thread-local storage
        // while still remaining part of the same linked object family.

        linked::instances!(static LINKED_CACHE: TokenCache = TokenCache::new(1000));
        thread_local!(static LOCAL_CACHE: Rc<TokenCache> = Rc::new(LINKED_CACHE.get()));

        let cache = LOCAL_CACHE.with(Rc::clone);
        assert_eq!(cache.value(), 1000);
        cache.increment();

        thread::spawn(move || {
            let cache = LOCAL_CACHE.with(Rc::clone);
            assert_eq!(cache.value(), 1001);
            cache.increment();
            assert_eq!(cache.value(), 1002);
        })
        .join()
        .unwrap();

        assert_eq!(cache.value(), 1002);

        let cache_rc_clone = LOCAL_CACHE.with(Rc::clone);
        assert_eq!(cache_rc_clone.value(), 1002);
    }
}
