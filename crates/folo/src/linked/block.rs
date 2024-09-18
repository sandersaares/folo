// Copyright (c) Microsoft Corporation.

use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::collections::hash_map;
use std::sync::{LazyLock, RwLock};

use hash_hasher::HashedMap;

use crate::linked::{Handle, Linked};

/// This is the real type of variables wrapped in the `link!` macro. It takes care of
/// the necessary wrapper logic to ensure lazy-initialization of linked variables. Instances of
/// this type are created by the `link!` macro, never by user code which should only use `.get()`
/// to access the value of the variable.
#[derive(Debug)]
pub struct Variable<T>
where
    T: Linked + From<Handle<T>> + 'static,
{
    /// A function we can call to obtain the lookup key. This only exists because
    /// `TypeId::of::<T>()` is not a const fn (yet?). If we can one day compile those calls in const
    /// contexts, we can remove this provider mechanism and always use raw values.
    /// Ref [#77125](https://github.com/rust-lang/rust/issues/77125)
    lookup_key_provider: fn() -> VariableKey,

    /// Used to assign the initial value. May be called concurrently multiple times due to
    /// optimistic concurrency control, so it must be idempotent and returned instances must be
    /// functionally equivalent. Even though this may be called multiple times, only one return
    /// value will ever be exposed to users of a linked variable.
    initializer: fn() -> T,
}

impl<T> Variable<T>
where
    T: Linked + From<Handle<T>> + 'static,
{
    /// Note: this function exists to serve the inner workings of the `link!` macro and should not
    /// be used directly. It is not part of the public API and may be removed or changed at any time.
    #[doc(hidden)]
    pub const fn new(lookup_key_provider: fn() -> TypeId, initializer: fn() -> T) -> Self {
        Self {
            lookup_key_provider,
            initializer,
        }
    }

    /// Gets a new `T` instance from the same family of linked objects.
    ///
    /// # Performance
    ///
    /// This creates a new instance of `T` on every call so caching
    /// the return value is performance-critical.
    pub fn get(&self) -> T {
        if let Some(value) = self.get_local() {
            return value;
        }

        self.try_set_global(self.initializer);

        let handle = self
            .get_handle_global()
            .expect("we just initialized it, the handle must exist");

        self.set_local(handle);

        // We can now be certain the local registry has the value.
        self.get_local()
            .expect("we just set the value, it must be there")
    }

    fn set_local(&self, value: Handle<T>) {
        LOCAL_REGISTRY.with(|local_registry| {
            local_registry
                .borrow_mut()
                .insert((self.lookup_key_provider)(), Box::new(value));
        });
    }

    fn get_handle_global(&self) -> Option<Handle<T>> {
        GLOBAL_REGISTRY
            .read()
            .expect(ERR_POISONED_LOCK)
            .get(&(self.lookup_key_provider)())
            .and_then(|w| w.downcast_ref::<Handle<T>>())
            .cloned()
    }

    /// Attempts to set the global value for this linked variable. If the value is already set,
    /// this will discard the new value. It is only intended to be used in places where you do not
    /// care whether the old or new value wins in case of conflict (e.g. because you will
    /// immediately read the assigned value before using it).
    fn try_set_global<I>(&self, initializer: I)
    where
        I: FnOnce() -> T,
    {
        let mut writer = GLOBAL_REGISTRY.write().expect(ERR_POISONED_LOCK);

        let entry = writer.entry((self.lookup_key_provider)());

        match entry {
            hash_map::Entry::Occupied(_) => (),
            hash_map::Entry::Vacant(vacant) => {
                let instance = initializer();
                vacant.insert(Box::new(instance.handle()));
            }
        }
    }

    // Attempts to obtain a new T using the local registry, returning None if the linked variable
    // has not yet been seen by this thread and is therefore not present in the local registry.
    fn get_local(&self) -> Option<T> {
        LOCAL_REGISTRY.with(|local_registry| {
            let registry = local_registry.borrow();

            let handle = registry
                .get(&(self.lookup_key_provider)())
                .and_then(|w| w.downcast_ref::<Handle<T>>());

            handle.map(|handle| handle.clone().into())
        })
    }
}

/// Declares the static variables within the macro body as containing [linked objects][folo::linked].
/// Call `.get()` on the static variable to obtain a new linked instance of the type within. All
/// instances obtained from the same variable are linked to each other, on any thread.
///
/// This macro exists to simplify usage of the linked object pattern, which in its natural form
/// requires complex wiring for efficient use on many threads. If you need dynamic storage (i.e.
/// not a single static variable), use [`Handle<T>`][folo::linked::Handle] instead of this macro.
///
/// # Example
///
/// ```
/// # use folo::linked::{self, link};
/// # #[linked::object]
/// # struct TokenCache { }
/// # impl TokenCache { fn with_capacity(capacity: usize) -> Self { linked::new!(Self { } ) } fn get_token(&self) -> usize { 42 } }
/// link!(static TOKEN_CACHE: TokenCache = TokenCache::with_capacity(1000));
///
/// fn do_something() {
///     let token_cache = TOKEN_CACHE.get();
///
///     let token = token_cache.get_token();
/// }
/// ```
#[doc(inline)]
pub use folo_decl_macros::__macro_linked_link as link;

// A poisoned lock means the process is in an unrecoverable/unsafe state and must exit (we panic).
const ERR_POISONED_LOCK: &str = "encountered poisoned lock";

/// An anonymous type is synthesized for every linked variable, used as a lookup key.
/// The lookup key is a `TypeId` because the expectation is that a unique empty type is generated
/// for each variable in a `link!` block, to be used as the lookup key for that variable.
type VariableKey = TypeId;

// We use HashedMap which takes the raw value from Hash::hash() and uses it directly as the key.
// This is OK because TypeId already returns a hashed value as its raw value, no need to hash more.
// We also do not care about any hash manipulation because none of this is untrusted user input.
type HandleRegistry = HashedMap<VariableKey, Box<dyn Any + Send + Sync>>;

// Global registry that is the ultimate authority where all linked variables are registered.
// The values inside are `Handle<T>` where T may be different for each entry.
static GLOBAL_REGISTRY: LazyLock<RwLock<HandleRegistry>> =
    LazyLock::new(|| RwLock::new(HandleRegistry::default()));

thread_local! {
    // Thread-local registry where we cache any linked variables that have been seen by the current
    // thread. The values inside are `Handle<T>` where T may be different for each entry.
    static LOCAL_REGISTRY: RefCell<HandleRegistry> = RefCell::new(HandleRegistry::default());
}

/// Clears all data stored in the linked variable system from the current thread's point of view.
///
/// This is intended for use in tests only. It is publicly exposed via the __private module because
/// it may need to be called from integration tests and benchmarks, which cannot access private
/// functions.
#[doc(hidden)]
pub fn __private_clear_linked_variables() {
    let mut global_registry = GLOBAL_REGISTRY.write().expect(ERR_POISONED_LOCK);
    global_registry.clear();

    LOCAL_REGISTRY.with(|local_registry| {
        local_registry.borrow_mut().clear();
    });
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    use super::*;
    use crate::linked;

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
    fn linked_lazy() {
        // Here we test the inner logic of the link! macro without applying the macro.
        // There is a separate test for executing the same logic via the macro itself.
        struct RedKey;
        struct GreenKey;

        const RED_TOKEN_CACHE: Variable<TokenCache> =
            Variable::new(TypeId::of::<RedKey>, || TokenCache::new(42));
        const GREEN_TOKEN_CACHE: Variable<TokenCache> =
            Variable::new(TypeId::of::<GreenKey>, || TokenCache::new(99));

        assert_eq!(RED_TOKEN_CACHE.get().value(), 42);
        assert_eq!(GREEN_TOKEN_CACHE.get().value(), 99);

        RED_TOKEN_CACHE.get().increment();
        GREEN_TOKEN_CACHE.get().increment();

        thread::spawn(move || {
            assert_eq!(RED_TOKEN_CACHE.get().value(), 43);
            assert_eq!(GREEN_TOKEN_CACHE.get().value(), 100);

            RED_TOKEN_CACHE.get().increment();
            GREEN_TOKEN_CACHE.get().increment();
        })
        .join()
        .unwrap();

        assert_eq!(RED_TOKEN_CACHE.get().value(), 44);
        assert_eq!(GREEN_TOKEN_CACHE.get().value(), 101);
    }

    #[test]
    fn linked_smoke_test() {
        link! {
            static BLUE_TOKEN_CACHE: TokenCache = TokenCache::new(1000);
            static YELLOW_TOKEN_CACHE: TokenCache = TokenCache::new(2000);
        }

        assert_eq!(BLUE_TOKEN_CACHE.get().value(), 1000);
        assert_eq!(YELLOW_TOKEN_CACHE.get().value(), 2000);

        assert_eq!(BLUE_TOKEN_CACHE.get().clone().value(), 1000);
        assert_eq!(YELLOW_TOKEN_CACHE.get().clone().value(), 2000);

        BLUE_TOKEN_CACHE.get().increment();
        YELLOW_TOKEN_CACHE.get().increment();

        thread::spawn(move || {
            assert_eq!(BLUE_TOKEN_CACHE.get().value(), 1001);
            assert_eq!(YELLOW_TOKEN_CACHE.get().value(), 2001);

            BLUE_TOKEN_CACHE.get().increment();
            YELLOW_TOKEN_CACHE.get().increment();
        })
        .join()
        .unwrap();

        assert_eq!(BLUE_TOKEN_CACHE.get().value(), 1002);
        assert_eq!(YELLOW_TOKEN_CACHE.get().value(), 2002);
    }

    #[test]
    fn thread_local_from_linked() {
        link!(static LINKED_CACHE: TokenCache = TokenCache::new(1000));
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
