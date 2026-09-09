use std::any::type_name;
#[cfg(debug_assertions)]
use std::backtrace::Backtrace;
use std::cell::UnsafeCell;
use std::fmt;
use std::marker::PhantomData;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::pin::Pin;
use std::ptr::NonNull;
#[cfg(debug_assertions)]
use std::sync::Arc;
use std::sync::Mutex;

#[cfg(debug_assertions)]
use crate::EventRegistry;
use crate::{
    NEVER_POISONED, PoolState, RawPooledReceiver, RawPooledRef, RawPooledSender, ReceiverCore,
    SenderCore,
};

/// A pool of reusable thread-safe one-time events with manual pool lifecycle management.
///
/// # Examples
///
/// ```
/// use events_once::RawEventPool;
///
/// # #[tokio::main]
/// # async fn main() {
/// let pool = Box::pin(RawEventPool::<String>::new());
///
/// for i in 0..3 {
///     // SAFETY: The pool is pinned outside the loop, and both endpoints are consumed before
///     // the iteration ends, so their storage remains alive and stationary.
///     let (tx, rx) = unsafe { pool.as_ref().rent() };
///
///     tx.send(format!("Message {i}"));
///
///     let message = rx.await.unwrap();
///     println!("{message}");
/// }
/// # }
/// ```
pub struct RawEventPool<T: 'static> {
    // The boxed core stays at a stable address so debug-only endpoint references can point to its
    // registry. Methods form only shared core references; the state and registry provide their
    // own synchronization.
    core: NonNull<UnsafeCell<RawEventPoolCore<T>>>,

    // The pointer conveys no ownership, so this marker is what records that the pool owns the
    // values of `T` stored in the events it hands out. The managed pools need no equivalent
    // because their `Arc`/`Rc` core field already expresses that ownership.
    _owns_some: PhantomData<T>,
}

#[cfg_attr(coverage_nightly, coverage(off))] // No API contract to test.
impl<T: Send + 'static> fmt::Debug for RawEventPool<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name::<Self>())
            .field("core", &self.core)
            .finish()
    }
}

impl<T: 'static> Drop for RawEventPool<T> {
    fn drop(&mut self) {
        // SAFETY: `self.core` is the unchanged pointer that the matching `Box::into_raw()` in
        // `new()` produced, so it carries the provenance and layout of that same allocation, and
        // no other code path replaces the field or rebuilds an owning box - this is the only
        // conversion back into a `Box`, so the allocation is freed exactly once. Every caller of
        // `rent()` promised that the pool outlives the endpoints it handed out, so no endpoint
        // can reach the core by the time the pool is dropped, and dropping the pool itself
        // requires exclusive access to it.
        drop(unsafe { Box::from_raw(self.core.as_ptr()) });
    }
}

/// Owns typed event storage and diagnostics at a stable address.
pub(crate) struct RawEventPoolCore<T: 'static> {
    pub(crate) state: Mutex<PoolState<T>>,

    #[cfg(debug_assertions)]
    pub(crate) registry: EventRegistry,
}

#[cfg_attr(coverage_nightly, coverage(off))] // No API contract to test.
impl<T: Send + 'static> fmt::Debug for RawEventPoolCore<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f.debug_struct(type_name::<Self>());

        f.field("state", &self.state);

        #[cfg(debug_assertions)]
        f.field("registry", &self.registry);

        f.finish()
    }
}

impl<T: Send + 'static> RawEventPool<T> {
    /// Creates a new empty event pool.
    #[must_use]
    pub fn new() -> Self {
        let core = RawEventPoolCore {
            state: Mutex::new(PoolState::new()),
            #[cfg(debug_assertions)]
            registry: EventRegistry::new(),
        };

        let core_ptr = Box::into_raw(Box::new(UnsafeCell::new(core)));

        Self {
            // SAFETY: Boxed object is never null.
            core: unsafe { NonNull::new_unchecked(core_ptr) },
            _owns_some: PhantomData,
        }
    }

    /// Returns a shared reference to the core.
    fn core(&self) -> &RawEventPoolCore<T> {
        // SAFETY: The pointer comes from the `Box::into_raw()` in `new()` and is never replaced,
        // so it names a live, initialized, correctly aligned core; the allocation is freed only
        // by `Drop`, which needs exclusive access to the pool and therefore cannot run while
        // this shared borrow exists.
        let core_cell = unsafe { self.core.as_ref() };

        // SAFETY: This method is the only path to the complete core, and it produces only shared
        // references. Endpoints retain an event pointer and, in debug builds, a pointer to the
        // registry rather than the core. Mutation of the state and registry is synchronized by
        // their own mutexes.
        unsafe { core_cell.get().as_ref_unchecked() }
    }

    /// Rents an event from the pool, returning its endpoints.
    ///
    /// The event will be returned to the pool when both endpoints are dropped.
    /// See [`RawPooledReceiver`] for the receiver's callback and reentrancy contract.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that the pool outlives the endpoints.
    #[must_use]
    pub unsafe fn rent(self: Pin<&Self>) -> (RawPooledSender<T>, RawPooledReceiver<T>) {
        let core = self.core();
        let event = core.state.lock().expect(NEVER_POISONED).rent();

        #[cfg(debug_assertions)]
        {
            // SAFETY: Plurality keeps this initialized slot at a stable address until release,
            // which occurs only after unregistration. Endpoints and the registry create only shared
            // references to the event throughout that interval.
            unsafe {
                core.registry.register(event);
            }
        }

        // SAFETY: The event was just rented from this pool's state and has not been released.
        // The endpoints below and the pool's debug-only registry are the only reachers of the
        // event, and none of them creates an exclusive reference to it. Our own caller promised
        // that this pool - the owner of the core - outlives both endpoints.
        let event_ref = unsafe {
            RawPooledRef::new(
                #[cfg(debug_assertions)]
                NonNull::from(&core.registry),
                event,
            )
        };

        let inner_sender = SenderCore::new(event_ref.clone());
        let inner_receiver = ReceiverCore::new(event_ref);

        (
            RawPooledSender::new(inner_sender),
            RawPooledReceiver::new(inner_receiver),
        )
    }

    /// Returns `true` if no events have currently been rented from the pool.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.core().state.lock().expect(NEVER_POISONED).is_empty()
    }

    /// Returns the number of events that have currently been rented from the pool.
    #[must_use]
    pub fn len(&self) -> usize {
        self.core().state.lock().expect(NEVER_POISONED).len()
    }

    /// Uses the provided closure to inspect the backtraces of the most recent awaiter of each
    /// awaited event in the pool.
    ///
    /// This method is only available in debug builds (`cfg(debug_assertions)`).
    /// For any data to be present, `RUST_BACKTRACE=1` or `RUST_LIB_BACKTRACE=1` must be set.
    ///
    /// The closure is called once for each event in the pool that has been awaited at some point
    /// in the past.
    ///
    /// # Reentrancy
    ///
    /// The closure may freely use this pool: it may rent events, drop endpoints of events it
    /// obtained earlier and call [`inspect_awaiters()`][Self::inspect_awaiters] again. The
    /// backtraces are snapshotted before the first call to the closure, so the sequence of
    /// backtraces the closure receives is unaffected by what the closure does to the pool.
    #[cfg(debug_assertions)]
    pub fn inspect_awaiters(&self, mut f: impl FnMut(&Backtrace)) {
        for backtrace in self.awaiter_backtraces() {
            f(&backtrace);
        }
    }

    /// Snapshots the backtrace of the most recent awaiter of each awaited event in the pool.
    ///
    /// The diagnostic registry lock is released before this returns, so the caller may pass the
    /// snapshots to user-supplied code without holding any lock. Each snapshot is a shared owner
    /// of the backtrace, so it stays valid even if its event is released in the meantime.
    #[cfg(debug_assertions)]
    pub(crate) fn awaiter_backtraces(&self) -> Vec<Arc<Backtrace>> {
        self.core().registry.awaiter_backtraces()
    }
}

impl<T: Send + 'static> Default for RawEventPool<T> {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: Automatic inference is unavailable only because of the `NonNull` field. Moving the
// pool moves that pointer while the core allocation stays where it is, and the pool reaches the
// core state exclusively through `RawEventPoolCore::state`, whose mutex synchronizes every such
// access. Values of `T` stored in rented events become reachable from whichever thread the pool
// moved to, so the payload must itself be movable between threads.
// The `'static` bound is already on the struct, so it is not repeated here. Repeating it
// would trigger a rustc bug (rust-lang/rust#110338) in async generator Send inference
// with trait object type params.
unsafe impl<T: Send> Send for RawEventPool<T> {}

// SAFETY: Automatic inference is unavailable only because of the `NonNull` field. A shared
// reference to the pool grants renting and diagnostics. `PoolState` is protected by the core
// mutex, while the debug-only registry independently synchronizes its pointer set and every
// registered event synchronizes its backtrace cell. Concurrent shared use therefore never
// produces unsynchronized access to the core or an event. Renting from several threads places
// values of `T` into events that other threads observe, so the payload must be movable between
// threads.
unsafe impl<T: Send> Sync for RawEventPool<T> {}

// The NonNull<UnsafeCell<RawEventPoolCore<T>>> field disables auto-trait inference for
// UnwindSafe/RefUnwindSafe. The pool state is mutated only while its mutex is held and no such
// mutation can unwind, so a pool observed after a panic still has consistent slot bookkeeping.
// This holds regardless of the payload, which the pool never exposes: a value is reachable only
// through the endpoints of the event that carries it.
impl<T: Send + 'static> UnwindSafe for RawEventPool<T> {}
impl<T: Send + 'static> RefUnwindSafe for RawEventPool<T> {}
