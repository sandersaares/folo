use std::any::type_name;
#[cfg(debug_assertions)]
use std::backtrace::Backtrace;
use std::fmt;
use std::sync::{Arc, Mutex};

#[cfg(debug_assertions)]
use crate::EventRegistry;
use crate::{
    NEVER_POISONED, PoolState, PooledReceiver, PooledRef, PooledSender, ReceiverCore, SenderCore,
};

/// A pool of reusable one-time thread-safe events.
///
/// # Examples
///
/// ```
/// use events_once::EventPool;
///
/// # #[tokio::main]
/// # async fn main() {
/// let pool = EventPool::<String>::new();
///
/// for i in 0..3 {
///     let (tx, rx) = pool.rent();
///
///     tx.send(format!("Message {i}"));
///
///     let message = rx.await.unwrap();
///     println!("{message}");
/// }
/// # }
/// ```
pub struct EventPool<T: 'static> {
    core: Arc<EventPoolCore<T>>,
}

#[cfg_attr(coverage_nightly, coverage(off))] // No API contract to test.
impl<T: Send + 'static> fmt::Debug for EventPool<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name::<Self>())
            .field("core", &self.core)
            .finish()
    }
}

/// Owns typed event storage and diagnostics shared by managed pool handles.
pub(crate) struct EventPoolCore<T: 'static> {
    pub(crate) state: Mutex<PoolState<T>>,

    #[cfg(debug_assertions)]
    pub(crate) registry: Arc<EventRegistry>,
}

impl<T: Send + 'static> EventPool<T> {
    /// Creates a new empty event pool.
    #[must_use]
    pub fn new() -> Self {
        Self {
            core: Arc::new(EventPoolCore {
                state: Mutex::new(PoolState::new()),
                #[cfg(debug_assertions)]
                registry: Arc::new(EventRegistry::new()),
            }),
        }
    }

    /// Rents an event from the pool, returning its endpoints.
    ///
    /// The event will be returned to the pool when both endpoints are dropped.
    /// See [`PooledReceiver`] for the receiver's callback and reentrancy contract.
    #[inline]
    #[must_use]
    pub fn rent(&self) -> (PooledSender<T>, PooledReceiver<T>) {
        let event = self.core.state.lock().expect(NEVER_POISONED).rent();

        #[cfg(debug_assertions)]
        {
            // SAFETY: Plurality keeps this initialized slot at a stable address until release,
            // which occurs only after unregistration. Endpoints and the registry create only shared
            // references to the event throughout that interval.
            unsafe {
                self.core.registry.register(event);
            }
        }

        // SAFETY: The event was just rented from this pool's state and has not been released.
        // The endpoints below and the pool's debug-only registry are the only reachers of the
        // event, and none of them creates an exclusive reference to it.
        let event_ref = unsafe {
            PooledRef::new(
                #[cfg(debug_assertions)]
                Arc::clone(&self.core.registry),
                event,
            )
        };

        let inner_sender = SenderCore::new(event_ref.clone());
        let inner_receiver = ReceiverCore::new(event_ref);

        (
            PooledSender::new(inner_sender),
            PooledReceiver::new(inner_receiver),
        )
    }

    /// Returns `true` if no events have currently been rented from the pool.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.core.state.lock().expect(NEVER_POISONED).is_empty()
    }

    /// Returns the number of events that have currently been rented from the pool.
    #[must_use]
    pub fn len(&self) -> usize {
        self.core.state.lock().expect(NEVER_POISONED).len()
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
        self.core.registry.awaiter_backtraces()
    }
}

impl<T: Send + 'static> Default for EventPool<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + 'static> Clone for EventPool<T> {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))] // No API contract to test.
impl<T: Send + 'static> fmt::Debug for EventPoolCore<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f.debug_struct(type_name::<Self>());

        f.field("state", &self.state);

        #[cfg(debug_assertions)]
        f.field("registry", &self.registry);

        f.finish()
    }
}
