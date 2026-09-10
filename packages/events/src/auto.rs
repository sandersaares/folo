use std::any::type_name;
use std::fmt;
use std::future::Future;
use std::marker::PhantomPinned;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{self, Poll, Waker};

use awaiter_set::{Awaiter, AwaiterSet};

use crate::NEVER_POISONED;

/// Thread-safe async auto-reset event.
///
/// Each [`set()`][Self::set] call releases at most one awaiter.
///
/// # Signal rules
///
/// * If one or more waiters are registered, `set()` releases exactly
///   one waiter and the event stays unset.
/// * If no one is waiting, `set()` stores the signal so that the next
///   [`wait()`][Self::wait] completes immediately (consuming the
///   signal).
/// * Multiple `set()` calls while no one is waiting are coalesced
///   into a single stored signal — only one future waiter is
///   released, not one per `set()` call.
///
/// # Fairness
///
/// The order in which waiters are released is unspecified.
///
/// # Storage
///
/// Use [`boxed()`][Self::boxed] for heap-allocated state (simple,
/// `Clone`-able handles) or [`embedded()`][Self::embedded] to borrow
/// caller-provided storage and avoid the allocation. See the
/// [crate-level documentation](crate) for guidance on when to use
/// each.
///
/// The event is a lightweight cloneable handle. All clones derived
/// from the same origin share the same underlying state.
///
/// # Reentrancy
///
/// A [`Waker`] invoked by this event may re-enter the same event.
/// The following operations are sound when performed from inside a
/// `Waker::wake` callback fired by this event:
///
/// * [`set()`][Self::set] and [`try_wait()`][Self::try_wait]
/// * Creating and polling a fresh [`wait()`][Self::wait] future
/// * Dropping another in-flight [`Future`][std::future::Future] from
///   this event, including one that is still pending
///
/// # Examples
///
/// ```
/// use events::AutoResetEvent;
///
/// #[tokio::main]
/// async fn main() {
///     let event = AutoResetEvent::boxed();
///     let setter = event.clone();
///
///     // Producer signals from a background task.
///     tokio::spawn(async move {
///         setter.set();
///     });
///
///     // Consumer waits for the signal.
///     event.wait().await;
///
///     // Signal was consumed.
///     assert!(!event.try_wait());
/// }
/// ```
#[derive(Clone)]
pub struct AutoResetEvent {
    inner: Arc<EventInner>,
}

// `EventInner::state` is an `AtomicU8` packing two independent flags
// plus the implicit IDLE (all-zero) state:
//
// * `IDLE`        — no other bits set.
// * `SIGNALED`    — a signal is stored, awaiting consumption by the
//                   next `wait()` / `try_wait()`.
// * `HAS_WAITERS` — one or more awaiters are registered in `slow`.
//
// All four combinations (`IDLE`, `SIGNALED`, `HAS_WAITERS`, and
// `SIGNALED | HAS_WAITERS`) are reachable. The combined state is
// transient: it appears when `set()` (fast path) races with a
// waiter's `fetch_or(HAS_WAITERS)`. The waiter's post-`fetch_or`
// re-check consumes the signal and clears `HAS_WAITERS`.
//
// Key invariant: `HAS_WAITERS` clear ⇒ `slow` is empty. The converse
// does not hold — the bit may briefly outlive the last waiter,
// because `set()` and `drop_wait()` clear it under the mutex after
// observing `slow.is_empty()`. The "no waiters despite HAS_WAITERS"
// branch in `set()` handles the resulting window.
//
// `slow` is only consulted on the slow path. The `SIGNALED` bit is
// flipped without holding the mutex.
const IDLE: u8 = 0;
const SIGNALED: u8 = 0x1;
const HAS_WAITERS: u8 = 0x2;

struct EventInner {
    state: AtomicU8,
    slow: Mutex<AwaiterSet>,
}

impl EventInner {
    fn set(&self) {
        // Fast path: no waiters — store the signal atomically.
        if self
            .state
            .compare_exchange(IDLE, SIGNALED, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }

        // If already set, nothing to do.
        let prev = self.state.load(Ordering::Relaxed);
        if prev & SIGNALED != 0 {
            return;
        }

        #[cfg(test)]
        crate::test_hooks::run(&crate::test_hooks::AUTO_SET_PRE_LOCK);

        // Slow path: waiters exist — pick a waker under the mutex,
        // then wake it after releasing the mutex to avoid deadlocks
        // with reentrant wakers.
        let waker: Option<Waker>;
        {
            let mut waiters = self.slow.lock().expect(NEVER_POISONED);

            if let Some(w) = waiters.notify_one() {
                if waiters.is_empty() {
                    self.state.fetch_and(!HAS_WAITERS, Ordering::Relaxed);
                }
                waker = Some(w);
            } else {
                // No waiters despite HAS_WAITERS — store SIGNALED.
                self.state.store(SIGNALED, Ordering::Release);
                waker = None;
            }
        }

        if let Some(w) = waker {
            w.wake();
        }
    }

    fn try_wait(&self) -> bool {
        // Atomically clear the SIGNALED bit, leaving HAS_WAITERS untouched.
        // Using fetch_and rather than compare_exchange(SIGNALED, IDLE) ensures
        // we still consume the signal when HAS_WAITERS happens to be set
        // (which can occur in the slow path of poll_wait after fetch_or).
        self.state.fetch_and(!SIGNALED, Ordering::Acquire) & SIGNALED != 0
    }

    unsafe fn poll_wait(&self, awaiter: *mut Awaiter, waker: Waker) -> Poll<()> {
        // Fast path: try to consume the signal atomically.
        if self.try_wait() {
            return Poll::Ready(());
        }

        // Check if we were directly notified by set() before taking the mutex.
        // SAFETY: Validity — the awaiter is pinned inside the owning future and outlives
        // this call. Aliasing — `Awaiter`'s public methods all take `&self`; the only
        // place `&mut Awaiter` is ever constructed is under `slow` by this same future's
        // poll/drop path, which is single-threaded (one future is polled by one task at a
        // time) and has not constructed `&mut Awaiter` here. Other threads access the
        // awaiter only via `AwaiterSet`, which uses `&Awaiter`.
        let awaiter_ref = unsafe { awaiter.as_ref_unchecked() };
        if awaiter_ref.take_notification() {
            return Poll::Ready(());
        }

        #[cfg(test)]
        crate::test_hooks::run(&crate::test_hooks::AUTO_PRE_MUTEX);

        // Slow path: acquire the mutex.
        let mut waiters = self.slow.lock().expect(NEVER_POISONED);

        // Re-check notification under the mutex. A concurrent set() may
        // have taken the slow path and notified us before we acquired
        // the mutex.
        if awaiter_ref.take_notification() {
            return Poll::Ready(());
        }

        #[cfg(test)]
        crate::test_hooks::run(&crate::test_hooks::AUTO_PRE_TRY_WAIT);

        // Re-check signal under the mutex. A concurrent set() may have
        // taken its fast path and stored SIGNALED before we acquired the
        // mutex.
        if self.try_wait() {
            return Poll::Ready(());
        }

        #[cfg(test)]
        crate::test_hooks::run(&crate::test_hooks::AUTO_PRE_FETCH_OR);

        // Register or update the waker. Set HAS_WAITERS before the
        // final signal check to close the race window: a concurrent
        // set() that observes HAS_WAITERS will enter the slow path
        // and wake us. If set() runs between our try_wait and this
        // fetch_or, the re-check below catches it.
        self.state.fetch_or(HAS_WAITERS, Ordering::Relaxed);

        // Re-check signal after setting HAS_WAITERS. A concurrent
        // set() that ran between try_wait and fetch_or would have
        // stored SIGNALED via its fast path, which requires state==IDLE,
        // which in turn requires the awaiter set to be empty. So when
        // this branch fires we are the only would-be waiter and can
        // unconditionally clear HAS_WAITERS.
        if self.try_wait() {
            self.state.fetch_and(!HAS_WAITERS, Ordering::Relaxed);
            return Poll::Ready(());
        }

        // SAFETY: Validity — the awaiter is pinned inside the owning future and outlives
        // this call. Aliasing — we hold `slow` (so no other thread can construct an
        // `Awaiter` reference via `AwaiterSet`); the awaiter is owned by a single future
        // polled by a single task (so no other poll/drop path runs concurrently); and
        // our prior `awaiter_ref` borrow is no longer in use past this point.
        let awaiter_mut = unsafe { awaiter.as_mut_unchecked() };
        // SAFETY: The awaiter is pinned inside the owning future.
        let awaiter_mut = unsafe { Pin::new_unchecked(awaiter_mut) };
        // SAFETY: We hold the mutex; the awaiter is pinned.
        unsafe {
            waiters.register(awaiter_mut, waker);
        }
        Poll::Pending
    }

    unsafe fn drop_wait(&self, awaiter: *mut Awaiter) {
        // SAFETY: Validity — the awaiter is pinned inside the owning future and outlives
        // this call. Aliasing — `Awaiter`'s public methods all take `&self`; the only
        // place `&mut Awaiter` is ever constructed is under `slow` by this same future's
        // poll/drop path, which is single-threaded (one future is polled by one task at a
        // time) and has not constructed `&mut Awaiter` here. Other threads access the
        // awaiter only via `AwaiterSet`, which uses `&Awaiter`.
        let awaiter_ref = unsafe { awaiter.as_ref_unchecked() };
        if !awaiter_ref.is_registered() {
            return;
        }

        let mut waiters = self.slow.lock().expect(NEVER_POISONED);

        if awaiter_ref.is_notified() {
            // We were notified but the future was cancelled. Forward
            // the notification to the next waiter.
            if let Some(waker) = waiters.notify_one() {
                if waiters.is_empty() {
                    self.state.fetch_and(!HAS_WAITERS, Ordering::Relaxed);
                }
                drop(waiters);
                waker.wake();
            } else {
                // No more waiters — restore the SIGNALED state.
                self.state.store(SIGNALED, Ordering::Release);
            }
        } else {
            // Not notified — just remove from the set.
            // SAFETY: Validity — the awaiter is pinned inside the owning future and
            // outlives this call. Aliasing — we hold `slow` (so no other thread can
            // construct an `Awaiter` reference via `AwaiterSet`); the awaiter is owned
            // by a single future polled by a single task (so no other poll/drop path
            // runs concurrently); and our prior `awaiter_ref` borrow is no longer in
            // use past this point.
            let awaiter_mut = unsafe { awaiter.as_mut_unchecked() };
            // SAFETY: The awaiter is pinned inside the owning future.
            let awaiter_mut = unsafe { Pin::new_unchecked(awaiter_mut) };
            // SAFETY: We hold the mutex.
            unsafe {
                waiters.unregister(awaiter_mut);
            }
            if waiters.is_empty() {
                self.state.fetch_and(!HAS_WAITERS, Ordering::Relaxed);
            }
        }
    }
}

impl AutoResetEvent {
    /// Creates a new event in the unset state.
    ///
    /// The state is heap-allocated. Clone the handle to share the same
    /// event. For caller-provided storage, see
    /// [`embedded()`][Self::embedded].
    ///
    /// # Examples
    ///
    /// ```
    /// use events::AutoResetEvent;
    ///
    /// let event = AutoResetEvent::boxed();
    /// let clone = event.clone();
    ///
    /// // Both handles operate on the same underlying event.
    /// clone.set();
    /// assert!(event.try_wait());
    /// ```
    #[must_use]
    pub fn boxed() -> Self {
        Self {
            inner: Arc::new(EventInner {
                state: AtomicU8::new(IDLE),
                slow: Mutex::new(AwaiterSet::new()),
            }),
        }
    }

    /// Creates a handle from an [`EmbeddedAutoResetEvent`] container,
    /// avoiding heap allocation.
    ///
    /// Calling this multiple times on the same container is safe and
    /// returns handles that all operate on the same shared state, just
    /// like copying or cloning a [`EmbeddedAutoResetEventRef`].
    ///
    /// # Safety
    ///
    /// The caller must ensure that the [`EmbeddedAutoResetEvent`] outlives
    /// all returned handles and any [`EmbeddedAutoResetWaitFuture`]s created
    /// from them.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::pin::pin;
    ///
    /// use events::{AutoResetEvent, EmbeddedAutoResetEvent};
    ///
    /// # futures::executor::block_on(async {
    /// let container = pin!(EmbeddedAutoResetEvent::new());
    ///
    /// // SAFETY: The container outlives the handle and all wait futures.
    /// let event = unsafe { AutoResetEvent::embedded(container.as_ref()) };
    /// let setter = event;
    ///
    /// setter.set();
    /// event.wait().await;
    /// # });
    /// ```
    #[must_use]
    pub unsafe fn embedded(place: Pin<&EmbeddedAutoResetEvent>) -> EmbeddedAutoResetEventRef {
        let inner = NonNull::from_ref(&place.get_ref().inner);
        EmbeddedAutoResetEventRef { inner }
    }

    /// Signals the event, releasing at most one waiter.
    ///
    /// * If one or more waiters are registered, a single waiter is
    ///   released and the event remains unset.
    /// * If no one is waiting, the event transitions to the set state
    ///   so that the next [`wait()`][Self::wait] or
    ///   [`try_wait()`][Self::try_wait] completes immediately.
    ///
    /// # Examples
    ///
    /// ```
    /// use events::AutoResetEvent;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let event = AutoResetEvent::boxed();
    ///     let setter = event.clone();
    ///
    ///     tokio::spawn(async move {
    ///         setter.set();
    ///     });
    ///
    ///     event.wait().await;
    /// }
    /// ```
    #[inline]
    #[cfg_attr(coverage_nightly, coverage(off))] // Trivial forwarder.
    pub fn set(&self) {
        self.inner.set();
    }

    /// Attempts to consume the signal without blocking.
    ///
    /// Returns `true` if the event was set, atomically transitioning it back
    /// to the unset state. Returns `false` if the event was not set.
    ///
    /// # Examples
    ///
    /// ```
    /// use events::AutoResetEvent;
    ///
    /// let event = AutoResetEvent::boxed();
    /// assert!(!event.try_wait());
    ///
    /// event.set();
    /// assert!(event.try_wait());
    ///
    /// // Signal was consumed.
    /// assert!(!event.try_wait());
    /// ```
    #[inline]
    #[must_use]
    #[cfg_attr(coverage_nightly, coverage(off))] // Trivial forwarder.
    pub fn try_wait(&self) -> bool {
        self.inner.try_wait()
    }

    /// Returns a future that completes when the event is signaled.
    ///
    /// When [`set()`][Self::set] is called, a single waiting future is
    /// released. If the event is already set (no prior waiter consumed it),
    /// the future completes immediately and consumes the signal.
    ///
    /// # Examples
    ///
    /// ```
    /// use events::AutoResetEvent;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let event = AutoResetEvent::boxed();
    ///     let setter = event.clone();
    ///
    ///     tokio::spawn(async move {
    ///         setter.set();
    ///     });
    ///
    ///     event.wait().await;
    /// }
    /// ```
    #[must_use]
    pub fn wait(&self) -> AutoResetWaitFuture {
        AutoResetWaitFuture {
            inner: Arc::clone(&self.inner),
            awaiter: Awaiter::new(),
        }
    }
}

/// Future returned by [`AutoResetEvent::wait()`].
///
/// Completes with `()` when the event signal is acquired.
pub struct AutoResetWaitFuture {
    inner: Arc<EventInner>,
    awaiter: Awaiter,
}

// Marker trait impl.
// SAFETY: Awaiter is Send. All awaiter access is protected by the event's
// Mutex. The Arc<EventInner> is Send + Sync.
unsafe impl Send for AutoResetWaitFuture {}

// Awaiter is UnwindSafe and RefUnwindSafe.
// Marker trait impl.
impl UnwindSafe for AutoResetWaitFuture {}
// Marker trait impl.
impl RefUnwindSafe for AutoResetWaitFuture {}

impl Future for AutoResetWaitFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<()> {
        // Clone the waker before acquiring the lock so a panicking clone
        // implementation cannot poison the mutex.
        let waker = cx.waker().clone();

        // SAFETY: We only access fields, we do not move self.
        let this = unsafe { self.get_unchecked_mut() };

        // Capture a raw pointer to the awaiter. No `&mut Awaiter` is
        // created here; the mutable reference is built later inside
        // the event mutex.
        let awaiter: *mut Awaiter = &raw mut this.awaiter;
        // SAFETY: The awaiter is pinned inside this future and outlives
        // the call; `inner.slow` is the mutex it registers with.
        unsafe { this.inner.poll_wait(awaiter, waker) }
    }
}

impl Drop for AutoResetWaitFuture {
    fn drop(&mut self) {
        // No `&mut Awaiter` is created here; the mutable reference is
        // built later inside the event mutex when needed.
        let awaiter: *mut Awaiter = &raw mut self.awaiter;
        // SAFETY: The awaiter is pinned inside this future and outlives
        // the call; `inner.slow` is the mutex it was registered with.
        unsafe { self.inner.drop_wait(awaiter) }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))] // No API contract for Debug format.
impl fmt::Debug for AutoResetEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name::<Self>()).finish_non_exhaustive()
    }
}

#[cfg_attr(coverage_nightly, coverage(off))] // No API contract for Debug format.
impl fmt::Debug for AutoResetWaitFuture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name::<Self>())
            // SAFETY: Debug output is best-effort; no concurrent
            // mutation during formatting.
            .finish_non_exhaustive()
    }
}

/// Embedded-state container for [`AutoResetEvent`].
///
/// Stores the event state inline in a struct, avoiding the heap allocation
/// that [`AutoResetEvent::boxed()`] requires. Create the container with
/// [`new()`][Self::new], pin it, then call [`AutoResetEvent::embedded()`]
/// to obtain a [`EmbeddedAutoResetEventRef`] handle.
///
/// # Examples
///
/// ```
/// use std::pin::pin;
///
/// use events::{AutoResetEvent, EmbeddedAutoResetEvent};
///
/// # futures::executor::block_on(async {
/// let container = pin!(EmbeddedAutoResetEvent::new());
///
/// // SAFETY: The container outlives the handle and all wait futures.
/// let event = unsafe { AutoResetEvent::embedded(container.as_ref()) };
/// let setter = event;
///
/// setter.set();
/// event.wait().await;
/// # });
/// ```
pub struct EmbeddedAutoResetEvent {
    inner: EventInner,
    _pinned: PhantomPinned,
}

impl EmbeddedAutoResetEvent {
    /// Creates a new embedded event container in the unset state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: EventInner {
                state: AtomicU8::new(IDLE),
                slow: Mutex::new(AwaiterSet::new()),
            },
            _pinned: PhantomPinned,
        }
    }
}

impl Default for EmbeddedAutoResetEvent {
    #[cfg_attr(coverage_nightly, coverage(off))] // Trivial forwarder to new().
    fn default() -> Self {
        Self::new()
    }
}

/// Handle to an embedded [`AutoResetEvent`].
///
/// Created via [`AutoResetEvent::embedded()`]. The caller is responsible
/// for ensuring the [`EmbeddedAutoResetEvent`] outlives all handles and
/// wait futures.
///
/// The API is identical to [`AutoResetEvent`].
#[derive(Clone, Copy)]
pub struct EmbeddedAutoResetEventRef {
    inner: NonNull<EventInner>,
}

// Marker trait impl.
// SAFETY: EventInner is Send + Sync. The raw pointer is only dereferenced
// to obtain &EventInner, which is safe to share across threads.
unsafe impl Send for EmbeddedAutoResetEventRef {}

// Marker trait impl.
// SAFETY: Same as Send — all mutable access is mediated by the Mutex.
unsafe impl Sync for EmbeddedAutoResetEventRef {}

// Marker trait impl.
impl UnwindSafe for EmbeddedAutoResetEventRef {}
// Marker trait impl.
impl RefUnwindSafe for EmbeddedAutoResetEventRef {}

impl EmbeddedAutoResetEventRef {
    fn inner(&self) -> &EventInner {
        // SAFETY: Validity — the caller of `embedded()` guarantees the container outlives
        // this handle. Aliasing — `EventInner`'s API never constructs `&mut EventInner`
        // (interior mutability lives behind atomics and `Mutex`), so multiple shared
        // references may coexist.
        unsafe { self.inner.as_ref() }
    }

    /// Signals the event, releasing exactly one waiter.
    #[inline]
    #[cfg_attr(coverage_nightly, coverage(off))] // Trivial forwarder.
    pub fn set(&self) {
        self.inner().set();
    }

    /// Attempts to consume the signal without blocking.
    ///
    /// Returns `true` if the event was set, atomically transitioning it
    /// back to the unset state. Returns `false` if the event was not set.
    #[inline]
    #[must_use]
    #[cfg_attr(coverage_nightly, coverage(off))] // Trivial forwarder.
    pub fn try_wait(&self) -> bool {
        self.inner().try_wait()
    }

    /// Returns a future that completes when the event is signaled.
    #[must_use]
    pub fn wait(&self) -> EmbeddedAutoResetWaitFuture {
        EmbeddedAutoResetWaitFuture {
            inner: self.inner,
            awaiter: Awaiter::new(),
        }
    }
}

/// Future returned by [`EmbeddedAutoResetEventRef::wait()`].
pub struct EmbeddedAutoResetWaitFuture {
    inner: NonNull<EventInner>,
    awaiter: Awaiter,
}

// Marker trait impl.
// SAFETY: Awaiter is Send. All awaiter access is protected by the event's
// Mutex.
unsafe impl Send for EmbeddedAutoResetWaitFuture {}

// Marker trait impl.
impl UnwindSafe for EmbeddedAutoResetWaitFuture {}
// Marker trait impl.
impl RefUnwindSafe for EmbeddedAutoResetWaitFuture {}

impl Future for EmbeddedAutoResetWaitFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<()> {
        // Clone the waker before acquiring the lock so a panicking clone
        // implementation cannot poison the mutex.
        let waker = cx.waker().clone();

        // SAFETY: We only access fields, we do not move self.
        let this = unsafe { self.get_unchecked_mut() };

        // SAFETY: Validity — the container outlives this future per the `embedded()`
        // contract. Aliasing — `EventInner`'s API never constructs `&mut EventInner`
        // (interior mutability lives behind atomics and `Mutex`), so multiple shared
        // references may coexist.
        let inner = unsafe { this.inner.as_ref() };
        // Capture a raw pointer to the awaiter. No `&mut Awaiter` is
        // created here; the mutable reference is built later inside
        // the event mutex.
        let awaiter: *mut Awaiter = &raw mut this.awaiter;
        // SAFETY: The awaiter is pinned inside this future and outlives
        // the call; `inner.slow` is the mutex it registers with.
        unsafe { inner.poll_wait(awaiter, waker) }
    }
}

impl Drop for EmbeddedAutoResetWaitFuture {
    fn drop(&mut self) {
        // SAFETY: Validity — the container outlives this future per the `embedded()`
        // contract. Aliasing — `EventInner`'s API never constructs `&mut EventInner`
        // (interior mutability lives behind atomics and `Mutex`), so multiple shared
        // references may coexist.
        let inner = unsafe { self.inner.as_ref() };
        // No `&mut Awaiter` is created here; the mutable reference is
        // built later inside the event mutex when needed.
        let awaiter: *mut Awaiter = &raw mut self.awaiter;
        // SAFETY: The awaiter is pinned inside this future and outlives
        // the call; `inner.slow` is the mutex it was registered with.
        unsafe { inner.drop_wait(awaiter) }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
impl fmt::Debug for EmbeddedAutoResetEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name::<Self>()).finish_non_exhaustive()
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
impl fmt::Debug for EmbeddedAutoResetEventRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name::<Self>()).finish_non_exhaustive()
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
impl fmt::Debug for EmbeddedAutoResetWaitFuture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name::<Self>())
            // SAFETY: Debug output is best-effort; no concurrent
            // mutation during formatting.
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::{iter, thread};

    use static_assertions::{assert_impl_all, assert_not_impl_any};

    use super::*;
    use crate::test_hooks::BarrierHook;

    assert_impl_all!(AutoResetEvent: Send, Sync, Clone, UnwindSafe, RefUnwindSafe);
    assert_impl_all!(AutoResetWaitFuture: Send, UnwindSafe, RefUnwindSafe);
    assert_not_impl_any!(AutoResetWaitFuture: Sync, Unpin);

    assert_impl_all!(EmbeddedAutoResetEvent: Send, Sync, UnwindSafe, RefUnwindSafe);
    assert_not_impl_any!(EmbeddedAutoResetEvent: Unpin);
    assert_impl_all!(EmbeddedAutoResetEventRef: Send, Sync, Clone, Copy, UnwindSafe, RefUnwindSafe);
    assert_impl_all!(EmbeddedAutoResetWaitFuture: Send, UnwindSafe, RefUnwindSafe);
    assert_not_impl_any!(EmbeddedAutoResetWaitFuture: Sync, Unpin);

    #[test]
    fn starts_unset() {
        let event = AutoResetEvent::boxed();
        assert!(!event.try_wait());
    }

    #[test]
    fn set_then_try_wait() {
        let event = AutoResetEvent::boxed();
        event.set();
        assert!(event.try_wait());
        // Signal consumed.
        assert!(!event.try_wait());
    }

    #[test]
    fn clone_shares_state() {
        let a = AutoResetEvent::boxed();
        let b = a.clone();
        a.set();
        assert!(b.try_wait());
    }

    #[test]
    fn double_set_without_waiter_only_stores_one_signal() {
        let event = AutoResetEvent::boxed();
        event.set();
        event.set();
        assert!(event.try_wait());
        // Second set was a no-op (already set).
        assert!(!event.try_wait());
    }

    #[test]
    fn wait_completes_when_already_set() {
        futures::executor::block_on(async {
            let event = AutoResetEvent::boxed();
            event.set();
            event.wait().await;
            // Signal consumed.
            assert!(!event.try_wait());
        });
    }

    #[test]
    fn wait_completes_after_set() {
        let event = AutoResetEvent::boxed();
        let mut future = Box::pin(event.wait());
        let waker = Waker::noop();
        let mut cx = task::Context::from_waker(waker);

        assert!(future.as_mut().poll(&mut cx).is_pending());
        event.set();
        assert!(future.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn only_one_waiter_released_per_set() {
        let event = AutoResetEvent::boxed();

        let mut f1 = Box::pin(event.wait());
        let mut f2 = Box::pin(event.wait());
        let mut f3 = Box::pin(event.wait());
        let waker = Waker::noop();
        let mut cx = task::Context::from_waker(waker);

        // All three register.
        assert!(f1.as_mut().poll(&mut cx).is_pending());
        assert!(f2.as_mut().poll(&mut cx).is_pending());
        assert!(f3.as_mut().poll(&mut cx).is_pending());

        // Signal once — exactly one waiter should complete.
        event.set();
        let mut ready_count = 0_u32;
        for f in [f1.as_mut(), f2.as_mut(), f3.as_mut()] {
            if f.poll(&mut cx).is_ready() {
                ready_count = ready_count.checked_add(1).unwrap();
            }
        }
        assert_eq!(ready_count, 1);

        // Signal twice more to release the remaining two.
        event.set();
        event.set();
        for f in [f1.as_mut(), f2.as_mut(), f3.as_mut()] {
            if f.poll(&mut cx).is_ready() {
                ready_count = ready_count.checked_add(1).unwrap();
            }
        }
        assert_eq!(ready_count, 3);
    }

    #[test]
    fn cancelled_waiter_forwards_notification() {
        let event = AutoResetEvent::boxed();

        let mut future = Box::pin(event.wait());
        let waker = Waker::noop();
        let mut cx = task::Context::from_waker(waker);

        // Register the waiter.
        assert!(future.as_mut().poll(&mut cx).is_pending());

        // Drop without completing — cancellation should not lose the signal.
        drop(future);

        // Set and wait again — should work because the cancelled waiter
        // was never notified.
        event.set();
        let mut future2 = Box::pin(event.wait());
        assert!(future2.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn drop_unpolled_future_is_safe() {
        let event = AutoResetEvent::boxed();
        {
            let _future = event.wait();
        }
        event.set();
        futures::executor::block_on(event.wait());
    }

    #[test]
    fn set_from_another_thread() {
        testing::with_watchdog(|| {
            let event = AutoResetEvent::boxed();
            let setter = event.clone();
            let barrier = Arc::new(Barrier::new(2));
            let b2 = Arc::clone(&barrier);

            let handle = thread::spawn(move || {
                b2.wait();
                setter.set();
            });

            barrier.wait();

            while !event.try_wait() {
                std::hint::spin_loop();
            }

            handle.join().unwrap();
        });
    }

    #[test]
    fn only_one_thread_acquires_signal() {
        testing::with_watchdog(|| {
            let event = AutoResetEvent::boxed();
            let waiter_count = 4;
            let barrier = Arc::new(Barrier::new(waiter_count + 1));
            let acquired_count = Arc::new(AtomicUsize::new(0));

            let handles: Vec<_> = iter::repeat_with(|| {
                let e = event.clone();
                let b = Arc::clone(&barrier);
                let count = Arc::clone(&acquired_count);

                thread::spawn(move || {
                    b.wait();

                    // Each thread tries to acquire many times.
                    for _ in 0..200 {
                        if e.try_wait() {
                            count.fetch_add(1, Ordering::Relaxed);
                        }
                        std::hint::spin_loop();
                    }
                })
            })
            .take(waiter_count)
            .collect();

            // Set before releasing threads so the signal is guaranteed
            // to be available when they start competing.
            event.set();
            barrier.wait();

            for h in handles {
                h.join().unwrap();
            }

            // Exactly one thread should have acquired the single signal.
            assert_eq!(acquired_count.load(Ordering::Relaxed), 1);
        });
    }

    #[test]
    fn multiple_sets_release_multiple_threads() {
        testing::with_watchdog(|| {
            let event = AutoResetEvent::boxed();
            let signal_count = 4;
            let barrier = Arc::new(Barrier::new(signal_count + 1));
            let acquired_count = Arc::new(AtomicUsize::new(0));

            let handles: Vec<_> = iter::repeat_with(|| {
                let e = event.clone();
                let b = Arc::clone(&barrier);
                let count = Arc::clone(&acquired_count);

                thread::spawn(move || {
                    b.wait();

                    // Spin until we acquire a signal.
                    while !e.try_wait() {
                        std::hint::spin_loop();
                    }

                    count.fetch_add(1, Ordering::Relaxed);
                })
            })
            .take(signal_count)
            .collect();

            barrier.wait();

            // Keep setting until all threads have acquired a signal.
            // Each set() stores at most one signal, so we must set
            // repeatedly rather than calling set() N times in a row.
            while acquired_count.load(Ordering::Relaxed) < signal_count {
                event.set();
                std::hint::spin_loop();
            }

            for h in handles {
                h.join().unwrap();
            }
        });
    }

    // The next three tests use the [`crate::test_hooks`] infrastructure
    // to deterministically exercise the race-resolution branches in
    // `poll_wait()` that would otherwise depend on thread interleaving.
    // Each test pauses the producer thread inside `poll_wait()` via a
    // barrier hook, performs the racing operation from the test thread,
    // then releases the producer. The producer's poll is guaranteed to
    // hit the targeted branch.

    #[test]
    fn poll_wait_post_mutex_take_notification_branch() {
        // Covers the post-mutex `take_notification()` → Ready branch.
        // Race: a concurrent `set()` notifies our awaiter between the
        // pre-mutex `take_notification()` check and the moment we
        // acquire the mutex.
        testing::with_watchdog(|| {
            let BarrierHook {
                entered,
                proceed,
                hook,
            } = crate::test_hooks::barrier_hook();
            crate::test_hooks::with_hook(&crate::test_hooks::AUTO_PRE_MUTEX, hook, || {
                let event = AutoResetEvent::boxed();

                // First poll on the test thread registers the awaiter.
                let mut future = Box::pin(event.wait());
                let waker = Waker::noop();
                let mut cx = task::Context::from_waker(waker);
                assert!(future.as_mut().poll(&mut cx).is_pending());

                // Second poll on a separate thread will pause at the
                // hook after the pre-mutex `take_notification()` check
                // but before locking the mutex.
                let producer = thread::spawn(move || {
                    crate::test_hooks::HOOK_PARTICIPANT.with(|c| c.set(true));
                    let waker = Waker::noop();
                    let mut cx = task::Context::from_waker(waker);
                    future.as_mut().poll(&mut cx)
                });

                entered.wait();
                event.set();
                proceed.wait();

                assert!(producer.join().unwrap().is_ready());
            });
        });
    }

    #[test]
    fn poll_wait_post_mutex_try_wait_branch() {
        // Covers the post-mutex `try_wait()` → Ready branch. Race: a
        // concurrent `set()` stores SIGNALED via its fast path between
        // our post-mutex `take_notification()` check and the post-mutex
        // signal re-check.
        testing::with_watchdog(|| {
            let BarrierHook {
                entered,
                proceed,
                hook,
            } = crate::test_hooks::barrier_hook();
            crate::test_hooks::with_hook(&crate::test_hooks::AUTO_PRE_TRY_WAIT, hook, || {
                let event = AutoResetEvent::boxed();

                let producer = thread::spawn({
                    let event = event.clone();
                    move || {
                        crate::test_hooks::HOOK_PARTICIPANT.with(|c| c.set(true));
                        let mut future = Box::pin(event.wait());
                        let waker = Waker::noop();
                        let mut cx = task::Context::from_waker(waker);
                        future.as_mut().poll(&mut cx)
                    }
                });

                entered.wait();
                event.set();
                proceed.wait();

                assert!(producer.join().unwrap().is_ready());
                // The signal was consumed by the producer.
                assert!(!event.try_wait());
            });
        });
    }

    #[test]
    fn poll_wait_post_fetch_or_try_wait_branch() {
        // Covers the post-`fetch_or(HAS_WAITERS)` `try_wait()` → Ready
        // branch. Regression coverage for the previously-fixed CAS bug.
        // Race: a concurrent `set()` stores SIGNALED via its fast path
        // between our post-mutex `try_wait()` check and the `fetch_or`.
        testing::with_watchdog(|| {
            let BarrierHook {
                entered,
                proceed,
                hook,
            } = crate::test_hooks::barrier_hook();
            crate::test_hooks::with_hook(&crate::test_hooks::AUTO_PRE_FETCH_OR, hook, || {
                let event = AutoResetEvent::boxed();

                let producer = thread::spawn({
                    let event = event.clone();
                    move || {
                        crate::test_hooks::HOOK_PARTICIPANT.with(|c| c.set(true));
                        let mut future = Box::pin(event.wait());
                        let waker = Waker::noop();
                        let mut cx = task::Context::from_waker(waker);
                        future.as_mut().poll(&mut cx)
                    }
                });

                entered.wait();
                event.set();
                proceed.wait();

                assert!(producer.join().unwrap().is_ready());
                assert!(!event.try_wait());
            });
        });
    }

    #[test]
    fn set_no_waiters_despite_has_waiters_branch() {
        // Covers `set()`'s "no waiters despite HAS_WAITERS — store SIGNALED"
        // else branch. Race: between `set()`'s state-load (which sees
        // HAS_WAITERS) and its mutex acquisition, a concurrent
        // `drop_wait` drains the awaiter set and clears HAS_WAITERS.
        testing::with_watchdog(|| {
            let BarrierHook {
                entered,
                proceed,
                hook,
            } = crate::test_hooks::barrier_hook();
            crate::test_hooks::with_hook(&crate::test_hooks::AUTO_SET_PRE_LOCK, hook, || {
                let event = AutoResetEvent::boxed();

                // Register an awaiter on the test thread. After this
                // poll the state has HAS_WAITERS set.
                let mut future = Box::pin(event.wait());
                let waker = Waker::noop();
                let mut cx = task::Context::from_waker(waker);
                assert!(future.as_mut().poll(&mut cx).is_pending());

                // Producer thread calls `set()` and pauses at the hook
                // after observing HAS_WAITERS but before locking the
                // slow-path mutex.
                let producer = thread::spawn({
                    let event = event.clone();
                    move || {
                        crate::test_hooks::HOOK_PARTICIPANT.with(|c| c.set(true));
                        event.set();
                    }
                });

                entered.wait();
                // Drop the registered future. `drop_wait` acquires the
                // mutex, removes the awaiter, and clears HAS_WAITERS.
                drop(future);
                proceed.wait();

                producer.join().unwrap();

                // `set()` took the else branch and stored SIGNALED even
                // though it found no waiters.
                assert!(event.try_wait());
            });
        });
    }

    #[test]
    fn await_races_with_set_across_threads() {
        // Regression test for a race where poll_wait() observed
        // HAS_WAITERS|SIGNALED state after fetch_or, but the CAS-based
        // try_wait failed because it required exact match on SIGNALED.
        // Many awaiters waited forever despite set() running. Each
        // iteration creates a real future and awaits it while a
        // separate thread calls set() concurrently.
        testing::with_watchdog(|| {
            // Miri explores interleavings across seeds; a few rounds still exercise reuse on
            // real competing threads without repeating the native stress workload.
            const ITERATIONS: usize = if cfg!(miri) { 4 } else { 200 };

            let event = AutoResetEvent::boxed();

            for _ in 0..ITERATIONS {
                let barrier = Arc::new(Barrier::new(2));

                let setter_handle = thread::spawn({
                    let event = event.clone();
                    let barrier = Arc::clone(&barrier);
                    move || {
                        barrier.wait();
                        event.set();
                    }
                });

                let waiter_handle = thread::spawn({
                    let event = event.clone();
                    let barrier = Arc::clone(&barrier);
                    move || {
                        barrier.wait();
                        futures::executor::block_on(event.wait());
                    }
                });

                setter_handle.join().unwrap();
                waiter_handle.join().unwrap();
            }
        });
    }

    #[test]
    fn embedded_set_from_another_thread() {
        testing::with_watchdog(|| {
            let container = Box::pin(EmbeddedAutoResetEvent::new());
            // SAFETY: The container outlives all handles in this test.
            let event = unsafe { AutoResetEvent::embedded(container.as_ref()) };
            let setter = event;
            let barrier = Arc::new(Barrier::new(2));
            let b2 = Arc::clone(&barrier);

            let handle = thread::spawn(move || {
                b2.wait();
                setter.set();
            });

            barrier.wait();

            while !event.try_wait() {
                std::hint::spin_loop();
            }

            handle.join().unwrap();
        });
    }

    #[test]
    fn embedded_set_and_wait() {
        futures::executor::block_on(async {
            let container = Box::pin(EmbeddedAutoResetEvent::new());
            // SAFETY: The container outlives the handle within this test.
            let event = unsafe { AutoResetEvent::embedded(container.as_ref()) };

            event.set();
            event.wait().await;
        });
    }

    #[test]
    fn embedded_clone_shares_state() {
        let container = Box::pin(EmbeddedAutoResetEvent::new());
        // SAFETY: The container outlives the handle within this test.
        let a = unsafe { AutoResetEvent::embedded(container.as_ref()) };
        let b = a;

        a.set();
        assert!(b.try_wait());
    }

    #[test]
    fn embedded_signal_consumed() {
        let container = Box::pin(EmbeddedAutoResetEvent::new());
        // SAFETY: The container outlives the handle within this test.
        let event = unsafe { AutoResetEvent::embedded(container.as_ref()) };

        event.set();
        assert!(event.try_wait());
        // Signal was consumed.
        assert!(!event.try_wait());
    }

    #[test]
    fn embedded_drop_future_while_waiting() {
        futures::executor::block_on(async {
            let container = Box::pin(EmbeddedAutoResetEvent::new());
            // SAFETY: The container outlives the handle within this test.
            let event = unsafe { AutoResetEvent::embedded(container.as_ref()) };

            {
                let _future = event.wait();
            }
            event.set();
            event.wait().await;
        });
    }

    #[test]
    fn notified_then_dropped_re_sets_event() {
        let event = AutoResetEvent::boxed();
        let mut future = Box::pin(event.wait());
        let waker = Waker::noop();
        let mut cx = task::Context::from_waker(waker);

        // Poll to register.
        assert!(future.as_mut().poll(&mut cx).is_pending());

        // set() pops the waiter and marks it notified.
        event.set();

        // Drop the notified future without re-polling. No other waiters
        // exist, so Drop must re-set the event.
        drop(future);

        assert!(event.try_wait());
    }

    #[test]
    fn notified_then_dropped_while_set_preserves_signal() {
        let event = AutoResetEvent::boxed();
        let mut future = Box::pin(event.wait());
        let waker = Waker::noop();
        let mut cx = task::Context::from_waker(waker);

        // Poll to register.
        assert!(future.as_mut().poll(&mut cx).is_pending());

        // First set() pops the waiter and marks it notified.
        event.set();

        // Second set() transitions the event back to Set (no
        // waiters remain in the set).
        event.set();

        // Drop the notified future. The state is already Set, so
        // drop_wait must preserve the signal.
        drop(future);

        assert!(event.try_wait());
    }

    #[test]
    fn notified_then_dropped_forwards_to_next() {
        let event = AutoResetEvent::boxed();
        let mut future1 = Box::pin(event.wait());
        let mut future2 = Box::pin(event.wait());
        let waker = Waker::noop();
        let mut cx = task::Context::from_waker(waker);

        // Both register.
        assert!(future1.as_mut().poll(&mut cx).is_pending());
        assert!(future2.as_mut().poll(&mut cx).is_pending());

        // set() notifies the first registered future.
        event.set();

        // Drop future1 without re-polling — notification should forward
        // to future2.
        drop(future1);

        // future2 should now be notified.
        assert!(future2.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn set_wakes_registered_waiter() {
        use crate::test_helpers::AtomicWakeTracker;

        let event = AutoResetEvent::boxed();

        let tracker = AtomicWakeTracker::new();
        // SAFETY: The tracker outlives the waker.
        let waker = unsafe { tracker.waker() };
        let mut cx = task::Context::from_waker(&waker);

        let mut future = Box::pin(event.wait());
        assert!(future.as_mut().poll(&mut cx).is_pending());

        event.set();

        assert!(tracker.was_woken());
    }

    #[test]
    fn embedded_wait_registers_then_completes() {
        let container = Box::pin(EmbeddedAutoResetEvent::new());
        // SAFETY: The container outlives the handle within this test.
        let event = unsafe { AutoResetEvent::embedded(container.as_ref()) };

        let mut future = Box::pin(event.wait());
        let waker = Waker::noop();
        let mut cx = task::Context::from_waker(waker);

        // First poll — not set, registers in awaiter set.
        assert!(future.as_mut().poll(&mut cx).is_pending());

        // set() pops and notifies the waiter.
        event.set();

        // Second poll — sees notified flag, returns Ready.
        assert!(future.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn embedded_drop_registered_future() {
        let container = Box::pin(EmbeddedAutoResetEvent::new());
        // SAFETY: The container outlives the handle within this test.
        let event = unsafe { AutoResetEvent::embedded(container.as_ref()) };

        let mut future = Box::pin(event.wait());
        let waker = Waker::noop();
        let mut cx = task::Context::from_waker(waker);

        assert!(future.as_mut().poll(&mut cx).is_pending());

        // Drop a registered (not notified) future.
        drop(future);

        // Event should still work.
        event.set();
        assert!(event.try_wait());
    }

    #[test]
    fn embedded_notified_then_dropped_re_sets_event() {
        let container = Box::pin(EmbeddedAutoResetEvent::new());
        // SAFETY: The container outlives the handle within this test.
        let event = unsafe { AutoResetEvent::embedded(container.as_ref()) };

        let mut future = Box::pin(event.wait());
        let waker = Waker::noop();
        let mut cx = task::Context::from_waker(waker);

        assert!(future.as_mut().poll(&mut cx).is_pending());
        event.set();
        drop(future);

        // Signal should be preserved.
        assert!(event.try_wait());
    }

    #[test]
    fn embedded_notified_then_dropped_forwards_to_next() {
        let container = Box::pin(EmbeddedAutoResetEvent::new());
        // SAFETY: The container outlives the handle within this test.
        let event = unsafe { AutoResetEvent::embedded(container.as_ref()) };

        let mut future1 = Box::pin(event.wait());
        let mut future2 = Box::pin(event.wait());
        let waker = Waker::noop();
        let mut cx = task::Context::from_waker(waker);

        assert!(future1.as_mut().poll(&mut cx).is_pending());
        assert!(future2.as_mut().poll(&mut cx).is_pending());

        event.set();
        drop(future1);

        assert!(future2.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn embedded_set_wakes_registered_waiter() {
        use crate::test_helpers::AtomicWakeTracker;

        let container = Box::pin(EmbeddedAutoResetEvent::new());
        // SAFETY: The container outlives the handle.
        let event = unsafe { AutoResetEvent::embedded(container.as_ref()) };

        let tracker = AtomicWakeTracker::new();
        // SAFETY: The tracker outlives the waker.
        let waker = unsafe { tracker.waker() };
        let mut cx = task::Context::from_waker(&waker);

        let mut future = Box::pin(event.wait());
        assert!(future.as_mut().poll(&mut cx).is_pending());

        event.set();

        assert!(tracker.was_woken());
    }

    // This tests a defense-in-depth branch in poll() that unregisters
    // a waiter when is_set is observed while still registered. In normal
    // usage, set() pops a waiter rather than setting is_set when the set
    // is non-empty, so this state cannot arise through the public API.
    // We force it by directly manipulating the guarded state.
    //
    // NOTE: These tests were removed because the enum-based State type
    // makes the "is_set + waiters" combination structurally impossible.

    const WAITER_COUNT: usize = 100;

    #[test]
    fn many_sets_release_all_waiters() {
        let event = AutoResetEvent::boxed();
        let waker = Waker::noop();
        let mut cx = task::Context::from_waker(waker);

        let mut futures: Vec<_> = iter::repeat_with(|| Box::pin(event.wait()))
            .take(WAITER_COUNT)
            .collect();

        // Register all waiters.
        for f in &mut futures {
            assert!(f.as_mut().poll(&mut cx).is_pending());
        }

        // Signal once for each waiter.
        for _ in 0..WAITER_COUNT {
            event.set();
        }

        // All waiters should now be ready (order is unspecified).
        for f in &mut futures {
            assert!(f.as_mut().poll(&mut cx).is_ready());
        }

        // No leftover signal.
        assert!(!event.try_wait());
    }

    #[test]
    fn embedded_many_sets_release_all_waiters() {
        let container = Box::pin(EmbeddedAutoResetEvent::new());
        // SAFETY: The container outlives the handle within this test.
        let event = unsafe { AutoResetEvent::embedded(container.as_ref()) };
        let waker = Waker::noop();
        let mut cx = task::Context::from_waker(waker);

        let mut futures: Vec<_> = iter::repeat_with(|| Box::pin(event.wait()))
            .take(WAITER_COUNT)
            .collect();

        for f in &mut futures {
            assert!(f.as_mut().poll(&mut cx).is_pending());
        }

        for _ in 0..WAITER_COUNT {
            event.set();
        }

        for f in &mut futures {
            assert!(f.as_mut().poll(&mut cx).is_ready());
        }

        assert!(!event.try_wait());
    }

    #[test]
    fn many_sets_without_waiters_coalesce() {
        let event = AutoResetEvent::boxed();

        for _ in 0..WAITER_COUNT {
            event.set();
        }

        // Only one signal should be latched.
        assert!(event.try_wait());
        assert!(!event.try_wait());
    }

    #[test]
    fn set_with_reentrant_waker_does_not_deadlock() {
        use testing::ReentrantWakerData;

        let event = AutoResetEvent::boxed();
        let event_for_waker = event.clone();

        let waker_data = ReentrantWakerData::new(move || {
            // Reentrantly call set() on the same event.
            event_for_waker.set();
        });
        // SAFETY: Data outlives waker, test is single-threaded.
        let waker = unsafe { waker_data.waker() };
        let mut cx = task::Context::from_waker(&waker);

        let mut future = Box::pin(event.wait());
        assert!(future.as_mut().poll(&mut cx).is_pending());

        // set() notifies the future, calling the reentrant waker
        // which calls set() again. The second set() should store
        // the signal (no waiters left).
        event.set();

        assert!(waker_data.was_woken());
        // The reentrant set() stored a signal.
        assert!(event.try_wait());
    }

    #[test]
    fn drop_forwarding_with_reentrant_waker_does_not_alias() {
        use testing::ReentrantWakerData;

        // Mirrors `LocalAutoResetEvent::drop_forwarding_with_reentrant_waker_does_not_alias`.
        // When future1 is dropped after being notified, it must hand
        // the signal off to future2 by calling notify_one on the
        // awaiter set. The mutex protecting the set must be released
        // before the reentrant waker fires.
        let event = AutoResetEvent::boxed();
        let event_clone = event.clone();

        let mut future1 = Box::pin(event.wait());
        let noop_waker = Waker::noop();
        let mut noop_cx = task::Context::from_waker(noop_waker);
        assert!(future1.as_mut().poll(&mut noop_cx).is_pending());

        let waker_data = ReentrantWakerData::new(move || {
            event_clone.set();
        });
        // SAFETY: Data outlives waker, single-threaded test.
        let waker = unsafe { waker_data.waker() };
        let mut reentrant_cx = task::Context::from_waker(&waker);
        let mut future2 = Box::pin(event.wait());
        assert!(future2.as_mut().poll(&mut reentrant_cx).is_pending());

        // set() notifies future1 (noop waker, harmless).
        event.set();

        // Drop future1 — it was notified, so it forwards to future2,
        // calling the reentrant waker which accesses the awaiter set
        // again.
        drop(future1);

        assert!(waker_data.was_woken());
    }

    #[test]
    fn embedded_default_creates_unset_event() {
        let container = Box::pin(EmbeddedAutoResetEvent::default());
        // SAFETY: The container outlives the handle.
        let event = unsafe { AutoResetEvent::embedded(container.as_ref()) };
        assert!(!event.try_wait());
    }
}
