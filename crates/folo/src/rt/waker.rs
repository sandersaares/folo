use crate::rt::async_task_engine::Task;
use negative_impl::negative_impl;
use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{RawWaker, RawWakerVTable, Waker},
};

/// A wake signal intended to be allocated inline as part of the task structure that is woken up.
///
/// # Usage
///
/// This provides access to wakers and indicates whether they have sent a wake-up signal.
///
/// ```ignore
/// let mut context = Context::from_waker(self.wake_signal.waker());
/// self.something.poll(&mut context);
///
/// let awakened = self.wake_signal.consume_awakened();
/// ```
///
/// # Safety
///
/// The signal must be pinned at all times after calling `waker()`.
///
/// This type is self-referential and should be used with care - do not deallocate it until it gives
/// you permission via `is_inert()`.
///
/// # Thread safety
///
/// The type itself is single-threaded, although the `std::task::Waker` obtained from it are thread-
/// safe as required by the Waker API contract.
#[derive(Debug)]
pub(crate) struct WakeSignal {
    // The task that we are waking up. We will insert this pointer into a list of awakened tasks.
    task_ptr: *mut Task,

    // The queue of tasks that have been awakened by a signal. If we can lock the mutex without
    // blocking and if there is room in the queue, we add our task. Otherwise, we update
    // the signal itself and set the "probe signals to find awakened ones" flag.
    awakened_queue: Arc<Mutex<VecDeque<*mut Task>>>,

    // If we cannot add the task to the set, we set this flag to inform the task engine that it
    // needs to read each signal to identify what has woken up.
    probe_embedded_wake_signals: Arc<AtomicBool>,

    /// Counts each waker we have created (both the initial one and any clones). The instance cannot
    /// be dropped until the clones are all gone because each clone holds a self-reference to the
    /// wake signal.
    ///
    /// This seems independent from any other memory operations, so we use Relaxed ordering.
    waker_count: AtomicUsize,

    /// Release ordering when setting, acquire ordering when consuming - we are passing a flag
    /// and expect memory writes before passing the flag to be synchronized.
    awakened: AtomicBool,

    /// The real waker that we construct on first use. We hand out references to this.
    /// This is self-referential and we need to initialize it lazily once we are pinned.
    /// Potentially there may be a way to not use UnsafeCell here but I could not convince the
    /// borrow checker that I was not doing anything wrong without it.
    waker: UnsafeCell<Option<Waker>>,

    // This type cannot be unpinned once it has been pinned (latest when calling `waker()`).
    _phantom_pinned: std::marker::PhantomPinned,
}

impl WakeSignal {
    pub(crate) fn new(
        awakened_queue: Arc<Mutex<VecDeque<*mut Task>>>,
        probe_embedded_wake_signals: Arc<AtomicBool>,
    ) -> Self {
        Self {
            task_ptr: std::ptr::null_mut(),
            awakened_queue,
            probe_embedded_wake_signals,
            waker_count: AtomicUsize::new(0),
            awakened: AtomicBool::new(false),
            waker: UnsafeCell::new(None),
            _phantom_pinned: std::marker::PhantomPinned,
        }
    }

    /// This is self-referential (the wake signal is part of the task), so needs to be
    /// lazy-initialized after the ctor.
    pub(crate) fn set_task_ptr(&mut self, task_ptr: *mut Task) {
        self.task_ptr = task_ptr;
    }

    /// Returns whether the signal has received a wake-up notification. If so, resets the signal
    /// to a not awakened state.
    pub(crate) fn consume_awakened(&self) -> bool {
        // Most of the time, the flag will be false so we at first probe it with Relaxed ordering.
        // If it is false, we can return early. If it is true, we need to ensure that we see all
        // memory operations that happened before the flag was set, so we use Acquire ordering and
        // load it one more time.
        self.awakened.load(Ordering::Relaxed) && self.awakened.swap(false, Ordering::Acquire)
    }

    /// Returns whether the signal is inert, meaning that no wakers are currently active and it is
    /// safe to drop the signal.
    pub(crate) fn is_inert(&self) -> bool {
        // We consider the reference count of 1 as inert. A count of 1 means that the waker
        // has been initialized but has not been cloned, so it is safe to say that nobody else is
        // using it (because the signal itself is single threaded - the owner thread can either be
        // in here or be using the waker but not both).
        self.waker_count.load(Ordering::Relaxed) <= 1
    }

    /// # Safety
    ///
    /// Once the waker has been used, the signal comes out of the inert state and is not valid to
    /// drop until it is inert again. You must check `is_inert()` before dropping the signal.
    pub(crate) unsafe fn waker(self: Pin<&Self>) -> &Waker {
        // SAFETY: It is fine to create this &mut in unsafe code as long as we don't return it.
        unsafe {
            let maybe_waker: &mut Option<Waker> = &mut *self.waker.get();

            if maybe_waker.is_none() {
                *maybe_waker = Some(self.create_waker());
            }

            (*maybe_waker)
                .as_ref()
                .expect("we just created the waker - the value must exist")
        }
    }

    unsafe fn create_waker(self: Pin<&Self>) -> Waker {
        self.waker_count.fetch_add(1, Ordering::Relaxed);

        // SAFETY: The raw pointer is used as an equivalent to a shared reference because all the
        // mutation happens via atomics, which do not require exclusive references. For lifecycle
        // considerations, see comment on type.
        unsafe {
            let signal_ptr = Pin::into_inner_unchecked(self) as *const _ as *const ();
            Waker::from_raw(RawWaker::new(signal_ptr, &VTABLE))
        }
    }

    fn wake(&self) {
        if let Ok(mut awakened_set) = self.awakened_queue.try_lock() {
            // We only add if we can do so without increasing capacity, because increasing capacity
            // from an arbitrary thread may require reallocation, which we do not want to do on a
            // different thread than the one that owns the set.
            if awakened_set.len() < awakened_set.capacity() {
                // If we experienced spurious awakenings, we might push the same task multiple
                // times. That is fine - it is up to the receiver of the notifications to deal
                // with spurious notifications (which may arrive anyway through other means).
                awakened_set.push_back(self.task_ptr);
                return;
            }
        }

        // We release the awakened flag here, which means when someone acquires it
        // they will see all the memory operations that happened up to this point.
        self.awakened.store(true, Ordering::Release);

        self.probe_embedded_wake_signals
            .store(true, Ordering::Release);
    }
}

impl Drop for WakeSignal {
    fn drop(&mut self) {
        debug_assert!(self.is_inert());
    }
}

#[negative_impl]
impl !Send for WakeSignal {}
#[negative_impl]
impl !Sync for WakeSignal {}

static VTABLE: RawWakerVTable = RawWakerVTable::new(
    waker_clone_waker,
    waker_wake,
    waker_wake_by_ref,
    waker_drop_waker,
);

fn waker_clone_waker(ptr: *const ()) -> RawWaker {
    let signal = unsafe { resurrect_signal_ptr(ptr) };

    // Cloning just increments the ref count, that's all. There is no "object" for the waker.
    signal.waker_count.fetch_add(1, Ordering::Relaxed);

    RawWaker::new(ptr, &VTABLE)
}

fn waker_wake(ptr: *const ()) {
    let signal = unsafe { resurrect_signal_ptr(ptr) };

    signal.wake();

    // This consumes the waker!
    signal.waker_count.fetch_sub(1, Ordering::Relaxed);
}

fn waker_wake_by_ref(ptr: *const ()) {
    let signal = unsafe { resurrect_signal_ptr(ptr) };

    signal.wake();
}

fn waker_drop_waker(ptr: *const ()) {
    let signal = unsafe { resurrect_signal_ptr(ptr) };

    signal.waker_count.fetch_sub(1, Ordering::Relaxed);
}

unsafe fn resurrect_signal_ptr(ptr: *const ()) -> &'static WakeSignal {
    &*(ptr as *const WakeSignal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn awaken_via_embedded_signal() {
        #[allow(clippy::arc_with_non_send_sync)] // False positive? Or needs more annotations in type layers?
        let awakened_queue = Arc::new(Mutex::new(VecDeque::with_capacity(10)));
        let probe_embedded_wake_signals = Arc::new(AtomicBool::new(false));

        // We hold the lock - the signal cannot use the set.
        let _awakened_set_lock_guard = awakened_queue.lock().unwrap();

        let signal = WakeSignal::new(
            Arc::clone(&awakened_queue),
            Arc::clone(&probe_embedded_wake_signals),
        );
        let signal = unsafe { Pin::new_unchecked(&signal) };

        let waker = unsafe { signal.waker() };

        // It is still counted as inert here because the first waker can only be used by the same
        // thread, so if the thread is cleaning up it must not be in use anymore.
        assert!(signal.is_inert());

        let waker_clone = waker.clone();

        // Now we have a second waker, which might have been given to some other thread, so may
        // be called at any time - we are no longer inert.
        assert!(!signal.is_inert());

        assert_eq!(signal.waker_count.load(Ordering::Relaxed), 2);

        assert!(!signal.consume_awakened());

        waker.wake_by_ref();
        assert!(probe_embedded_wake_signals.load(Ordering::Relaxed));
        assert!(signal.consume_awakened());

        assert!(!signal.is_inert());

        // Once we drop the clone, we are again inert because only the original remains.
        drop(waker_clone);

        assert_eq!(signal.waker_count.load(Ordering::Relaxed), 1);
        assert!(signal.is_inert());
    }

    #[test]
    fn awaken_via_awakened_set() {
        #[allow(clippy::arc_with_non_send_sync)] // False positive? Or needs more annotations in type layers?
        let awakened_queue = Arc::new(Mutex::new(VecDeque::with_capacity(10)));
        let probe_embedded_wake_signals = Arc::new(AtomicBool::new(false));

        let signal = WakeSignal::new(
            Arc::clone(&awakened_queue),
            Arc::clone(&probe_embedded_wake_signals),
        );
        let signal = unsafe { Pin::new_unchecked(&signal) };

        let waker = unsafe { signal.waker() };

        // It is still counted as inert here because the first waker can only be used by the same
        // thread, so if the thread is cleaning up it must not be in use anymore.
        assert!(signal.is_inert());

        let waker_clone = waker.clone();

        // Now we have a second waker, which might have been given to some other thread, so may
        // be called at any time - we are no longer inert.
        assert!(!signal.is_inert());

        assert_eq!(signal.waker_count.load(Ordering::Relaxed), 2);

        assert!(!signal.consume_awakened());

        waker.wake_by_ref();
        // It should not have set the embedded signal here because we use the awakened set.
        assert!(!probe_embedded_wake_signals.load(Ordering::Relaxed));
        assert!(!signal.consume_awakened());
        assert!(!awakened_queue.lock().unwrap().is_empty());

        assert!(!signal.is_inert());

        // Once we drop the clone, we are again inert because only the original remains.
        drop(waker_clone);

        assert_eq!(signal.waker_count.load(Ordering::Relaxed), 1);
        assert!(signal.is_inert());
    }

    #[test]
    fn awaken_via_full_awakened_set() {
        // Capacity is 0 so the queue is not allowed to allocate (== is never used).
        #[allow(clippy::arc_with_non_send_sync)] // False positive? Or needs more annotations in type layers?
        let awakened_queue = Arc::new(Mutex::new(VecDeque::with_capacity(0)));
        let probe_embedded_wake_signals = Arc::new(AtomicBool::new(false));

        let signal = WakeSignal::new(
            Arc::clone(&awakened_queue),
            Arc::clone(&probe_embedded_wake_signals),
        );
        let signal = unsafe { Pin::new_unchecked(&signal) };

        let waker = unsafe { signal.waker() };

        // It is still counted as inert here because the first waker can only be used by the same
        // thread, so if the thread is cleaning up it must not be in use anymore.
        assert!(signal.is_inert());

        let waker_clone = waker.clone();

        // Now we have a second waker, which might have been given to some other thread, so may
        // be called at any time - we are no longer inert.
        assert!(!signal.is_inert());

        assert_eq!(signal.waker_count.load(Ordering::Relaxed), 2);

        assert!(!signal.consume_awakened());

        waker.wake_by_ref();
        // Even though it could lock the set, it could not use it because it was at capacity.
        assert!(probe_embedded_wake_signals.load(Ordering::Relaxed));
        assert!(signal.consume_awakened());

        assert!(!signal.is_inert());

        // Once we drop the clone, we are again inert because only the original remains.
        drop(waker_clone);

        assert_eq!(signal.waker_count.load(Ordering::Relaxed), 1);
        assert!(signal.is_inert());
    }
}
