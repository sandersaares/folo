use negative_impl::negative_impl;
use std::{
    cell::UnsafeCell,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::{RawWaker, RawWakerVTable, Waker},
};

// TODO: Double-check the atomics ordering here with the help of someone smarter.
// A good stress test suite would not hurt, either!

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
/// The signal must be pinned at all times.
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
    /// Counts each waker we have created (both the initial one and any clones). The instance cannot
    /// be dropped until the clones are all gone because each clone holds a self-reference to the
    /// wake signal.
    ///
    /// This seems independent from any other memory operations, so we use Relaxed ordering.
    waker_count: AtomicUsize,

    // Release ordering when setting, acquire ordering when consuming - we are passing a flag
    // and expect memory writes before passing the flag to be synchronized.
    awakened: AtomicBool,

    // The real waker that we construct on first use. We hand out references to this.
    // TODO: Unsure if there is any point in lazy-initializing this. Very cheap! Laziness is legacy
    // from older more expensive implementation.
    waker: UnsafeCell<Option<Waker>>,
}

impl WakeSignal {
    pub(crate) fn new() -> Self {
        Self {
            waker_count: AtomicUsize::new(0),
            awakened: AtomicBool::new(false),
            waker: UnsafeCell::new(None),
        }
    }

    /// Returns whether the signal has received a wake-up notification. If so, resets the signal
    /// to a not awakened state.
    pub(crate) fn consume_awakened(self: Pin<&Self>) -> bool {
        // We acquire the awakened flag here and expect to ensure we see all memory operations
        // that occurred before the release of the awakened flag.
        self.awakened.swap(false, Ordering::Acquire)
    }

    /// Returns whether the signal is inert, meaning that no wakers are currently active and it is
    /// safe to drop the signal.
    pub(crate) fn is_inert(self: Pin<&Self>) -> bool {
        // We consider the reference count of 1 as inert. A count of 1 means that the waker
        // has been initialized but has not been cloned, so it is safe to say that nobody else is
        // using it (because the signal itself is single threaded - the owner thread can either be
        // in here or be using the waker but not both).
        self.waker_count.load(Ordering::Relaxed) <= 1
    }

    pub(crate) fn waker(self: Pin<&Self>) -> &Waker {
        // SAFETY: It is always fine to create &mut in unsafe code as long as we don't return it.
        // We are not returning it, so this is fine - mutating in unsafe code is allowed regardless
        // of shared references existing.
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

    fn create_waker(self: Pin<&Self>) -> Waker {
        self.waker_count.fetch_add(1, Ordering::Relaxed);

        // SAFETY: Aliasing rules require that any transformation into a mutably used raw pointer
        // (which we are - the waker will mutate the signal by awakening it) must be done by
        // starting from a regular `&mut` exclusive reference. We have that here in `&mut self`.
        //
        // We also need to consider lifecycle logic, as the waker may be passed to foreign threads.
        // See type level comments for info on how we deal with foreign threads dropping wakers at
        // unspecified moments in time.
        unsafe {
            let signal_ptr = Pin::into_inner_unchecked(self) as *const _ as *const ();
            Waker::from_raw(RawWaker::new(signal_ptr, &VTABLE))
        }
    }
}

impl Drop for WakeSignal {
    fn drop(&mut self) {
        debug_assert!(Pin::new(self as &Self).is_inert());
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

    // We release the awakened flag here, which means when someone acquires it
    // they will see all the memory operations that happened up to this point.
    signal.awakened.store(true, Ordering::Release);

    // This consumes the waker!
    signal.waker_count.fetch_sub(1, Ordering::Relaxed);
}

fn waker_wake_by_ref(ptr: *const ()) {
    let signal = unsafe { resurrect_signal_ptr(ptr) };

    // We release the awakened flag here, which means when someone acquires it
    // they will see all the memory operations that happened up to this point.
    signal.awakened.store(true, Ordering::Release);
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
    fn smoke_test() {
        let signal = WakeSignal::new();
        let signal = Pin::new(&signal);

        let waker = signal.waker();
        let waker_clone = waker.clone();

        assert_eq!(signal.waker_count.load(Ordering::Relaxed), 2);

        assert!(!signal.consume_awakened());

        waker.wake_by_ref();
        assert!(signal.consume_awakened());

        drop(waker_clone);
        assert_eq!(signal.waker_count.load(Ordering::Relaxed), 1);

        assert!(signal.is_inert());
    }
}
