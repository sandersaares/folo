use negative_impl::negative_impl;
use std::{
    cell::OnceCell,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::{RawWaker, RawWakerVTable, Waker},
};

// TODO: Double-check the atomics ordering here with the help of someone smarter.
// A good stress test suite would not hurt, either!

/// A wake signal intended to be allocated inline as part of the task structure that is woken up.
/// This implies it is unsafe and self-referential, and must be used with care!
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
/// The signal must be pinned at all times. It does not require a `Pin<T>` for this, allowing you to
/// accomplish the pinning using unsafe code not compatible with `Pin<T>`. To be clear, all usage
/// to this type after `::new()` requires operating on a pinned reference, even if not `Pin<T>`.
///
/// Before you can destroy the signal, you must first ensure that the signal has become inert by
/// calling `is_inert()`. If the signal is not inert, you must keep it alive and pinned and try
/// again later - some task is still keeping references to its wakers if it is not yet inert.
///
/// # Safety
///
/// The signal must be pinned at all times.
///
/// The signal must not be dropped until it has become inert.
///
/// # Thread safety
///
/// The type itself is single-threaded, although the `std::task::Waker` obtained from it are thread-
/// safe as required by the Waker API contract.
#[derive(Debug)]
pub(crate) struct WakeSignal {
    /// Counts each active transaction (for completeness) and clone of the waker, ensuring that we
    /// do not make it inert while any usage remains.
    ///
    /// It is safe to clean up the type at both 0 and 1 references, since we only ever allow cleanup
    /// (or indeed, any use of this type) from the same thread that owns it, so the caller cannot be
    /// holding a waker reference while dropping the type, as that would violate the Rust lifetime
    /// checks. The 0 case is for the situation where the waker is never created in the first place.
    ///
    /// This seems independent from any other memory operations, so we use Relaxed ordering.
    reference_count: AtomicUsize,

    // Release ordering when setting, acquire ordering when consuming - we are passing a flag
    // and expect memory writes before passing the flag to be synchronized.
    awakened: AtomicBool,

    // The real waker that we construct on first use. We hand out references to this.
    real: OnceCell<Waker>,
}

impl WakeSignal {
    pub(crate) fn new() -> Self {
        Self {
            reference_count: AtomicUsize::new(0),
            awakened: AtomicBool::new(false),
            real: OnceCell::new(),
        }
    }

    /// Returns whether the signal has received a wake-up notification. If so, resets the signal
    /// to a not awakened state.
    ///
    /// # Safety
    ///
    /// Requires `&self` to be pinned.
    pub(crate) unsafe fn consume_awakened(&self) -> bool {
        // We acquire the awakened flag here and expect to see all memory operations
        // that occurred before the release of the awakened flag.
        self.awakened.swap(false, Ordering::Acquire)
    }

    /// Returns whether the signal is inert, meaning that no wakers are currently active and it is
    /// safe to drop.
    ///
    /// # Safety
    ///
    /// Requires `&self` to be pinned.
    pub(crate) unsafe fn is_inert(&self) -> bool {
        self.reference_count.load(Ordering::Relaxed) < 2
    }

    /// # Safety
    ///
    /// Requires `&self` to be pinned.
    pub(crate) unsafe fn waker(&self) -> &Waker {
        self.real.get_or_init(|| self.create_waker())
    }

    unsafe fn create_waker(&self) -> Waker {
        self.reference_count.fetch_add(1, Ordering::Relaxed);

        let ptr = self as *const _ as *const ();
        Waker::from_raw(RawWaker::new(ptr, &VTABLE))
    }
}

impl Drop for WakeSignal {
    fn drop(&mut self) {
        // SAFETY: We are only dropping the type when it is inert, which means that no wakers are
        // active and no references are held to the waker. This must be guaranteed by the caller.
        unsafe {
            debug_assert!(self.is_inert());
        }
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
    signal.reference_count.fetch_add(1, Ordering::Relaxed);

    RawWaker::new(ptr, &VTABLE)
}

fn waker_wake(ptr: *const ()) {
    let signal = unsafe { resurrect_signal_ptr(ptr) };

    // We release the awakened flag here, which means when someone acquires it
    // they will see all the memory operations that happened up to this point.
    signal.awakened.store(true, Ordering::Release);

    // This consumes the waker!
    signal.reference_count.fetch_sub(1, Ordering::Relaxed);
}

fn waker_wake_by_ref(ptr: *const ()) {
    let signal = unsafe { resurrect_signal_ptr(ptr) };

    // We release the awakened flag here, which means when someone acquires it
    // they will see all the memory operations that happened up to this point.
    signal.awakened.store(true, Ordering::Release);
}

fn waker_drop_waker(ptr: *const ()) {
    let signal = unsafe { resurrect_signal_ptr(ptr) };

    signal.reference_count.fetch_sub(1, Ordering::Relaxed);
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

        let waker = unsafe { signal.waker() };
        let waker_clone = waker.clone();

        assert_eq!(signal.reference_count.load(Ordering::Relaxed), 2);

        assert!(!unsafe { signal.consume_awakened() });

        waker.wake_by_ref();
        assert!(unsafe { signal.consume_awakened() });

        drop(waker_clone);
        assert_eq!(signal.reference_count.load(Ordering::Relaxed), 1);

        assert!(unsafe { signal.is_inert() });
    }
}
