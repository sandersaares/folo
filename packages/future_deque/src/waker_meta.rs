use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{RawWaker, RawWakerVTable, Waker};

use plurality::Pool;

// Per-slot metadata for activation tracking and waker management.
//
// Allocated in a thread-local pool that hands out stable addresses. The RawWaker data
// pointer points directly at the pooled value, so wakers need no heap allocation of their
// own.
//
// Each WakerMeta is reference-counted: one reference for the owning Slot, plus one per
// outstanding waker clone. The pointer inside `MetaPtr` owns the pool slot; when the
// refcount reaches zero the owning handle is reconstructed from that pointer and dropped,
// returning the slot to the pool for reuse.
//
// The atomic `ref_count`/`activated` and the `Arc<Mutex<Waker>>` parent are mandatory even
// when this metadata backs a `!Send` `LocalFutureDeque`: the `std::task::Waker` built from
// it is `Send + Sync` and may be cloned/woken/dropped from another thread. See the design
// comment on `FutureDequeCore::shared_parent` for why a non-atomic local variant is unsound.
pub(crate) struct WakerMeta {
    ref_count: AtomicUsize,

    // Per-slot activation flag. Set by the waker when the future is woken,
    // cleared by poll() when the future is polled. Represented as an
    // AtomicUsize with strict 0/1 semantics: 0 = not activated, 1 = activated.
    pub(crate) activated: AtomicUsize,

    // Shared parent waker, one per FutureDequeCore instance. All slots in the same deque
    // share this Arc, ensuring that parent waker changes propagate automatically without
    // per-slot iteration. Initialized to Waker::noop() and updated in poll() when the
    // executor provides a real waker.
    shared_parent: Arc<Mutex<Waker>>,
}

// Thread-local pool for waker metadata. Pooled values have stable addresses, which the
// RawWaker data pointer depends on for the lifetime of the metadata. Releasing a slot is
// lock-free and thread-safe, and the pool's storage stays alive while any allocation made
// from it is outstanding, so a waker may be woken and dropped on any thread — including
// after the thread that created the metadata has exited.
thread_local! {
    static WAKER_META_POOL: Pool<WakerMeta> = Pool::new();
}

static WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_raw_waker,
    wake_raw_waker,
    wake_by_ref_raw_waker,
    drop_raw_waker,
);

/// All fields of `WakerMeta` are thread-safe (atomics, Arc, Mutex), so this pointer is safe
/// to send across threads.
#[derive(Clone, Copy)]
pub(crate) struct MetaPtr(*const WakerMeta);

// SAFETY: `WakerMeta` consists solely of thread-safe types (`AtomicUsize` and
// `Arc<Mutex<Waker>>`), so it is `Send` on its own. The manual impl is needed only because
// raw pointers are never automatically `Send`. The pointed-to value has a stable address
// and its slot stays alive while the refcount is positive.
unsafe impl Send for MetaPtr {}

// SAFETY: Same reasoning as `Send` above — `WakerMeta` is `Sync` on its own and the manual
// impl exists only because raw pointers are never automatically `Sync`.
unsafe impl Sync for MetaPtr {}

/// Creates a new [`WakerMeta`] in the thread-local pool and returns a [`MetaPtr`] to it.
///
/// The returned pointer has a stable address and owns the pool slot. It stays valid until
/// the refcount reaches zero, at which point [`release_ref`] returns the slot to the pool.
// Inlining exposes the typed pool allocation to the deque insertion hot path measured by
// the Callgrind push benchmarks.
#[inline]
pub(crate) fn create_waker_meta(shared_parent: &Arc<Mutex<Waker>>) -> MetaPtr {
    WAKER_META_POOL.with(|pool| {
        let handle = pool.alloc_box(WakerMeta {
            ref_count: AtomicUsize::new(1),
            activated: AtomicUsize::new(1),
            shared_parent: Arc::clone(shared_parent),
        });

        MetaPtr(plurality::Box::into_raw(handle).as_ptr())
    })
}

/// Creates a [`Waker`] from a metadata pointer, incrementing the refcount.
pub(crate) fn make_waker(meta: MetaPtr) -> Waker {
    // SAFETY: The metadata is valid (refcount > 0 guarantees it has not been removed).
    let meta_ref = unsafe { meta.0.as_ref_unchecked() };
    meta_ref.ref_count.fetch_add(1, Ordering::Relaxed);

    // SAFETY: The vtable functions correctly match the data pointer layout.
    unsafe { Waker::from_raw(RawWaker::new(meta.0 as *const (), &WAKER_VTABLE)) }
}

/// Reads the activation flag, atomically clearing it. Returns `true` if the slot was
/// activated since the last call.
// Mutations to this function (replacing the return value, flipping the comparison)
// create infinite polling loops in tests that use block_on, preventing the test binary
// from exiting within the mutation testing timeout. Non-blocking tests catch these
// mutations, but cannot prevent the blocking tests from hanging.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn check_activated(meta: MetaPtr) -> bool {
    // SAFETY: The metadata is valid (refcount > 0 guarantees it has not been removed).
    let meta_ref = unsafe { meta.0.as_ref_unchecked() };
    meta_ref.activated.swap(0, Ordering::AcqRel) != 0
}

/// Decrements the refcount and returns the metadata to the pool if this was the
/// last reference. Called when a Slot releases its reference (future completes or
/// deque is dropped) and when the last waker clone is dropped.
// Mutating the `previous == 1` guard (to `!=` or `true`) frees the pooled slot while other
// references remain, so a live slot is reused for a new WakerMeta while stale wakers still
// point at it. The resulting corrupted waker state leaves block_on-based tests polling or
// parking forever, so the test binary never exits within the mutation testing timeout.
// Non-blocking tests catch these mutations, but cannot prevent the blocking tests from hanging.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn release_ref(meta: MetaPtr) {
    // SAFETY: The metadata is valid (refcount > 0 guarantees it has not been removed).
    let previous = unsafe { meta.0.as_ref_unchecked() }
        .ref_count
        .fetch_sub(1, Ordering::AcqRel);

    if previous == 1 {
        let ptr = NonNull::new(meta.0.cast_mut())
            .expect("metadata pointers come from Box::into_raw, which never yields null");

        // SAFETY: The refcount reached zero, so no other reference to the metadata remains
        // and this is the single `from_raw` call matching the `Box::into_raw` that
        // `create_waker_meta` performed for this pointer. Dropping the handle releases the
        // pool slot, so we hold no reference to the metadata across the drop.
        drop(unsafe { plurality::Box::<WakerMeta>::from_raw(ptr) });
    }
}

// --- RawWaker vtable functions ---

unsafe fn clone_raw_waker(data: *const ()) -> RawWaker {
    // SAFETY: The data pointer is a valid WakerMeta pointer (guaranteed by
    // construction in make_waker and create_waker_meta).
    let meta = unsafe { (data as *const WakerMeta).as_ref_unchecked() };
    meta.ref_count.fetch_add(1, Ordering::Relaxed);
    RawWaker::new(data, &WAKER_VTABLE)
}

unsafe fn wake_raw_waker(data: *const ()) {
    // Owned wake: activate, wake parent, then release this reference.
    // SAFETY: Delegating to vtable function with the same valid pointer.
    unsafe {
        wake_by_ref_raw_waker(data);
    }

    // SAFETY: Delegating to vtable function with the same valid pointer.
    unsafe {
        drop_raw_waker(data);
    }
}

unsafe fn wake_by_ref_raw_waker(data: *const ()) {
    // SAFETY: The data pointer is a valid WakerMeta pointer.
    let meta = unsafe { (data as *const WakerMeta).as_ref_unchecked() };

    // Only wake the parent if we are the first to set the activation flag.
    // If it was already set, the parent was already woken by a prior activation.
    if meta.activated.swap(1, Ordering::AcqRel) == 0 {
        // Clone the parent waker under the lock, then drop the lock before waking
        // to avoid potential deadlock if the wake path re-enters and tries to lock
        // shared_parent again (e.g. some executor waker implementations).
        let parent = meta
            .shared_parent
            .lock()
            .expect("we never panic while holding this lock")
            .clone();

        parent.wake_by_ref();
    }
}

unsafe fn drop_raw_waker(data: *const ()) {
    release_ref(MetaPtr(data as *const WakerMeta));
}

#[cfg(test)]
mod tests {
    use static_assertions::assert_impl_all;

    use super::*;

    // The `unsafe impl Send`/`Sync for MetaPtr` justifications rest on `WakerMeta` being
    // thread-safe by composition, so hold that claim to a machine-checked assertion.
    assert_impl_all!(WakerMeta: Send, Sync);

    // The pool is thread-local, so the assertions below only hold on a thread whose pool
    // this test owns exclusively.
    fn live_metadata_count() -> u64 {
        WAKER_META_POOL.with(Pool::len)
    }

    #[test]
    fn slot_is_returned_only_when_last_reference_is_released() {
        // A dedicated thread guarantees a pristine thread-local pool regardless of how the
        // test harness schedules the rest of the binary.
        std::thread::spawn(|| {
            assert_eq!(live_metadata_count(), 0);

            let shared_parent = Arc::new(Mutex::new(Waker::noop().clone()));
            let meta = create_waker_meta(&shared_parent);
            assert_eq!(live_metadata_count(), 1);

            let waker = make_waker(meta);
            let waker_clone = waker.clone();

            // The slot stays occupied while any reference remains.
            release_ref(meta);
            assert_eq!(live_metadata_count(), 1);

            drop(waker);
            assert_eq!(live_metadata_count(), 1);

            drop(waker_clone);
            assert_eq!(live_metadata_count(), 0);
        })
        .join()
        .unwrap();
    }

    #[test]
    fn released_slots_are_reused() {
        std::thread::spawn(|| {
            let shared_parent = Arc::new(Mutex::new(Waker::noop().clone()));

            let first = create_waker_meta(&shared_parent);
            let capacity_for_one = WAKER_META_POOL.with(Pool::capacity);
            release_ref(first);

            // One allocation beyond the initial capacity proves that released slots are reused:
            // retaining every slot would force growth. Asserting on capacity rather than slot
            // addresses keeps this independent of which free slot the pool hands back.
            for _ in 0..capacity_for_one {
                let meta = create_waker_meta(&shared_parent);
                release_ref(meta);
            }

            assert_eq!(WAKER_META_POOL.with(Pool::capacity), capacity_for_one);
            assert_eq!(live_metadata_count(), 0);
        })
        .join()
        .unwrap();
    }
}
