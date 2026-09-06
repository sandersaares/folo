//! Allocation wrapper for tracking memory allocations.

use std::alloc::{GlobalAlloc, Layout};
use std::any::type_name;
use std::fmt;
#[cfg(feature = "panic_on_next_alloc")]
use std::sync::atomic::{self, AtomicBool};

use crate::counters::{track_allocation, track_deallocation, track_reallocation};

/// Global flag to control whether the next memory allocation should panic.
/// When set to true, the next allocation attempt will panic and then reset the flag to false.
#[cfg(feature = "panic_on_next_alloc")]
static PANIC_ON_NEXT_ALLOCATION: AtomicBool = AtomicBool::new(false);

/// Controls whether the next memory allocation should panic.
///
/// When enabled, the next attempt to allocate memory will panic with a descriptive message
/// and then automatically reset the flag to false. This "one-shot" behavior is useful for
/// tracking down unexpected allocations in performance-critical code sections.
///
/// This function is only available when the `panic_on_next_alloc` feature is enabled.
///
/// # Arguments
///
/// * `enabled` - Whether to enable panic-on-next-allocation behavior
///
/// # Examples
///
/// ```rust
/// use alloc_tracker::{Allocator, panic_on_next_alloc};
///
/// #[global_allocator]
/// static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();
///
/// fn main() {
///     // Enable panic on next allocation
///     panic_on_next_alloc(true);
///
///     // This would panic (and reset the flag):
///     // let _vec = vec![1, 2, 3];
///
///     // Subsequent allocations are now safe again:
///     // let _another_vec = vec![4, 5, 6]; // This would work
/// }
/// ```
#[cfg(feature = "panic_on_next_alloc")]
pub fn panic_on_next_alloc(enabled: bool) {
    PANIC_ON_NEXT_ALLOCATION.store(enabled, atomic::Ordering::Relaxed);
}

/// Checks if panic-on-next-allocation is enabled and panics if so, automatically resetting the flag.
/// This is called before any allocation operation to implement the one-shot panic behavior.
#[cfg(feature = "panic_on_next_alloc")]
fn check_and_panic_if_enabled() {
    // Check if we should panic on this allocation and reset flag if so
    #[expect(
        clippy::manual_assert,
        reason = "We need to atomically swap the flag, not just check it"
    )]
    if PANIC_ON_NEXT_ALLOCATION.swap(false, atomic::Ordering::Relaxed) {
        panic!("Memory allocation attempted while panic-on-next-allocation was enabled");
    }
}

/// No-op version when `panic_on_next_alloc` feature is disabled.
#[cfg(not(feature = "panic_on_next_alloc"))]
#[inline]
fn check_and_panic_if_enabled() {}

/// A memory allocator that enables tracking of memory allocations and deallocations.
///
/// This allocator wraps any [`GlobalAlloc`] implementation to provide allocation tracking
/// capabilities while maintaining the same allocation behavior and performance characteristics
/// as the underlying allocator.
///
/// # Examples
///
/// ```rust
/// use alloc_tracker::Allocator;
///
/// #[global_allocator]
/// static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();
/// ```
pub struct Allocator<A: GlobalAlloc> {
    inner: A,
}

#[cfg_attr(coverage_nightly, coverage(off))] // No API contract to test.
impl<A: GlobalAlloc> fmt::Debug for Allocator<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name::<Self>())
            .field("inner", &"<allocator>")
            .finish()
    }
}

impl Allocator<std::alloc::System> {
    /// Creates a new tracking allocator using the system's default allocator.
    ///
    /// This is a convenience method for the common case of wanting to track
    /// allocations without changing the underlying allocation strategy.
    #[must_use]
    #[inline]
    // Only ever executed in const context, which is not covered by coverage measurement.
    #[cfg_attr(coverage_nightly, coverage(off))]
    pub const fn system() -> Self {
        Self {
            inner: std::alloc::System,
        }
    }
}

impl<A: GlobalAlloc> Allocator<A> {
    /// Creates a new tracking allocator that enables allocation tracking for the provided allocator.
    ///
    /// The resulting allocator will have the same performance and behavior characteristics
    /// as the underlying allocator, with the addition of allocation tracking capabilities.
    #[must_use]
    #[inline]
    // Only ever executed in const context, which is not covered by coverage measurement.
    #[cfg_attr(coverage_nightly, coverage(off))]
    pub const fn new(allocator: A) -> Self {
        Self { inner: allocator }
    }
}

// SAFETY: `GlobalAlloc` requires an implementation to hand out blocks that match the
// requested layout and to keep them valid until they are released. Every pointer this
// wrapper returns comes from `self.inner`, which already upholds that; the tracking added
// around each call is counter arithmetic that neither reads nor writes the blocks
// themselves, so it cannot affect their validity.
unsafe impl<A: GlobalAlloc> GlobalAlloc for Allocator<A> {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        check_and_panic_if_enabled();

        // SAFETY: `GlobalAlloc::alloc` requires `layout` to have non-zero size. This method
        // is itself `GlobalAlloc::alloc`, so its caller already owes that guarantee for this
        // exact `layout`, which is forwarded unchanged to an allocator posing the identical
        // requirement.
        let ptr = unsafe { self.inner.alloc(layout) };

        // A failed allocation reserves nothing, so recording it would permanently inflate the
        // outstanding total against a block that will never be freed.
        if !ptr.is_null() {
            track_allocation(layout.size());
        }

        ptr
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `GlobalAlloc::dealloc` requires that `ptr` was allocated by this same
        // allocator under `layout` and has not been freed. This method is itself
        // `GlobalAlloc::dealloc`, so its caller owes exactly that. Every block this wrapper
        // hands out is one `self.inner` produced, because all of its allocating methods
        // return the inner allocator's pointer unchanged, so the obligation carries over.
        unsafe {
            self.inner.dealloc(ptr, layout);
        }

        track_deallocation(layout.size());
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        check_and_panic_if_enabled();

        // SAFETY: `GlobalAlloc::alloc_zeroed` places the same non-zero-size requirement on
        // `layout` as `alloc` does. This method is itself `GlobalAlloc::alloc_zeroed`, so its
        // caller owes that guarantee for this exact `layout`, which is forwarded unchanged.
        let ptr = unsafe { self.inner.alloc_zeroed(layout) };

        if !ptr.is_null() {
            track_allocation(layout.size());
        }

        ptr
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        check_and_panic_if_enabled();

        // SAFETY: `GlobalAlloc::realloc` requires that `ptr` was allocated by this same
        // allocator under `layout`, and that `new_size` is non-zero and does not overflow
        // `isize::MAX` once rounded up to `layout`'s alignment. This method is itself
        // `GlobalAlloc::realloc`, so its caller owes all three. Every block this wrapper hands
        // out is one `self.inner` produced, and the three values reach it unchanged.
        let new_ptr = unsafe { self.inner.realloc(ptr, layout, new_size) };

        // On failure the original block is still live and unchanged, so no counter moves.
        if !new_ptr.is_null() {
            track_reallocation(layout.size(), new_size);
        }

        new_ptr
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::panic::{RefUnwindSafe, UnwindSafe};
    use std::{ptr, thread};

    use testing::with_watchdog;

    use super::*;
    use crate::counters::{get_or_init_thread_counters, thread_has_counters};

    // Static assertions for thread safety.
    static_assertions::assert_impl_all!(Allocator<std::alloc::System>: Send, Sync);

    // Static assertions for unwind safety.
    static_assertions::assert_impl_all!(
        Allocator<std::alloc::System>: UnwindSafe, RefUnwindSafe
    );

    /// A `GlobalAlloc` that fails every allocation request.
    ///
    /// It never hands out memory, so it is never asked to release or resize a block and the
    /// unimplemented operations are unreachable.
    struct FailingAllocator;

    // SAFETY: Returning null is the documented way to report allocation failure. Because every
    // request fails, this allocator never owns a block and the remaining operations, whose
    // contracts require a block obtained from this allocator, cannot be called.
    unsafe impl GlobalAlloc for FailingAllocator {
        unsafe fn alloc(&self, _layout: Layout) -> *mut u8 {
            ptr::null_mut()
        }

        unsafe fn dealloc(&self, _ptr: *mut u8, _layout: Layout) {
            unreachable!("this allocator never hands out a block that could be released");
        }
    }

    /// A `GlobalAlloc` that serves allocations from the system allocator but fails every
    /// reallocation request.
    ///
    /// Reallocation may only be called on a block obtained from the same allocator, so failing
    /// only the resize lets a test hold a genuine block from this allocator across a failed
    /// reallocation.
    struct FailingReallocator;

    // SAFETY: Allocation and deallocation forward the caller's obligations unchanged to the
    // system allocator. Returning null is the documented way for `realloc` to report failure,
    // which by contract leaves the original block allocated and unchanged.
    unsafe impl GlobalAlloc for FailingReallocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            // SAFETY: `System::alloc` requires `layout` to have non-zero size. This method is
            // itself `GlobalAlloc::alloc`, so its caller owes that for this exact `layout`,
            // which is forwarded unchanged.
            unsafe { std::alloc::System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            // SAFETY: `System::dealloc` requires a block that the system allocator produced
            // under `layout`. This method is itself `GlobalAlloc::dealloc`, so its caller owes
            // that the block came from this allocator, and the only method here that hands one
            // out is `alloc` above, which returns the system allocator's block unchanged.
            unsafe { std::alloc::System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, _ptr: *mut u8, _layout: Layout, _new_size: usize) -> *mut u8 {
            ptr::null_mut()
        }
    }

    /// An arbitrary layout, large enough that a real allocation is unlikely to be optimized away.
    fn test_layout(size: usize) -> Layout {
        Layout::from_size_align(size, 8).unwrap()
    }

    #[test]
    fn allocation_and_deallocation_move_outstanding() {
        const SIZE: usize = 1024;

        let allocator = Allocator::new(std::alloc::System);
        let layout = test_layout(SIZE);
        let counters = get_or_init_thread_counters();

        let before = counters.outstanding();

        // SAFETY: The layout has a non-zero size and a power-of-two alignment.
        let block = unsafe { allocator.alloc(layout) };
        assert!(!block.is_null());

        let after_alloc = counters.outstanding();

        // SAFETY: The block was just obtained from this allocator with this exact layout.
        unsafe {
            allocator.dealloc(block, layout);
        }
        let after_dealloc = counters.outstanding();

        assert_eq!(
            after_alloc.wrapping_sub(before),
            i64::try_from(SIZE).unwrap()
        );
        assert_eq!(after_dealloc, before);
    }

    #[test]
    fn zeroed_allocation_zeroes_memory_and_moves_every_counter() {
        const SIZE: usize = 1024;

        let allocator = Allocator::new(std::alloc::System);
        let layout = test_layout(SIZE);
        let counters = get_or_init_thread_counters();

        let before_bytes = counters.bytes();
        let before_count = counters.count();
        let before_outstanding = counters.outstanding();

        // Drop the watermark to the current level so the rise this allocation causes is
        // attributable to it rather than to whatever ran earlier on this thread.
        counters.set_watermark(before_outstanding);

        // SAFETY: The layout has a non-zero size and a power-of-two alignment.
        let block = unsafe { allocator.alloc_zeroed(layout) };
        assert!(!block.is_null());

        // SAFETY: The allocator just returned this block for a layout of SIZE bytes, so
        // that many bytes are initialized and readable.
        let contents = unsafe { std::slice::from_raw_parts(block, SIZE) };
        assert!(contents.iter().all(|&byte| byte == 0));

        let size = i64::try_from(SIZE).unwrap();
        assert_eq!(counters.bytes(), before_bytes.wrapping_add(SIZE as u64));
        assert_eq!(counters.count(), before_count.wrapping_add(1));
        assert_eq!(
            counters.outstanding().wrapping_sub(before_outstanding),
            size
        );
        assert_eq!(counters.watermark().wrapping_sub(before_outstanding), size);

        // SAFETY: The block was just obtained from this allocator with this exact layout.
        unsafe {
            allocator.dealloc(block, layout);
        }

        assert_eq!(counters.outstanding(), before_outstanding);
    }

    #[test]
    fn failed_allocation_does_not_move_counters() {
        let allocator = Allocator::new(FailingAllocator);
        let layout = test_layout(1024);
        let counters = get_or_init_thread_counters();

        let before_bytes = counters.bytes();
        let before_outstanding = counters.outstanding();

        // SAFETY: The layout has a non-zero size and a power-of-two alignment.
        let block = unsafe { allocator.alloc(layout) };
        assert!(block.is_null());

        assert_eq!(counters.bytes(), before_bytes);
        assert_eq!(counters.outstanding(), before_outstanding);
    }

    #[test]
    fn failed_zeroed_allocation_does_not_move_counters() {
        let allocator = Allocator::new(FailingAllocator);
        let layout = test_layout(1024);
        let counters = get_or_init_thread_counters();

        let before_bytes = counters.bytes();
        let before_count = counters.count();
        let before_outstanding = counters.outstanding();
        let before_watermark = counters.watermark();

        // SAFETY: The layout has a non-zero size and a power-of-two alignment.
        let block = unsafe { allocator.alloc_zeroed(layout) };
        assert!(block.is_null());

        assert_eq!(counters.bytes(), before_bytes);
        assert_eq!(counters.count(), before_count);
        assert_eq!(counters.outstanding(), before_outstanding);
        assert_eq!(counters.watermark(), before_watermark);
    }

    #[test]
    fn successful_reallocation_moves_counters() {
        const INITIAL: usize = 64;
        const GROWN: usize = 256;

        let allocator = Allocator::new(std::alloc::System);
        let layout = test_layout(INITIAL);
        let counters = get_or_init_thread_counters();

        // SAFETY: The layout has a non-zero size and a power-of-two alignment.
        let block = unsafe { allocator.alloc(layout) };
        assert!(!block.is_null());

        let before_bytes = counters.bytes();
        let before_count = counters.count();
        let before_outstanding = counters.outstanding();

        // SAFETY: The block was just obtained from this allocator with this exact layout, and
        // the new size is non-zero and does not overflow when rounded up to the alignment.
        let grown = unsafe { allocator.realloc(block, layout, GROWN) };
        assert!(!grown.is_null());

        let after_bytes = counters.bytes();
        let after_count = counters.count();
        let after_outstanding = counters.outstanding();

        // SAFETY: The grown block came from this allocator, whose layout is now the new size.
        unsafe {
            allocator.dealloc(grown, test_layout(GROWN));
        }

        // A reallocation is one allocator request of the new size, so the cumulative totals
        // grow by the whole new size while outstanding grows only by the difference.
        assert_eq!(
            after_bytes.wrapping_sub(before_bytes),
            u64::try_from(GROWN).unwrap()
        );
        assert_eq!(after_count.wrapping_sub(before_count), 1);
        assert_eq!(
            after_outstanding.wrapping_sub(before_outstanding),
            i64::try_from(GROWN - INITIAL).unwrap()
        );
    }

    #[test]
    fn failed_reallocation_does_not_move_counters() {
        const INITIAL: usize = 64;
        const GROWN: usize = 256;

        let allocator = Allocator::new(FailingReallocator);
        let layout = test_layout(INITIAL);
        let counters = get_or_init_thread_counters();

        // SAFETY: The layout has a non-zero size and a power-of-two alignment.
        let block = unsafe { allocator.alloc(layout) };
        assert!(!block.is_null());

        let before_bytes = counters.bytes();
        let before_outstanding = counters.outstanding();

        // SAFETY: The block was just obtained from this allocator with this exact layout, and
        // the new size is non-zero and does not overflow when rounded up to the alignment.
        let grown = unsafe { allocator.realloc(block, layout, GROWN) };
        assert!(grown.is_null());

        let after_bytes = counters.bytes();
        let after_outstanding = counters.outstanding();

        // SAFETY: The failed reallocation left the block allocated by this allocator with its
        // original layout.
        unsafe {
            allocator.dealloc(block, layout);
        }

        assert_eq!(after_bytes, before_bytes);
        assert_eq!(after_outstanding, before_outstanding);
    }

    #[test]
    fn deallocation_on_untracked_thread_creates_no_counters() {
        with_watchdog(|| {
            // This needs a thread that has never allocated. The watchdog runs its closure on
            // the calling thread under mutation testing, and libtest reuses worker threads
            // across tests, so the freshness has to come from a thread spawned here.
            thread::scope(|scope| {
                scope.spawn(|| {
                    assert!(!thread_has_counters());

                    let layout = test_layout(64);

                    // SAFETY: The layout has a non-zero size and a power-of-two alignment.
                    let block = unsafe { std::alloc::System.alloc(layout) };
                    assert!(!block.is_null());

                    let allocator = Allocator::new(std::alloc::System);

                    // SAFETY: The block came from the system allocator with this exact
                    // layout, and the tracking allocator forwards the release to that same
                    // allocator.
                    unsafe {
                        allocator.dealloc(block, layout);
                    }

                    // Creating counters allocates and locks the registry, which a free must
                    // never do.
                    assert!(!thread_has_counters());
                });
            });
        });
    }

    #[test]
    #[cfg(feature = "panic_on_next_alloc")]
    fn panic_on_next_alloc_can_be_enabled_and_disabled() {
        assert!(!PANIC_ON_NEXT_ALLOCATION.load(atomic::Ordering::Relaxed));

        panic_on_next_alloc(true);
        assert!(PANIC_ON_NEXT_ALLOCATION.load(atomic::Ordering::Relaxed));

        panic_on_next_alloc(false);
        assert!(!PANIC_ON_NEXT_ALLOCATION.load(atomic::Ordering::Relaxed));
    }
}
