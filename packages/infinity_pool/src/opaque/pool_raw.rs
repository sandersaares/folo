use std::alloc::Layout;
use std::iter::{self, FusedIterator};
use std::mem::{MaybeUninit, size_of};
#[cfg(test)]
use std::num::NonZero;
use std::ptr::NonNull;

use crate::opaque::slab::SlabIterator;
use crate::{
    DropPolicy, RawOpaquePoolBuilder, RawPooled, RawPooledMut, Slab, SlabLayout, VacancyTracker,
};

/// A pool of objects with uniform memory layout.
///
/// Stores objects of any type that match a [`Layout`] defined at pool creation
/// time. All values in the pool remain pinned for their entire lifetime.
///
/// The pool automatically expands its capacity when needed.
/// # Thread safety
///
/// The pool is nominally single-threaded because the compiler cannot know what types of objects
/// are stored inside a given instance, so it must default to assuming they are single-threaded
/// objects.
///
/// If all the objects inserted are `Send` then the owner of the pool is allowed to treat
/// the pool itself as thread-safe (`Send` and `Sync`) but must do so using unsafe code,
/// such as via a wrapper type that explicitly implements `Send` and `Sync`.
///
/// # Example: unique object ownership
///
/// ```rust
/// use std::fmt::Display;
///
/// use infinity_pool::RawOpaquePool;
///
/// let mut pool = RawOpaquePool::with_layout_of::<String>();
///
/// // Insert an object into the pool, returning a unique handle to it.
/// let mut handle = pool.insert("Hello, world!".to_string());
///
/// // A unique handle allows us to create exclusive references to the target object.
/// // SAFETY: We promise to keep the pool alive for the duration of this reference.
/// let value_mut = unsafe { handle.as_mut() };
/// value_mut.push_str(" Welcome to Infinity Pool!");
///
/// println!("Updated value: {value_mut}");
///
/// // This is optional - we could also just drop the pool.
/// // SAFETY: We promise that this handle really is for an object present in this pool.
/// unsafe {
///     pool.remove(handle);
/// }
/// ```
///
/// # Example: shared object ownership
///
/// ```rust
/// use std::fmt::Display;
///
/// use infinity_pool::RawOpaquePool;
///
/// let mut pool = RawOpaquePool::with_layout_of::<String>();
///
/// // Insert an object into the pool, returning a unique handle to it.
/// let handle = pool.insert("Hello, world!".to_string());
///
/// // The unique handle can be converted into a shared handle,
/// // allowing multiple copies of the handle to be created.
/// let shared_handle = handle.into_shared();
/// let shared_handle_copy = shared_handle;
///
/// // Shared handles allow only shared references to be created.
/// // SAFETY: We promise to keep the pool alive for the duration of this reference.
/// let value_ref = unsafe { shared_handle.as_ref() };
///
/// println!("Shared access to value: {value_ref}");
///
/// // This is optional - we could also just drop the pool.
/// // SAFETY: We promise that the object has not already been removed
/// // via a different shared handle - look up to verify that.
/// unsafe {
///     pool.remove(shared_handle);
/// }
/// ```
#[derive(Debug)]
pub struct RawOpaquePool {
    /// The layout of each slab in the pool, determined based on the object
    /// layout provided at pool creation time.
    slab_layout: SlabLayout,

    /// The slabs that make up the pool's memory capacity. Automatically extended
    /// with new slabs as needed. Shrinking is supported but must be manually commanded.
    slabs: Vec<Slab>,

    /// Drop policy that determines how the pool handles remaining items when dropped.
    drop_policy: DropPolicy,

    /// Number of items currently in the pool. We track this explicitly to avoid repeatedly
    /// summing across slabs when calculating the length.
    length: usize,

    /// Tracks which slabs have vacancies, acting as a cache for fast insertion.
    /// Guaranteed 100% accurate - we update the tracker whenever there is a status change.
    vacancy_tracker: VacancyTracker,
}

impl RawOpaquePool {
    /// Sets the slab capacity of an empty fixture so boundary tests need few live objects.
    #[cfg(test)]
    #[cfg_attr(coverage_nightly, coverage(off))]
    pub(crate) fn set_slab_capacity(&mut self, capacity: NonZero<usize>) {
        assert!(self.slabs.is_empty());
        self.slab_layout = self.slab_layout.with_capacity(capacity);
    }

    /// Starts configuring and creating a new instance of the pool.
    #[cfg_attr(test, mutants::skip)] // Gets mutated to alternate version of itself.
    pub fn builder() -> RawOpaquePoolBuilder {
        RawOpaquePoolBuilder::new()
    }

    /// Creates a new instance of the pool with the specified layout.
    ///
    /// Shorthand for a builder that keeps all other options at their default values.
    ///
    /// # Panics
    ///
    /// Panics if the layout is zero-sized.
    #[must_use]
    pub fn with_layout(object_layout: Layout) -> Self {
        Self::builder().layout(object_layout).build()
    }

    /// Creates a new instance of the pool with the layout of `T`.
    ///
    /// Shorthand for a builder that keeps all other options at their default values.
    #[must_use]
    pub fn with_layout_of<T: Sized>() -> Self {
        const {
            assert!(
                size_of::<T>() > 0,
                "cannot create a pool of zero-sized objects"
            );
        };

        Self::builder().layout_of::<T>().build()
    }

    /// Creates a new pool for objects of the specified layout.
    ///
    /// # Panics
    ///
    /// Panics if the object layout has zero size.
    #[must_use]
    pub(crate) fn new_inner(object_layout: Layout, drop_policy: DropPolicy) -> Self {
        let slab_layout = SlabLayout::new(object_layout);

        Self {
            slab_layout,
            slabs: Vec::new(),
            drop_policy,
            length: 0,
            vacancy_tracker: VacancyTracker::new(),
        }
    }

    /// The layout of objects stored in this pool.
    ///
    /// All inserted objects must match this layout.
    #[must_use]
    #[inline]
    pub fn object_layout(&self) -> Layout {
        self.slab_layout.object_layout()
    }

    /// The number of objects currently in the pool.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    /// The total capacity of the pool.
    ///
    /// This is the maximum number of objects (including current contents) that the pool can contain
    /// without capacity extension. The pool will automatically extend its capacity if more than
    /// this many objects are inserted.
    #[must_use]
    #[inline]
    pub fn capacity(&self) -> usize {
        // Wrapping here would imply capacity is greater than virtual memory,
        // which is impossible because we can never create that many slabs.
        self.slabs
            .len()
            .wrapping_mul(self.slab_layout.capacity().get())
    }

    /// Whether the pool contains zero objects.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Ensures that the pool has capacity for at least `additional` more objects.
    ///
    /// # Panics
    ///
    /// Panics if the new capacity would exceed the size of virtual memory (`usize::MAX`).
    pub fn reserve(&mut self, additional: usize) {
        let required_capacity = self
            .len()
            .checked_add(additional)
            .expect("requested capacity exceeds size of virtual memory");

        if self.capacity() >= required_capacity {
            return;
        }

        // Calculate how many additional slabs we need
        let current_slabs = self.slabs.len();
        let required_slabs = required_capacity.div_ceil(self.slab_layout.capacity().get());
        let additional_slabs = required_slabs.saturating_sub(current_slabs);

        self.slabs.extend(
            iter::repeat_with(|| Slab::new(self.slab_layout, self.drop_policy))
                .take(additional_slabs),
        );

        self.vacancy_tracker.update_slab_count(self.slabs.len());
    }

    /// Drops unused pool capacity to reduce memory usage.
    ///
    /// There is no guarantee that any unused capacity can be dropped. The exact outcome depends
    /// on the specific pool structure and which objects remain in the pool.
    #[cfg_attr(test, mutants::skip)] // Vacant slot cache mutation - hard to test. Revisit later.
    pub fn shrink_to_fit(&mut self) {
        // Find the last non-empty slab by scanning from the end
        let new_len = self
            .slabs
            .iter()
            .enumerate()
            .rev()
            .find_map(|(idx, slab)| {
                if !slab.is_empty() {
                    // Cannot wrap because that would imply we have more slabs than the size
                    // of virtual memory, which is impossible.
                    Some(idx.wrapping_add(1))
                } else {
                    None
                }
            })
            .unwrap_or(0);

        if new_len == self.slabs.len() {
            // Nothing to do.
            return;
        }

        // Truncate the slabs vector to remove empty slabs from the end.
        self.slabs.truncate(new_len);

        self.vacancy_tracker.update_slab_count(self.slabs.len());
    }

    /// Inserts an object into the pool and returns a handle to it.
    ///
    /// # Panics
    /// Panics if the layout of `T` does not match the object layout of the pool.
    #[inline]
    #[cfg_attr(test, mutants::skip)] // All mutations are unviable - skip them to save time.
    pub fn insert<T: 'static>(&mut self, value: T) -> RawPooledMut<T> {
        assert_eq!(
            Layout::new::<T>(),
            self.object_layout(),
            "layout of T does not match object layout of the pool"
        );

        // SAFETY: We just verified that T's layout matches the pool's layout.
        unsafe { self.insert_unchecked(value) }
    }

    /// Inserts an object into the pool and returns a handle to it.
    /// # Safety
    /// The caller must ensure that the layout of `T` matches the pool's object layout.
    #[inline]
    pub unsafe fn insert_unchecked<T: 'static>(&mut self, value: T) -> RawPooledMut<T> {
        // Implement insert() in terms of insert_with() to reduce logic duplication.
        // SAFETY: Forwarding safety requirements to the caller.
        unsafe {
            self.insert_with_unchecked(|uninit: &mut MaybeUninit<T>| {
                uninit.write(value);
            })
        }
    }

    /// Inserts an object into the pool via closure and returns a handle to it.
    ///
    /// This method allows the caller to partially initialize the object, skipping any `MaybeUninit`
    /// fields that are intentionally not initialized at insertion time. This can make insertion of
    /// objects containing `MaybeUninit` fields faster, although requires unsafe code to implement.
    ///
    /// This method is NOT faster than `insert()` for fully initialized objects.
    /// Prefer `insert()` for a better safety posture if you do not intend to
    /// skip initialization of any `MaybeUninit` fields.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::mem::MaybeUninit;
    /// use std::ptr;
    ///
    /// use infinity_pool::RawOpaquePool;
    ///
    /// struct DataBuffer {
    ///     id: u32,
    ///     data: MaybeUninit<[u8; 1024]>,
    /// }
    ///
    /// let mut pool = RawOpaquePool::with_layout_of::<DataBuffer>();
    ///
    /// // Initialize only the id, leaving data uninitialized for performance.
    /// let handle = unsafe {
    ///     pool.insert_with(|uninit: &mut MaybeUninit<DataBuffer>| {
    ///         let ptr = uninit.as_mut_ptr();
    ///
    ///         // SAFETY: We are writing to a correctly located field within the object.
    ///         unsafe {
    ///             ptr::addr_of_mut!((*ptr).id).write(42);
    ///         }
    ///     })
    /// };
    ///
    /// // SAFETY: We promise that the pool is not dropped while we hold this reference.
    /// let item = unsafe { handle.as_ref() };
    /// assert_eq!(item.id, 42);
    /// ```
    ///
    /// # Panics
    /// Panics if the layout of `T` does not match the object layout of the pool.
    ///
    /// # Safety
    /// The closure must correctly initialize the object. All fields that
    /// are not `MaybeUninit` must be initialized when the closure returns.
    #[inline]
    pub unsafe fn insert_with<T, F>(&mut self, f: F) -> RawPooledMut<T>
    where
        F: FnOnce(&mut MaybeUninit<T>),
        T: 'static,
    {
        assert_eq!(
            Layout::new::<T>(),
            self.object_layout(),
            "layout of T does not match object layout of the pool"
        );

        // SAFETY: We just verified that T's layout matches the pool's layout.
        unsafe { self.insert_with_unchecked(f) }
    }

    /// Inserts an object into the pool via closure and returns a handle to it.
    ///
    /// This method allows the caller to partially initialize the object, skipping any `MaybeUninit`
    /// fields that are intentionally not initialized at insertion time. This can make insertion of
    /// objects containing `MaybeUninit` fields faster, although requires unsafe code to implement.
    ///
    /// This method is NOT faster than `insert()` for fully initialized objects.
    /// Prefer `insert()` for a better safety posture if you do not intend to
    /// skip initialization of any `MaybeUninit` fields.
    ///
    /// This unchecked variant of the method skips the layout verification step, requiring
    /// the caller to ensure that the object has a matching layout with the pool.
    ///
    /// # Safety
    /// The caller must ensure that the layout of `T` matches the pool's object layout.
    ///
    /// The closure must correctly initialize the object. All fields that
    /// are not `MaybeUninit` must be initialized when the closure returns.
    #[inline]
    pub unsafe fn insert_with_unchecked<T, F>(&mut self, f: F) -> RawPooledMut<T>
    where
        F: FnOnce(&mut MaybeUninit<T>),
        T: 'static,
    {
        let slab_index = self.index_of_slab_to_insert_into();

        // SAFETY: We just received knowledge that there is a slab with a vacant slot at this index.
        let slab = unsafe { self.slabs.get_unchecked_mut(slab_index) };

        // SAFETY: Forwarding guarantee from caller that T's layout matches the pool's layout
        // and that the closure properly initializes the value.
        //
        // We ensure that the slab is not full via the logic that gets us `slab_index`,
        // with the logic always guaranteeing that there is a vacancy in that slab.
        let slab_handle = unsafe { slab.insert_with_unchecked(f) };

        // Update our tracked length since we just inserted an object.
        // This can never overflow since that would mean the pool is greater than virtual memory.
        self.length = self.length.wrapping_add(1);

        // We invalidate the "slab with vacant slot" cache here if this is the last vacant slot.
        if slab.is_full() {
            // SAFETY: We are currently operating on the slab, so it must be an existing slab.
            // The mechanism that adds slabs is responsible for updating vacancy tracker bounds.
            unsafe {
                self.vacancy_tracker.update_slab_status(slab_index, false);
            }
        }

        // The pool itself does not care about the type T but for the convenience of the caller
        // we imbue the RawPooledMut with the type information, to reduce required casting by caller.
        RawPooledMut::new(slab_index, slab_handle)
    }

    /// Removes an object from the pool, dropping the object.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that the handle is for an object currently present in this pool.
    pub unsafe fn remove<T: ?Sized>(&mut self, handle: impl Into<RawPooled<T>>) {
        let handle = handle.into();

        // SAFETY: Caller guarantees the handle is valid for this pool.
        let slab = unsafe { self.slabs.get_unchecked_mut(handle.slab_index()) };

        // SAFETY: Caller guarantees the handle is valid for this pool.
        unsafe {
            slab.remove(handle.slab_handle());
        }

        // Update our tracked length since we just removed an object.
        // This cannot wrap around because we just removed an object,
        // so the value must be at least 1 before subtraction.
        self.length = self.length.wrapping_sub(1);

        if slab.len() == self.slab_layout.capacity().get().wrapping_sub(1) {
            // We removed from a full slab.
            // This means we have a vacant slot where there was not one before.

            // SAFETY: We are currently operating on the slab, so it must be an existing slab.
            // The mechanism that adds slabs is responsible for updating vacancy tracker bounds.
            unsafe {
                self.vacancy_tracker
                    .update_slab_status(handle.slab_index(), true);
            }
        }
    }

    /// Removes an object from the pool and returns the object.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that the handle is for an object currently present in this pool.
    #[must_use]
    pub unsafe fn remove_unpin<T: Unpin>(&mut self, handle: impl Into<RawPooled<T>>) -> T {
        let handle = handle.into();

        const {
            assert!(
                size_of::<T>() > 0,
                "cannot extract zero-sized types from pool"
            );
        };

        // SAFETY: Caller guarantees the handle is valid for this pool.
        let slab = unsafe { self.slabs.get_unchecked_mut(handle.slab_index()) };

        // SAFETY: Caller guarantees the handle is valid for this pool.
        let value = unsafe { slab.remove_unpin::<T>(handle.slab_handle()) };

        // Update our tracked length since we just removed an object.
        // This cannot wrap around because we just removed an object,
        // so the value must be at least 1 before subtraction.
        self.length = self.length.wrapping_sub(1);

        if slab.len() == self.slab_layout.capacity().get().wrapping_sub(1) {
            // We removed from a full slab.
            // This means we have a vacant slot where there was not one before.

            // SAFETY: We are currently operating on the slab, so it must be an existing slab.
            // The mechanism that adds slabs is responsible for updating vacancy tracker bounds.
            unsafe {
                self.vacancy_tracker
                    .update_slab_status(handle.slab_index(), true);
            }
        }

        value
    }

    /// Returns an iterator over all objects in the pool.
    ///
    /// The iterator yields untyped pointers (`NonNull<()>`) to the objects stored in the pool.
    /// It is the caller's responsibility to cast these pointers to the appropriate type.
    #[must_use]
    #[inline]
    pub fn iter(&self) -> RawOpaquePoolIterator<'_> {
        RawOpaquePoolIterator::new(self)
    }

    /// Returns the index of a slab with a vacant slot, allocating a new slab if necessary.
    ///
    /// The fast path is a single cached lookup in the vacancy tracker. The cold path
    /// — allocating a new slab when every existing slab is full — is outlined into
    /// [`Self::allocate_slab_for_insert`] so the fast path stays small enough to inline
    /// into the per-insert hot path of every `OpaquePool` variant.
    #[must_use]
    #[inline]
    fn index_of_slab_to_insert_into(&mut self) -> usize {
        if let Some(index) = self.vacancy_tracker.next_vacancy() {
            // There is a vacancy, so use it.
            return index;
        }

        self.allocate_slab_for_insert()
    }

    /// Allocates a new slab and returns its index.
    ///
    /// This is the cold path of [`Self::index_of_slab_to_insert_into`]: it only runs
    /// when every existing slab is full. Marked `#[cold]` and `#[inline(never)]` to keep
    /// the slab-allocation code (and its call into the global allocator) out of the
    /// per-insert fast path's instruction-cache footprint.
    #[cold]
    #[inline(never)]
    #[must_use]
    fn allocate_slab_for_insert(&mut self) -> usize {
        // If we got here, there are no vacancies and we need to extend the pool.
        debug_assert_eq!(self.len(), self.capacity());

        self.slabs
            .push(Slab::new(self.slab_layout, self.drop_policy));

        self.vacancy_tracker.update_slab_count(self.slabs.len());

        // This can never wrap around because we just added a slab, so len() is at least 1.
        self.slabs.len().wrapping_sub(1)
    }
}

/// Iterator over all objects in a raw opaque pool.
///
/// This iterator yields untyped pointers to objects stored across all slabs in the pool.
/// Since the pool can contain objects of different types (as long as they have the same layout),
/// the iterator returns `NonNull<()>` and leaves type casting to the caller.
///
/// # Thread safety
///
/// The type is single-threaded.
#[derive(Debug)]
pub struct RawOpaquePoolIterator<'p> {
    pool: &'p RawOpaquePool,

    // Current slab index for forward iteration.
    // This is the index of the next slab we will take items from.
    // If iterator is exhausted, will point to undefined value.
    current_front_slab_index: usize,

    // Current slab index for backward iteration.
    // This is the index of the next slab we will take items from.
    // If iterator is exhausted, will point to undefined value.
    current_back_slab_index: usize,

    // Iterator for the current front slab (if any).
    current_front_slab_iter: Option<SlabIterator<'p>>,

    // Iterator for the current back slab (if any).
    current_back_slab_iter: Option<SlabIterator<'p>>,

    // Total number of items already yielded.
    yielded_count: usize,
}

impl<'p> RawOpaquePoolIterator<'p> {
    fn new(pool: &'p RawOpaquePool) -> Self {
        Self {
            pool,
            current_front_slab_index: 0,
            // This is allowed to wrap - if the iterator is exhausted, we point to undefined value.
            current_back_slab_index: pool.slabs.len().wrapping_sub(1),
            current_front_slab_iter: None,
            current_back_slab_iter: None,
            yielded_count: 0,
        }
    }
}

impl Iterator for RawOpaquePoolIterator<'_> {
    type Item = NonNull<()>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.len() > 0 {
            // If no current iterator, get one for the current slab.
            let slab_iter = self.current_front_slab_iter.get_or_insert_with(|| {
                self.pool.slabs
                    .get(self.current_front_slab_index)
                    .expect("iterator has items remaining, so there must still be a slab to get them from")
                    .iter()
            });

            // Try to get the next item from current iterator
            if let Some(item) = slab_iter.next() {
                // Will never wrap because that would mean we have more
                // items than we have virtual memory.
                self.yielded_count = self.yielded_count.wrapping_add(1);
                return Some(item);
            }

            // No more items from this slab, move to next
            // This is allowed to wrap - if the iterator is exhausted, we point to undefined value.
            self.current_front_slab_index = self.current_front_slab_index.wrapping_add(1);
            self.current_front_slab_iter = None;
        }

        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len();
        (remaining, Some(remaining))
    }
}

impl DoubleEndedIterator for RawOpaquePoolIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        while self.len() > 0 {
            // If no current iterator, get one for the current slab.
            let slab_iter = self.current_back_slab_iter.get_or_insert_with(|| {
                self.pool.slabs
                    .get(self.current_back_slab_index)
                    .expect("iterator has items remaining, so there must still be a slab to get them from")
                    .iter()
            });

            // Try to get the next item from current iterator
            if let Some(item) = slab_iter.next_back() {
                // Will never wrap because that would mean we have more
                // items than we have virtual memory.
                self.yielded_count = self.yielded_count.wrapping_add(1);
                return Some(item);
            }

            // No more items from this slab, move to next
            // This is allowed to wrap - if the iterator is exhausted, we point to undefined value.
            self.current_back_slab_index = self.current_back_slab_index.wrapping_sub(1);
            self.current_back_slab_iter = None;
        }

        None
    }
}

impl ExactSizeIterator for RawOpaquePoolIterator<'_> {
    fn len(&self) -> usize {
        // Total objects in pool minus those we've already yielded
        // Will not wrap because we cannot yield more items than exist in the pool.
        self.pool.len().wrapping_sub(self.yielded_count)
    }
}

// Once we return None, we will keep returning None.
impl FusedIterator for RawOpaquePoolIterator<'_> {}

impl<'p> IntoIterator for &'p RawOpaquePool {
    type Item = NonNull<()>;
    type IntoIter = RawOpaquePoolIterator<'p>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
#[allow(
    clippy::indexing_slicing,
    clippy::multiple_unsafe_ops_per_block,
    clippy::undocumented_unsafe_blocks,
    reason = "tests focus on succinct code and do not need to tick all the boxes"
)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::alloc::Layout;
    use std::mem::MaybeUninit;
    use std::panic::{RefUnwindSafe, UnwindSafe};

    use new_zealand::nz;
    use static_assertions::{assert_impl_all, assert_not_impl_any};

    use super::*;

    // We are nominally single-threaded.
    assert_not_impl_any!(RawOpaquePool: Send, Sync);
    assert_impl_all!(RawOpaquePool: UnwindSafe, RefUnwindSafe);

    assert_impl_all!(RawOpaquePoolIterator<'_>: Iterator, DoubleEndedIterator, ExactSizeIterator, FusedIterator);
    assert_not_impl_any!(RawOpaquePoolIterator<'_>: Send, Sync);
    assert_impl_all!(RawOpaquePoolIterator<'_>: UnwindSafe, RefUnwindSafe);

    assert_impl_all!(&RawOpaquePool: IntoIterator);

    #[test]
    fn new_pool_is_empty() {
        let pool = RawOpaquePool::with_layout_of::<u64>();

        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());
        assert_eq!(pool.capacity(), 0);
        assert_eq!(pool.object_layout(), Layout::new::<u64>());
    }

    #[test]
    fn with_layout_results_in_pool_with_correct_layout() {
        let layout = Layout::new::<i64>();
        let pool = RawOpaquePool::with_layout(layout);

        assert_eq!(pool.object_layout(), layout);
        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());
        assert_eq!(pool.capacity(), 0);
    }

    #[test]
    fn instance_creation_through_builder_succeeds() {
        let pool = RawOpaquePool::builder()
            .layout_of::<i64>()
            .drop_policy(DropPolicy::MustNotDropContents)
            .build();

        assert_eq!(pool.object_layout(), Layout::new::<i64>());
        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());
        assert_eq!(pool.capacity(), 0);
    }

    #[test]
    fn insert_and_length() {
        let mut pool = RawOpaquePool::with_layout_of::<u32>();

        let _handle1 = pool.insert(42_u32);
        assert_eq!(pool.len(), 1);
        assert!(!pool.is_empty());

        let _handle2 = pool.insert(100_u32);
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn capacity_grows_with_slabs() {
        let mut pool = RawOpaquePool::with_layout_of::<u64>();
        // Keep multiple live slots before crossing the slab boundary.
        pool.set_slab_capacity(nz!(2));

        assert_eq!(pool.capacity(), 0);

        let _handle = pool.insert(123_u64);

        // Should have at least one slab's worth of capacity now
        assert!(pool.capacity() > 0);
        let initial_capacity = pool.capacity();

        // Fill up the slab to force creation of a new one
        for i in 1..initial_capacity {
            let _handle = pool.insert(i as u64);
        }

        // One more insert should create a new slab
        let _handle = pool.insert(999_u64);

        assert!(pool.capacity() >= initial_capacity.checked_mul(2).unwrap());
    }

    #[test]
    fn reserve_creates_capacity() {
        let mut pool = RawOpaquePool::with_layout_of::<u8>();

        pool.reserve(100);
        assert!(pool.capacity() >= 100);

        let initial_capacity = pool.capacity();
        pool.reserve(50); // Should not increase capacity
        assert_eq!(pool.capacity(), initial_capacity);

        pool.reserve(200); // Should increase capacity
        assert!(pool.capacity() >= 200);
    }

    #[test]
    fn insert_with_closure() {
        let mut pool = RawOpaquePool::with_layout_of::<u64>();

        let handle = unsafe {
            pool.insert_with(|uninit: &mut MaybeUninit<u64>| {
                uninit.write(42);
            })
        };

        assert_eq!(pool.len(), 1);

        let value = unsafe { pool.remove_unpin(handle) };
        assert_eq!(value, 42);
    }

    #[test]
    fn remove_decreases_length() {
        let mut pool = RawOpaquePool::with_layout_of::<String>();

        let handle1 = pool.insert("hello".to_string());
        let handle2 = pool.insert("world".to_string());

        assert_eq!(pool.len(), 2);

        unsafe {
            pool.remove(handle1);
        }
        assert_eq!(pool.len(), 1);

        unsafe {
            pool.remove(handle2);
        }
        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());
    }

    #[test]
    fn remove_unpin_returns_value() {
        let mut pool = RawOpaquePool::with_layout_of::<i32>();

        let handle = pool.insert(-456_i32);

        let value = unsafe { pool.remove_unpin(handle) };
        assert_eq!(value, -456);
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn shrink_to_fit_removes_empty_slabs() {
        let mut pool = RawOpaquePool::with_layout_of::<u8>();

        // Add some items.
        let mut handles = Vec::new();
        for i in 0..10 {
            handles.push(pool.insert(u8::try_from(i).unwrap()));
        }

        // Remove all items.
        for handle in handles {
            unsafe {
                pool.remove(handle);
            }
        }

        assert!(pool.is_empty());

        pool.shrink_to_fit();

        // We have white-box knowledge that an empty pool will shrink to zero.
        // This may become untrue with future algorithm changes, at which point
        // we will need to adjust the tests.
        assert_eq!(pool.capacity(), 0);
    }

    #[test]
    fn handle_provides_access_to_object() {
        let mut pool = RawOpaquePool::with_layout_of::<u64>();

        let handle = pool.insert(12345_u64);

        assert_eq!(unsafe { *handle.as_ref() }, 12345);

        // Access the value through the handle's pointer
        let ptr = handle.ptr();

        let value = unsafe { ptr.as_ref() };

        assert_eq!(*value, 12345);
    }

    #[test]
    fn shared_handles_are_copyable() {
        let mut pool = RawOpaquePool::with_layout_of::<u32>();

        let handle1 = pool.insert(789_u32).into_shared();
        let handle2 = handle1;
        #[expect(clippy::clone_on_copy, reason = "intentional, testing cloning")]
        let handle3 = handle1.clone();

        unsafe {
            assert_eq!(*handle1.as_ref(), *handle2.as_ref());
            assert_eq!(*handle1.as_ref(), *handle3.as_ref());
            assert_eq!(*handle2.as_ref(), *handle3.as_ref());
        }
    }

    #[test]
    fn multiple_removals_and_insertions() {
        let mut pool = RawOpaquePool::with_layout_of::<usize>();

        // Insert, remove, insert again to test slot reuse
        let handle1 = pool.insert(1_usize);
        unsafe {
            pool.remove(handle1);
        }

        let handle2 = pool.insert(2_usize);

        assert_eq!(pool.len(), 1);

        let value = unsafe { pool.remove_unpin(handle2) };
        assert_eq!(value, 2);
    }

    #[test]
    fn remove_with_shared_handle() {
        let mut pool = RawOpaquePool::with_layout_of::<i64>();

        let handle = pool.insert(999_i64).into_shared();

        assert_eq!(pool.len(), 1);

        unsafe {
            pool.remove(handle);
        }

        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());
    }

    #[test]
    fn remove_unpin_with_shared_handle() {
        let mut pool = RawOpaquePool::with_layout_of::<i32>();

        let handle = pool.insert(42_i32).into_shared();

        assert_eq!(pool.len(), 1);

        let value = unsafe { pool.remove_unpin(handle) };

        assert_eq!(value, 42);
        assert_eq!(pool.len(), 0);
    }

    #[test]
    #[should_panic]
    fn insert_panics_if_provided_type_with_wrong_layout() {
        let mut pool = RawOpaquePool::with_layout_of::<u32>();

        // Try to insert a u64 into a pool configured for u32
        let _handle = pool.insert(123_u64);
    }

    #[test]
    #[should_panic]
    fn insert_with_panics_if_provided_type_with_wrong_layout() {
        let mut pool = RawOpaquePool::with_layout_of::<u16>();

        // Try to insert a u32 into a pool configured for u16
        let _handle = unsafe {
            pool.insert_with(|uninit: &mut MaybeUninit<u32>| {
                uninit.write(456);
            })
        };
    }

    #[test]
    fn iter_empty_pool() {
        let pool = RawOpaquePool::with_layout_of::<u32>();

        let mut iter = pool.iter();
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert_eq!(iter.len(), 0);

        assert_eq!(iter.next(), None);
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert_eq!(iter.len(), 0);
    }

    #[test]
    fn iter_single_item() {
        let mut pool = RawOpaquePool::with_layout_of::<u32>();

        let _handle = pool.insert(42_u32);

        let mut iter = pool.iter();
        assert_eq!(iter.len(), 1);

        // First item should be the object we inserted
        let ptr = iter.next().unwrap();

        let value = unsafe { ptr.cast::<u32>().as_ref() };
        assert_eq!(*value, 42);

        // No more items
        assert_eq!(iter.next(), None);
        assert_eq!(iter.len(), 0);
    }

    #[test]
    fn iter_multiple_items_single_slab() {
        let mut pool = RawOpaquePool::with_layout_of::<u32>();

        // Insert multiple items that should fit in a single slab
        let _handle1 = pool.insert(100_u32);
        let _handle2 = pool.insert(200_u32);
        let _handle3 = pool.insert(300_u32);

        let values: Vec<u32> = pool
            .iter()
            .map(|ptr| unsafe { *ptr.cast::<u32>().as_ref() })
            .collect();

        // Should get all values in order of their slot indices
        assert_eq!(values, vec![100, 200, 300]);
    }

    #[test]
    fn iter_multiple_items_multiple_slabs() {
        let mut pool = RawOpaquePool::with_layout_of::<u8>();

        // Insert enough items to span multiple slabs
        #[expect(
            clippy::collection_is_never_read,
            reason = "handles are used for ownership"
        )]
        let mut handles = Vec::new();
        for i in 0..50 {
            handles.push(pool.insert(u8::try_from(i).unwrap()));
        }

        let values: Vec<u8> = pool
            .iter()
            .map(|ptr| unsafe { *ptr.cast::<u8>().as_ref() })
            .collect();

        // Should get all values we inserted
        assert_eq!(values.len(), 50);
        for (i, &value) in values.iter().enumerate() {
            assert_eq!(value, u8::try_from(i).unwrap());
        }
    }

    #[test]
    fn iter_with_gaps() {
        let mut pool = RawOpaquePool::with_layout_of::<u32>();

        // Insert items
        let _handle1 = pool.insert(100_u32);
        let handle2 = pool.insert(200_u32);
        let _handle3 = pool.insert(300_u32);

        // Remove the middle item to create a gap
        unsafe {
            pool.remove(handle2);
        }

        let values: Vec<u32> = pool
            .iter()
            .map(|ptr| unsafe { *ptr.cast::<u32>().as_ref() })
            .collect();

        // Should get only the remaining values
        assert_eq!(values, vec![100, 300]);
    }

    #[test]
    fn iter_with_empty_slabs() {
        let mut pool = RawOpaquePool::with_layout_of::<u64>();

        // Force creation of multiple slabs by inserting many items
        let mut handles = Vec::new();
        for i in 0_u64..20 {
            handles.push(pool.insert(i));
        }

        // Remove all items from some slabs to create empty slabs
        for handle in handles.drain(5..15) {
            unsafe {
                pool.remove(handle);
            }
        }

        let values: Vec<u64> = pool
            .iter()
            .map(|ptr| unsafe { *ptr.cast::<u64>().as_ref() })
            .collect();

        // Should get values from non-empty slabs only
        let expected: Vec<u64> = (0..5_u64).chain(15..20_u64).collect();
        assert_eq!(values, expected);
    }

    #[test]
    fn iter_size_hint() {
        let mut pool = RawOpaquePool::with_layout_of::<u32>();

        // Empty pool
        let iter = pool.iter();
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert_eq!(iter.len(), 0);

        // Add some items
        let _handle1 = pool.insert(100_u32);
        let _handle2 = pool.insert(200_u32);

        let mut iter = pool.iter();
        assert_eq!(iter.size_hint(), (2, Some(2)));
        assert_eq!(iter.len(), 2);

        // Consume one item
        let first_item = iter.next();
        assert!(first_item.is_some());
        assert_eq!(iter.size_hint(), (1, Some(1)));
        assert_eq!(iter.len(), 1);

        // Consume another
        let second_item = iter.next();
        assert!(second_item.is_some());
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert_eq!(iter.len(), 0);

        // Should be exhausted now
        assert_eq!(iter.next(), None);
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert_eq!(iter.len(), 0);
    }

    #[test]
    fn iter_double_ended_basic() {
        let mut pool = RawOpaquePool::with_layout_of::<u32>();

        // Insert items
        let _handle1 = pool.insert(100_u32);
        let _handle2 = pool.insert(200_u32);
        let _handle3 = pool.insert(300_u32);

        let mut iter = pool.iter();

        // Iterate from the back
        let last_ptr = iter.next_back().unwrap();
        let last_value = unsafe { *last_ptr.cast::<u32>().as_ref() };
        assert_eq!(last_value, 300);

        let middle_ptr = iter.next_back().unwrap();
        let middle_value = unsafe { *middle_ptr.cast::<u32>().as_ref() };
        assert_eq!(middle_value, 200);

        let first_ptr = iter.next_back().unwrap();
        let first_value = unsafe { *first_ptr.cast::<u32>().as_ref() };
        assert_eq!(first_value, 100);

        // Should be exhausted
        assert_eq!(iter.next_back(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn iter_double_ended_mixed_directions() {
        let mut pool = RawOpaquePool::with_layout_of::<u32>();

        // Insert 5 items
        let _handle1 = pool.insert(100_u32);
        let _handle2 = pool.insert(200_u32);
        let _handle3 = pool.insert(300_u32);
        let _handle4 = pool.insert(400_u32);
        let _handle5 = pool.insert(500_u32);

        let mut iter = pool.iter();
        assert_eq!(iter.len(), 5);

        // Get first from front
        let first_ptr = iter.next().unwrap();
        let first_value = unsafe { *first_ptr.cast::<u32>().as_ref() };
        assert_eq!(first_value, 100);
        assert_eq!(iter.len(), 4);

        // Get last from back
        let last_ptr = iter.next_back().unwrap();
        let last_value = unsafe { *last_ptr.cast::<u32>().as_ref() };
        assert_eq!(last_value, 500);
        assert_eq!(iter.len(), 3);

        // Get second from front
        let second_ptr = iter.next().unwrap();
        let second_value = unsafe { *second_ptr.cast::<u32>().as_ref() };
        assert_eq!(second_value, 200);
        assert_eq!(iter.len(), 2);

        // Get fourth from back
        let fourth_ptr = iter.next_back().unwrap();
        let fourth_value = unsafe { *fourth_ptr.cast::<u32>().as_ref() };
        assert_eq!(fourth_value, 400);
        assert_eq!(iter.len(), 1);

        // Get middle item
        let middle_ptr = iter.next().unwrap();
        let middle_value = unsafe { *middle_ptr.cast::<u32>().as_ref() };
        assert_eq!(middle_value, 300);
        assert_eq!(iter.len(), 0);

        // Should be exhausted
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next_back(), None);
        assert_eq!(iter.len(), 0);
    }

    #[test]
    fn iter_fused_behavior() {
        let mut pool = RawOpaquePool::with_layout_of::<u32>();

        // Test with empty pool
        let mut iter = pool.iter();
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None); // Should still be None
        assert_eq!(iter.next_back(), None);
        assert_eq!(iter.next_back(), None); // Should still be None

        // Test with some items
        let _handle1 = pool.insert(100_u32);
        let _handle2 = pool.insert(200_u32);

        let mut iter = pool.iter();

        // Consume all items
        let first = iter.next();
        assert!(first.is_some());
        let second = iter.next();
        assert!(second.is_some());

        // Now iterator should be exhausted
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None); // FusedIterator guarantee: still None
        assert_eq!(iter.next(), None); // Still None
        assert_eq!(iter.next_back(), None); // Should also be None from back
        assert_eq!(iter.next_back(), None); // Still None from back

        // Test bidirectional exhaustion
        let mut iter = pool.iter();

        // Consume from both ends until exhausted
        iter.next(); // Consume from front
        iter.next_back(); // Consume from back

        // Now should be exhausted
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next_back(), None);
        assert_eq!(iter.next(), None); // FusedIterator guarantee
        assert_eq!(iter.next_back(), None); // FusedIterator guarantee
    }

    #[test]
    fn iter_across_multiple_slabs_with_gaps() {
        let mut pool = RawOpaquePool::with_layout_of::<usize>();

        // Create a pattern: insert many items, remove some to create gaps across slabs
        let mut handles = Vec::new();
        for i in 0_usize..30 {
            handles.push(pool.insert(i));
        }

        // Remove every third item to create gaps across slabs
        let mut to_remove = Vec::new();
        for (index, _) in handles.iter().enumerate().step_by(3) {
            to_remove.push(index);
        }

        // Remove in reverse order to maintain indices
        for &index in to_remove.iter().rev() {
            unsafe {
                pool.remove(handles.swap_remove(index));
            }
        }

        let values: Vec<usize> = pool
            .iter()
            .map(|ptr| unsafe { *ptr.cast::<usize>().as_ref() })
            .collect();

        // Should get all non-removed values
        let expected: Vec<usize> = (0_usize..30).filter(|&i| i % 3 != 0).collect();
        assert_eq!(values, expected);
    }

    #[test]
    fn into_iterator_trait_works() {
        let mut pool = RawOpaquePool::with_layout_of::<u32>();

        let _handle1 = pool.insert(100_u32);
        let _handle2 = pool.insert(200_u32);
        let _handle3 = pool.insert(300_u32);

        // Test using for-in loop (which uses IntoIterator)
        let mut values = Vec::new();
        for ptr in &pool {
            let value = unsafe { *ptr.cast::<u32>().as_ref() };
            values.push(value);
        }

        assert_eq!(values, vec![100, 200, 300]);
    }
}
