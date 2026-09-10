use std::alloc::Layout;
use std::mem::{MaybeUninit, size_of};

use crate::{
    DropPolicy, LayoutKey, RawBlindPoolBuilder, RawBlindPoolInnerMap, RawBlindPooled,
    RawBlindPooledMut, RawOpaquePool,
};

/// An object pool that accepts any type of object.
///
/// All values in the pool remain pinned for their entire lifetime.
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
/// use infinity_pool::RawBlindPool;
///
/// let mut pool = RawBlindPool::new();
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
/// use infinity_pool::RawBlindPool;
///
/// let mut pool = RawBlindPool::new();
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
pub struct RawBlindPool {
    /// Internal pools, one for each unique memory layout encountered.
    pools: RawBlindPoolInnerMap,

    drop_policy: DropPolicy,
}

impl RawBlindPool {
    /// Starts configuring and creating a new instance of the pool.
    #[cfg_attr(test, mutants::skip)] // Gets mutated to alternate version of itself.
    pub fn builder() -> RawBlindPoolBuilder {
        RawBlindPoolBuilder::new()
    }

    /// Creates a new pool with the default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::builder().build()
    }

    #[must_use]
    pub(crate) fn new_inner(drop_policy: DropPolicy) -> Self {
        Self {
            pools: RawBlindPoolInnerMap::new(),
            drop_policy,
        }
    }

    /// The number of objects currently in the pool.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.pools.values().map(RawOpaquePool::len).sum()
    }

    /// The total capacity of the pool for objects of type `T`.
    ///
    /// This is the maximum number of objects (including current contents) that the pool can contain
    /// without capacity extension. The pool will automatically extend its capacity if more than
    /// this many objects of type `T` are inserted.
    ///
    /// Capacity may be shared between different types of objects.
    #[must_use]
    #[inline]
    pub fn capacity_for<T>(&self) -> usize {
        self.inner_pool_of::<T>()
            .map(RawOpaquePool::capacity)
            .unwrap_or_default()
    }

    /// Whether the pool contains zero objects.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Ensures that the pool has capacity for at least `additional` more objects of type `T`.
    ///
    /// # Panics
    ///
    /// Panics if the new capacity would exceed the size of virtual memory (`usize::MAX`).
    #[inline]
    pub fn reserve_for<T>(&mut self, additional: usize) {
        self.inner_pool_of_mut::<T>().reserve(additional);
    }

    /// Drops unused pool capacity to reduce memory usage.
    ///
    /// There is no guarantee that any unused capacity can be dropped. The exact outcome depends
    /// on the specific pool structure and which objects remain in the pool.
    #[inline]
    pub fn shrink_to_fit(&mut self) {
        for pool in self.pools.values_mut() {
            pool.shrink_to_fit();
        }
    }

    /// Inserts an object into the pool and returns a handle to it.
    #[inline]
    #[cfg_attr(test, mutants::skip)] // All mutations are unviable - skip them to save time.
    pub fn insert<T: 'static>(&mut self, value: T) -> RawBlindPooledMut<T> {
        let layout = Layout::new::<T>();
        let key = LayoutKey::new(layout);

        let pool = self.inner_pool_mut(layout, key);

        // SAFETY: inner pool selector guarantees matching layout.
        let inner_handle = unsafe { pool.insert_unchecked(value) };

        RawBlindPooledMut::new(key, inner_handle)
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
    /// use infinity_pool::RawBlindPool;
    ///
    /// struct DataBuffer {
    ///     id: u32,
    ///     data: MaybeUninit<[u8; 1024]>,
    /// }
    ///
    /// let mut pool = RawBlindPool::new();
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
    /// # Safety
    /// The closure must correctly initialize the object. All fields that
    /// are not `MaybeUninit` must be initialized when the closure returns.
    #[inline]
    pub unsafe fn insert_with<T, F>(&mut self, f: F) -> RawBlindPooledMut<T>
    where
        T: 'static,
        F: FnOnce(&mut MaybeUninit<T>),
    {
        let layout = Layout::new::<T>();
        let key = LayoutKey::new(layout);

        let pool = self.inner_pool_mut(layout, key);

        // SAFETY: inner pool selector guarantees matching layout.
        // Initialization guarantee is forwarded from the caller.
        let inner_handle = unsafe { pool.insert_with_unchecked(f) };

        RawBlindPooledMut::new(key, inner_handle)
    }

    /// Removes an object from the pool, dropping the object.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that the handle is for an object currently present in this pool.
    #[inline]
    pub unsafe fn remove<T: ?Sized>(&mut self, handle: impl Into<RawBlindPooled<T>>) {
        let handle = handle.into();

        let key = handle.layout_key();

        let pool = self
            .try_inner_pool_mut(key)
            .expect("attempted to remove an item that is not present in the pool");

        // SAFETY: Forwarding safety guarantees from the caller.
        unsafe {
            pool.remove(handle.into_inner());
        }
    }

    /// Removes an object from the pool and returns the object.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that the handle is for an object currently present in this pool.
    #[must_use]
    #[inline]
    pub unsafe fn remove_unpin<T: Unpin>(&mut self, handle: impl Into<RawBlindPooled<T>>) -> T {
        const {
            assert!(
                size_of::<T>() > 0,
                "cannot extract zero-sized types from pool"
            );
        };

        let handle = handle.into();

        let key = handle.layout_key();

        let pool = self
            .try_inner_pool_mut(key)
            .expect("attempted to remove an item that is not present in the pool");

        // SAFETY: Forwarding safety guarantees from the caller.
        unsafe { pool.remove_unpin(handle.into_inner()) }
    }

    fn inner_pool_of<T>(&self) -> Option<&RawOpaquePool> {
        let key = LayoutKey::with_layout_of::<T>();

        self.pools.get(&key)
    }

    fn inner_pool_of_mut<T>(&mut self) -> &mut RawOpaquePool {
        let layout = Layout::new::<T>();
        let key = LayoutKey::new(layout);

        self.inner_pool_mut(layout, key)
    }

    fn inner_pool_mut(&mut self, layout: Layout, key: LayoutKey) -> &mut RawOpaquePool {
        self.pools.entry(key).or_insert_with(|| {
            RawOpaquePool::builder()
                .drop_policy(self.drop_policy)
                .layout(layout)
                .build()
        })
    }

    fn try_inner_pool_mut(&mut self, key: LayoutKey) -> Option<&mut RawOpaquePool> {
        self.pools.get_mut(&key)
    }
}

impl Default for RawBlindPool {
    fn default() -> Self {
        Self::new()
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
    use std::mem::MaybeUninit;
    use std::panic::{RefUnwindSafe, UnwindSafe};

    use new_zealand::nz;
    use static_assertions::{assert_impl_all, assert_not_impl_any};

    use super::*;

    // We are nominally single-threaded.
    assert_not_impl_any!(RawBlindPool: Send, Sync);
    assert_impl_all!(RawBlindPool: UnwindSafe, RefUnwindSafe);

    #[test]
    fn new_pool_is_empty() {
        let pool = RawBlindPool::new();

        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());
        assert_eq!(pool.capacity_for::<String>(), 0);
        assert_eq!(pool.capacity_for::<u32>(), 0);
    }

    #[test]
    fn default_pool_is_empty() {
        let pool = RawBlindPool::default();

        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());
    }

    #[test]
    fn instance_creation_through_builder_succeeds() {
        let pool = RawBlindPool::builder().build();

        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());
    }

    #[test]
    fn insert_and_length() {
        let mut pool = RawBlindPool::new();

        let handle = pool.insert(42_u32);

        assert_eq!(pool.len(), 1);
        assert!(!pool.is_empty());
        assert_eq!(unsafe { *handle.as_ref() }, 42);
    }

    #[test]
    fn capacity_grows_when_needed() {
        let mut pool = RawBlindPool::new();

        assert_eq!(pool.capacity_for::<String>(), 0);

        let _handle1 = pool.insert("Hello".to_string());
        let capacity_after_first = pool.capacity_for::<String>();
        assert!(capacity_after_first >= 1);

        let _handle2 = pool.insert("World".to_string());
        let capacity_after_second = pool.capacity_for::<String>();
        assert!(capacity_after_second >= 2);
    }

    #[test]
    fn reserve_creates_capacity() {
        let mut pool = RawBlindPool::new();

        pool.reserve_for::<String>(10);
        assert!(pool.capacity_for::<String>() >= 10);

        pool.reserve_for::<u32>(5);
        assert!(pool.capacity_for::<u32>() >= 5);
    }

    #[test]
    fn insert_with_closure() {
        let mut pool = RawBlindPool::new();

        let handle = unsafe {
            pool.insert_with(|uninit: &mut MaybeUninit<String>| {
                uninit.write("initialized".to_string());
            })
        };

        assert_eq!(unsafe { handle.as_ref() }, "initialized");
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn remove_decreases_length() {
        let mut pool = RawBlindPool::new();
        // Length bookkeeping needs an occupied slot, not the default bulk-allocation capacity.
        pool.inner_pool_of_mut::<u32>().set_slab_capacity(nz!(2));

        let handle = pool.insert(42_u32);
        assert_eq!(pool.len(), 1);

        // SAFETY: This live handle belongs to the pool and no references to its value remain.
        unsafe {
            pool.remove(handle);
        }
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn remove_with_shared_handle() {
        let mut pool = RawBlindPool::new();
        // Handle conversion and removal do not need the default bulk-allocation capacity.
        pool.inner_pool_of_mut::<u32>().set_slab_capacity(nz!(2));

        let handle_mut = pool.insert(42_u32);
        let handle_shared = handle_mut.into_shared();

        // SAFETY: This live handle belongs to the pool and no references to its value remain.
        unsafe {
            pool.remove(handle_shared);
        }

        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn shrink_to_fit_reduces_unused_capacity() {
        let mut pool = RawBlindPool::new();

        // Reserve more than we need
        pool.reserve_for::<String>(100);

        // Insert only a few items
        let _handle1 = pool.insert("One".to_string());
        let _handle2 = pool.insert("Two".to_string());

        // Shrink should not panic
        pool.shrink_to_fit();

        // Pool should still work normally
        assert_eq!(pool.len(), 2);
        let _handle3 = pool.insert("Three".to_string());
        assert_eq!(pool.len(), 3);
    }

    #[test]
    fn shrink_to_fit_with_zero_items_shrinks_to_zero_capacity() {
        let mut pool = RawBlindPool::new();

        // Add some items to create capacity
        let handle1 = pool.insert("Item1".to_string());
        let handle2 = pool.insert(42_u32);
        let handle3 = pool.insert("Item3".to_string());

        // Verify we have capacity
        assert!(pool.capacity_for::<String>() > 0);
        assert!(pool.capacity_for::<u32>() > 0);

        // Remove all items
        unsafe {
            pool.remove(handle1);
            pool.remove(handle2);
            pool.remove(handle3);
        }

        assert!(pool.is_empty());

        pool.shrink_to_fit();

        // Testing implementation detail: empty pool should shrink capacity to zero
        // This may become untrue with future algorithm changes, at which point
        // we will need to adjust the tests.
        assert_eq!(pool.capacity_for::<String>(), 0);
        assert_eq!(pool.capacity_for::<u32>(), 0);
    }

    #[test]
    fn handle_provides_access_to_object() {
        let mut pool = RawBlindPool::new();

        let string_handle = pool.insert("test".to_string());
        let u32_handle = pool.insert(42_u32);

        assert_eq!(unsafe { string_handle.as_ref() }, "test");
        assert_eq!(unsafe { *u32_handle.as_ref() }, 42);
    }

    #[test]
    fn shared_handles_are_copyable() {
        let mut pool = RawBlindPool::new();

        let handle_mut = pool.insert(42_u32);
        let handle1 = handle_mut.into_shared();
        let handle2 = handle1;

        assert_eq!(unsafe { *handle1.as_ref() }, 42);
        assert_eq!(unsafe { *handle2.as_ref() }, 42);
    }

    #[test]
    fn multiple_removals_and_insertions() {
        let mut pool = RawBlindPool::new();

        // Insert multiple items
        let handle1 = pool.insert("one".to_string());
        let handle2 = pool.insert(2_u32);
        let handle3 = pool.insert("three".to_string());

        assert_eq!(pool.len(), 3);

        // Remove them
        unsafe {
            pool.remove(handle2);
        }
        assert_eq!(pool.len(), 2);

        unsafe {
            pool.remove(handle1);
        }
        assert_eq!(pool.len(), 1);

        unsafe {
            pool.remove(handle3);
        }
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn remove_unpin_returns_value() {
        let mut pool = RawBlindPool::new();

        let handle = pool.insert(42_u32);
        let value = unsafe { pool.remove_unpin(handle) };

        assert_eq!(value, 42);
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn remove_unpin_with_shared_handle() {
        let mut pool = RawBlindPool::new();

        let handle_mut = pool.insert(42_u32);
        let handle_shared = handle_mut.into_shared();

        // SAFETY: Handle belongs to this pool and has not been removed
        let value = unsafe { pool.remove_unpin(handle_shared) };

        assert_eq!(value, 42);
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn multiple_types_different_layouts() {
        let mut pool = RawBlindPool::new();
        // Type dispatch needs shared slots, not the bulk-allocation capacity policy.
        pool.inner_pool_of_mut::<String>().set_slab_capacity(nz!(2));
        pool.inner_pool_of_mut::<u32>().set_slab_capacity(nz!(2));
        pool.inner_pool_of_mut::<u64>().set_slab_capacity(nz!(2));
        pool.inner_pool_of_mut::<Vec<i32>>()
            .set_slab_capacity(nz!(2));

        // Insert different types with different layouts
        let string_handle = pool.insert("test".to_string());
        let u32_handle = pool.insert(42_u32);
        let u64_handle = pool.insert(123_u64);
        let vec_handle = pool.insert(vec![1, 2, 3]);

        assert_eq!(pool.len(), 4);

        // Each type should have its own capacity
        assert!(pool.capacity_for::<String>() >= 1);
        assert!(pool.capacity_for::<u32>() >= 1);
        assert!(pool.capacity_for::<u64>() >= 1);
        assert!(pool.capacity_for::<Vec<i32>>() >= 1);

        // Verify values are correct
        assert_eq!(unsafe { string_handle.as_ref() }, "test");
        assert_eq!(unsafe { *u32_handle.as_ref() }, 42);
        assert_eq!(unsafe { *u64_handle.as_ref() }, 123);
        assert_eq!(unsafe { vec_handle.as_ref() }, &[1, 2, 3]);
    }

    #[test]
    fn same_layout_different_types() {
        let mut pool = RawBlindPool::new();

        // u32 and i32 have the same layout
        let u32_handle = pool.insert(42_u32);
        let i32_handle = pool.insert(-42_i32);

        assert_eq!(pool.len(), 2);

        // Both should share capacity since they have the same layout
        let u32_capacity = pool.capacity_for::<u32>();
        let i32_capacity = pool.capacity_for::<i32>();
        assert_eq!(u32_capacity, i32_capacity);
        assert!(u32_capacity >= 2);

        // Values should be accessible
        assert_eq!(unsafe { *u32_handle.as_ref() }, 42);
        assert_eq!(unsafe { *i32_handle.as_ref() }, -42);
    }

    #[test]
    #[should_panic]
    fn zero_sized_types() {
        let mut pool = RawBlindPool::new();

        // Insert unit types (zero-sized) - this should panic
        let _unit_handle = pool.insert(());
    }
}
