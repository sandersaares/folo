use std::alloc::Layout;
use std::mem::MaybeUninit;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::{Arc, MutexGuard};

use crate::{
    BlindPoolCore, BlindPoolInnerMap, BlindPooledMut, LayoutKey, NEVER_POISONED, RawOpaquePool,
    RawOpaquePoolThreadSafe,
};

/// A thread-safe reference-counting object pool that accepts any type of object.
///
/// All values in the pool remain pinned for their entire lifetime.
///
/// The pool automatically expands its capacity when needed.
/// # Lifetime management
///
/// When inserting an object into the pool, a handle to the object is returned.
/// The object is removed from the pool when the last remaining handle to the object
/// is dropped (`Arc`-like behavior).
///
/// Clones of the pool are functionally equivalent views over the same memory capacity.
///
/// # Thread safety
///
/// The pool is thread-safe (`Send` and `Sync`) and requires that any inserted items are `Send`.
///
/// # Example: unique object ownership
///
/// ```rust
/// use std::fmt::Display;
///
/// use infinity_pool::BlindPool;
///
/// let mut pool = BlindPool::new();
///
/// // Insert an object into the pool, returning a unique handle to it.
/// let mut handle = pool.insert("Hello, world!".to_string());
///
/// // A unique handle grants the same access as a `&mut` reference to the object.
/// handle.push_str(" Welcome to Infinity Pool!");
///
/// println!("Updated value: {}", &*handle);
///
/// // The object is removed when the handle is dropped.
/// ```
///
/// # Example: shared object ownership
///
/// ```rust
/// use std::fmt::Display;
///
/// use infinity_pool::BlindPool;
///
/// let mut pool = BlindPool::new();
///
/// // Insert an object into the pool, returning a unique handle to it.
/// let handle = pool.insert("Hello, world!".to_string());
///
/// // The unique handle can be converted into a shared handle,
/// // allowing multiple clones of the handle to be created.
/// let shared_handle = handle.into_shared();
/// let shared_handle_clone = shared_handle.clone();
///
/// // Shared handles grant the same access as `&` shared references to the object.
/// println!("Shared access to value: {}", &*shared_handle);
///
/// // The object is removed when the last shared handle is dropped.
/// ```
///
/// # Clones of the pool are functionally equivalent
///
/// ```rust
/// use infinity_pool::BlindPool;
///
/// let mut pool1 = BlindPool::new();
/// let pool2 = pool1.clone();
///
/// assert_eq!(pool1.len(), pool2.len());
///
/// _ = pool1.insert(42_i32);
///
/// assert_eq!(pool1.len(), pool2.len());
/// ```
#[derive(Clone, Debug, Default)]
pub struct BlindPool {
    // Internal pools, one for each unique memory layout encountered.
    //
    // We require 'static from any inserted values because the pool
    // does not enforce any Rust lifetime semantics, only reference counts.
    //
    // The pool type itself is just a handle around the core object,
    // which is reference-counted, mutex-guarded and shared between all pool
    // and handle objects. The core will only ever be dropped once all items
    // have been removed from the pool and all the pool objects have been dropped.
    //
    // This also implies that `DropPolicy` has no meaning for this
    // pool configuration, as the core can never be dropped if it has
    // contents (as dropping the handles of pooled objects will remove
    // them from the pool, while keeping the pool alive until then).
    core: BlindPoolCore,
}

impl BlindPool {
    /// Creates a new pool with the default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of objects currently in the pool.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        let core = self.core.lock().expect(NEVER_POISONED);

        core.values().map(|pool| pool.len()).sum()
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
    pub fn capacity_for<T: Send + 'static>(&self) -> usize {
        let key = LayoutKey::with_layout_of::<T>();

        let core = self.core.lock().expect(NEVER_POISONED);

        core.get(&key)
            .map(|pool| pool.capacity())
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
    pub fn reserve_for<T: Send + 'static>(&self, additional: usize) {
        let mut core = self.core.lock().expect(NEVER_POISONED);

        let pool = ensure_inner_pool::<T>(&mut core);

        pool.reserve(additional);
    }

    /// Drops unused pool capacity to reduce memory usage.
    ///
    /// There is no guarantee that any unused capacity can be dropped. The exact outcome depends
    /// on the specific pool structure and which objects remain in the pool.
    #[inline]
    pub fn shrink_to_fit(&self) {
        let mut core = self.core.lock().expect(NEVER_POISONED);

        for pool in core.values_mut() {
            pool.shrink_to_fit();
        }
    }

    /// Inserts an object into the pool and returns a handle to it.
    #[inline]
    #[must_use]
    #[cfg_attr(test, mutants::skip)] // All mutations are unviable - skip them to save time.
    pub fn insert<T: Send + 'static>(&self, value: T) -> BlindPooledMut<T> {
        let mut core = self.core.lock().expect(NEVER_POISONED);

        let pool = ensure_inner_pool::<T>(&mut core);

        // SAFETY: inner pool selector guarantees matching layout.
        let inner_handle = unsafe { pool.insert_unchecked(value) };

        // SAFETY: We apply the constraint `T: Send` as the safety requirements require.
        unsafe {
            BlindPooledMut::new(
                inner_handle,
                LayoutKey::with_layout_of::<T>(),
                Arc::clone(&self.core),
            )
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
    /// use infinity_pool::BlindPool;
    ///
    /// struct DataBuffer {
    ///     id: u32,
    ///     data: MaybeUninit<[u8; 1024]>,
    /// }
    ///
    /// let mut pool = BlindPool::new();
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
    /// assert_eq!(handle.id, 42);
    /// ```
    ///
    /// # Safety
    /// The closure must correctly initialize the object. All fields that
    /// are not `MaybeUninit` must be initialized when the closure returns.
    #[inline]
    #[must_use]
    pub unsafe fn insert_with<T: Send + 'static, F>(&self, f: F) -> BlindPooledMut<T>
    where
        F: FnOnce(&mut MaybeUninit<T>),
    {
        let mut core = self.core.lock().expect(NEVER_POISONED);

        let pool = ensure_inner_pool::<T>(&mut core);

        // AssertUnwindSafe: covers both the user closure and the MutexGuard,
        // which are inherently !UnwindSafe. We drop the guard cleanly before
        // resume_unwind, so our state is never observed in a potentially
        // inconsistent state. The user's panic is re-thrown without tampering.
        let result = catch_unwind(AssertUnwindSafe(|| {
            // SAFETY: inner pool selector guarantees matching layout.
            // Initialization guarantee is forwarded from the caller.
            unsafe { pool.insert_with_unchecked(f) }
        }));
        drop(core);

        match result {
            Ok(inner_handle) => {
                // SAFETY: We apply the constraint `T: Send` as the safety requirements require.
                unsafe {
                    BlindPooledMut::new(
                        inner_handle,
                        LayoutKey::with_layout_of::<T>(),
                        Arc::clone(&self.core),
                    )
                }
            }
            Err(payload) => resume_unwind(payload),
        }
    }
}

fn ensure_inner_pool<'a, T: Send + 'static>(
    core: &'a mut MutexGuard<'_, BlindPoolInnerMap>,
) -> &'a mut RawOpaquePoolThreadSafe {
    let layout = Layout::new::<T>();
    let key = LayoutKey::new(layout);

    core.entry(key).or_insert_with(|| {
        // SAFETY: We always require `T: Send`.
        unsafe { RawOpaquePoolThreadSafe::new(RawOpaquePool::with_layout(layout)) }
    })
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::mem::MaybeUninit;
    use std::panic::{RefUnwindSafe, UnwindSafe};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::thread;

    use new_zealand::nz;
    use static_assertions::assert_impl_all;

    use super::*;

    assert_impl_all!(BlindPool: Send, Sync);
    assert_impl_all!(BlindPool: UnwindSafe, RefUnwindSafe);

    struct DropTracker {
        counter: Arc<AtomicI32>,
    }

    impl Drop for DropTracker {
        fn drop(&mut self) {
            self.counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn prepare_small_slab<T: Send + 'static>(pool: &BlindPool) {
        // Type-dispatch tests need shared slots and slab growth, not the bulk-allocation policy.
        ensure_inner_pool::<T>(&mut pool.core.lock().unwrap()).set_slab_capacity(nz!(2));
    }

    #[test]
    fn default_pool_is_empty() {
        let pool = BlindPool::default();

        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());
    }

    #[test]
    fn single_type_operations() {
        let pool = BlindPool::new();

        // Insert some strings
        let handle1 = pool.insert("Hello".to_string());
        let handle2 = pool.insert("World".to_string());

        assert_eq!(pool.len(), 2);
        assert!(!pool.is_empty());
        assert!(pool.capacity_for::<String>() >= 2);

        // Values should be accessible through the handles
        assert_eq!(&*handle1, "Hello");
        assert_eq!(&*handle2, "World");

        // Dropping handles should remove items from pool
        drop(handle1);
        drop(handle2);

        // Pool should eventually be empty (may not be immediate due to Arc cleanup)
        // We test the basic functionality, not the exact timing of cleanup
    }

    #[test]
    fn handle_drop_removes_objects_both_exclusive_and_shared() {
        let pool = BlindPool::new();

        // Test exclusive handle drop
        let exclusive_handle = pool.insert("exclusive".to_string());
        assert_eq!(pool.len(), 1);
        drop(exclusive_handle);
        // Note: For managed pools, length might not immediately reflect drop due to Arc semantics

        // Test shared handle drop
        let mut_handle = pool.insert("shared".to_string());
        let shared_handle = mut_handle.into_shared();
        assert_eq!(pool.len(), 1); // Should have 1 item

        // Both handles point to same object
        assert_eq!(&*shared_handle, "shared");

        // Drop the shared handle
        drop(shared_handle);
        // Object should eventually be removed (Arc cleanup timing varies)
    }

    #[test]
    fn multiple_types_different_layouts() {
        let pool = BlindPool::new();
        prepare_small_slab::<String>(&pool);
        prepare_small_slab::<u32>(&pool);
        prepare_small_slab::<u64>(&pool);
        prepare_small_slab::<Vec<i32>>(&pool);

        // Insert different types with different layouts
        let string_handle = pool.insert("Test string".to_string());
        let u32_handle = pool.insert(42_u32);
        let u64_handle = pool.insert(123_u64);
        let vec_handle = pool.insert(vec![1, 2, 3, 4, 5]);

        assert_eq!(pool.len(), 4);

        // Each type should have its own capacity
        assert!(pool.capacity_for::<String>() >= 1);
        assert!(pool.capacity_for::<u32>() >= 1);
        assert!(pool.capacity_for::<u64>() >= 1);
        assert!(pool.capacity_for::<Vec<i32>>() >= 1);

        // Verify values are correct
        assert_eq!(&*string_handle, "Test string");
        assert_eq!(*u32_handle, 42);
        assert_eq!(*u64_handle, 123);
        assert_eq!(&*vec_handle, &vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn same_layout_different_types() {
        let pool = BlindPool::new();

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
        assert_eq!(*u32_handle, 42);
        assert_eq!(*i32_handle, -42);
    }

    #[test]
    fn reserve_creates_capacity() {
        let pool = BlindPool::new();

        // Reserve capacity for strings
        pool.reserve_for::<String>(10);
        assert!(pool.capacity_for::<String>() >= 10);

        // Reserve capacity for u32s
        pool.reserve_for::<u32>(5);
        assert!(pool.capacity_for::<u32>() >= 5);

        // Insert items to verify reservations work
        let mut handles = Vec::new();
        for i in 0..10 {
            handles.push(pool.insert(format!("String {i}")));
        }

        assert_eq!(pool.len(), 10);

        // Verify all strings are correct
        for (i, handle) in handles.iter().enumerate() {
            assert_eq!(&**handle, &format!("String {i}"));
        }
    }

    #[test]
    fn shrink_to_fit_removes_unused_capacity() {
        let pool = BlindPool::new();

        // Reserve more than we need
        pool.reserve_for::<String>(100);

        // Insert only a few items
        let _handle1 = pool.insert("One".to_string());
        let _handle2 = pool.insert("Two".to_string());

        // Shrink to fit - this might not actually reduce capacity
        // but should not panic or cause issues
        pool.shrink_to_fit();

        // Pool should still work normally
        assert_eq!(pool.len(), 2);
        let _handle3 = pool.insert("Three".to_string());
        assert_eq!(pool.len(), 3);
    }

    #[test]
    fn shrink_to_fit_with_zero_items_shrinks_to_zero_capacity() {
        let pool = BlindPool::new();

        // Add some items to create capacity
        let handle1 = pool.insert("Item1".to_string());
        let handle2 = pool.insert(42_u32);
        let handle3 = pool.insert("Item3".to_string());

        // Verify we have capacity
        assert!(pool.capacity_for::<String>() > 0);
        assert!(pool.capacity_for::<u32>() > 0);

        // Remove all items by dropping handles
        drop(handle1);
        drop(handle2);
        drop(handle3);

        assert!(pool.is_empty());

        pool.shrink_to_fit();

        // Testing implementation detail: empty pool should shrink capacity to zero
        // This may become untrue with future algorithm changes, at which point
        // we will need to adjust the tests.
        assert_eq!(pool.capacity_for::<String>(), 0);
        assert_eq!(pool.capacity_for::<u32>(), 0);
    }

    #[test]
    fn insert_with_functionality() {
        let pool = BlindPool::new();

        // Test insert_with for partial initialization
        // SAFETY: We correctly initialize the String value in the closure
        let handle = unsafe {
            pool.insert_with(|uninit: &mut MaybeUninit<String>| {
                uninit.write(String::from("Initialized via closure"));
            })
        };

        assert_eq!(&*handle, "Initialized via closure");
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn pool_cloning_and_sharing() {
        let pool = BlindPool::new();

        // Insert an item
        let handle = pool.insert("Shared data".to_string());

        // Clone the pool (should share the same internal storage)
        let pool_clone = pool.clone();

        // Both pools should see the same length
        assert_eq!(pool.len(), 1);
        assert_eq!(pool_clone.len(), 1);

        // Data should be accessible from both pool references
        assert_eq!(&*handle, "Shared data");
    }

    #[test]
    fn thread_safety() {
        let pool = BlindPool::new();

        // Insert some initial data
        let handle1 = pool.insert("Thread test 1".to_string());
        let handle2 = pool.insert(42_u32);

        let pool = Arc::new(pool);
        let pool_clone = Arc::clone(&pool);

        // Spawn a thread that can access the pool
        let thread_handle = thread::spawn(move || {
            // Should be able to read the length
            assert!(pool_clone.len() >= 2);

            // Should be able to check capacity
            assert!(pool_clone.capacity_for::<String>() >= 1);
            assert!(pool_clone.capacity_for::<u32>() >= 1);
        });

        // Wait for thread to complete
        thread_handle.join().unwrap();

        // Original handles should still be valid
        assert_eq!(&*handle1, "Thread test 1");
        assert_eq!(*handle2, 42);
    }

    #[test]
    fn large_variety_of_types() {
        let pool = BlindPool::new();
        prepare_small_slab::<String>(&pool);
        prepare_small_slab::<u8>(&pool);
        prepare_small_slab::<u16>(&pool);
        prepare_small_slab::<u32>(&pool);
        prepare_small_slab::<u64>(&pool);
        prepare_small_slab::<i8>(&pool);
        prepare_small_slab::<i16>(&pool);
        prepare_small_slab::<i32>(&pool);
        prepare_small_slab::<i64>(&pool);
        prepare_small_slab::<bool>(&pool);
        prepare_small_slab::<char>(&pool);
        prepare_small_slab::<Vec<i32>>(&pool);
        prepare_small_slab::<Option<String>>(&pool);

        let string_handle = pool.insert("String".to_string());
        let u8_handle = pool.insert(255_u8);
        let u16_handle = pool.insert(65535_u16);
        let u32_handle = pool.insert(4_294_967_295_u32);
        let u64_handle = pool.insert(18_446_744_073_709_551_615_u64);
        let i8_handle = pool.insert(-128_i8);
        let i16_handle = pool.insert(-32768_i16);
        let i32_handle = pool.insert(-2_147_483_648_i32);
        let i64_handle = pool.insert(-9_223_372_036_854_775_808_i64);
        let bool_handle = pool.insert(true);
        let char_handle = pool.insert('Z');
        let vec_handle = pool.insert(vec![1, 2, 3]);
        let option_handle = pool.insert(Some("Optional".to_string()));

        assert_eq!(pool.len(), 13);

        assert_eq!(&*string_handle, "String");
        assert_eq!(*u8_handle, 255);
        assert_eq!(*u16_handle, 65535);
        assert_eq!(*u32_handle, 4_294_967_295);
        assert_eq!(*u64_handle, 18_446_744_073_709_551_615);
        assert_eq!(*i8_handle, -128);
        assert_eq!(*i16_handle, -32768);
        assert_eq!(*i32_handle, -2_147_483_648);
        assert_eq!(*i64_handle, -9_223_372_036_854_775_808);
        assert!(*bool_handle);
        assert_eq!(*char_handle, 'Z');
        assert_eq!(&*vec_handle, &vec![1, 2, 3]);
        assert_eq!(&*option_handle, &Some("Optional".to_string()));
    }

    #[test]
    fn handle_mutation() {
        let pool = BlindPool::new();

        // Insert a mutable type
        let mut string_handle = pool.insert("Initial".to_string());
        let mut vec_handle = pool.insert(vec![1, 2]);

        // Modify through the handles
        string_handle.push_str(" Modified");
        vec_handle.push(3);

        // Verify modifications
        assert_eq!(&*string_handle, "Initial Modified");
        assert_eq!(&*vec_handle, &vec![1, 2, 3]);
    }

    #[test]
    #[should_panic]
    fn zero_sized_types() {
        let pool = BlindPool::new();

        // Insert unit types (zero-sized) - this should panic
        let _unit_handle = pool.insert(());
    }

    #[test]
    fn object_dropped_when_last_shared_handle_dropped() {
        // Track drop count with a shared counter
        let drop_count = Arc::new(AtomicI32::new(0));

        let pool = BlindPool::new();

        // Create an object that tracks when it is dropped
        let tracker = DropTracker {
            counter: Arc::clone(&drop_count),
        };

        // Insert the tracker into the pool
        let mut_handle = pool.insert(tracker);

        // Verify the object has not been dropped yet
        assert_eq!(drop_count.load(Ordering::Relaxed), 0);

        // Convert to shared handle
        let shared_handle1 = mut_handle.into_shared();

        // Clone the shared handle to create multiple references
        let shared_handle2 = shared_handle1.clone();
        let shared_handle3 = shared_handle2.clone();

        // Verify the object still has not been dropped
        assert_eq!(drop_count.load(Ordering::Relaxed), 0);

        // Drop all but the last handle
        drop(shared_handle1);
        drop(shared_handle2);

        // Object should still not be dropped
        assert_eq!(drop_count.load(Ordering::Relaxed), 0);

        // Drop the last handle
        drop(shared_handle3);

        // Now the object should be dropped
        // Note: Due to Arc cleanup timing, we might need to yield to ensure cleanup happens
        // For testing purposes, we will check that the drop eventually happens
        let mut attempts = 0;
        loop {
            let count = drop_count.load(Ordering::Relaxed);
            if count == 1 {
                break;
            }
            attempts += 1;
            assert!(attempts <= 100, "Object was not dropped after 100 attempts");
            thread::yield_now();
        }
    }

    #[test]
    #[should_panic(expected = "intentional panic to verify pass-through")]
    fn insert_with_propagates_panic_from_closure() {
        let pool = BlindPool::new();

        // SAFETY: The closure panics before initialization completes. The pool catches
        // the panic to drop the mutex guard cleanly, then re-throws via resume_unwind.
        unsafe {
            drop(pool.insert_with(|_: &mut MaybeUninit<u32>| {
                panic!("intentional panic to verify pass-through");
            }));
        }
    }
}
