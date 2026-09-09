use std::alloc::Layout;
use std::iter::FusedIterator;
use std::mem::MaybeUninit;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};

use crate::opaque::pool_raw::RawOpaquePoolIterator;
use crate::{NEVER_POISONED, PooledMut, RawOpaquePool, RawOpaquePoolThreadSafe};

/// A thread-safe pool of reference-counted objects with uniform memory layout.
///
/// Stores objects of any `Send` type that match a [`Layout`] defined at pool creation
/// time. All values in the pool remain pinned for their entire lifetime.
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
/// use infinity_pool::OpaquePool;
///
/// let mut pool = OpaquePool::with_layout_of::<String>();
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
/// use infinity_pool::OpaquePool;
///
/// let mut pool = OpaquePool::with_layout_of::<String>();
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
/// use infinity_pool::OpaquePool;
///
/// let mut pool1 = OpaquePool::with_layout_of::<i32>();
/// let pool2 = pool1.clone();
///
/// assert_eq!(pool1.len(), pool2.len());
///
/// _ = pool1.insert(42_i32);
///
/// assert_eq!(pool1.len(), pool2.len());
/// ```
#[derive(Debug)]
pub struct OpaquePool {
    // We require 'static from any inserted values because the pool
    // does not enforce any Rust lifetime semantics, only reference counts.
    //
    // The pool type itself is just a handle around the inner pool,
    // which is reference-counted and mutex-guarded. The inner pool
    // will only ever be dropped once all items have been removed from
    // it and no more `OpaquePool` instances exist that point to it.
    //
    // This also implies that `DropPolicy` has no meaning for this
    // pool configuration, as the pool can never be dropped if it has
    // contents (as dropping the handles of pooled objects will remove
    // them from the pool, while keeping the pool alive until then).
    inner: Arc<Mutex<RawOpaquePoolThreadSafe>>,
}

impl OpaquePool {
    /// Creates a new instance of the pool with the specified layout.
    ///
    /// Shorthand for a builder that keeps all other options at their default values.
    ///
    /// # Panics
    ///
    /// Panics if the layout is zero-sized.
    #[must_use]
    pub fn with_layout(object_layout: Layout) -> Self {
        let inner = RawOpaquePool::with_layout(object_layout);

        // SAFETY: All insertion methods require `T: Send`.
        let inner = unsafe { RawOpaquePoolThreadSafe::new(inner) };

        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// Creates a new instance of the pool with the layout of `T`.
    ///
    /// Shorthand for a builder that keeps all other options at their default values.
    #[must_use]
    pub fn with_layout_of<T: Sized + Send>() -> Self {
        const {
            assert!(
                size_of::<T>() > 0,
                "cannot create a pool of zero-sized objects"
            );
        };

        Self::with_layout(Layout::new::<T>())
    }

    /// The layout of objects stored in this pool.
    ///
    /// All inserted objects must match this layout.
    #[must_use]
    #[inline]
    pub fn object_layout(&self) -> Layout {
        self.inner.lock().expect(NEVER_POISONED).object_layout()
    }

    /// The number of objects currently in the pool.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.lock().expect(NEVER_POISONED).len()
    }

    /// The total capacity of the pool.
    ///
    /// This is the maximum number of objects (including current contents) that the pool can contain
    /// without capacity extension. The pool will automatically extend its capacity if more than
    /// this many objects are inserted.
    #[must_use]
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.lock().expect(NEVER_POISONED).capacity()
    }

    /// Whether the pool contains zero objects.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.lock().expect(NEVER_POISONED).is_empty()
    }

    /// Ensures that the pool has capacity for at least `additional` more objects.
    ///
    /// # Panics
    ///
    /// Panics if the new capacity would exceed the size of virtual memory (`usize::MAX`).
    #[inline]
    pub fn reserve(&self, additional: usize) {
        self.inner.lock().expect(NEVER_POISONED).reserve(additional);
    }

    /// Drops unused pool capacity to reduce memory usage.
    ///
    /// There is no guarantee that any unused capacity can be dropped. The exact outcome depends
    /// on the specific pool structure and which objects remain in the pool.
    #[inline]
    pub fn shrink_to_fit(&self) {
        self.inner.lock().expect(NEVER_POISONED).shrink_to_fit();
    }

    /// Inserts an object into the pool and returns a handle to it.
    ///
    /// # Panics
    /// Panics if the layout of `T` does not match the object layout of the pool.
    #[inline]
    #[must_use]
    #[cfg_attr(test, mutants::skip)] // All mutations are unviable - skip them to save time.
    pub fn insert<T: Send + 'static>(&self, value: T) -> PooledMut<T> {
        let inner = self.inner.lock().expect(NEVER_POISONED).insert(value);

        // SAFETY: We apply the constraint `T: Send` as the safety requirements require.
        unsafe { PooledMut::new(inner, Arc::clone(&self.inner)) }
    }

    /// Inserts an object into the pool and returns a handle to it.
    /// # Safety
    /// The caller must ensure that the layout of `T` matches the pool's object layout.
    #[inline]
    #[must_use]
    pub unsafe fn insert_unchecked<T: Send + 'static>(&self, value: T) -> PooledMut<T> {
        // SAFETY: Forwarding safety guarantees from caller.
        let inner = unsafe {
            self.inner
                .lock()
                .expect(NEVER_POISONED)
                .insert_unchecked(value)
        };

        // SAFETY: We apply the constraint `T: Send` as the safety requirements require.
        unsafe { PooledMut::new(inner, Arc::clone(&self.inner)) }
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
    /// use infinity_pool::OpaquePool;
    ///
    /// struct DataBuffer {
    ///     id: u32,
    ///     data: MaybeUninit<[u8; 1024]>,
    /// }
    ///
    /// let mut pool = OpaquePool::with_layout_of::<DataBuffer>();
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
    /// # Panics
    /// Panics if the layout of `T` does not match the object layout of the pool.
    ///
    /// # Safety
    /// The closure must correctly initialize the object. All fields that
    /// are not `MaybeUninit` must be initialized when the closure returns.
    #[inline]
    #[must_use]
    pub unsafe fn insert_with<T, F>(&self, f: F) -> PooledMut<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut MaybeUninit<T>),
    {
        let mut inner = self.inner.lock().expect(NEVER_POISONED);

        // AssertUnwindSafe: covers both the user closure and the MutexGuard,
        // which are inherently !UnwindSafe. We drop the guard cleanly before
        // resume_unwind, so our state is never observed in a potentially
        // inconsistent state. The user's panic is re-thrown without tampering.
        let result = catch_unwind(AssertUnwindSafe(|| {
            // SAFETY: Forwarding safety guarantees from caller.
            unsafe { inner.insert_with(f) }
        }));
        drop(inner);

        match result {
            Ok(inner) => {
                // SAFETY: We apply the constraint `T: Send` as the safety requirements require.
                unsafe { PooledMut::new(inner, Arc::clone(&self.inner)) }
            }
            Err(payload) => resume_unwind(payload),
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
    /// This unchecked variant of the method skips the layout verification step, requiring
    /// the caller to ensure that the object has a matching layout with the pool.
    ///
    /// # Safety
    /// The caller must ensure that the layout of `T` matches the pool's object layout.
    ///
    /// The closure must correctly initialize the object. All fields that
    /// are not `MaybeUninit` must be initialized when the closure returns.
    #[inline]
    #[must_use]
    pub unsafe fn insert_with_unchecked<T, F>(&self, f: F) -> PooledMut<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut MaybeUninit<T>),
    {
        let mut inner = self.inner.lock().expect(NEVER_POISONED);

        // AssertUnwindSafe: covers both the user closure and the MutexGuard,
        // which are inherently !UnwindSafe. We drop the guard cleanly before
        // resume_unwind, so our state is never observed in a potentially
        // inconsistent state. The user's panic is re-thrown without tampering.
        let result = catch_unwind(AssertUnwindSafe(|| {
            // SAFETY: Forwarding safety guarantees from caller.
            unsafe { inner.insert_with_unchecked(f) }
        }));
        drop(inner);

        match result {
            Ok(inner) => {
                // SAFETY: We apply the constraint `T: Send` as the safety requirements require.
                unsafe { PooledMut::new(inner, Arc::clone(&self.inner)) }
            }
            Err(payload) => resume_unwind(payload),
        }
    }

    /// Calls a closure with an iterator over all objects in the pool.
    ///
    /// The iterator only yields pointers to the objects, not references, because the pool
    /// does not have the authority to create references to its contents as user code may
    /// concurrently be holding a conflicting exclusive reference via `PooledMut<T>`.
    ///
    /// Therefore, obtaining actual references to pool contents via iteration is only possible
    /// by using the pointer to create such references in unsafe code and relies on the caller
    /// guaranteeing that no conflicting exclusive references exist.
    ///
    /// The pool is locked for the entire duration of the closure, ensuring that objects
    /// cannot be removed while iteration is in progress. This guarantees that all pointers
    /// yielded by the iterator remain valid for the duration of the closure.
    ///
    /// # Examples
    ///
    /// ```
    /// # use infinity_pool::OpaquePool;
    /// let mut pool = OpaquePool::with_layout_of::<u32>();
    /// let _handle1 = pool.insert(42u32);
    /// let _handle2 = pool.insert(100u32);
    ///
    /// let values: Vec<u32> = pool.with_iter(|iter| {
    ///     // SAFETY: We ensure that no conflicting references to the pooled objects
    ///     // exist. Simply look up - we just inserted the values, so there is nothing
    ///     // else that could have a conflicting exclusive reference to them.
    ///     iter.map(|ptr| unsafe { *ptr.cast::<u32>().as_ref() })
    ///         .collect()
    /// });
    ///
    /// assert_eq!(values.iter().sum::<u32>(), 142);
    /// ```
    pub fn with_iter<F, R>(&self, f: F) -> R
    where
        F: FnOnce(OpaquePoolIterator<'_>) -> R,
    {
        let guard = self.inner.lock().expect(NEVER_POISONED);
        let iter = OpaquePoolIterator::new(&guard);
        // AssertUnwindSafe: covers both the user closure and `iter` (which
        // borrows from a MutexGuard and is therefore inherently !UnwindSafe).
        // We drop the guard cleanly before resume_unwind, so our state is never
        // observed in a potentially inconsistent state.
        let result = catch_unwind(AssertUnwindSafe(|| f(iter)));
        drop(guard);

        match result {
            Ok(value) => value,
            Err(payload) => resume_unwind(payload),
        }
    }
}

impl Clone for OpaquePool {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Iterator over all objects in an opaque pool.
///
/// The iterator only yields pointers to the objects, not references, because the pool
/// does not have the authority to create references to its contents as user code may
/// concurrently be holding a conflicting exclusive reference via `PooledMut<T>`.
///
/// Therefore, obtaining actual references to pool contents via iteration is only possible
/// by using the pointer to create such references in unsafe code and relies on the caller
/// guaranteeing that no conflicting exclusive references exist.
///
/// The iterator holds a reference to the locked pool, ensuring that objects cannot be
/// removed while iteration is in progress and that all yielded pointers remain valid.
#[derive(Debug)]
pub struct OpaquePoolIterator<'p> {
    raw_iter: RawOpaquePoolIterator<'p>,
}

impl<'p> OpaquePoolIterator<'p> {
    fn new(pool: &'p RawOpaquePoolThreadSafe) -> Self {
        Self {
            raw_iter: pool.iter(),
        }
    }
}

impl Iterator for OpaquePoolIterator<'_> {
    type Item = NonNull<()>;

    fn next(&mut self) -> Option<Self::Item> {
        self.raw_iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.raw_iter.size_hint()
    }
}

impl DoubleEndedIterator for OpaquePoolIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.raw_iter.next_back()
    }
}

impl ExactSizeIterator for OpaquePoolIterator<'_> {
    fn len(&self) -> usize {
        self.raw_iter.len()
    }
}

impl FusedIterator for OpaquePoolIterator<'_> {}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::panic::{RefUnwindSafe, UnwindSafe};

    use new_zealand::nz;
    use static_assertions::{assert_impl_all, assert_not_impl_any};

    use super::*;

    assert_impl_all!(OpaquePool: Send, Sync);
    assert_impl_all!(OpaquePool: UnwindSafe, RefUnwindSafe);

    assert_impl_all!(OpaquePoolIterator<'_>: Iterator, DoubleEndedIterator, ExactSizeIterator, FusedIterator);
    assert_not_impl_any!(OpaquePoolIterator<'_>: Send, Sync);
    assert_impl_all!(OpaquePoolIterator<'_>: UnwindSafe, RefUnwindSafe);

    #[test]
    fn new_pool_with_layout_of_is_empty() {
        let pool = OpaquePool::with_layout_of::<u64>();

        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());
        assert_eq!(pool.capacity(), 0);
        assert_eq!(pool.object_layout(), Layout::new::<u64>());
    }

    #[test]
    fn new_pool_with_layout_is_empty() {
        let layout = Layout::new::<i64>();
        let pool = OpaquePool::with_layout(layout);

        assert_eq!(pool.object_layout(), layout);
        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());
        assert_eq!(pool.capacity(), 0);
    }

    #[test]
    fn insert_and_length() {
        let pool = OpaquePool::with_layout_of::<u32>();

        let _handle1 = pool.insert(42_u32);
        assert_eq!(pool.len(), 1);
        assert!(!pool.is_empty());

        let _handle2 = pool.insert(100_u32);
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn capacity_grows_when_needed() {
        let pool = OpaquePool::with_layout_of::<u64>();
        // Keep multiple live slots before crossing the slab boundary.
        pool.inner.lock().unwrap().set_slab_capacity(nz!(2));

        assert_eq!(pool.capacity(), 0);

        let _handle = pool.insert(123_u64);

        // Should have some capacity now
        assert!(pool.capacity() > 0);
        let initial_capacity = pool.capacity();

        // Fill up the pool to force capacity expansion
        #[expect(
            clippy::collection_is_never_read,
            reason = "handles are used for ownership"
        )]
        let mut handles = Vec::new();
        for i in 1..initial_capacity {
            handles.push(pool.insert(i as u64));
        }

        // One more insert should expand capacity
        let _handle = pool.insert(999_u64);

        assert!(pool.capacity() > initial_capacity);
    }

    #[test]
    fn reserve_creates_capacity() {
        let pool = OpaquePool::with_layout_of::<u8>();

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
        let pool = OpaquePool::with_layout_of::<u64>();

        // SAFETY: we correctly initialize the slot.
        let handle = unsafe {
            pool.insert_with(|uninit: &mut MaybeUninit<u64>| {
                uninit.write(42);
            })
        };

        assert_eq!(pool.len(), 1);
        assert_eq!(*handle, 42);
    }

    #[test]
    fn shrink_to_fit_removes_unused_capacity() {
        let pool = OpaquePool::with_layout_of::<u8>();

        // Reserve more than we need
        pool.reserve(100);

        // Insert only a few items
        let _handle1 = pool.insert(1_u8);
        let _handle2 = pool.insert(2_u8);

        // Shrink should not panic
        pool.shrink_to_fit();

        // Pool should still work normally
        assert_eq!(pool.len(), 2);
        let _handle3 = pool.insert(3_u8);
        assert_eq!(pool.len(), 3);
    }

    #[test]
    fn shrink_to_fit_with_zero_items_shrinks_to_zero_capacity() {
        let pool = OpaquePool::with_layout_of::<u8>();

        // Add some items to create capacity
        let handle1 = pool.insert(1_u8);
        let handle2 = pool.insert(2_u8);
        let handle3 = pool.insert(3_u8);

        // Verify we have capacity
        assert!(pool.capacity() > 0);

        // Remove all items by dropping handles
        drop(handle1);
        drop(handle2);
        drop(handle3);

        assert!(pool.is_empty());

        pool.shrink_to_fit();

        // Testing implementation detail: empty pool should shrink capacity to zero
        // This may become untrue with future algorithm changes, at which point
        // we will need to adjust the tests.
        assert_eq!(pool.capacity(), 0);
    }

    #[test]
    fn handle_provides_access_to_object() {
        let pool = OpaquePool::with_layout_of::<u64>();

        let handle = pool.insert(12345_u64);

        assert_eq!(*handle, 12345);
    }

    #[test]
    fn multiple_handles_to_same_type() {
        let pool = OpaquePool::with_layout_of::<String>();

        let handle1 = pool.insert("hello".to_string());
        let handle2 = pool.insert("world".to_string());

        assert_eq!(pool.len(), 2);

        assert_eq!(&*handle1, "hello");
        assert_eq!(&*handle2, "world");

        // Dropping handles should remove items from pool
        drop(handle1);
        assert_eq!(pool.len(), 1);

        drop(handle2);
        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());
    }

    #[test]
    fn handle_drop_removes_objects_both_exclusive_and_shared() {
        let pool = OpaquePool::with_layout_of::<String>();

        // Test exclusive handle drop
        let exclusive_handle = pool.insert("exclusive".to_string());
        assert_eq!(pool.len(), 1);
        drop(exclusive_handle);
        assert_eq!(pool.len(), 0);

        // Test shared handle drop
        let mut_handle = pool.insert("shared".to_string());
        let shared_handle = mut_handle.into_shared();
        assert_eq!(pool.len(), 1);

        // Verify shared handle works
        assert_eq!(&*shared_handle, "shared");

        // Drop the shared handle should remove from pool
        drop(shared_handle);
        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());
    }

    #[test]
    fn iter_empty_pool() {
        let pool = OpaquePool::with_layout_of::<u32>();

        pool.with_iter(|mut iter| {
            assert_eq!(iter.size_hint(), (0, Some(0)));
            assert_eq!(iter.len(), 0);

            assert_eq!(iter.next(), None);
            assert_eq!(iter.size_hint(), (0, Some(0)));
            assert_eq!(iter.len(), 0);
        });
    }

    #[test]
    fn iter_single_item() {
        let pool = OpaquePool::with_layout_of::<u32>();

        let _handle = pool.insert(42_u32);

        pool.with_iter(|mut iter| {
            assert_eq!(iter.len(), 1);

            // First item should be the object we inserted
            let ptr = iter.next().unwrap();

            // SAFETY: We know this points to a u32 we just inserted
            let value = unsafe { ptr.cast::<u32>().as_ref() };
            assert_eq!(*value, 42);

            // No more items
            assert_eq!(iter.next(), None);
            assert_eq!(iter.len(), 0);
        });
    }

    #[test]
    fn iter_multiple_items() {
        let pool = OpaquePool::with_layout_of::<u32>();

        let _handle1 = pool.insert(100_u32);
        let _handle2 = pool.insert(200_u32);
        let _handle3 = pool.insert(300_u32);

        pool.with_iter(|iter| {
            let values: Vec<u32> = iter
                .map(|ptr| {
                    // SAFETY: We know these point to u32s we inserted
                    unsafe { *ptr.cast::<u32>().as_ref() }
                })
                .collect();

            assert_eq!(values, vec![100, 200, 300]);
        });
    }

    #[test]
    fn iter_double_ended_basic() {
        let pool = OpaquePool::with_layout_of::<u32>();

        let _handle1 = pool.insert(100_u32);
        let _handle2 = pool.insert(200_u32);
        let _handle3 = pool.insert(300_u32);

        pool.with_iter(|mut iter| {
            // Iterate from the back
            let last_ptr = iter.next_back().unwrap();
            // SAFETY: We know this points to a u32 we inserted
            let last_value = unsafe { *last_ptr.cast::<u32>().as_ref() };
            assert_eq!(last_value, 300);

            let middle_ptr = iter.next_back().unwrap();
            // SAFETY: We know this points to a u32 we inserted
            let middle_value = unsafe { *middle_ptr.cast::<u32>().as_ref() };
            assert_eq!(middle_value, 200);

            let first_ptr = iter.next().unwrap();
            // SAFETY: We know this points to a u32 we inserted
            let first_value = unsafe { *first_ptr.cast::<u32>().as_ref() };
            assert_eq!(first_value, 100);

            // Should be exhausted now
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next_back(), None);
        });
    }

    #[test]
    fn with_iter_scoped_access() {
        let pool = OpaquePool::with_layout_of::<u32>();

        let _handle1 = pool.insert(100_u32);
        let _handle2 = pool.insert(200_u32);
        let _handle3 = pool.insert(300_u32);

        // Test that we can use the iterator in a scoped manner
        let result = pool.with_iter(|iter| {
            let mut values = Vec::new();
            for ptr in iter {
                // SAFETY: We know these point to u32s we just inserted
                let value = unsafe { *ptr.cast::<u32>().as_ref() };
                values.push(value);
            }
            values
        });

        assert_eq!(result, vec![100, 200, 300]);
    }

    #[test]
    fn with_iter_holds_lock() {
        let pool = OpaquePool::with_layout_of::<u32>();

        let _handle1 = pool.insert(100_u32);
        let _handle2 = pool.insert(200_u32);

        // Test that iteration sees a consistent view
        pool.with_iter(|iter| {
            assert_eq!(iter.len(), 2);

            let values: Vec<u32> = iter
                .map(|ptr| {
                    // SAFETY: We know these point to u32s we inserted
                    unsafe { *ptr.cast::<u32>().as_ref() }
                })
                .collect();

            assert_eq!(values, vec![100, 200]);
        });

        // After the scope, we can modify the pool again
        let _handle3 = pool.insert(300_u32);

        // A new with_iter call should see all 3 items
        pool.with_iter(|iter| {
            assert_eq!(iter.len(), 3);
        });
    }

    #[test]
    fn iter_size_hint_and_exact_size() {
        let pool = OpaquePool::with_layout_of::<u32>();

        // Empty pool
        pool.with_iter(|iter| {
            assert_eq!(iter.size_hint(), (0, Some(0)));
            assert_eq!(iter.len(), 0);
        });

        // Add some items
        let _handle1 = pool.insert(100_u32);
        let _handle2 = pool.insert(200_u32);

        pool.with_iter(|mut iter| {
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
        });
    }

    #[test]
    fn clone_behavior() {
        let pool1 = OpaquePool::with_layout_of::<u32>();

        // Clone the pool handle
        let pool2 = pool1.clone();

        // Both should have the same object layout
        assert_eq!(pool1.object_layout(), pool2.object_layout());

        // Insert via first handle
        let _handle1 = pool1.insert(100_u32);

        // Second handle should see the same pool state
        assert_eq!(pool2.len(), 1);
        assert!(!pool2.is_empty());

        // Insert via second handle
        let _handle2 = pool2.insert(200_u32);

        // First handle should see the updated state
        assert_eq!(pool1.len(), 2);

        // Both handles should see the objects via iteration
        pool1.with_iter(|iter| {
            let values: Vec<u32> = iter
                // SAFETY: We know these point to u32s we inserted
                .map(|ptr| unsafe { *ptr.cast::<u32>().as_ref() })
                .collect();
            assert_eq!(values, vec![100, 200]);
        });

        pool2.with_iter(|iter| {
            let values: Vec<u32> = iter
                // SAFETY: We know these point to u32s we inserted
                .map(|ptr| unsafe { *ptr.cast::<u32>().as_ref() })
                .collect();
            assert_eq!(values, vec![100, 200]);
        });
    }

    #[test]
    fn lifecycle_management_pool_keeps_inner_alive() {
        let pool = OpaquePool::with_layout_of::<String>();

        // Insert an object and get a handle
        let handle = pool.insert("test data".to_string());

        // Clone the pool before dropping the original
        let pool_clone = pool.clone();

        // Drop the original pool
        drop(pool);

        // The handle should still be valid because pool_clone keeps the inner pool alive
        assert_eq!(&*handle, "test data");

        // We should still be able to access pool operations through the clone
        assert_eq!(pool_clone.len(), 1);
        assert!(!pool_clone.is_empty());

        // Dropping the handle should remove the object
        drop(handle);

        // Pool clone should reflect the removal
        assert_eq!(pool_clone.len(), 0);
        assert!(pool_clone.is_empty());
    }

    #[test]
    fn lifecycle_management_handles_keep_pool_alive() {
        let handle = {
            let pool = OpaquePool::with_layout_of::<String>();
            // Pool goes out of scope here, but handle should keep inner pool alive
            pool.insert("persistent data".to_string())
        };

        // Handle should still be valid and accessible
        assert_eq!(&*handle, "persistent data");

        // Even though the original pool is dropped, the object remains accessible
        // This tests that PooledMut holds a reference to the inner pool
        drop(handle);
        // Object is cleaned up when handle is dropped
    }

    #[test]
    fn concurrent_access_basic() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let pool = Arc::new(Mutex::new(OpaquePool::with_layout_of::<u32>()));
        let barrier = Arc::new(Barrier::new(3));

        let mut handles = vec![];

        // Spawn threads that insert concurrently
        for i in 0..3 {
            let pool_clone = Arc::clone(&pool);
            let barrier_clone = Arc::clone(&barrier);

            let handle = thread::spawn(move || {
                barrier_clone.wait();

                let value = (i + 1) * 100;
                let handle = {
                    let pool_guard = pool_clone.lock().unwrap();
                    pool_guard.insert(value)
                };

                // Return the handle and the value we inserted
                (handle, value)
            });

            handles.push(handle);
        }

        // Collect results and keep the pooled handles alive
        let mut pooled_handles = vec![];
        for handle in handles {
            let (pooled_handle, value) = handle.join().unwrap();
            assert_eq!(*pooled_handle, value);
            pooled_handles.push(pooled_handle);
        }

        // Verify final pool state
        {
            let pool_guard = pool.lock().unwrap();
            assert_eq!(pool_guard.len(), 3);

            let mut values: Vec<u32> = pool_guard.with_iter(|iter| {
                iter
                    // SAFETY: We know these point to u32s we inserted
                    .map(|ptr| unsafe { *ptr.cast::<u32>().as_ref() })
                    .collect()
            });

            values.sort_unstable();
            assert_eq!(values, vec![100, 200, 300]);
        }

        // Clean up by dropping the handles
        drop(pooled_handles);
    }

    #[test]
    fn pooled_mut_integration() {
        let pool = OpaquePool::with_layout_of::<String>();

        // Test that PooledMut works correctly with OpaquePool
        let mut handle = pool.insert("initial".to_string());

        // Test deref access
        assert_eq!(&*handle, "initial");

        // Test mutable access
        handle.push_str(" value");
        assert_eq!(&*handle, "initial value");

        // Test that the pool sees the mutation
        assert_eq!(pool.len(), 1);

        // Test that iteration sees the mutated value
        pool.with_iter(|iter| {
            let values: Vec<String> = iter
                // SAFETY: We know these point to Strings we inserted
                .map(|ptr| unsafe { ptr.cast::<String>().as_ref().clone() })
                .collect();
            assert_eq!(values, vec!["initial value"]);
        });

        // Test that dropping the handle removes from pool
        drop(handle);
        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());
    }

    #[test]
    fn multiple_handles_to_different_objects() {
        let pool = OpaquePool::with_layout_of::<u32>();

        // Insert multiple objects
        let handle1 = pool.insert(100_u32);
        let handle2 = pool.insert(200_u32);
        let handle3 = pool.insert(300_u32);

        assert_eq!(pool.len(), 3);

        // All handles should be independently accessible
        assert_eq!(*handle1, 100);
        assert_eq!(*handle2, 200);
        assert_eq!(*handle3, 300);

        // Drop one handle
        drop(handle2);
        assert_eq!(pool.len(), 2);

        // Remaining handles should still work
        assert_eq!(*handle1, 100);
        assert_eq!(*handle3, 300);

        // Pool should reflect the removal
        pool.with_iter(|iter| {
            let mut values: Vec<u32> = iter
                // SAFETY: We know these point to u32s we inserted
                .map(|ptr| unsafe { *ptr.cast::<u32>().as_ref() })
                .collect();
            values.sort_unstable();
            assert_eq!(values, vec![100, 300]);
        });

        drop(handle1);
        drop(handle3);
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn insert_methods_with_lifecycle() {
        let pool = OpaquePool::with_layout_of::<String>();

        // Test regular insert
        let handle1 = pool.insert("regular".to_string());
        assert_eq!(pool.len(), 1);

        // Test unsafe insert_unchecked
        // SAFETY: String layout matches the pool's object layout
        let handle2 = unsafe { pool.insert_unchecked("unchecked".to_string()) };
        assert_eq!(pool.len(), 2);

        // Test insert_with
        // SAFETY: We properly initialize the string value
        let handle3 = unsafe {
            pool.insert_with(|uninit| {
                uninit.write("with_closure".to_string());
            })
        };
        assert_eq!(pool.len(), 3);

        // Test insert_with_unchecked
        // SAFETY: String layout matches the pool and we properly initialize the value
        let handle4 = unsafe {
            pool.insert_with_unchecked(|uninit| {
                uninit.write("with_closure_unchecked".to_string());
            })
        };
        assert_eq!(pool.len(), 4);

        // Verify all values
        assert_eq!(&*handle1, "regular");
        assert_eq!(&*handle2, "unchecked");
        assert_eq!(&*handle3, "with_closure");
        assert_eq!(&*handle4, "with_closure_unchecked");

        // Test cleanup
        drop(handle1);
        assert_eq!(pool.len(), 3);

        drop(handle2);
        drop(handle3);
        drop(handle4);
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn pool_operations_with_arc_semantics() {
        let pool1 = OpaquePool::with_layout_of::<u32>();

        // Insert some initial data
        let _handle1 = pool1.insert(100_u32);
        let _handle2 = pool1.insert(200_u32);

        // Clone the pool
        let pool2 = pool1.clone();

        // Test capacity operations on both handles
        assert_eq!(pool1.capacity(), pool2.capacity());

        let initial_capacity = pool1.capacity();
        pool1.reserve(10);

        // Both should see the capacity change (note: reserve is a minimum, actual capacity may be higher)
        assert!(pool1.capacity() >= initial_capacity);
        assert_eq!(pool1.capacity(), pool2.capacity());

        // Test shrink_to_fit
        pool2.shrink_to_fit();
        assert_eq!(pool1.capacity(), pool2.capacity());

        // Both should see the same length and state
        assert_eq!(pool1.len(), pool2.len());
        assert_eq!(pool1.is_empty(), pool2.is_empty());
        assert_eq!(pool1.object_layout(), pool2.object_layout());
    }

    #[test]
    fn into_inner_removes_and_returns_value() {
        let pool = OpaquePool::with_layout_of::<String>();

        // Insert an item into the pool
        let mut handle = pool.insert("initial value".to_string());
        assert_eq!(pool.len(), 1);
        assert_eq!(&*handle, "initial value");

        // Modify the value while it is in the pool
        handle.push_str(" - modified");
        assert_eq!(&*handle, "initial value - modified");
        assert_eq!(pool.len(), 1);

        // Extract the value from the pool using into_inner()
        let extracted_value = handle.into_inner();

        // Verify the extracted value is correct
        assert_eq!(extracted_value, "initial value - modified");

        // Verify the pool is now empty (item was removed)
        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());

        // The caller now owns the value and can continue using it
        let final_value = extracted_value + " - after extraction";
        assert_eq!(final_value, "initial value - modified - after extraction");
    }

    #[test]
    fn pool_operations_work_with_shared_references() {
        // Test that all insertion methods work with &self (shared references)
        let pool = OpaquePool::with_layout_of::<String>();

        // Test basic insert
        let handle1 = pool.insert("hello".to_string());
        assert_eq!(pool.len(), 1);
        assert_eq!(&*handle1, "hello");

        // Test insert_with
        // SAFETY: We properly initialize the value in the closure.
        let handle2 = unsafe {
            pool.insert_with(|uninit| {
                uninit.write("world".to_string());
            })
        };
        assert_eq!(pool.len(), 2);
        assert_eq!(&*handle2, "world");

        // Test insert_unchecked
        // SAFETY: String layout matches the expected layout for this pool.
        let handle3 = unsafe { pool.insert_unchecked("test".to_string()) };
        assert_eq!(pool.len(), 3);
        assert_eq!(&*handle3, "test");

        // Test insert_with_unchecked
        // SAFETY: We properly initialize the value in the closure.
        let handle4 = unsafe {
            pool.insert_with_unchecked(|uninit| {
                uninit.write("unchecked".to_string());
            })
        };
        assert_eq!(pool.len(), 4);
        assert_eq!(&*handle4, "unchecked");

        // Test reserve and shrink_to_fit work with shared references
        pool.reserve(10);
        pool.shrink_to_fit();

        // Clean up
        drop(handle1);
        drop(handle2);
        drop(handle3);
        drop(handle4);
        assert_eq!(pool.len(), 0);
    }

    #[test]
    #[should_panic(expected = "intentional panic to verify pass-through")]
    fn insert_with_propagates_panic_from_closure() {
        let pool = OpaquePool::with_layout_of::<u32>();

        // SAFETY: The closure panics before initialization completes. The pool catches
        // the panic to drop the mutex guard cleanly, then re-throws via resume_unwind.
        unsafe {
            drop(pool.insert_with(|_: &mut MaybeUninit<u32>| {
                panic!("intentional panic to verify pass-through");
            }));
        }
    }

    #[test]
    #[should_panic(expected = "intentional panic to verify pass-through")]
    fn insert_with_unchecked_propagates_panic_from_closure() {
        let pool = OpaquePool::with_layout_of::<u32>();

        // SAFETY: The closure panics before initialization completes, and the layout
        // matches. The pool catches the panic and re-throws via resume_unwind.
        unsafe {
            drop(pool.insert_with_unchecked(|_: &mut MaybeUninit<u32>| {
                panic!("intentional panic to verify pass-through");
            }));
        }
    }

    #[test]
    #[should_panic(expected = "intentional panic to verify pass-through")]
    fn with_iter_propagates_panic_from_closure() {
        let pool = OpaquePool::with_layout_of::<u32>();
        let _handle = pool.insert(42_u32);

        pool.with_iter(|_iter| {
            panic!("intentional panic to verify pass-through");
        });
    }
}
