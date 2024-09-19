// Copyright (c) Microsoft Corporation.

use std::any::Any;
use std::cell::{Ref, RefCell, RefMut};
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use hash_hasher::HashedMap;
use negative_impl::negative_impl;
use xxhash_rust::xxh3::xxh3_64;

use super::{ReadStorage, StorageHandle, WithData, WriteStorage};

/// A storage pattern where every clone of the storage object references a single per-thread value initialized
/// from a global initial value, with no relation between threads. The same per-thread value may be accessed
/// from any storage object. The per-thread value is dropped when the last storage object referencing
/// it is dropped.
///
/// This differs from [`ThreadLocalInlineStorage`][super::ThreadLocalInlineStorage] by keeping the thread-value map in a per-thread
/// structure shared by all boxes instead of in a shared per-box-family structure.
///
/// This type is intended to support an object-based storage mechanism, which has some overhead compared to the
/// variable-based [`LocalKey`][std::thread::LocalKey]. If `LocalKey` is sufficient, you may get better performance by using it.
///
/// # Concurrency
///
/// Each thread accesses independent data - there is no relation between data on different threads.
///
/// It is possible to create multiple storage instances in a single thread, pointing to the same
/// stored data. This pattern is discouraged - you should only use a single instance per thread.
/// Concurrent write access from multiple instances in the same thread will panic due to violating
/// the "single mutable reference" constraint.
#[derive(Debug)]
pub struct ThreadLocalStorage<T>
where
    T: 'static,
{
    local: LocalValueReference<T>,
    slot: Slot<T>,
}

// This is a single-threaded object that cannot touch other threads.
#[negative_impl]
impl<T: 'static> !Send for ThreadLocalStorage<T> {}
#[negative_impl]
impl<T: 'static> !Sync for ThreadLocalStorage<T> {}

// Contains all the data passed around between threads, sufficient to initialize and use the storage slot on any thread.
// Implementation of this type contains the slot-management mechanics, which are publicly exposed via [ThreadLocalStorage].
struct Slot<T>
where
    T: 'static,
{
    // If there is no value in the current thread's storage slot, we get the value from here.
    initial_value_provider: Arc<dyn Fn() -> T + Send + Sync>,

    key: StorageKey,
}

impl<T: 'static> Slot<T> {
    // Expands the slot objects into a full storage object, consuming the original slot object in the process.
    // The slot object actually remains in existence as an inseparable part of the returned storage object and may be further cloned from there.
    // You can think of a detached slot object as a "pre-storage" object, not yet fully formed as a storage object.
    fn into_storage(self) -> ThreadLocalStorage<T> {
        ThreadLocalStorage {
            local: self.get_or_init(),
            slot: self,
        }
    }

    fn get_or_init(&self) -> LocalValueReference<T> {
        STORAGE_MAP.with(|map_cell| {
            let mut map = map_cell.borrow_mut();

            let storage_cell = map.entry(self.key).or_insert_with(|| {
                Box::new(Rc::new(RefCell::new((self.initial_value_provider)())))
            });

            Rc::clone(
                storage_cell
                    .downcast_ref::<LocalValueReference<T>>()
                    .expect("Thread-local storage contained value of incorrect type."),
            )
        })
    }

    fn remove(&self) {
        STORAGE_MAP.with(|map_cell| {
            let mut map = map_cell.borrow_mut();
            map.remove(&self.key);
        });
    }
}

impl<T: 'static> Clone for Slot<T> {
    fn clone(&self) -> Self {
        Self {
            initial_value_provider: Arc::clone(&self.initial_value_provider),
            key: self.key,
        }
    }
}

impl<T: Debug> Debug for Slot<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Slot")
            .field(
                "initial_value_provider",
                &format_args!("Fn() -> {:?}", std::any::type_name::<T>()),
            )
            .field("key", &self.key)
            .finish()
    }
}

impl<T: 'static> ThreadLocalStorage<T> {
    pub fn new(initial_value_provider: impl Fn() -> T + Send + Sync + 'static) -> Self {
        let key = StorageKey::new();
        let slot = Slot {
            initial_value_provider: Arc::new(initial_value_provider),
            key,
        };

        slot.into_storage()
    }

    pub fn handle(&self) -> ThreadLocalStorageHandle<T> {
        ThreadLocalStorageHandle {
            slot: self.slot.clone(),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> ThreadLocalStorage<T> {
    /// If there is a thread-safe cloneable initial value, we can use it to easily initialize
    /// the storage on each thread. This is a convenience method for the common case where the
    /// initial value is a simple cloneable value.
    pub fn with_initial_value(initial_value: T) -> Self {
        Self::new(move || initial_value.clone())
    }
}

impl<T: 'static> WithData<T> for ThreadLocalStorage<T> {
    fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let reader = self.read();
        f(&reader)
    }

    fn with_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut writer = self.write();
        f(&mut writer)
    }
}

#[derive(Debug)]
pub struct ThreadLocalStorageReader<'a, T> {
    inner: Ref<'a, T>,
}

impl<T: 'static> Deref for ThreadLocalStorageReader<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<'s, T: 'static> ReadStorage<'s, ThreadLocalStorageReader<'s, T>, T> for ThreadLocalStorage<T> {
    fn read(&'s self) -> ThreadLocalStorageReader<'s, T> {
        ThreadLocalStorageReader {
            inner: self.local.borrow(),
        }
    }
}

#[derive(Debug)]
pub struct ThreadLocalStorageWriter<'a, T> {
    inner: RefMut<'a, T>,
}

impl<T: 'static> Deref for ThreadLocalStorageWriter<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T: 'static> DerefMut for ThreadLocalStorageWriter<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<'s, T: 'static> WriteStorage<'s, ThreadLocalStorageWriter<'s, T>, T>
    for ThreadLocalStorage<T>
{
    fn write(&'s self) -> ThreadLocalStorageWriter<'s, T> {
        ThreadLocalStorageWriter {
            inner: self.local.borrow_mut(),
        }
    }
}

impl<T: 'static> Drop for ThreadLocalStorage<T> {
    fn drop(&mut self) {
        // If we are dropping the last storage object referencing this value on this thread,
        // we need to also clean up the slot from the map to ensure the map does not accumulate trash.
        let remaining_references = Rc::strong_count(&self.local);

        assert!(remaining_references >= 2, "Thread-local storage object was dropped when there were already insufficient remaining references.");

        // 2 because we have 1 from the storage object and 1 from the reference in the thread local map.
        if remaining_references == 2 {
            self.slot.remove();
        }
    }
}

type LocalValueReference<T> = Rc<RefCell<T>>;

#[derive(Debug)]
pub struct ThreadLocalStorageHandle<T>
where
    T: 'static,
{
    slot: Slot<T>,
}

impl<T> Clone for ThreadLocalStorageHandle<T>
where
    T: 'static,
{
    fn clone(&self) -> Self {
        Self {
            slot: self.slot.clone(),
        }
    }
}

impl<T: 'static> StorageHandle<ThreadLocalStorage<T>> for ThreadLocalStorageHandle<T> {
    fn into_storage(self) -> ThreadLocalStorage<T> {
        self.slot.into_storage()
    }
}

// We have a 2-level storage architecture here.
//
// Level 1: We assign each storage entry a unique key, which is valid on every thread (same key,
// thread-local value). We pass around the identical key between threads to ensure that all threads
// are aware of which storage entry we are talking about.
//
// The key points into a thread-local HashMap. The HashMap stores opaque values,
// only the caller using the thread local storage knows what the real data type in there is.
//
// Level 2: the value in the HashMap is of a type known to the caller, who can cast it as needed.
//
// # Comparison to LocalKey
//
// We have a separate keyed lookup step for every access, which is less efficient. However, this allows
// us to bypass a crucial limitation of LocalKey - it requires a static variable! Requiring a static
// variable to be defined limits LocalKey to only usages where a fixed number of instances can exist.
//
// This limitation is not desirable for the general purpose mechanism we are building.
// If you are in a situation where this limitation is OK, consider using LocalKey for more efficiency.

thread_local! {
    // This map stores all the thread-local data for our thread local storage implementation.
    // What is stored in here specifically depends on the caller who owns the key for the storage entry.
    //
    // Outer RefCell is used to control access to the HashMap - when accessing storage, we may need to mutate the storage map.
    // This pattern is required by LocalKey, which is used to implement the root reference to the map.
    static STORAGE_MAP: RefCell<HashedMap<StorageKey, StorageCell>> = RefCell::new(HashedMap::default())
}

/// Obtains the number of storage slots occupied in the current thread's storage map. For diagnostic purposes.
pub fn thread_local_storage_slot_count() -> usize {
    STORAGE_MAP.with_borrow(HashedMap::len)
}

// We must box the inner value because only the caller knows what type of data it stores.
// The data type will be LocalValueReference<T>.
type StorageCell = Box<dyn Any>;

// The key is globally scoped and the same on every thread, so we need a shared key generator.
// Optimization: if this gets very hot, we can potentially partition by thread. Probably will not happen, though.
static NEXT_STORAGE_KEY_VALUE: AtomicUsize = AtomicUsize::new(0);

/// Each storage entry is given a unique key that points to the thread-local storage for that entry.
/// The same key is used by all clones of the storage object, ensuring that no matter which clone
/// is used, they all point to the current thread's version of the stored value.
#[derive(Eq, PartialEq, Copy, Clone)]
struct StorageKey {
    // Just for uniqueness. A global counter, incremented for each new storage key.
    value: usize,

    // Precomputed hash. We assume that an instance of storage key is never mutated.
    // Optimization: It is unclear if we even need a hash here as the unique counter
    // might be good enough to satisfy the needs of the hash map. Should benchmark, though.
    hash: u64,
}

impl StorageKey {
    fn new() -> Self {
        let value = NEXT_STORAGE_KEY_VALUE.fetch_add(1, Ordering::Relaxed);
        let hash = xxh3_64(&value.to_ne_bytes());

        StorageKey { value, hash }
    }
}

// This is deliberately not tested and excluded from mutation testing as we do not have a contract
// for what (if anything) Debug::fmt() should return and have no need for such a contract.
impl std::fmt::Debug for StorageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageKey")
            .field("value", &self.value)
            .finish()
    }
}

impl Hash for StorageKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // We use a HashedMap which uses a pass-through implementation that does
        // no further hashing - this value is the final hash value.
        state.write_u64(self.hash);
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use mockall::predicate::*;
    use mockall::*;

    use super::*;
    use crate::mem::storage::test_types::{SendAndSyncAndCloneType, SingleThreadedType};

    #[test]
    fn is_thread_local() {
        let initial = SendAndSyncAndCloneType::new(5);

        let storage1 = ThreadLocalStorage::with_initial_value(initial);

        storage1.with(|x| assert_eq!(5, x.value));
        storage1.with_mut(|x| x.value = 6);
        storage1.with(|x| assert_eq!(6, x.value));

        let handle1 = storage1.handle();
        let handle2 = handle1.clone();

        {
            let storage2 = handle1.into_storage();

            storage2.with(|x| assert_eq!(6, x.value));
            storage2.with_mut(|x| x.value = 7);
            storage2.with(|x| assert_eq!(7, x.value));
        }

        {
            let storage3 = handle2.into_storage();

            storage3.with(|x| assert_eq!(7, x.value));
            storage3.with_mut(|x| x.value = 8);
            storage3.with(|x| assert_eq!(8, x.value));
        }

        // All storage boxes access the same value.
        storage1.with(|x| assert_eq!(8, x.value));

        let handle = storage1.handle();

        thread::spawn(move || {
            let storage4 = handle.into_storage();

            // Storage4 is on a new thread, so it gets initialized with the initial value.
            storage4.with(|x| assert_eq!(5, x.value));
        })
        .join()
        .unwrap();
    }

    #[test]
    fn resets_on_zero_references() {
        let initial = SendAndSyncAndCloneType::new(5);

        let storage1 = ThreadLocalStorage::with_initial_value(initial);
        storage1.with_mut(|x| x.value = 9);
        storage1.with(|x| assert_eq!(9, x.value));

        let handle = storage1.handle();

        // Removes the last remaining slot reference (handles do not hold references).
        // Next time the slot is used, it will have its initial value.
        drop(storage1);

        let storage3 = handle.into_storage();

        // There were no references remaining while wrapped (1 dropped manually, 2 dropped automatically, 3 wrapped).
        // This means the storage slot was deallocated and should have reset to initial value when unwrapped.
        storage3.with(|x| assert_eq!(5, x.value));
    }

    #[test]
    fn new_simple_type() {
        let value = SendAndSyncAndCloneType::new(5);
        let storage = ThreadLocalStorage::with_initial_value(value);

        storage.with(|x| assert_eq!(5, x.value));
    }

    #[test]
    fn single_threaded_type() {
        // Single-threaded types need to be constructed on the destination thread,
        // so we use a closure to do that - we cannot just pass around an object.
        let storage = ThreadLocalStorage::new(|| SingleThreadedType::new(5));

        storage.with(|x| assert_eq!(5, x.value));
        storage.with_mut(|x| x.value = 6);

        let handle = storage.handle();

        thread::spawn(move || {
            let storage2 = handle.into_storage();

            // Storage2 is on a new thread, so it gets initialized with the initial value.
            storage2.with(|x| assert_eq!(5, x.value));
        })
        .join()
        .unwrap();
    }

    #[test]
    fn read_write() {
        let stored = ThreadLocalStorage::new(|| SingleThreadedType::new(5));

        stored.with(|x| assert_eq!(5, x.value));

        {
            let x = stored.read();
            assert_eq!(5, x.value);

            // Concurrent reads are always OK.
            let y = stored.read();
            assert_eq!(5, y.value);

            assert_eq!(5, x.value);
        }

        {
            // Writes are exclusive.
            // This is enforced by mutability logic as long as there is just 1 storage instance
            // on the current thread. Values in different threads are independent.
            let mut x = stored.write();
            assert_eq!(5, x.value);
            x.value = 6;
        }

        let x = stored.read();
        assert_eq!(6, x.value);
    }

    #[test]
    fn slot_count_changes_as_expected() {
        assert_eq!(0, thread_local_storage_slot_count());

        // Start a
        let a = ThreadLocalStorage::with_initial_value(12345);
        assert_eq!(1, thread_local_storage_slot_count());

        // b uses the same slot as a
        let b = a.handle().into_storage();
        assert_eq!(1, thread_local_storage_slot_count());

        // making a handle does not modify the slots
        let handle = b.handle();
        assert_eq!(1, thread_local_storage_slot_count());

        // c uses the same slot as a and b
        let c = handle.into_storage();
        assert_eq!(1, thread_local_storage_slot_count());

        // dropping a decrements reference count to 2
        drop(a);
        assert_eq!(1, thread_local_storage_slot_count());

        // other is completely independent, a new slot with 1 reference
        let other = ThreadLocalStorage::with_initial_value(8988);
        assert_eq!(2, thread_local_storage_slot_count());

        // the first slot now dropped to 0 references so goes away
        drop(b);
        drop(c);
        assert_eq!(1, thread_local_storage_slot_count());

        // and the other slot goes away
        drop(other);
        assert_eq!(0, thread_local_storage_slot_count());
    }

    #[test]
    fn storagekey_hash_impl() {
        let key = StorageKey::new();

        let mut mock_hasher = MockHasher::new();

        // We expect exactly 1 item to be written, which we consume as the raw hash value.
        mock_hasher.expect_write().times(1).returning(|_| ());

        key.hash(&mut mock_hasher);
    }

    #[test]
    fn storagekey_clone() {
        let key1 = StorageKey::new();
        let key2 = Clone::clone(&key1);

        assert_eq!(key1.hash, key2.hash);
        assert_eq!(key1.value, key2.value);
    }

    mock! {
        Hasher {}

        impl std::hash::Hasher for Hasher {
            fn finish(&self) -> u64;
            fn write(&mut self, bytes: &[u8]);
        }
    }
}
