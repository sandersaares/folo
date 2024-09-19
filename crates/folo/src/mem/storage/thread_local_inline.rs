// Copyright (c) Microsoft Corporation.

use std::cell::{Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use std::thread::{self, ThreadId};

use negative_impl::negative_impl;

use crate::constants;

use super::{ReadStorage, StorageHandle, WithData, WriteStorage};

/// A storage pattern where every clone of the storage object references a single per-thread value initialized
/// from a global initial value, with no relation between threads. The same per-thread value may be accessed
/// from any storage object. The per-thread value is dropped when the last storage object referencing
/// it is dropped.
///
/// This differs from [`ThreadLocalStorage`][super::ThreadLocalStorage] by keeping the thread-value map inline in a
/// shared per-box-family structure, instead of in a per-thread structure shared by all boxes.
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
pub struct ThreadLocalInlineStorage<T>
where
    T: 'static,
{
    local: LocalValueReference<T>,
    slot: Slot<T>,
}

// This is a single-threaded object that cannot touch other threads.
#[negative_impl]
impl<T: 'static> !Send for ThreadLocalInlineStorage<T> {}
#[negative_impl]
impl<T: 'static> !Sync for ThreadLocalInlineStorage<T> {}

/// Contains all the data passed around between threads, sufficient to initialize and use the storage slot on any thread.
/// Implementation of this type contains the slot-management mechanics, which are publicly exposed via [`ThreadLocalInlineSlotStorage`].
struct Slot<T>
where
    T: 'static,
{
    // If there is no value in the current thread's storage slot, we get the value from here.
    initial_value_provider: Arc<dyn Fn() -> T + Send + Sync>,

    // The value for each thread. Items are removed from the map via reference counting.
    thread_slots: SlotMap<T>,
}

type SlotKey = ThreadId;

// Each value in the map is dedicated to a single thread and only accessed from that thread.
// We use the SingleThreaded promise to avoid having to layer additional protection around the inner value.
type SlotValue<T> = TreatAsSyncAndSend<LocalValueReference<T>>;

// The map is shared by all clones of the same storage.
type SlotMap<T> = Arc<RwLock<HashMap<SlotKey, SlotValue<T>>>>;

impl<T: 'static> Slot<T> {
    // Expands the slot objects into a full storage object, consuming the original slot object in the process.
    // The slot object actually remains in existence as an inseparable part of the returned storage object and may be further cloned from there.
    // You can think of a detached slot object as a "pre-storage" object, not yet fully formed as a storage object.
    fn into_storage(self) -> ThreadLocalInlineStorage<T> {
        ThreadLocalInlineStorage {
            local: self.get_or_init(),
            slot: self,
        }
    }

    fn get_or_init(&self) -> LocalValueReference<T> {
        let mut map = self.thread_slots.write().expect(constants::POISONED_LOCK);

        Rc::clone(
            &map.entry(thread::current().id())
                .or_insert_with(|| {
                    TreatAsSyncAndSend::new(LocalValueReference::new(RefCell::new((self
                        .initial_value_provider)(
                    ))))
                })
                .value,
        )
    }

    fn remove(&self) {
        let mut map = self.thread_slots.write().expect(constants::POISONED_LOCK);
        map.remove(&thread::current().id());
    }
}

impl<T: 'static> Clone for Slot<T> {
    fn clone(&self) -> Self {
        Self {
            initial_value_provider: Arc::clone(&self.initial_value_provider),
            thread_slots: Arc::clone(&self.thread_slots),
        }
    }
}

impl<T> Debug for Slot<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Slot")
            .field(
                "initial_value_provider",
                &format_args!("Arc<dyn Fn() -> {:?}>", std::any::type_name::<T>()),
            )
            .field("thread_slots", &format_args!("{:p}", &self.thread_slots))
            .finish()
    }
}

impl<T: 'static> ThreadLocalInlineStorage<T> {
    pub fn new(initial_value_provider: impl Fn() -> T + Send + Sync + 'static) -> Self {
        let slot = Slot {
            initial_value_provider: Arc::new(initial_value_provider),
            thread_slots: Arc::new(RwLock::new(HashMap::new())),
        };

        slot.into_storage()
    }

    pub fn handle(&self) -> ThreadLocalInlineStorageHandle<T> {
        ThreadLocalInlineStorageHandle {
            slot: self.slot.clone(),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> ThreadLocalInlineStorage<T> {
    /// If there is a thread-safe cloneable initial value, we can use it to easily initialize
    /// the storage on each thread. This is a convenience method for the common case where the
    /// initial value is a simple cloneable value.
    pub fn with_initial_value(initial_value: T) -> Self {
        Self::new(move || initial_value.clone())
    }
}

impl<T: 'static> WithData<T> for ThreadLocalInlineStorage<T> {
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
pub struct ThreadLocalInlineStorageReader<'a, T> {
    inner: Ref<'a, T>,
}

impl<T: 'static> Deref for ThreadLocalInlineStorageReader<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<'s, T: 'static> ReadStorage<'s, ThreadLocalInlineStorageReader<'s, T>, T>
    for ThreadLocalInlineStorage<T>
{
    fn read(&'s self) -> ThreadLocalInlineStorageReader<'s, T> {
        ThreadLocalInlineStorageReader {
            inner: self.local.borrow(),
        }
    }
}

#[derive(Debug)]
pub struct ThreadLocalInlineStorageWriter<'a, T> {
    inner: RefMut<'a, T>,
}

impl<T: 'static> Deref for ThreadLocalInlineStorageWriter<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T: 'static> DerefMut for ThreadLocalInlineStorageWriter<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<'s, T: 'static> WriteStorage<'s, ThreadLocalInlineStorageWriter<'s, T>, T>
    for ThreadLocalInlineStorage<T>
{
    fn write(&'s self) -> ThreadLocalInlineStorageWriter<'s, T> {
        ThreadLocalInlineStorageWriter {
            inner: self.local.borrow_mut(),
        }
    }
}

impl<T: 'static> Drop for ThreadLocalInlineStorage<T> {
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

/// A wrapper type that pinky promises to the compiler that we only use the value within from a single thread,
/// therefore eliminating concerns about Sync. Specifically, we need it to package a Rc into the thread-local
/// slot in the global map - the Rc is not Sync, but we know it is only used by a single thread.
///
/// 1. We only access this value from `get_or_init()` passing the current thread's ID, meaning it's always accessed
///    from the owning thread.
/// 1. We ensure the value is dropped on that thread by clearing it from the map before the last Rc is dropped
///    (again, on the owning thread since `ThreadLocalInlineStorage` is !Send).
///
/// This does technically violate the !Send/!Sync bounds of the `Rc` type. We operate here under the belief
/// that this is safe as long as we do not ever access an `Rc` from different threads explicitly (see above)
/// and only perform implicit access (when resizing the map) under a lock. We know that resizing the map is
/// safe because the `Rc` implementation in the standard library stores reference counts externally to the type
/// itself, so the individual `Rc` in the map is only touched when the map lock is held.
struct TreatAsSyncAndSend<T> {
    value: T,
}

impl<T> Debug for TreatAsSyncAndSend<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TreatAsSyncAndSend")
            .field("value", &self.value)
            .finish()
    }
}

// SAFETY: See comments on type.
unsafe impl<T> Sync for TreatAsSyncAndSend<T> {}

// SAFETY: See comments on type.
unsafe impl<T> Send for TreatAsSyncAndSend<T> {}

impl<T> TreatAsSyncAndSend<T> {
    fn new(value: T) -> Self {
        Self { value }
    }
}

#[derive(Debug)]
pub struct ThreadLocalInlineStorageHandle<T>
where
    T: 'static,
{
    slot: Slot<T>,
}

impl<T> Clone for ThreadLocalInlineStorageHandle<T>
where
    T: 'static,
{
    fn clone(&self) -> Self {
        Self {
            slot: self.slot.clone(),
        }
    }
}

impl<T: 'static> StorageHandle<ThreadLocalInlineStorage<T>> for ThreadLocalInlineStorageHandle<T> {
    fn into_storage(self) -> ThreadLocalInlineStorage<T> {
        self.slot.into_storage()
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    use crate::mem::storage::test_types::{SendAndSyncAndCloneType, SingleThreadedType};

    #[test]
    fn is_thread_local() {
        let initial = SendAndSyncAndCloneType::new(5);

        let storage1 = ThreadLocalInlineStorage::with_initial_value(initial);

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

        let storage1 = ThreadLocalInlineStorage::with_initial_value(initial);
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
        let storage = ThreadLocalInlineStorage::with_initial_value(value);

        storage.with(|x| assert_eq!(5, x.value));
    }

    #[test]
    fn single_threaded_type() {
        // Single-threaded types need to be constructed on the destination thread,
        // so we use a closure to do that - we cannot just pass around an object.
        let storage = ThreadLocalInlineStorage::new(|| SingleThreadedType::new(5));

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
        let stored = ThreadLocalInlineStorage::new(|| SingleThreadedType::new(5));

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
}
