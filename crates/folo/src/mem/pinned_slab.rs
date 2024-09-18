use crate::mem::DropPolicy;
use core::panic;
use std::alloc::{alloc, dealloc, Layout};
use std::any::type_name;
use std::mem::{self, MaybeUninit};
use std::pin::Pin;

/// This is the backing storage of a `PinnedSlabChain` and is not meant to be directly used unless
/// you specifically require a fixed-capacity collection.
///
/// A pinned fixed-capacity heap-allocated collection. Works similar to a Vec but all items are
/// pinned and the collection has a fixed capacity, operating using an index for lookup. When you
/// insert an item, you cam get back the index to use for accessing or removing the item.
///
/// There are multiple ways to insert items into the collection:
///
/// * `insert()` - inserts a value and returns the index. This is the simplest way to add an item
///   but requires you to later look it up by the index. That lookup is fast but not free.
/// * `begin_insert().insert()` - returns a shared reference to the inserted item; you may also
///   obtain the index in advance from the inserter through `index()` which may be useful if the
///   item needs to know its own index in the collection.
/// * `begin_insert().insert_raw()` - returns a mutable pointer to the inserted item, for cases
///   where you want to access the item through unsafe code (e.g. to disable borrow checking).
/// * `begin_insert().insert_uninit()` - returns a mutable `MaybeUninit<T>` pointer to an
///   uninitialized version of the item. This is useful for in-place initialization, to avoid
///   copying the item from the stack into the collection.
///  
/// What happens to dropped items depends on the `DropPolicy` configured on the type.
///
/// To share the collection between threads, wrapping in `Mutex` is the recommended approach.
///
/// The collection itself does not need to be pinned - only the contents are pinned.
#[derive(Debug)]
pub struct PinnedSlab<T, const CAPACITY: usize> {
    ptr: *mut Entry<T>,

    /// Index of the next free slot in the collection. Think of this as a virtual stack, with the
    /// stack entries stored in the collection entries themselves. This will point out of bounds if
    /// the collection is full.
    next_free_index: usize,

    /// The total number of items in the collection. This is not used by the collection itself but
    /// may be valuable to callers who want to know if the collection is empty because in many use
    /// cases the collection is the backing store for a custom allocation/pinning scheme and may not
    /// be safe to drop when any items are still present.
    count: usize,

    drop_policy: DropPolicy,
}

enum Entry<T> {
    Occupied { value: T },

    Vacant { next_free_index: usize },
}

impl<T, const CAPACITY: usize> PinnedSlab<T, CAPACITY> {
    /// Creates a new slab with the specified drop policy. If the caller takes raw pointers to the
    /// contents of the slab contents, it is ultimately their responsibility to ensure the pointers
    /// do not outlive the slab itself. A drop policy may help safeguard this by panicking if the
    /// slab still contains items when it is dropped.
    pub fn new(drop_policy: DropPolicy) -> Self {
        // SAFETY: MaybeUninit is a ZST, so its layout is guaranteed to match Entry<T>.
        let ptr = unsafe { alloc(Self::layout()) as *mut MaybeUninit<Entry<T>> };

        // Initialize them all to `Vacant` to start with.
        for index in 0..CAPACITY {
            // SAFETY: We ensure in `layout()` that there is enough space for all items up to our
            // indicated capacity.
            unsafe {
                let slot = ptr.add(index);
                (*slot).write(Entry::Vacant {
                    next_free_index: index + 1,
                });
            }
        }

        // We can now assume the collection is initialized - safe to access without causing UB.
        Self {
            // SAFETY: MaybeUninit is a ZST, so the layout is guaranteed to match.
            ptr: unsafe { mem::transmute::<*mut MaybeUninit<Entry<T>>, *mut Entry<T>>(ptr) },
            next_free_index: 0,
            count: 0,
            drop_policy,
        }
    }

    fn layout() -> Layout {
        Layout::array::<MaybeUninit<Entry<T>>>(CAPACITY)
            .expect("simple flat array layout must be calculable")
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn is_full(&self) -> bool {
        self.next_free_index >= CAPACITY
    }

    /// # Panics
    ///
    /// Panics if the index is out of bounds or is not associated with an item.
    pub fn get(&self, index: usize) -> Pin<&T> {
        assert!(
            index < CAPACITY,
            "get({index}) index out of bounds in slab of {}",
            type_name::<T>()
        );

        // SAFETY: We did a bounds check and ensured in the ctor that every entry is initialized.
        match unsafe {
            self.ptr
                .add(index)
                .as_ref()
                .expect("we expect the resulting pointer to always be valid")
        } {
            // SAFETY: Items are always guaranteed pinned in this collection.
            Entry::Occupied { value } => unsafe { Pin::new_unchecked(value) },
            Entry::Vacant { .. } => panic!(
                "get({index}) entry was vacant in slab of {}",
                type_name::<T>()
            ),
        }
    }

    /// # Panics
    ///
    /// Panics if the index is out of bounds or is not associated with an item.
    pub fn get_mut(&mut self, index: usize) -> Pin<&mut T> {
        assert!(index < CAPACITY, "index {index} out of bounds");

        // SAFETY: We did a bounds check and ensured in the ctor that every entry is initialized.
        match unsafe {
            self.ptr
                .add(index)
                .as_mut()
                .expect("we expect the resulting pointer to always be valid")
        } {
            // SAFETY: Items are always pinned - that is the point of this collection.
            Entry::Occupied { ref mut value } => unsafe { Pin::new_unchecked(value) },
            Entry::Vacant { .. } => panic!(
                "get_mut({index}) entry was vacant in slab of {}",
                type_name::<T>()
            ),
        }
    }

    /// # Panics
    ///
    /// Panics if the collection is full.
    pub fn begin_insert<'s, 'i>(&'s mut self) -> PinnedSlabInserter<'i, T, CAPACITY>
    where
        's: 'i,
    {
        #[cfg(test)]
        self.integrity_check();

        assert!(
            !self.is_full(),
            "cannot insert into a full slab of {}",
            type_name::<T>()
        );

        let next_free_index = self.next_free_index;

        PinnedSlabInserter {
            slab: self,
            index: next_free_index,
        }
    }

    /// # Panics
    ///
    /// Panics if the collection is full.
    pub fn insert(&mut self, value: T) -> usize {
        let inserter = self.begin_insert();
        let index = inserter.index();
        inserter.insert(value);
        index
    }

    /// # Panics
    ///
    /// Panics if the index is out of bounds or is not associated with an item.
    pub fn remove(&mut self, index: usize) {
        assert!(
            index < CAPACITY,
            "remove({index}) index out of bounds in slab of {}",
            type_name::<T>()
        );

        // SAFETY: We did a bounds check and ensured in the ctor that every entry is initialized.
        let slot = unsafe {
            self.ptr
                .add(index)
                .as_mut()
                .expect("we expect the resulting pointer to always be valid")
        };

        if matches!(slot, Entry::Vacant { .. }) {
            panic!(
                "remove({index}) entry was vacant in slab of {}",
                type_name::<T>()
            );
        }

        *slot = Entry::Vacant {
            next_free_index: self.next_free_index,
        };

        self.next_free_index = index;
        self.count -= 1;
    }

    #[cfg(test)]
    pub fn integrity_check(&self) {
        let mut observed_is_vacant: [Option<bool>; CAPACITY] = [None; CAPACITY];
        let mut observed_next_free_index: [Option<usize>; CAPACITY] = [None; CAPACITY];
        let mut observed_occupied_count = 0;

        for index in 0..CAPACITY {
            // SAFETY: We are operating within bounds. We initialized all slots in the ctor. Is OK.
            match unsafe {
                self.ptr
                    .add(index)
                    .as_ref()
                    .expect("we expect the resulting pointer to always be valid")
            } {
                Entry::Occupied { .. } => {
                    observed_is_vacant[index] = Some(false);
                    observed_occupied_count += 1;
                }
                Entry::Vacant { next_free_index } => {
                    observed_is_vacant[index] = Some(true);
                    observed_next_free_index[index] = Some(*next_free_index);
                }
            };
        }

        if self.next_free_index < CAPACITY && !observed_is_vacant[self.next_free_index].unwrap() {
            panic!(
                "self.next_free_index points to an occupied slot {} in slab of {}",
                self.next_free_index,
                type_name::<T>()
            );
        }

        if self.count != observed_occupied_count {
            panic!(
                "self.count {} does not match the observed occupied count {} in slab of {}",
                self.count,
                observed_occupied_count,
                type_name::<T>()
            );
        }

        for index in 0..CAPACITY {
            if !observed_is_vacant[index].unwrap() {
                continue;
            }

            let next_free_index = observed_next_free_index[index].unwrap();

            if next_free_index == CAPACITY {
                // This is fine - it means the slab became full once we inserted this one.
                continue;
            }

            if next_free_index > CAPACITY {
                panic!(
                    "entry {} is vacant but has an out-of-bounds next_free_index beyond COUNT {} in slab of {}",
                    index, next_free_index, type_name::<T>()
                );
            }

            if !observed_is_vacant[next_free_index].unwrap() {
                panic!(
                    "entry {} is vacant but its next_free_index {} points to an occupied slot in slab of {}",
                    index, next_free_index, type_name::<T>()
                );
            }
        }
    }
}

impl<T, const CAPACITY: usize> Drop for PinnedSlab<T, CAPACITY> {
    fn drop(&mut self) {
        let was_empty = self.is_empty();

        // SAFETY: All slots were already initialized by the ctor so nothing can be invalid here.
        // We are using the correct size and the same layout as was used in the ctor, everything OK.
        unsafe {
            // Initialize them all to `Vacant` to drop any occupied data.
            for index in 0..CAPACITY {
                let slot = self.ptr.add(index);
                *slot = Entry::Vacant {
                    next_free_index: index + 1,
                };
            }

            dealloc(self.ptr as *mut u8, Self::layout());
        }

        // We do this at the end so we clean up the memory first. Mostly to make Miri happy - since
        // we are going to panic anyway, there is little good to expect for the app itself.
        if self.drop_policy == DropPolicy::MustNotDropItems {
            assert!(
                was_empty,
                "dropped a non-empty slab of {} with a policy that says it must be empty when dropped", type_name::<T>()
            );
        }
    }
}

// Yes, there are raw pointers in there but nothing inherently non-thread-safe about it, as long as
// T itself can move between threads.
unsafe impl<T: Send, const CAPACITY: usize> Send for PinnedSlab<T, CAPACITY> {}

pub struct PinnedSlabInserter<'s, T, const CAPACITY: usize> {
    slab: &'s mut PinnedSlab<T, CAPACITY>,

    /// Index at which the item will be inserted.
    index: usize,
}

impl<'s, T, const CAPACITY: usize> PinnedSlabInserter<'s, T, CAPACITY> {
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn insert<'v>(self, value: T) -> Pin<&'v T>
    where
        's: 'v,
    {
        // SAFETY: We did a bounds check and ensured in the ctor that every entry is initialized.
        let slot = unsafe {
            self.slab
                .ptr
                .add(self.index)
                .as_mut()
                .expect("we expect the resulting pointer to always be valid")
        };

        let previous_entry = mem::replace(slot, Entry::Occupied { value });

        self.slab.next_free_index = match previous_entry {
            Entry::Vacant { next_free_index } => next_free_index,
            Entry::Occupied { .. } => panic!(
                "entry {} was not vacant when we inserted into it in slab of {}",
                self.index,
                type_name::<T>()
            ),
        };

        let pinned_ref: Pin<&'v T> = match slot {
            // SAFETY: Items are always pinned - that is the point of this collection.
            Entry::Occupied { value } => unsafe { Pin::new_unchecked(value) },
            Entry::Vacant { .. } => panic!(
                "entry {} was not occupied after we inserted into it in slab of {}",
                self.index,
                type_name::<T>()
            ),
        };

        self.slab.count += 1;

        pinned_ref
    }

    pub fn insert_raw(self, value: T) -> *mut T {
        // SAFETY: We did a bounds check and ensured in the ctor that every entry is initialized.
        let slot = unsafe {
            self.slab
                .ptr
                .add(self.index)
                .as_mut()
                .expect("we expect the resulting pointer to always be valid")
        };

        let previous_entry = mem::replace(slot, Entry::Occupied { value });

        self.slab.next_free_index = match previous_entry {
            Entry::Vacant { next_free_index } => next_free_index,
            Entry::Occupied { .. } => panic!(
                "entry {} was not vacant when we inserted into it in slab of {}",
                self.index,
                type_name::<T>()
            ),
        };

        let ptr = match slot {
            Entry::Occupied { value } => value as *mut T,
            Entry::Vacant { .. } => panic!(
                "entry {} was not occupied after we inserted into it in slab of {}",
                self.index,
                type_name::<T>()
            ),
        };

        self.slab.count += 1;

        ptr
    }

    /// Inserts an item without initializing it. Depending on the type T, the caller may want to
    /// initialize it with all-zero bytes, skip initializing it entirely (if all bit patterns are
    /// valid) or initialize it manually with some custom value or pointer writes.
    pub fn insert_uninit(self) -> *mut MaybeUninit<T> {
        // SAFETY: We did a bounds check and ensured in the ctor that every entry is initialized.
        let slot = unsafe {
            self.slab
                .ptr
                .add(self.index)
                .as_mut()
                .expect("we expect the resulting pointer to always be valid")
        };

        // We pretend that we are a slab of MaybeUninit<T> instead of T.
        // SAFETY: MaybeUninit<T> is guaranteed to have the same memory layout as T. It is the
        // caller's business how to ensure that the contents of the inserted T are valid - they
        // have multiple options for that and the specifics are none of our concern.
        let slot: &mut Entry<MaybeUninit<T>> = unsafe { mem::transmute(slot) };

        let previous_entry = mem::replace(
            slot,
            Entry::Occupied {
                value: MaybeUninit::uninit(),
            },
        );

        self.slab.next_free_index = match previous_entry {
            Entry::Vacant { next_free_index } => next_free_index,
            Entry::Occupied { .. } => panic!(
                "entry {} was not vacant when we inserted into it in slab of {}",
                self.index,
                type_name::<T>()
            ),
        };

        let ptr = match slot {
            Entry::Occupied { value } => value as *mut MaybeUninit<T>,
            Entry::Vacant { .. } => panic!(
                "entry {} was not occupied after we inserted into it in slab of {}",
                self.index,
                type_name::<T>()
            ),
        };

        self.slab.count += 1;

        ptr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
        sync::{Arc, Mutex},
        thread,
    };

    #[test]
    fn smoke_test() {
        let mut slab = PinnedSlab::<u32, 3>::new(DropPolicy::MayDropItems);

        let a = slab.insert(42);
        let b = slab.insert(43);
        let c = slab.insert(44);

        assert_eq!(*slab.get(a), 42);
        assert_eq!(*slab.get(b), 43);
        assert_eq!(*slab.get(c), 44);

        slab.remove(b);

        let d = slab.insert(45);

        assert_eq!(*slab.get(a), 42);
        assert_eq!(*slab.get(c), 44);
        assert_eq!(*slab.get(d), 45);

        assert!(slab.is_full());
    }

    #[test]
    #[should_panic]
    fn panic_when_full() {
        let mut slab = PinnedSlab::<u32, 3>::new(DropPolicy::MayDropItems);

        slab.insert(42);
        slab.insert(43);
        slab.insert(44);

        slab.insert(45);
    }

    #[test]
    #[should_panic]
    fn panic_when_oob_get() {
        let mut slab = PinnedSlab::<u32, 3>::new(DropPolicy::MayDropItems);

        slab.insert(42);
        slab.get(1234);
    }

    #[test]
    fn begin_insert_returns_correct_key() {
        let mut slab = PinnedSlab::<u32, 3>::new(DropPolicy::MayDropItems);

        let inserter = slab.begin_insert();
        assert_eq!(inserter.index(), 0);
        inserter.insert(10);
        assert_eq!(*slab.get(0), 10);

        let inserter = slab.begin_insert();
        assert_eq!(inserter.index(), 1);
        inserter.insert(11);
        assert_eq!(*slab.get(1), 11);

        let inserter = slab.begin_insert();
        assert_eq!(inserter.index(), 2);
        inserter.insert(12);
        assert_eq!(*slab.get(2), 12);
    }

    #[test]
    fn abandoned_inserter_is_noop() {
        let mut slab = PinnedSlab::<u32, 3>::new(DropPolicy::MayDropItems);

        let inserter = slab.begin_insert();
        assert_eq!(inserter.index(), 0);

        let inserter = slab.begin_insert();
        assert_eq!(inserter.index(), 0);
        inserter.insert(20);

        assert_eq!(*slab.get(0), 20);

        // There must still be room for 2 more.
        slab.insert(123);
        slab.insert(456);
    }

    #[test]
    fn remove_makes_room() {
        let mut slab = PinnedSlab::<u32, 3>::new(DropPolicy::MayDropItems);

        let a = slab.insert(42);
        let b = slab.insert(43);
        let c = slab.insert(44);

        slab.remove(b);

        let d = slab.insert(45);

        assert_eq!(*slab.get(a), 42);
        assert_eq!(*slab.get(c), 44);
        assert_eq!(*slab.get(d), 45);
    }

    #[test]
    #[should_panic]
    fn remove_vacant_panics() {
        let mut slab = PinnedSlab::<u32, 3>::new(DropPolicy::MayDropItems);

        slab.remove(1);
    }

    #[test]
    #[should_panic]
    fn get_vacant_panics() {
        let slab = PinnedSlab::<u32, 3>::new(DropPolicy::MayDropItems);

        slab.get(1);
    }

    #[test]
    #[should_panic]
    fn get_mut_vacant_panics() {
        let mut slab = PinnedSlab::<u32, 3>::new(DropPolicy::MayDropItems);

        slab.get_mut(1);
    }

    #[test]
    fn in_refcell_works_fine() {
        let slab = RefCell::new(PinnedSlab::<u32, 3>::new(DropPolicy::MayDropItems));

        {
            let mut slab = slab.borrow_mut();
            let a = slab.insert(42);
            let b = slab.insert(43);
            let c = slab.insert(44);

            assert_eq!(*slab.get(a), 42);
            assert_eq!(*slab.get(b), 43);
            assert_eq!(*slab.get(c), 44);

            slab.remove(b);

            let d = slab.insert(45);

            assert_eq!(*slab.get(a), 42);
            assert_eq!(*slab.get(c), 44);
            assert_eq!(*slab.get(d), 45);
        }

        {
            let slab = slab.borrow();
            assert_eq!(*slab.get(0), 42);
            assert!(slab.is_full());
        }
    }

    #[test]
    fn calls_drop_on_remove() {
        struct Droppable {
            dropped: Rc<Cell<bool>>,
        }

        impl Drop for Droppable {
            fn drop(&mut self) {
                self.dropped.set(true);
            }
        }

        let dropped = Rc::new(Cell::new(false));
        let mut slab = PinnedSlab::<Droppable, 3>::new(DropPolicy::MayDropItems);

        let a = slab.insert(Droppable {
            dropped: dropped.clone(),
        });
        slab.remove(a);

        assert!(dropped.get());
    }

    #[test]
    fn multithreaded_via_mutex() {
        let slab = Arc::new(Mutex::new(PinnedSlab::<u32, 3>::new(
            DropPolicy::MayDropItems,
        )));

        let a;
        let b;
        let c;

        {
            let mut slab = slab.lock().unwrap();
            a = slab.insert(42);
            b = slab.insert(43);
            c = slab.insert(44);

            assert_eq!(*slab.get(a), 42);
            assert_eq!(*slab.get(b), 43);
            assert_eq!(*slab.get(c), 44);
        }

        let slab_clone = Arc::clone(&slab);
        thread::spawn(move || {
            let mut slab = slab_clone.lock().unwrap();

            slab.remove(b);

            let d = slab.insert(45);

            assert_eq!(*slab.get(a), 42);
            assert_eq!(*slab.get(c), 44);
            assert_eq!(*slab.get(d), 45);
        });

        let slab = slab.lock().unwrap();
        assert!(slab.is_full());
    }

    #[test]
    #[should_panic]
    fn drop_item_with_forbidden_to_drop_policy_panics() {
        let mut slab = PinnedSlab::<u32, 3>::new(DropPolicy::MustNotDropItems);
        slab.insert(123);
    }

    #[test]
    fn drop_itemless_with_forbidden_to_drop_policy_ok() {
        _ = PinnedSlab::<u32, 3>::new(DropPolicy::MustNotDropItems);
    }
}
