use core::panic;
use std::alloc::{alloc, dealloc, Layout};
use std::mem::{self, MaybeUninit};
use std::pin::Pin;

/// A pinned fixed-size heap-allocated slab of values. Works similar to a Vec
/// but pinned and with a fixed size, operating using an index for lookup.
///
/// Mutation of items is possible but be aware that taking an exclusive `&mut` reference to an item
/// via `get_mut()` will exclusively borrow the slab itself. If you wish to preserve an exclusive
/// reference for a longer duration, you must use interior mutability in your items.
#[derive(Debug)]
pub struct PinnedSlab<T, const CAPACITY: usize> {
    ptr: *mut Entry<T>,

    /// Index of the next free slot in the slab. Think of this as a virtual stack, with the stack
    /// entries stored in the slab entries themselves. This will point out of bounds if the slab
    /// is full.
    next_free_index: usize,

    /// The total number of items in the slab. This is not used by the slab itself but
    /// may be valuable to callers who want to know if the slab is empty because in many use
    /// cases the slab is the backing store for a custom allocation/pinning scheme and may not
    /// be dropped when any items are still present.
    count: usize,
}

enum Entry<T> {
    Occupied { value: T },

    Vacant { next_free_index: usize },
}

impl<T, const CAPACITY: usize> PinnedSlab<T, CAPACITY> {
    pub fn new() -> Self {
        let ptr = unsafe { alloc(Self::layout()) as *mut MaybeUninit<Entry<T>> };

        // Initialize them all to `Vacant` to start with.
        // We can now assume the slab is initialized - safe to access without causing UB.
        for index in 0..CAPACITY {
            unsafe {
                let slot = ptr.add(index);
                (*slot).write(Entry::Vacant {
                    next_free_index: index + 1,
                });
            }
        }

        Self {
            // SAFETY: MaybeUninit is a ZST, so the layout is guaranteed to match.
            ptr: unsafe { mem::transmute(ptr) },
            next_free_index: 0,
            count: 0,
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

    pub fn get(&self, index: usize) -> Pin<&T> {
        assert!(index < CAPACITY, "get({index}) index out of bounds");

        // SAFETY: We did a bounds check and ensured in the ctor that every entry is initialized.
        match unsafe {
            self.ptr
                .add(index)
                .as_ref()
                .expect("we expect the resulting pointer to always be valid")
        } {
            // SAFETY: Items are always pinned - that is the point of this collection.
            Entry::Occupied { value } => unsafe { Pin::new_unchecked(value) },
            Entry::Vacant { .. } => panic!("get({index}) entry was vacant"),
        }
    }

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
            Entry::Vacant { .. } => panic!("get_mut({index}) entry was vacant"),
        }
    }

    pub fn begin_insert<'s, 'i>(&'s mut self) -> PinnedSlabInserter<'i, T, CAPACITY>
    where
        's: 'i,
    {
        #[cfg(test)]
        self.integrity_check();

        assert!(!self.is_full(), "cannot insert into a full slab");

        let next_free_index = self.next_free_index;

        PinnedSlabInserter {
            slab: self,
            index: next_free_index,
        }
    }

    pub fn insert(&mut self, value: T) -> usize {
        let inserter = self.begin_insert();
        let index = inserter.index();
        inserter.insert(value);
        index
    }

    pub fn remove(&mut self, index: usize) {
        assert!(index < CAPACITY, "remove({index}) index out of bounds");

        // SAFETY: We did a bounds check and ensured in the ctor that every entry is initialized.
        let slot = unsafe {
            self.ptr
                .add(index)
                .as_mut()
                .expect("we expect the resulting pointer to always be valid")
        };

        if matches!(slot, Entry::Vacant { .. }) {
            panic!("remove({index}) entry was vacant");
        }

        // SAFETY: We know the slot is valid, as per above. We want to explicit run the drop logic
        // in-place because the slots are pinned - we do not want to move the value out in order
        // to drop it.
        unsafe {
            // We know it is initialized, we just use this to facilitate the in-place drop.
            let slot: *mut MaybeUninit<Entry<T>> = mem::transmute(slot);
            (*slot).assume_init_drop();

            slot.write(MaybeUninit::new(Entry::Vacant {
                next_free_index: self.next_free_index,
            }))
        }

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
                "self.next_free_index points to an occupied slot: {}",
                self.next_free_index
            );
        }

        if self.count != observed_occupied_count {
            panic!(
                "self.count {} does not match the observed occupied count {}",
                self.count, observed_occupied_count
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
                    "entry {} is vacant but has an out-of-bounds next_free_index beyond COUNT: {}",
                    index, next_free_index
                );
            }

            if !observed_is_vacant[next_free_index].unwrap() {
                panic!(
                    "entry {} is vacant but its next_free_index {} points to an occupied slot",
                    index, next_free_index
                );
            }
        }
    }
}

impl<T, const COUNT: usize> Drop for PinnedSlab<T, COUNT> {
    fn drop(&mut self) {
        let ptr = self.ptr as *mut MaybeUninit<Entry<T>>;

        // SAFETY: MaybeUninit is a ZST, so the layout is guaranteed to match.
        // We ensure that all slots are initialized in the ctor, so they are OK to touch.
        // The slot type itself takes care of any drop logic, we just give it the opportunity.
        unsafe {
            for index in 0..COUNT {
                let slot = ptr.add(index);
                (*slot).as_mut_ptr().drop_in_place();
            }

            dealloc(self.ptr as *mut u8, Self::layout());
        }
    }
}

pub struct PinnedSlabInserter<'s, T, const COUNT: usize> {
    slab: &'s mut PinnedSlab<T, COUNT>,

    /// Index at which the item will be inserted.
    index: usize,
}

impl<'s, T, const COUNT: usize> PinnedSlabInserter<'s, T, COUNT> {
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
                "entry {} was not vacant when we inserted into it",
                self.index
            ),
        };

        let pinned_ref: Pin<&'v T> = match slot {
            // SAFETY: Items are always pinned - that is the point of this collection.
            Entry::Occupied { value } => unsafe { Pin::new_unchecked(value) },
            Entry::Vacant { .. } => panic!(
                "entry {} was not occupied after we inserted into it",
                self.index
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
                "entry {} was not vacant when we inserted into it",
                self.index
            ),
        };

        let ptr = match slot {
            Entry::Occupied { value } => value as *mut T,
            Entry::Vacant { .. } => panic!(
                "entry {} was not occupied after we inserted into it",
                self.index
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

        let previous_entry = mem::replace(slot, Entry::Occupied { value: MaybeUninit::uninit() });

        self.slab.next_free_index = match previous_entry {
            Entry::Vacant { next_free_index } => next_free_index,
            Entry::Occupied { .. } => panic!(
                "entry {} was not vacant when we inserted into it",
                self.index
            ),
        };

        let ptr = match slot {
            Entry::Occupied { value } => value as *mut MaybeUninit<T>,
            Entry::Vacant { .. } => panic!(
                "entry {} was not occupied after we inserted into it",
                self.index
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
    };

    #[test]
    fn smoke_test() {
        let mut slab = PinnedSlab::<u32, 3>::new();

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
        let mut slab = PinnedSlab::<u32, 3>::new();

        slab.insert(42);
        slab.insert(43);
        slab.insert(44);

        slab.insert(45);
    }

    #[test]
    #[should_panic]
    fn panic_when_oob_get() {
        let mut slab = PinnedSlab::<u32, 3>::new();

        slab.insert(42);
        slab.get(1234);
    }

    #[test]
    fn begin_insert_returns_correct_key() {
        let mut slab = PinnedSlab::<u32, 3>::new();

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
        let mut slab = PinnedSlab::<u32, 3>::new();

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
        let mut slab = PinnedSlab::<u32, 3>::new();

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
        let mut slab = PinnedSlab::<u32, 3>::new();

        slab.remove(1);
    }

    #[test]
    #[should_panic]
    fn get_vacant_panics() {
        let slab = PinnedSlab::<u32, 3>::new();

        slab.get(1);
    }

    #[test]
    #[should_panic]
    fn get_mut_vacant_panics() {
        let mut slab = PinnedSlab::<u32, 3>::new();

        slab.get_mut(1);
    }

    #[test]
    fn in_refcell_works_fine() {
        let slab = RefCell::new(PinnedSlab::<u32, 3>::new());

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
        let mut slab = PinnedSlab::<Droppable, 3>::new();

        let a = slab.insert(Droppable {
            dropped: dropped.clone(),
        });
        slab.remove(a);

        assert!(dropped.get());
    }
}
