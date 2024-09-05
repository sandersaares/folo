use core::panic;
use std::alloc::{alloc, dealloc, Layout};
use std::cell::{Ref, RefCell, RefMut};
use std::mem::{self, MaybeUninit};

/// A pinned fixed-size heap-allocated slab of values. Works similar to a Vec
/// but pinned and with a fixed size, operating using an index for lookup.
#[derive(Debug)]
pub struct PinnedSlab<T, const COUNT: usize> {
    ptr: *mut Entry<T>,

    /// Index of the next free slot in the slab. Think of this as a virtual stack, with the stack
    /// entries stored in the slab entries themselves. This will point out of bounds if the slab
    /// is full.
    next_free_index: usize,
}

enum Entry<T> {
    /// We use a RefCell here to disconnect the lifetime of the references we return from the
    /// lifetime of the slab itself (returning an exclusive reference to an item does not imply
    /// that the slab itself is borrowed exclusively).
    Occupied {
        value: RefCell<T>,
    },

    Vacant {
        next_free_index: usize,
    },
}

impl<T, const COUNT: usize> PinnedSlab<T, COUNT> {
    pub fn new() -> Self {
        let ptr = unsafe { alloc(Self::layout()) as *mut MaybeUninit<Entry<T>> };

        // Initialize them all to `Vacant` to start with.
        // We can now assume the slab is initialized - safe to access without causing UB.
        for index in 0..COUNT {
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
        }
    }

    fn layout() -> Layout {
        Layout::array::<MaybeUninit<Entry<T>>>(COUNT)
            .expect("simple flat array layout must be calculable")
    }

    pub fn is_full(&self) -> bool {
        self.next_free_index >= COUNT
    }

    pub fn get<'v>(&self, index: usize) -> Ref<'v, T> {
        assert!(index < COUNT, "index out of bounds");

        // SAFETY: We did a bounds check and ensured in the ctor that every entry is initialized.
        match unsafe {
            self.ptr
                .add(index)
                .as_ref()
                .expect("we expect the resulting pointer to always be valid")
        } {
            Entry::Occupied { value } => value.borrow(),
            Entry::Vacant { .. } => panic!("entry at given index does not exist"),
        }
    }

    pub fn get_mut<'v>(&mut self, index: usize) -> RefMut<'v, T> {
        assert!(index < COUNT, "index out of bounds");

        // SAFETY: We did a bounds check and ensured in the ctor that every entry is initialized.
        match unsafe {
            self.ptr
                .add(index)
                .as_mut()
                .expect("we expect the resulting pointer to always be valid")
        } {
            Entry::Occupied { ref mut value } => value.borrow_mut(),
            Entry::Vacant { .. } => panic!("entry at given index does not exist"),
        }
    }

    pub fn begin_insert<'s, 'i>(&'s mut self) -> PinnedSlabInserter<'i, T, COUNT>
    where
        's: 'i,
    {
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
        assert!(index < COUNT, "index out of bounds");

        // SAFETY: We did a bounds check and ensured in the ctor that every entry is initialized.
        let slot = unsafe {
            self.ptr
                .add(index)
                .as_mut()
                .expect("we expect the resulting pointer to always be valid")
        };

        if matches!(slot, Entry::Vacant { .. }) {
            panic!("entry at given index does not exist");
        }

        // SAFETY: We know the slot is valid, as per above. We want to explicit run the drop logic
        // in-place because the slots are pinned - we do not want to move the value out in order
        // to drop it.
        unsafe {
            // We know it is initialized, we just use this to facilitate the in-place drop.
            let slot: *mut MaybeUninit<Entry<T>> = mem::transmute(slot);
            slot.drop_in_place();

            slot.write(MaybeUninit::new(Entry::Vacant {
                next_free_index: self.next_free_index,
            }))
        }

        self.next_free_index = index;
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

    pub fn insert<'v>(self, value: T) -> RefMut<'v, T> {
        // SAFETY: We did a bounds check and ensured in the ctor that every entry is initialized.
        let slot = unsafe {
            self.slab
                .ptr
                .add(self.index)
                .as_mut()
                .expect("we expect the resulting pointer to always be valid")
        };

        let previous_entry = mem::replace(
            slot,
            Entry::Occupied {
                value: RefCell::new(value),
            },
        );

        self.slab.next_free_index = match previous_entry {
            Entry::Vacant { next_free_index } => next_free_index,
            Entry::Occupied { .. } => panic!("entry was not vacant when we inserted into it"),
        };

        match slot {
            Entry::Occupied { value } => value.borrow_mut(),
            Entry::Vacant { .. } => panic!("entry was not occupied after we inserted into it"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
