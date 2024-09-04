use core::panic;
use std::alloc::{alloc, dealloc, Layout};
use std::mem::{self, MaybeUninit};

/// A pinned fixed-size heap-allocated slab of values. Works similar to a Vec
/// but pinned and with a fixed size, operating using an index for lookup.
/// 
/// We return regular references from the slab but they are all guaranteed to be pinned.
#[derive(Debug)]
pub struct PinnedSlab<T, const COUNT: usize> {
    ptr: *mut Entry<T>,

    len: usize,

    /// Index of the next free slot in the slab. Think of this as a virtual stack, with the stack
    /// entries stored in the slab entries themselves. This will point out of bounds if the slab
    /// is full.
    next_free_index: usize,
}

enum Entry<T> {
    Occupied { value: T },
    Vacant { next_free_index: usize },
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
            len: COUNT,
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

    pub fn get(&self, index: usize) -> Option<&T> {
        if index >= COUNT {
            return None;
        }

        // SAFETY: We did a bounds check and ensured in the ctor that every entry is initialized.
        match unsafe {
            self.ptr
                .add(index)
                .as_ref()
                .expect("we expect the resulting pointer to always be valid")
        } {
            Entry::Occupied { value } => Some(&value),
            Entry::Vacant { .. } => None,
        }
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        if index >= COUNT {
            return None;
        }

        // SAFETY: We did a bounds check and ensured in the ctor that every entry is initialized.
        match unsafe {
            self.ptr
                .add(index)
                .as_mut()
                .expect("we expect the resulting pointer to always be valid")
        } {
            Entry::Occupied { ref mut value } => Some(value),
            Entry::Vacant { .. } => None,
        }
    }

    pub fn begin_insert<'a, 'b>(&'a mut self) -> PinnedSlabInserter<'b, T, COUNT>
    where
        'a: 'b,
    {
        let index = if self.is_full() {
            None
        } else {
            Some(self.next_free_index)
        };

        PinnedSlabInserter { slab: self, index }
    }

    pub fn remove(&mut self, index: usize) {
        if index >= COUNT {
            return;
        }

        // SAFETY: We did a bounds check and ensured in the ctor that every entry is initialized.
        let slot = unsafe {
            self.ptr
                .add(index)
                .as_mut()
                .expect("we expect the resulting pointer to always be valid")
        };

        // SAFETY: We know the slot is valid, as per above. We want to explicit run the drop logic
        // in-place because the slots are pinned - we do not want to move the value out in order
        // to drop it.
        unsafe {
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

pub struct PinnedSlabInserter<'a, T, const COUNT: usize> {
    slab: &'a mut PinnedSlab<T, COUNT>,

    /// Index at which the next item will be inserted. None if the slab is full.
    index: Option<usize>,
}

impl<'a, T, const COUNT: usize> PinnedSlabInserter<'a, T, COUNT> {
    pub fn index(&self) -> Option<usize> {
        self.index
    }

    pub fn insert(self, value: T) -> &'a mut T {
        let index = self.index.expect("cannot insert into a full slab");

        // SAFETY: We did a bounds check and ensured in the ctor that every entry is initialized.
        let slot = unsafe {
            self.slab
                .ptr
                .add(index)
                .as_mut()
                .expect("we expect the resulting pointer to always be valid")
        };

        let previous_entry = mem::replace(slot, Entry::Occupied { value });

        self.slab.next_free_index = match previous_entry {
            Entry::Vacant { next_free_index } => next_free_index,
            Entry::Occupied { .. } => panic!("entry was not vacant when we inserted into it"),
        };

        match slot {
            Entry::Occupied { value } => value,
            Entry::Vacant { .. } => panic!("entry was not occupied after we inserted into it"),
        }
    }
}
