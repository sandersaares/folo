use crate::mem::{DropPolicy, PinnedSlab, PinnedSlabInserter};
use std::{mem::MaybeUninit, pin::Pin};

/// Links up an arbitrary number of PinnedSlabs into a dynamically sized chain that contains any
/// number of items. The capacity grows automatically as needed, to meet demand, creating room
/// for SLAB_CAPACITY more items linearly each time the chain grows.
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
///
/// As of today, the collection never shrinks, though future versions may do so.
#[derive(Debug)]
pub struct PinnedSlabChain<T, const SLAB_CAPACITY: usize = 128> {
    /// The slabs in the chain. We use a Vec here to allow for dynamic sizing.
    /// For now, we only grow the Vec but in theory, one could implement shrinking as well.
    slabs: Vec<PinnedSlab<T, SLAB_CAPACITY>>,

    drop_policy: DropPolicy,
}

impl<T, const SLAB_CAPACITY: usize> PinnedSlabChain<T, SLAB_CAPACITY> {
    pub fn new(drop_policy: DropPolicy) -> Self {
        Self {
            slabs: Vec::new(),
            drop_policy,
        }
    }

    pub fn len(&self) -> usize {
        self.slabs.iter().map(|slab| slab.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.slabs.iter().all(|slab| slab.is_empty())
    }

    /// # Panics
    ///
    /// Panics if the index is out of bounds or is not associated with an item.
    pub fn get(&self, index: usize) -> Pin<&T> {
        let index = ChainIndex::<SLAB_CAPACITY>::from_whole(index);

        self.slabs
            .get(index.slab)
            .map(|slab| slab.get(index.index_in_slab))
            .expect("index was out of bounds of slab chain")
    }

    /// # Panics
    ///
    /// Panics if the index is out of bounds or is not associated with an item.
    pub fn get_mut(&mut self, index: usize) -> Pin<&mut T> {
        let index = ChainIndex::<SLAB_CAPACITY>::from_whole(index);

        self.slabs
            .get_mut(index.slab)
            .map(|slab| slab.get_mut(index.index_in_slab))
            .expect("index was out of bounds of slab chain")
    }

    pub fn begin_insert<'a, 'b>(&'a mut self) -> PinnedSlabChainInserter<'b, T, SLAB_CAPACITY>
    where
        'a: 'b,
    {
        let slab_index = self.index_of_slab_with_vacant_slot();
        let slab = self
            .slabs
            .get_mut(slab_index)
            .expect("we just verified that there is a slab with a vacant slot at this index");

        let slab_inserter = slab.begin_insert();

        PinnedSlabChainInserter {
            slab_inserter,
            slab_index,
        }
    }

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
        let index = ChainIndex::<SLAB_CAPACITY>::from_whole(index);

        let Some(slab) = self.slabs.get_mut(index.slab) else {
            panic!("index was out of bounds of slab chain")
        };

        slab.remove(index.index_in_slab);
    }

    fn index_of_slab_with_vacant_slot(&mut self) -> usize {
        if let Some((index, _)) = self
            .slabs
            .iter()
            .enumerate()
            .find(|(_, slab)| !slab.is_full())
        {
            index
        } else {
            self.slabs.push(PinnedSlab::new(self.drop_policy));
            self.slabs.len() - 1
        }
    }

    #[cfg(test)]
    pub fn integrity_check(&self) {
        for slab in &self.slabs {
            slab.integrity_check();
        }
    }
}

pub struct PinnedSlabChainInserter<'s, T, const SLAB_CAPACITY: usize> {
    slab_inserter: PinnedSlabInserter<'s, T, SLAB_CAPACITY>,
    slab_index: usize,
}

impl<'s, T, const SLAB_CAPACITY: usize> PinnedSlabChainInserter<'s, T, SLAB_CAPACITY> {
    pub fn insert<'v>(self, value: T) -> Pin<&'v T>
    where
        's: 'v,
    {
        self.slab_inserter.insert(value)
    }

    pub fn insert_raw(self, value: T) -> *mut T {
        self.slab_inserter.insert_raw(value)
    }

    /// Inserts an item without initializing it. Depending on the type T, the caller may want to
    /// initialize it with all-zero bytes, skip initializing it entirely (if all bit patterns are
    /// valid) or initialize it manually with some custom value or pointer writes.
    pub fn insert_uninit(self) -> *mut MaybeUninit<T> {
        self.slab_inserter.insert_uninit()
    }

    pub fn index(&self) -> usize {
        ChainIndex::<SLAB_CAPACITY>::from_parts(self.slab_index, self.slab_inserter.index())
            .to_whole()
    }
}

struct ChainIndex<const SLAB_CAPACITY: usize> {
    slab: usize,
    index_in_slab: usize,
}

impl<const SLAB_CAPACITY: usize> ChainIndex<SLAB_CAPACITY> {
    fn from_parts(slab: usize, index_in_slab: usize) -> Self {
        Self {
            slab,
            index_in_slab,
        }
    }

    fn from_whole(whole: usize) -> Self {
        Self {
            slab: whole / SLAB_CAPACITY,
            index_in_slab: whole % SLAB_CAPACITY,
        }
    }

    fn to_whole(&self) -> usize {
        self.slab * SLAB_CAPACITY + self.index_in_slab
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::RefCell,
        sync::{Arc, Mutex},
        thread,
    };

    #[test]
    fn smoke_test() {
        let mut chain = PinnedSlabChain::<u32, 3>::new(DropPolicy::MayDropItems);

        let a = chain.insert(42);
        let b = chain.insert(43);
        let c = chain.insert(44);

        assert_eq!(*chain.get(a), 42);
        assert_eq!(*chain.get(b), 43);
        assert_eq!(*chain.get(c), 44);

        chain.remove(b);

        let d = chain.insert(45);

        assert_eq!(*chain.get(a), 42);
        assert_eq!(*chain.get(c), 44);
        assert_eq!(*chain.get(d), 45);

        // Should expand chain to 2nd slab.
        chain.insert(90);
    }

    #[test]
    #[should_panic]
    fn panic_when_empty_oob_get() {
        let chain = PinnedSlabChain::<u32, 3>::new(DropPolicy::MayDropItems);

        chain.get(0);
    }

    #[test]
    #[should_panic]
    fn panic_when_oob_get() {
        let mut chain = PinnedSlabChain::<u32, 3>::new(DropPolicy::MayDropItems);

        chain.insert(42);
        chain.get(1234);
    }

    #[test]
    fn begin_insert_returns_correct_key() {
        let mut chain = PinnedSlabChain::<u32, 3>::new(DropPolicy::MayDropItems);

        let inserter = chain.begin_insert();
        assert_eq!(inserter.index(), 0);
        inserter.insert(10);
        assert_eq!(*chain.get(0), 10);

        let inserter = chain.begin_insert();
        assert_eq!(inserter.index(), 1);
        inserter.insert(11);
        assert_eq!(*chain.get(1), 11);

        let inserter = chain.begin_insert();
        assert_eq!(inserter.index(), 2);
        inserter.insert(12);
        assert_eq!(*chain.get(2), 12);
    }

    #[test]
    fn abandoned_inserter_is_noop() {
        let mut chain = PinnedSlabChain::<u32, 3>::new(DropPolicy::MayDropItems);

        let inserter = chain.begin_insert();
        assert_eq!(inserter.index(), 0);

        let inserter = chain.begin_insert();
        assert_eq!(inserter.index(), 0);
        inserter.insert(20);

        assert_eq!(*chain.get(0), 20);

        chain.insert(123);
        chain.insert(456);
    }

    #[test]
    fn remove_makes_room() {
        let mut chain = PinnedSlabChain::<u32, 3>::new(DropPolicy::MayDropItems);

        let a = chain.insert(42);
        let b = chain.insert(43);
        let c = chain.insert(44);

        chain.remove(b);

        let d = chain.insert(45);

        assert_eq!(*chain.get(a), 42);
        assert_eq!(*chain.get(c), 44);
        assert_eq!(*chain.get(d), 45);
    }

    #[test]
    #[should_panic]
    fn remove_empty_panics() {
        let mut chain = PinnedSlabChain::<u32, 3>::new(DropPolicy::MayDropItems);

        chain.remove(11234);
    }

    #[test]
    #[should_panic]
    fn remove_vacant_panics() {
        let mut chain = PinnedSlabChain::<u32, 3>::new(DropPolicy::MayDropItems);

        chain.insert(1234); // Ensure there is at least one slab, for

        chain.remove(1);
    }

    #[test]
    #[should_panic]
    fn remove_oob_panics() {
        let mut chain = PinnedSlabChain::<u32, 3>::new(DropPolicy::MayDropItems);

        chain.insert(1234); // Ensure there is at least one slab, for

        chain.remove(1536735);
    }

    #[test]
    #[should_panic]
    fn get_vacant_panics() {
        let mut chain = PinnedSlabChain::<u32, 3>::new(DropPolicy::MayDropItems);

        chain.insert(1234); // Ensure there is at least one slab, for

        chain.get(1);
    }

    #[test]
    #[should_panic]
    fn get_mut_vacant_panics() {
        let mut chain = PinnedSlabChain::<u32, 3>::new(DropPolicy::MayDropItems);

        chain.insert(1234); // Ensure there is at least one slab, for

        chain.get_mut(1);
    }

    #[test]
    fn in_refcell_works_fine() {
        let chain = RefCell::new(PinnedSlabChain::<u32, 3>::new(DropPolicy::MayDropItems));

        {
            let mut chain = chain.borrow_mut();
            let a = chain.insert(42);
            let b = chain.insert(43);
            let c = chain.insert(44);

            assert_eq!(*chain.get(a), 42);
            assert_eq!(*chain.get(b), 43);
            assert_eq!(*chain.get(c), 44);

            chain.remove(b);

            let d = chain.insert(45);

            assert_eq!(*chain.get(a), 42);
            assert_eq!(*chain.get(c), 44);
            assert_eq!(*chain.get(d), 45);
        }

        {
            let chain = chain.borrow();
            assert_eq!(*chain.get(0), 42);
        }
    }

    #[test]
    fn multithreaded_via_mutex() {
        let chain = Arc::new(Mutex::new(PinnedSlabChain::<u32, 3>::new(DropPolicy::MayDropItems)));

        let a;
        let b;
        let c;

        {
            let mut chain = chain.lock().unwrap();
            a = chain.insert(42);
            b = chain.insert(43);
            c = chain.insert(44);

            assert_eq!(*chain.get(a), 42);
            assert_eq!(*chain.get(b), 43);
            assert_eq!(*chain.get(c), 44);
        }

        let chain_clone = Arc::clone(&chain);
        thread::spawn(move || {
            let mut chain = chain_clone.lock().unwrap();

            chain.remove(b);

            let d = chain.insert(45);

            assert_eq!(*chain.get(a), 42);
            assert_eq!(*chain.get(c), 44);
            assert_eq!(*chain.get(d), 45);
        });

        let chain = chain.lock().unwrap();
        assert!(!chain.is_empty());
    }

    #[test]
    #[should_panic]
    fn drop_item_with_forbidden_to_drop_policy_panics() {
        let mut chain = PinnedSlabChain::<u32, 3>::new(DropPolicy::MustNotDropItems);
        chain.insert(123);
    }

    #[test]
    fn drop_itemless_with_forbidden_to_drop_policy_ok() {
        _ = PinnedSlabChain::<u32, 3>::new(DropPolicy::MustNotDropItems);
    }
}
