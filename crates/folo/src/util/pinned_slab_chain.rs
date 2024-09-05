use std::cell::{Ref, RefMut};

use super::{PinnedSlab, PinnedSlabInserter};

/// Links up an arbitrary number of PinnedSlabs into a dynamically sized chain. The API surface is
/// intended to be equivalent to that of a single PinnedSlab, but with the ability to grow beyond
/// a single slab.
#[derive(Debug)]
pub struct PinnedSlabChain<T, const SLAB_SIZE: usize = 1024> {
    /// The slabs in the chain. We use a Vec here to allow for dynamic sizing.
    /// For now, we only grow the Vec but in theory, one could implement shrinking as well.
    slabs: Vec<PinnedSlab<T, SLAB_SIZE>>,
}

impl<T, const SLAB_SIZE: usize> PinnedSlabChain<T, SLAB_SIZE> {
    pub fn new() -> Self {
        Self { slabs: Vec::new() }
    }

    pub fn get<'v>(&self, index: usize) -> Ref<'v, T> {
        let index = ChainIndex::<SLAB_SIZE>::from_whole(index);

        self.slabs
            .get(index.slab)
            .and_then(|slab| Some(slab.get(index.index_in_slab)))
            .expect("index was out of bounds of slab chain")
    }

    pub fn get_mut<'v>(&mut self, index: usize) -> RefMut<'v, T> {
        let index = ChainIndex::<SLAB_SIZE>::from_whole(index);

        self.slabs
            .get_mut(index.slab)
            .and_then(|slab| Some(slab.get_mut(index.index_in_slab)))
            .expect("index was out of bounds of slab chain")
    }

    pub fn begin_insert<'a, 'b>(&'a mut self) -> PinnedSlabChainInserter<'b, T, SLAB_SIZE>
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

    pub fn remove(&mut self, index: usize) {
        let index = ChainIndex::<SLAB_SIZE>::from_whole(index);

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
            self.slabs.push(PinnedSlab::new());
            self.slabs.len() - 1
        }
    }
}

pub struct PinnedSlabChainInserter<'a, T, const SLAB_SIZE: usize> {
    slab_inserter: PinnedSlabInserter<'a, T, SLAB_SIZE>,
    slab_index: usize,
}

impl<'a, T, const SLAB_SIZE: usize> PinnedSlabChainInserter<'a, T, SLAB_SIZE> {
    pub fn insert<'v>(self, value: T) -> RefMut<'v, T> {
        self.slab_inserter.insert(value)
    }

    pub fn index(&self) -> usize {
        ChainIndex::<SLAB_SIZE>::from_parts(self.slab_index, self.slab_inserter.index()).to_whole()
    }
}

struct ChainIndex<const SLAB_SIZE: usize> {
    slab: usize,
    index_in_slab: usize,
}

impl<const SLAB_SIZE: usize> ChainIndex<SLAB_SIZE> {
    fn from_parts(slab: usize, index_in_slab: usize) -> Self {
        Self {
            slab,
            index_in_slab,
        }
    }

    fn from_whole(whole: usize) -> Self {
        Self {
            slab: whole / SLAB_SIZE,
            index_in_slab: whole % SLAB_SIZE,
        }
    }

    fn to_whole(&self) -> usize {
        self.slab * SLAB_SIZE + self.index_in_slab
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn smoke_test() {
        let mut chain = PinnedSlabChain::<u32, 3>::new();

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
        let chain = PinnedSlabChain::<u32, 3>::new();

        chain.get(0);
    }

    #[test]
    #[should_panic]
    fn panic_when_oob_get() {
        let mut chain = PinnedSlabChain::<u32, 3>::new();

        chain.insert(42);
        chain.get(1234);
    }

    #[test]
    fn begin_insert_returns_correct_key() {
        let mut chain = PinnedSlabChain::<u32, 3>::new();

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
        let mut chain = PinnedSlabChain::<u32, 3>::new();

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
        let mut chain = PinnedSlabChain::<u32, 3>::new();

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
        let mut chain = PinnedSlabChain::<u32, 3>::new();

        chain.remove(11234);
    }

    #[test]
    #[should_panic]
    fn remove_vacant_panics() {
        let mut chain = PinnedSlabChain::<u32, 3>::new();

        chain.insert(1234); // Ensure there is at least one slab, for

        chain.remove(1);
    }

    #[test]
    #[should_panic]
    fn remove_oob_panics() {
        let mut chain = PinnedSlabChain::<u32, 3>::new();

        chain.insert(1234); // Ensure there is at least one slab, for

        chain.remove(1536735);
    }

    #[test]
    #[should_panic]
    fn get_vacant_panics() {
        let mut chain = PinnedSlabChain::<u32, 3>::new();

        chain.insert(1234); // Ensure there is at least one slab, for

        chain.get(1);
    }

    #[test]
    #[should_panic]
    fn get_mut_vacant_panics() {
        let mut chain = PinnedSlabChain::<u32, 3>::new();

        chain.insert(1234); // Ensure there is at least one slab, for

        chain.get_mut(1);
    }

    #[test]
    fn in_refcell_works_fine() {
        let chain = RefCell::new(PinnedSlabChain::<u32, 3>::new());

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
}
