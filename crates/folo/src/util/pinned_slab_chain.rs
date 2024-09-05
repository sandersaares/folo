use super::{PinnedSlab, PinnedSlabInserter};

/// Links up an arbitrary number of PinnedSlabs into a dynamically sized chain.
///
/// Each slab is pinned but the chain itself is not pinned - links can be added/removed on demand
/// to respond to the changes in the size of the data set.
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

    #[allow(dead_code)] // Usage is work in progress.
    pub fn get(&self, index: usize) -> Option<&T> {
        let index = ChainIndex::<SLAB_SIZE>::from_whole(index);

        self.slabs
            .get(index.slab)
            .and_then(|slab| slab.get(index.index_in_slab))
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        let index = ChainIndex::<SLAB_SIZE>::from_whole(index);

        self.slabs
            .get_mut(index.slab)
            .and_then(|slab| slab.get_mut(index.index_in_slab))
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

    pub fn remove(&mut self, index: usize) {
        let index = ChainIndex::<SLAB_SIZE>::from_whole(index);

        if let Some(slab) = self.slabs.get_mut(index.slab) {
            slab.remove(index.index_in_slab);
        }
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
    pub fn insert(self, value: T) -> &'a mut T {
        self.slab_inserter.insert(value)
    }

    pub fn index(&self) -> usize {
        ChainIndex::<SLAB_SIZE>::from_parts(
            self.slab_index,
            self.slab_inserter
                .index()
                .expect("we picked a slab we know contains a vacancy"),
        )
        .to_whole()
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
