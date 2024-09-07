use super::PinnedSlabChain;
use std::{
    cell::{Cell, RefCell},
    pin::Pin,
};

/// Can be used as the item type in a pinned slab chain to transform it into a reference-counting
/// slab chain, where an item is removed from the chain when the last reference to it is dropped.
///
/// This is an opaque type whose utility ends after it has been inserted into a slab chain. Insert
/// the item via `.insert_into()` and thereafter access it via the `SlabRc` you obtain from this.
pub struct SlabRcCell<T> {
    value: T,
    ref_count: Cell<usize>,
}

impl<T> SlabRcCell<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            ref_count: Cell::new(0),
        }
    }

    pub fn insert_into<'slab, 'value>(
        self,
        slab_chain: &'slab RefCell<PinnedSlabChain<SlabRcCell<T>>>,
    ) -> SlabRc<'slab, T> {
        let mut slab_chain_mut = slab_chain.borrow_mut();
        let inserter = slab_chain_mut.begin_insert();
        let index = inserter.index();

        // We are creating the first reference here, embodied in the first SlabRc we return.
        self.ref_count.set(1);

        // In principle, someone could go around removing arbitrary items from the slab chain and
        // cause memory corruption. However, we do not consider that in scope of our safety model
        // because we are not even exposing the index, so the only attack is to guess the index,
        // which is a sufficient low probability event to happen by accident that it is not worth
        // thinking about (and not worth adding comments about if we chose to mark this unsafe).
        let value = inserter.insert(self);

        SlabRc {
            slab_chain,
            // SAFETY: The risk is that we un-pin something !Unpin. We do not do that - all pinned
            // slab items are forever pinned and we always expose them as pinned pointers.
            value: unsafe { Pin::into_inner_unchecked(value) } as *const _,
            index,
        }
    }
}

impl<T> From<T> for SlabRcCell<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

/// A reference-counting smart pointer to an item stored in a PinnedSlabChain<AsSlabRc<T>>. You can
/// get a pinned reference to it via `deref_pin()` and you can clone it and that's about it.
///
/// # Panics
///
/// Dropping the last reference to an item via `SlabRc` while holding an exclusive reference to the
/// slab chain itself will panic.
pub struct SlabRc<'slab, T> {
    // We may need to mutate the chain at any time, so we require it to be in a RefCell.
    slab_chain: &'slab RefCell<PinnedSlabChain<SlabRcCell<T>>>,

    index: usize,

    // We ourselves are keeping this value alive, so we do not take a reference to it but rather
    // store it directly as a pointer that we can turn into an appropriately-lifetimed reference
    // on demand.
    value: *const SlabRcCell<T>,
}

impl<T> SlabRc<'_, T> {
    pub fn deref_pin(&self) -> Pin<&T> {
        // SAFETY: We are the thing keeping the `value` pointer alive, so this is safe.
        // The value we point to is guaranteed pinned, so we are not at risk of unpinning anything.
        unsafe { Pin::new_unchecked(&(&*self.value).value) }
    }
}

impl<T> Clone for SlabRc<'_, T> {
    fn clone(&self) -> Self {
        // SAFETY: We are the thing keeping the `value` pointer alive, so this is safe.
        let value = unsafe { &*self.value };
        value.ref_count.set(value.ref_count.get() + 1);

        Self {
            slab_chain: self.slab_chain,
            value: self.value,
            index: self.index,
        }
    }
}

impl<T> Drop for SlabRc<'_, T> {
    fn drop(&mut self) {
        // SAFETY: We are the thing keeping the `value` pointer alive, so this is safe.
        let ref_count = unsafe { &*self.value }.ref_count.get();

        assert!(ref_count > 0);

        if ref_count == 1 {
            self.slab_chain.borrow_mut().remove(self.index);
            // `value` points to invalid memory now, which is fine because we are dropping.
        } else {
            // SAFETY: We are the thing keeping the `value` pointer alive, so this is safe.
            unsafe { &*self.value }.ref_count.set(ref_count - 1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn smoke_test() {
        let slab_chain = RefCell::new(PinnedSlabChain::new());
        let item = SlabRcCell::new(42);
        let rc = item.insert_into(&slab_chain);

        assert_eq!(*rc.deref_pin(), 42);
        assert_eq!(*rc.clone().deref_pin(), 42);

        drop(rc);
    }

    #[test]
    fn is_dropped_after_last_rc_drop() {
        // While we do not exactly have a way to introspect a slab chain, we can do our own checks
        // by holding a weak reference and seeing if the weak reference becomes dead when the last
        // strong reference is dropped via the SlabRc.

        let canary = Arc::new(55);
        let canary_weak = Arc::downgrade(&canary);

        let slab_chain = RefCell::new(PinnedSlabChain::new());
        let item = SlabRcCell::new(canary);
        let rc = item.insert_into(&slab_chain);
        assert_eq!(**rc.deref_pin(), 55);
        assert_eq!(**rc.deref_pin(), 55);

        drop(rc);

        assert!(canary_weak.upgrade().is_none());
    }
}
