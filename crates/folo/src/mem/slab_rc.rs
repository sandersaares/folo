use crate::mem::PinnedSlabChain;
use std::{
    cell::{Cell, RefCell},
    pin::Pin,
    rc::Rc,
};

use super::DropPolicy;

/// Type alias for a PinnedSlabChain with the right type to contain a SlabRcBox holding a T, with
/// an outer RefCell that performs runtime borrow checking for safety. This is the backing storage
/// for reference-counting SlabRc smart pointers.
///
/// This storage is compatible with all types of SlabRc smart pointers, though you may need to wrap
/// it in some extra layers they you can call the desired `insert_into_*()` method on it:
///
/// * `Rc<SlabRcStorage<T>>` if you want to use `RcSlabRc`.
/// * `Pin<Box<SlabRcStorage<T>>>>` if you want to use `UnsafeSlabRc`.
///
/// There is also a shorthand function for creating a new slab chain with this type, specialized
/// for the different kinds of smart pointers:
/// * `SlabRcBox<T>::new_storage_ref()`
/// * `SlabRcBox<T>::new_storage_rc()`
/// * `SlabRcBox<T>::new_storage_unsafe()`
pub type SlabRcStorage<T> = RefCell<PinnedSlabChain<SlabRcBox<T>>>;

/// Can be used as the item type in a pinned slab chain to transform it into a reference-counting
/// slab chain, where an item is removed from the chain when the last reference to it is dropped.
///
/// This is an opaque type whose utility ends after it has been inserted into a slab chain. Insert
/// the item via `.insert_into()` and thereafter access it via the `SlabRc` you obtain from this.
///
/// There are different forms of SlabRc that can be created to point at this item, differing by the
/// way in which they reference the slab itself:
///
/// * `RefSlabRc` maintains a reference to the slab chain, which means the slab chain is borrowed
///   for as long as any smart pointer into it is alive. Simple for lifetime management but you
///   will need to add lifetime annotations EVERYWHERE you use the smart pointers.
/// * `RcSlabRc` maintains a reference to the slab chain via another `Rc`, which removes the need to
///   track lifetimes but incurs extra reference counting cost for each operation (which may be
///   negligible).
/// * `UnsafeSlabRc` maintains a reference to a the slab chain using a raw pointer. Obviously rather
///   unsafe to use and requires the slab chain itself to be pinned but if you can guarantee that no
///   smart pointer will ever be alive after the slab chain is dropped, this is essentially free of
///   any runtime overhead.
///
/// # Example
///
/// Using `RefSlabRc` whereby each smart pointer maintains a direct reference to the storage:
///
/// ```
/// use folo::mem::{SlabRcBox, RefSlabRc};
///
/// let storage = SlabRcBox::<usize>::new_storage_ref();
///
/// let item = SlabRcBox::new(42).insert_into_ref(&storage);
/// assert_eq!(*item.deref_pin(), 42);
///
/// let item_clone = RefSlabRc::clone(&item);
/// assert_eq!(*item_clone.deref_pin(), 42);
/// ```
///
/// Using `RcSlabRc` whereby each smart pointer maintains a reference to the storage via an `Rc`:
///
/// ```
/// use std::rc::Rc;
/// use folo::mem::{SlabRcBox, RcSlabRc};
///
/// let storage = SlabRcBox::<usize>::new_storage_rc();
///
/// let item = SlabRcBox::new(42).insert_into_rc(Rc::clone(&storage));
/// assert_eq!(*item.deref_pin(), 42);
///
/// let item_clone = RcSlabRc::clone(&item);
/// assert_eq!(*item_clone.deref_pin(), 42);
/// ```
///
/// Using `UnsafeSlabRc` whereby each smart pointer maintains a raw pointer to the storage:
///
/// ```
/// use folo::mem::{SlabRcBox, UnsafeSlabRc};
///
/// let storage = SlabRcBox::<usize>::new_storage_unsafe();
///
/// // SAFETY: We are responsible for ensuring the slab chain outlives all the smart pointers.
/// // In this case, they both are dropped in the same function, so life is easy. At other
/// // times, it may not be so easy!
/// let item = unsafe { SlabRcBox::new(42).insert_into_unsafe(storage.as_ref()) };
/// assert_eq!(*item.deref_pin(), 42);
///
/// let item_clone = UnsafeSlabRc::clone(&item);
/// assert_eq!(*item_clone.deref_pin(), 42);
/// ```
#[derive(Debug)]
pub struct SlabRcBox<T> {
    value: T,
    ref_count: Cell<usize>,
}

impl<T> SlabRcBox<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            ref_count: Cell::new(0),
        }
    }

    /// Inserts the boxed value into a slab chain that will be referenced via direct reference.
    ///
    /// You can easily allocate such a slab chain via `new_storage_ref()`.
    pub fn insert_into_ref(
        self,
        slab_chain: &RefCell<PinnedSlabChain<SlabRcBox<T>>>,
    ) -> RefSlabRc<'_, T> {
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

        RefSlabRc {
            slab_chain,
            // SAFETY: The risk is that we un-pin something !Unpin. We do not do that - all pinned
            // slab items are forever pinned and we always expose them as pinned pointers.
            value: unsafe { Pin::into_inner_unchecked(value) } as *const _,
            index,
        }
    }

    /// Inserts the boxed value into a slab chain that will be referenced via `Rc`.
    ///
    /// You can easily allocate such a slab chain via `new_storage_rc()`.
    pub fn insert_into_rc(
        self,
        slab_chain: Rc<RefCell<PinnedSlabChain<SlabRcBox<T>>>>,
    ) -> RcSlabRc<T> {
        let (index, value) = {
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

            // SAFETY: The risk is that we un-pin something !Unpin. We do not do that - all pinned
            // slab items are forever pinned and we always expose them via `Pin`.
            let value = unsafe { Pin::into_inner_unchecked(value) } as *const _;

            (index, value)
        };

        RcSlabRc {
            slab_chain,
            value,
            index,
        }
    }

    /// Inserts the boxed value into a slab chain that will be referenced via a raw pointer.
    ///
    /// You can easily allocate such a slab chain via `new_storage_unsafe()`.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that the slab chain outlives every box inserted into it.
    pub unsafe fn insert_into_unsafe(
        self,
        slab_chain: Pin<&RefCell<PinnedSlabChain<SlabRcBox<T>>>>,
    ) -> UnsafeSlabRc<T> {
        let (index, value) = {
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

            // SAFETY: The risk is that we un-pin something !Unpin. We do not do that - all pinned
            // slab items are forever pinned and we always expose them as pinned pointers.
            let value = unsafe { Pin::into_inner_unchecked(value) } as *const _;

            (index, value)
        };

        UnsafeSlabRc {
            slab_chain: Pin::into_inner_unchecked(slab_chain) as *const _,
            value,
            index,
        }
    }

    /// Allocates a new `SlabRc` storage intended for use with `insert_into_ref()`.
    ///
    /// # Panics
    ///
    /// All `SlabRc` values must be dropped by the time the storage is dropped or it will panic.
    pub fn new_storage_ref() -> RefCell<PinnedSlabChain<SlabRcBox<T>>> {
        // We configure "must not drop items" policy because if all the SlabRcs are holding
        // references to the slab then they should be cleaning up items when the SlabRcs are
        // dropped. Therefore, if something still exists in the slab chain afterwards, something
        // went very wrong and we need to raise the alarm.
        RefCell::new(PinnedSlabChain::new(DropPolicy::MustNotDropItems))
    }

    /// Allocates a new `SlabRc` storage intended for use with `insert_into_rc()`.
    ///
    /// # Panics
    ///
    /// All `SlabRc` values must be dropped by the time the storage is dropped or it will panic.
    pub fn new_storage_rc() -> Rc<RefCell<PinnedSlabChain<SlabRcBox<T>>>> {
        // We configure "must not drop items" policy because if all the SlabRcs are holding
        // references to the slab via Rc then it should be impossible for the slab chain to drop
        // first because the references from its own items should be holding it alive.
        Rc::new(RefCell::new(PinnedSlabChain::new(
            DropPolicy::MustNotDropItems,
        )))
    }

    /// Allocates a new `SlabRc` storage intended for use with `insert_into_unsafe()`.
    ///
    /// # Panics
    ///
    /// All `SlabRc` values must be dropped by the time the storage is dropped or it will panic.
    pub fn new_storage_unsafe() -> Pin<Box<RefCell<PinnedSlabChain<SlabRcBox<T>>>>> {
        // It is the responsibility of the caller to ensure that the slab chain outlives all the
        // smart pointers that point into it. Dropping the slab chain while there are still items
        // in it here indicates that the caller failed to perform their duty.
        Box::pin(RefCell::new(PinnedSlabChain::new(
            DropPolicy::MustNotDropItems,
        )))
    }
}

impl<T> From<T> for SlabRcBox<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

// ################## RefSlabRc ################## //

/// A reference-counting smart pointer to an item stored in a PinnedSlabChain<SlabRcBox<T>>. You can
/// get a pinned reference to the item via `deref_pin()` and you can clone the smart pointer and
/// that's about it.
///
/// # Panics
///
/// Dropping a `SlabRc` may take an exclusive reference on the slab chain via runtime borrow
/// checking. Make sure you are not holding any references to the slab chain yourself when dropping
/// any `SlabRc` values or the drop will panic.
#[derive(Debug)]
pub struct RefSlabRc<'slab, T> {
    // We may need to mutate the chain at any time, so we require it to be in a RefCell.
    slab_chain: &'slab RefCell<PinnedSlabChain<SlabRcBox<T>>>,

    index: usize,

    // We ourselves are keeping this value alive, so we do not take a reference to it but rather
    // store it directly as a pointer that we can turn into an appropriately-lifetimed reference
    // on demand.
    value: *const SlabRcBox<T>,
}

impl<T> RefSlabRc<'_, T> {
    pub fn deref_pin(&self) -> Pin<&T> {
        // SAFETY: We are the thing keeping the `value` pointer alive, so this is safe.
        // The value we point to is guaranteed pinned, so we are not at risk of unpinning anything.
        unsafe { Pin::new_unchecked(&(*self.value).value) }
    }
}

impl<T> Clone for RefSlabRc<'_, T> {
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

impl<T> Drop for RefSlabRc<'_, T> {
    fn drop(&mut self) {
        // SAFETY: We are the thing keeping the `value` pointer alive, so this is safe.
        let ref_count = unsafe { &*self.value }.ref_count.get();

        assert!(ref_count > 0);

        if ref_count == 1 {
            self.slab_chain.borrow_mut().remove(self.index);
            // `value` points to invalid memory now, which is allowed for raw pointers.
            // There is no regular reference to `value` existing in this branch.
        } else {
            // SAFETY: We are the thing keeping the `value` pointer alive, so this is safe.
            unsafe { &*self.value }.ref_count.set(ref_count - 1);
        }
    }
}

// ################## RcSlabRc ################## //

/// A reference-counting smart pointer to an item stored in a PinnedSlabChain<SlabRcBox<T>>. You can
/// get a pinned reference to the item via `deref_pin()` and you can clone the smart pointer and
/// that's about it.
///
/// # Panics
///
/// Dropping a `SlabRc` may take an exclusive reference on the slab chain via runtime borrow
/// checking. Make sure you are not holding any references to the slab chain yourself when dropping
/// any `SlabRc` values or the drop will panic.
#[derive(Debug)]
pub struct RcSlabRc<T> {
    // We may need to mutate the chain at any time, so we require it to be in a RefCell.
    slab_chain: Rc<RefCell<PinnedSlabChain<SlabRcBox<T>>>>,

    index: usize,

    // We ourselves are keeping this value alive, so we do not take a reference to it but rather
    // store it directly as a pointer that we can turn into an appropriately-lifetimed reference
    // on demand.
    value: *const SlabRcBox<T>,
}

impl<T> RcSlabRc<T> {
    pub fn deref_pin(&self) -> Pin<&T> {
        // SAFETY: We are the thing keeping the `value` pointer alive, so this is safe.
        // The value we point to is guaranteed pinned, so we are not at risk of unpinning anything.
        unsafe { Pin::new_unchecked(&(*self.value).value) }
    }
}

impl<T> Clone for RcSlabRc<T> {
    fn clone(&self) -> Self {
        // SAFETY: We are the thing keeping the `value` pointer alive, so this is safe.
        let value = unsafe { &*self.value };
        value.ref_count.set(value.ref_count.get() + 1);

        Self {
            slab_chain: Rc::clone(&self.slab_chain),
            value: self.value,
            index: self.index,
        }
    }
}

impl<T> Drop for RcSlabRc<T> {
    fn drop(&mut self) {
        // SAFETY: We are the thing keeping the `value` pointer alive, so this is safe.
        let ref_count = unsafe { &*self.value }.ref_count.get();

        assert!(ref_count > 0);

        if ref_count == 1 {
            self.slab_chain.borrow_mut().remove(self.index);
            // `value` points to invalid memory now, which is allowed for raw pointers.
            // There is no regular reference to `value` existing in this branch.
        } else {
            // SAFETY: We are the thing keeping the `value` pointer alive, so this is safe.
            unsafe { &*self.value }.ref_count.set(ref_count - 1);
        }
    }
}

// ################## UnsafeSlabRc ################## //

/// A reference-counting smart pointer to an item stored in a PinnedSlabChain<SlabRcBox<T>>. You can
/// get a pinned reference to the item via `deref_pin()` and you can clone the smart pointer and
/// that's about it.
///
/// # Safety
///
/// This smart pointer maintains a raw reference to the underlying slab chain. The caller is
/// responsible for ensuring that the lifetime of the slab chain exceeds the lifetime of every
/// smart pointer into the slab chain.
///
/// # Panics
///
/// Dropping a `SlabRc` may take an exclusive reference on the slab chain via runtime borrow
/// checking. Make sure you are not holding any references to the slab chain yourself when dropping
/// any `SlabRc` values or the drop will panic.
#[derive(Debug)]
pub struct UnsafeSlabRc<T> {
    // We may need to mutate the chain at any time, so we require it to be in a RefCell.
    // The caller is responsible for ensuring this outlives us.
    slab_chain: *const RefCell<PinnedSlabChain<SlabRcBox<T>>>,

    index: usize,

    // We ourselves are keeping this value alive, so we do not take a reference to it but rather
    // store it directly as a pointer that we can turn into an appropriately-lifetimed reference
    // on demand.
    value: *const SlabRcBox<T>,
}

impl<T> UnsafeSlabRc<T> {
    pub fn deref_pin(&self) -> Pin<&T> {
        // SAFETY: We are the thing keeping the `value` pointer alive, so this is safe.
        // The value we point to is guaranteed pinned, so we are not at risk of unpinning anything.
        unsafe { Pin::new_unchecked(&(*self.value).value) }
    }
}

impl<T> Clone for UnsafeSlabRc<T> {
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

impl<T> Drop for UnsafeSlabRc<T> {
    fn drop(&mut self) {
        // SAFETY: We are the thing keeping the `value` pointer alive, so this is safe.
        let ref_count = unsafe { &*self.value }.ref_count.get();

        assert!(ref_count > 0);

        if ref_count == 1 {
            // SAFETY: The caller is responsible for ensuring the slab chain outlives us.
            let slab_chain = unsafe { &*self.slab_chain };
            slab_chain.borrow_mut().remove(self.index);
            // `value` points to invalid memory now, which is allowed for raw pointers.
            // There is no regular reference to `value` existing in this branch.
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
    fn ref_smoke_test() {
        let storage = SlabRcBox::<usize>::new_storage_ref();

        let item = SlabRcBox::new(42).insert_into_ref(&storage);
        assert_eq!(*item.deref_pin(), 42);

        let item_clone = RefSlabRc::clone(&item);
        assert_eq!(*item_clone.deref_pin(), 42);

        drop(item);
    }

    #[test]
    fn ref_value_is_dropped_after_last_rc_drop() {
        // While we do not exactly have a way to introspect a slab chain, we can do our own checks
        // by holding a weak reference and seeing if the weak reference becomes dead when the last
        // strong reference is dropped via the SlabRc.

        let canary = Arc::new(55);
        let canary_weak = Arc::downgrade(&canary);

        let storage = SlabRcBox::<Arc<usize>>::new_storage_ref();

        let item = SlabRcBox::new(canary).insert_into_ref(&storage);
        assert_eq!(**item.deref_pin(), 55);

        let item_clone = RefSlabRc::clone(&item);
        assert_eq!(**item_clone.deref_pin(), 55);

        drop(item);
        drop(item_clone);

        assert!(canary_weak.upgrade().is_none());
    }

    #[test]
    fn rc_smoke_test() {
        let storage = SlabRcBox::<usize>::new_storage_rc();

        let item = SlabRcBox::new(42).insert_into_rc(Rc::clone(&storage));
        assert_eq!(*item.deref_pin(), 42);

        let item_clone = RcSlabRc::clone(&item);
        assert_eq!(*item_clone.deref_pin(), 42);

        drop(item);
    }

    #[test]
    fn rc_value_is_dropped_after_last_rc_drop() {
        // While we do not exactly have a way to introspect a slab chain, we can do our own checks
        // by holding a weak reference and seeing if the weak reference becomes dead when the last
        // strong reference is dropped via the SlabRc.

        let canary = Arc::new(55);
        let canary_weak = Arc::downgrade(&canary);

        let storage = SlabRcBox::<Arc<usize>>::new_storage_rc();

        let item = SlabRcBox::new(canary).insert_into_rc(Rc::clone(&storage));
        assert_eq!(**item.deref_pin(), 55);

        let item_clone = RcSlabRc::clone(&item);
        assert_eq!(**item_clone.deref_pin(), 55);

        drop(item);
        drop(item_clone);

        assert!(canary_weak.upgrade().is_none());
    }

    #[test]
    fn unsafe_smoke_test() {
        let storage = SlabRcBox::<usize>::new_storage_unsafe();

        // SAFETY: We are responsible for ensuring the slab chain outlives all the smart pointers.
        // In this case, they both are dropped in the same function, so life is easy. At other
        // times, it may not be so easy!
        let item = unsafe { SlabRcBox::new(42).insert_into_unsafe(storage.as_ref()) };
        assert_eq!(*item.deref_pin(), 42);

        let item_clone = UnsafeSlabRc::clone(&item);
        assert_eq!(*item_clone.deref_pin(), 42);

        drop(item);
    }

    #[test]
    fn unsafe_value_is_dropped_after_last_rc_drop() {
        // While we do not exactly have a way to introspect a slab chain, we can do our own checks
        // by holding a weak reference and seeing if the weak reference becomes dead when the last
        // strong reference is dropped via the SlabRc.

        let canary = Arc::new(55);
        let canary_weak = Arc::downgrade(&canary);

        let storage = SlabRcBox::<Arc<usize>>::new_storage_unsafe();

        // SAFETY: We are responsible for ensuring the slab chain outlives all the smart pointers.
        // In this case, they both are dropped in the same function, so life is easy. At other
        // times, it may not be so easy!
        let item = unsafe { SlabRcBox::new(canary).insert_into_unsafe(storage.as_ref()) };
        assert_eq!(**item.deref_pin(), 55);

        let item_clone = UnsafeSlabRc::clone(&item);
        assert_eq!(**item_clone.deref_pin(), 55);

        drop(item);
        drop(item_clone);

        assert!(canary_weak.upgrade().is_none());
    }
}
