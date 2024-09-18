use crate::constants;
use crate::mem::DropPolicy;
use crate::{linked, mem::PinnedSlabChain};
use crossbeam::utils::CachePadded;
use std::cell::{RefCell, UnsafeCell};
use std::collections::VecDeque;
use std::pin::Pin;
use std::rc::Rc;
use std::slice;
use std::sync::{Arc, Mutex};

/// A thread-safe shared pool of [u8; N], which hands out handles with pinned shared references
/// to the arrays. All elements must be returned to the pool when the caller is done with them.
///
/// The standard pattern is a `link_ref!` static variable where you store an instance of this.
#[linked::object]
pub struct SharedArrayPool<const LEN: usize> {
    // This is the shared backing storage where every item is located for the duration of its life.
    // This collection guarantees that all items are pinned.
    //
    // We use UnsafeCell because the arrays do not participate in the standard Rust referencing
    // rules model and we simply hand an exclusive reference to PooledArray, which may later be
    // turned into raw pointers and so forth - the user of PooledArray is really who determines what
    // exclusivity/safety rules apply to the array, all we care about is that it is dropped before
    // the storage itself is dropped.
    //
    // There may be additional efficiency to be gained from allocation models that preferable
    // allocate in thread-local pools and only opportunistically access data access threads but that
    // extra complexity is not worth it at this time.
    #[allow(clippy::type_complexity)]
    // Sometimes life is complex and you just got to deal with it.
    shared: Arc<Mutex<PinnedSlabChain<CachePadded<UnsafeCell<[u8; LEN]>>>>>,

    // Any arrays dropped on this thread get stashed in here for fast reuse. We only return to the
    // shared pool in the core when the thread-local linked instance of the pool is dropped.
    local: Rc<RefCell<VecDeque<PooledArray<LEN>>>>,
}

impl<const LEN: usize> SharedArrayPool<LEN> {
    pub fn new() -> Self {
        // MustNotDropItems because we expect all items to be returned to the pool before
        // it is dropped. True, the typical scenario is a static pool that is never dropped
        // but nobody says that is the only way to use it, and we might want to use limited
        // scope pools in testing.
        let core = Arc::new(Mutex::new(PinnedSlabChain::new(
            DropPolicy::MustNotDropItems,
        )));

        linked::new!(Self {
            shared: Arc::clone(&core),
            local: Rc::new(RefCell::new(VecDeque::new())),
        })
    }

    pub fn get(&self) -> PooledArrayLease<LEN> {
        // Try to recycle an array from the local cache if there is one available.
        if let Some(local) = self.local.borrow_mut().pop_front() {
            return PooledArrayLease {
                inner: Some(local),
                return_to: Rc::clone(&self.local),
            };
        }

        // TODO: Steal from another instance of the pool (perhaps on another thread).

        // If we got here, we did not find an existing array, so let's make a new one.
        self.new_array()
    }

    fn new_array(&self) -> PooledArrayLease<LEN> {
        let mut shared = self.shared.lock().expect(constants::POISONED_LOCK);

        let inserter = shared.begin_insert();
        let index = inserter.index();

        // We do not initialize the buffer when we take it from the pool. It has whatever data
        // it had at the start (maybe zeroes, maybe old I/O operation data).
        let storage = inserter.insert_uninit();

        // SAFETY: UnsafeCell<T> is layout-compatible with T in most cases (and definitely in
        // this case), so we can convert that freely. Likewise, MaybeUninit<T> is layout-
        // compatible with T. Finally, we do not care what bit patterns our buffers are
        // initialized with because they will be overwritten by new data anyway as part of some
        // I/O operation. We assume we do not need to worry about dirty contents being somehow
        // dangerous/sensitive here (perhaps might want to consider zeroing per-usecase but that
        // would be the responsibility of whoever put sensitive data in there, before dropping).
        let storage = unsafe { (*storage).assume_init_mut() };

        let array = PooledArray {
            ptr: storage.get() as *mut u8,
            index_in_storage: index,
        };

        PooledArrayLease {
            inner: Some(array),
            return_to: Rc::clone(&self.local),
        }
    }
}

impl<const LEN: usize> Drop for SharedArrayPool<LEN> {
    fn drop(&mut self) {
        // "Return" all local arrays to the shared pool (actually drop them).
        let mut shared = self.shared.lock().expect(constants::POISONED_LOCK);
        let mut local = self.local.borrow_mut();

        for array in local.drain(..) {
            shared.remove(array.index_in_storage);
        }
    }
}

impl<const LEN: usize> Default for SharedArrayPool<LEN> {
    fn default() -> Self {
        Self::new()
    }
}

/// A handle to an array that was obtained from a pool.
/// It will be returned to the pool when the handle is dropped.
pub struct PooledArray<const LEN: usize> {
    ptr: *mut u8,

    index_in_storage: usize,
}

/// A temporary lease entitling the holder to own a pooled array until the lease is dropped.
pub struct PooledArrayLease<const LEN: usize> {
    inner: Option<PooledArray<LEN>>,
    return_to: Rc<RefCell<VecDeque<PooledArray<LEN>>>>,
}

impl<const LEN: usize> PooledArrayLease<LEN> {
    pub fn to_slice(&self) -> Pin<&[u8]> {
        // SAFETY: The slab chain storage guarantees all arrays are pinned. LEN is the correct size.
        unsafe {
            Pin::new_unchecked(slice::from_raw_parts(
                self.inner
                    .as_ref()
                    .expect("value must exist until lease is dropped")
                    .ptr,
                LEN,
            ))
        }
    }

    pub fn to_mut_slice(&mut self) -> Pin<&mut [u8]> {
        // SAFETY: The slab chain storage guarantees all arrays are pinned. LEN is the correct size.
        unsafe {
            Pin::new_unchecked(slice::from_raw_parts_mut(
                self.inner
                    .as_ref()
                    .expect("value must exist until lease is dropped")
                    .ptr,
                LEN,
            ))
        }
    }
}

impl<const LEN: usize> Drop for PooledArrayLease<LEN> {
    fn drop(&mut self) {
        self.return_to.borrow_mut().push_back(
            self.inner
                .take()
                .expect("the lease has already been dropped"),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke_test() {
        let pool = SharedArrayPool::<16>::new();

        let mut lease = pool.get();
        let mut slice = lease.to_mut_slice();
        slice[0..13].copy_from_slice(b"hello, world!");

        assert_eq!(&slice[0..13], b"hello, world!");
    }
}
