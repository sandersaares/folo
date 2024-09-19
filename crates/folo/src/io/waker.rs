use crate::{collections::BuildPointerHasher, windows::OwnedHandle};
use std::{
    cell::RefCell,
    collections::HashSet,
    hash::{Hash, Hasher},
    sync::Weak,
};
use windows::Win32::{Foundation::HANDLE, System::IO::PostQueuedCompletionStatus};

// Value is meaningless, just has to be unique.
pub(crate) const WAKE_UP_COMPLETION_KEY: usize = 0x23546789897;

/// A cross-thread element that can be used to wake up an I/O driver from another thread.
///
/// The waker itself is a "client" of sorts that can be handed over to any thread. It has a handle
/// to the completion port of the I/O driver. When it wants to wake up the I/O driver, it must post
/// a specific completion packet to the completion port.
///
/// The completion packet is simply a completion message without any payload and the completion key
/// `WAKE_UP_COMPLETION_KEY`. The OVERLAPPED pointer is null for these messages.
#[derive(Clone, Debug)]
pub(crate) struct IoWaker {
    completion_port: Weak<OwnedHandle<HANDLE>>,
}

impl IoWaker {
    pub(crate) fn new(completion_port: Weak<OwnedHandle<HANDLE>>) -> Self {
        Self { completion_port }
    }

    /// Wakes up the target thread via the I/O driver by sending a completion packet to its
    /// completion port. This is a non-blocking operation.
    pub(crate) fn wake(&self) {
        if self.try_add_to_batch() {
            return;
        }

        let completion_port = match self.completion_port.upgrade() {
            Some(port) => port,
            None => return,
        };

        Self::wake_core(&completion_port);
    }

    /// Enables I/O wakers to be processed in batches from the current thread. After this, a call
    /// to wake() will merely queue the operation and it will not be submitted until we call the
    /// submit_batch() method. This allows for de-duplication of wake-up calls.
    pub fn enable_batching() {
        BATCH.with_borrow_mut(|batch| {
            assert!(batch.is_none());
            *batch = Some(HashSet::with_hasher(BuildPointerHasher::default()));
        });
    }

    pub fn submit_batch() {
        BATCH.with_borrow_mut(|batch| {
            let batch = batch.as_mut().expect(
                "if the caller is calling to submit a batch, they must enable batching first",
            );

            for request in batch.drain() {
                let completion_port = match request.completion_port.upgrade() {
                    Some(port) => port,
                    None => continue,
                };

                Self::wake_core(&completion_port);
            }
        });
    }

    // Returns true if the wake was added to the batch and no explicit wake is needed.
    fn try_add_to_batch(&self) -> bool {
        BATCH.with_borrow_mut(|batch| {
            let Some(batch) = batch.as_mut() else {
                return false;
            };

            batch.insert(WakeRequest {
                completion_port: self.completion_port.clone(),
            });

            true
        })
    }

    fn wake_core(completion_port: &OwnedHandle<HANDLE>) {
        // SAFETY: We just need to be concerned with the completion port being valid. This is
        // ensured by OwnedHandle.
        unsafe {
            // Note that OVERLAPPED is null here - we do not need to provide one for this operation
            // because only real operations require it - plain notifications do not.
            //
            // We ignore the result from this because it does not really matter - if anything goes
            // wrong, the target thread fails to wake up and that's too bad but nothing for us to
            // worry about - probably the entire app is going away if that happened anyway.
            _ = PostQueuedCompletionStatus(**completion_port, 0, WAKE_UP_COMPLETION_KEY, None);
        }
    }
}

struct WakeRequest {
    completion_port: Weak<OwnedHandle<HANDLE>>,
}

impl Hash for WakeRequest {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(self.completion_port.as_ptr() as usize);
    }
}

impl Eq for WakeRequest {}

impl PartialEq for WakeRequest {
    fn eq(&self, other: &Self) -> bool {
        self.completion_port.as_ptr() == other.completion_port.as_ptr()
    }
}

thread_local! {
    static BATCH: RefCell<Option<HashSet<WakeRequest, BuildPointerHasher>>> =
        const { RefCell::new(None) };
}
