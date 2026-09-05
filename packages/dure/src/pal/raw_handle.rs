//! Windows handle wrappers shared by the PAL implementations.

use std::sync::{Arc, Mutex};

use windows::Win32::Foundation::{CloseHandle, HANDLE};
use windows::Win32::System::IO::CancelIoEx;

/// A Windows handle value this type does not own.
///
/// Copying a handle around is all this offers: closing it stays with whoever
/// owns the object, and dropping this changes nothing.
// Stored as an integer because `HANDLE` is a raw pointer and therefore `!Send`,
// which would keep a handle table from being shared across the supervisor's
// relay threads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RawHandle(isize);

impl RawHandle {
    pub(crate) fn from_handle(handle: HANDLE) -> Self {
        Self(handle.0 as isize)
    }

    pub(crate) fn as_handle(self) -> HANDLE {
        HANDLE(self.0 as *mut core::ffi::c_void)
    }
}

/// A pipe handle that stays valid for as long as anyone is using it.
///
/// Every holder keeps the handle alive, so an operation in flight can never be
/// left addressing a closed handle, nor one Windows has reused for an unrelated
/// object. Teardown cancels whatever is outstanding — which is what unblocks a
/// waiting reader — and refuses any operation started after that, since a later
/// operation would have nothing left to release it. The handle is closed once
/// the last holder lets go.
/// Ref: docs/transport.md and "Pseudoconsole".
pub(crate) struct PipeHandle {
    handle: RawHandle,
    /// Whether teardown has cancelled this handle.
    ///
    /// Also serialises starting an operation against cancelling one, which is
    /// what makes `cancel` reliable; see `issue`.
    cancelled: Mutex<bool>,
}

impl PipeHandle {
    pub(crate) fn new(handle: HANDLE) -> Arc<Self> {
        Arc::new(Self {
            handle: RawHandle::from_handle(handle),
            cancelled: Mutex::new(false),
        })
    }

    pub(crate) fn as_handle(&self) -> HANDLE {
        self.handle.as_handle()
    }

    /// Starts an overlapped operation, unless this handle is already cancelled.
    ///
    /// `CancelIoEx` only cancels operations that are already pending, so a
    /// caller that started one after teardown had cancelled would wait with
    /// nothing left to release it. Starting and cancelling therefore exclude
    /// each other: an operation either becomes pending before `cancel` runs, and
    /// is cancelled by it, or is refused outright, reported here as `None`.
    ///
    /// `start` must only issue the operation. Waiting for it belongs outside,
    /// once the caller has the result.
    pub(crate) fn issue<T>(&self, start: impl FnOnce(HANDLE) -> T) -> Option<T> {
        let cancelled = self.cancelled.lock().expect("the cancellation flag is only read and set, never held across a panic");
        if *cancelled {
            return None;
        }
        Some(start(self.as_handle()))
    }

    /// Abort the I/O outstanding on this handle so blocked operations return.
    ///
    /// Operations started later are refused rather than cancelled, because this
    /// is the last cancellation the handle receives: teardown has already
    /// dropped the table's reference, so nothing can reach the handle to cancel
    /// it again.
    pub(crate) fn cancel(&self) {
        let mut cancelled = self.cancelled.lock().expect("the cancellation flag is only read and set, never held across a panic");
        *cancelled = true;
        // SAFETY: `self` owns the handle and keeps it alive across this call. A
        // null OVERLAPPED cancels every operation this process has pending on
        // the handle, so a blocked read or write completes with an aborted
        // status instead of waiting forever.
        _ = unsafe { CancelIoEx(self.as_handle(), None) };
    }
}

impl Drop for PipeHandle {
    fn drop(&mut self) {
        let handle = self.as_handle();
        if handle.is_invalid() {
            return;
        }
        // SAFETY: this is the last reference to a handle we own, so nothing
        // uses it again.
        _ = unsafe { CloseHandle(handle) };
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::sync::Barrier;
    use std::thread;

    use super::*;

    #[test]
    fn round_trips_a_handle_value() {
        // Arbitrary non-null value. The type only carries the integer around and
        // never dereferences it, so no real kernel object is needed here.
        let handle = HANDLE(0x1234 as *mut core::ffi::c_void);
        assert_eq!(RawHandle::from_handle(handle).as_handle(), handle);
    }

    /// A null handle names no kernel object, so the cancel and close this test
    /// provokes both fail harmlessly instead of acting on something real.
    fn detached_pipe() -> Arc<PipeHandle> {
        PipeHandle::new(HANDLE::default())
    }

    #[test]
    fn issues_an_operation_while_live() {
        assert_eq!(detached_pipe().issue(|_handle| 7), Some(7));
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Cancellation calls a real Windows API, which Miri cannot execute.
    fn refuses_an_operation_started_after_cancellation() {
        let pipe = detached_pipe();
        pipe.cancel();
        // Refusing is the point: `CancelIoEx` cannot reach an operation that
        // does not exist yet, so one started now would never be released.
        assert_eq!(pipe.issue(|_handle| 7), None);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Cancellation calls a real Windows API, which Miri cannot execute.
    fn every_racing_operation_is_either_started_or_refused() {
        // The contract this checks is an exclusion, not a count: an operation
        // that runs concurrently with a cancellation must land on one side of
        // it, never half-way. A thread that observed a started operation after
        // cancellation had completed would be the failure.
        const WORKERS: usize = 8;
        const ATTEMPTS: usize = 200;

        let pipe = detached_pipe();
        let start = Arc::new(Barrier::new(WORKERS.saturating_add(1)));
        let mut workers = Vec::with_capacity(WORKERS);
        for _ in 0..WORKERS {
            let pipe = Arc::clone(&pipe);
            let start = Arc::clone(&start);
            workers.push(thread::spawn(move || {
                start.wait();
                let mut refused_then_started = false;
                let mut refused = false;
                for _ in 0..ATTEMPTS {
                    match pipe.issue(|_handle| ()) {
                        Some(()) => refused_then_started |= refused,
                        None => refused = true,
                    }
                }
                refused_then_started
            }));
        }

        start.wait();
        pipe.cancel();

        for worker in workers {
            assert!(
                !worker.join().unwrap(),
                "an operation started after this thread had seen the handle cancelled"
            );
        }
        assert_eq!(pipe.issue(|_handle| 7), None);
    }
}
