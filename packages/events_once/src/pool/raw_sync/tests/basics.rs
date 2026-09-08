#[cfg(debug_assertions)]
pub(in crate::pool::raw_sync) use std::cell::RefCell;
pub(in crate::pool::raw_sync) use std::panic::{RefUnwindSafe, UnwindSafe};
pub(in crate::pool::raw_sync) use std::sync::{Arc, Barrier};
pub(in crate::pool::raw_sync) use std::task::{self, Poll, Waker};
pub(in crate::pool::raw_sync) use std::{iter, thread};

pub(in crate::pool::raw_sync) use futures::executor::block_on;
use static_assertions::assert_impl_all;
#[cfg(debug_assertions)]
pub(in crate::pool::raw_sync) use testing::assert_panics_with;
pub(in crate::pool::raw_sync) use testing::with_watchdog;

pub(in crate::pool::raw_sync) use super::super::*;
#[cfg(debug_assertions)]
pub(in crate::pool::raw_sync) use crate::assert_inspect_awaiters_is_reentrant;
pub(in crate::pool::raw_sync) use crate::{
    Disconnected,
    PanickingPayload,
    RawPooledSender,
    // Shared helpers keep callback-safety regression coverage consistent across containers.
    assert_disconnected_send_payload_panic_releases_event,
    assert_receiver_waker_panic_handoff_releases_event,
    assert_unread_payload_panic_releases_event,
};

// The payload satisfies only the bound that the pool's API requires (`Send`) and lacks every
// trait asserted here, so each of them is supplied by the pool's own synchronization and
// storage rather than inherited from the payload. A trait object payload also has to preserve
// the thread-safety traits (regression test for #142).
assert_impl_all!(RawEventPool<Box<dyn Send>>: Send, Sync, UnwindSafe, RefUnwindSafe);

#[test]
fn disconnected_send_payload_panic_releases_event() {
    let pool = Box::pin(RawEventPool::<PanickingPayload>::new());

    assert_disconnected_send_payload_panic_releases_event(
        // SAFETY: The pool remains pinned and alive until the helper consumes both endpoints.
        || unsafe { pool.as_ref().rent() },
        RawPooledSender::send,
        || pool.is_empty(),
    );
}

#[test]
fn receiver_waker_panic_handoff_releases_event() {
    let pool = Box::pin(RawEventPool::<i32>::new());

    assert_receiver_waker_panic_handoff_releases_event(
        // SAFETY: The pool remains pinned and alive until the helper consumes both endpoints.
        || unsafe { pool.as_ref().rent() },
        RawPooledSender::send,
        || pool.is_empty(),
    );
}

#[test]
fn unread_payload_panic_releases_event() {
    let pool = Box::pin(RawEventPool::<PanickingPayload>::new());

    assert_unread_payload_panic_releases_event(
        // SAFETY: The pool remains pinned and alive until the helper consumes both endpoints.
        || unsafe { pool.as_ref().rent() },
        RawPooledSender::send,
        || pool.is_empty(),
    );
}
