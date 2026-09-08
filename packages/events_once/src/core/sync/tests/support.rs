pub(in crate::core::sync) use std::cell::RefCell;
pub(in crate::core::sync) use std::panic::{
    AssertUnwindSafe, RefUnwindSafe, UnwindSafe, catch_unwind, resume_unwind,
};
pub(in crate::core::sync) use std::pin::Pin;
pub(in crate::core::sync) use std::rc::Rc;
pub(in crate::core::sync) use std::sync::{Arc, Barrier, Mutex, atomic};
pub(in crate::core::sync) use std::task::{Poll, Waker};
pub(in crate::core::sync) use std::{mem, task, thread};

pub(in crate::core::sync) use futures::executor::block_on;
use static_assertions::assert_impl_all;
pub(in crate::core::sync) use testing::{
    DropOnWakerRelease, assert_panics, assert_panics_with, clone_action_waker,
    clone_action_waker_panicking_on_clone_release, drop_waker, wake_action_waker, with_watchdog,
};

pub(in crate::core::sync) use super::super::event::test_hooks::{
    HOOK_PARTICIPANT, HOOK_POLL_AWAITING_PRE_CAS, HOOK_POLL_BOUND_PRE_CAS,
    HOOK_SERIALIZATION_MUTEX, HOOK_SET_IN_SIGNALING, HookFn,
};
pub(in crate::core::sync) use super::super::*;
pub(in crate::core::sync) use crate::{BoxedReceiver, Disconnected, EmbeddedEvent, IntoValueError};

assert_impl_all!(Event<u32>: Send, Sync, UnwindSafe, RefUnwindSafe);

// `Box<dyn Send>` and `&'static mut u32` satisfy the payload bounds of the public API
// (`Send + 'static`) while lacking auto traits of their own: the trait object is neither
// `Sync` nor unwind-safe in either form, and the exclusive reference is not `UnwindSafe`.
// These assertions therefore only pass if the event supplies thread safety and unwind safety
// through its own state machine instead of inheriting them structurally from the payload,
// which is the contract the endpoints and containers depend on. Preserving `Send` for a trait
// object payload is also a regression test for #142.
assert_impl_all!(Event<Box<dyn Send>>: Send, Sync, UnwindSafe, RefUnwindSafe);
assert_impl_all!(Event<&'static mut u32>: Send, Sync, UnwindSafe, RefUnwindSafe);
