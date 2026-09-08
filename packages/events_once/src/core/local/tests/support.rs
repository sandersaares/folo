pub(in crate::core::local) use std::cell::{Cell, RefCell};
pub(in crate::core::local) use std::mem;
pub(in crate::core::local) use std::panic::{RefUnwindSafe, UnwindSafe};
pub(in crate::core::local) use std::pin::Pin;
pub(in crate::core::local) use std::rc::Rc;
pub(in crate::core::local) use std::sync::Arc;
pub(in crate::core::local) use std::task::{self, Poll, Waker};

use static_assertions::{assert_impl_all, assert_not_impl_any};
pub(in crate::core::local) use testing::{
    DropOnWakerRelease, assert_panics, assert_panics_with, clone_action_waker,
    clone_action_waker_panicking_on_clone_release, drop_waker, wake_action_waker, with_watchdog,
};

pub(in crate::core::local) use super::super::*;
pub(in crate::core::local) use crate::{
    Disconnected, EmbeddedLocalEvent, IntoValueError, RawLocalReceiver, RawLocalSender,
};

assert_not_impl_any!(LocalEvent<i32>: Send, Sync);

// The payload is one that is itself neither `UnwindSafe` nor `RefUnwindSafe`, so the
// assertion can only pass if the event supplies both regardless of what it carries.
assert_impl_all!(LocalEvent<Rc<RefCell<u32>>>: UnwindSafe, RefUnwindSafe);

/// Places an event into freshly allocated pinned storage, returning the storage together
/// with the endpoints, so that every test does not have to repeat the placement proof.
///
/// The storage comes first in the returned tuple, which makes it outlive the endpoints
/// bound alongside it.
pub(in crate::core::local) fn placed<T: 'static>() -> (
    Pin<Box<EmbeddedLocalEvent<T>>>,
    RawLocalSender<T>,
    RawLocalReceiver<T>,
) {
    let mut place = Box::pin(EmbeddedLocalEvent::<T>::new());

    // SAFETY: The container was created right here, so no other event is using it. It is
    // returned to the caller alongside the endpoints and is dropped after them, so it stays
    // alive and writable for their entire lifetime, and `Box::pin` keeps the event at a
    // stable address for that time.
    let (sender, receiver) = unsafe { LocalEvent::placed(place.as_mut()) };

    (place, sender, receiver)
}

/// Reads the event out of the storage it was placed into, so a test can inspect state that
/// the endpoints do not expose.
///
/// The only such state is the diagnostic backtrace, which exists in debug builds only.
#[cfg(debug_assertions)]
pub(in crate::core::local) fn placed_event<T: 'static>(
    place: &EmbeddedLocalEvent<T>,
) -> &LocalEvent<T> {
    // SAFETY: The container is only ever accessed through shared references, matching the
    // access that the endpoints make, and the pointer of an `UnsafeCell` is never null.
    let event = unsafe { place.inner.get().as_ref_unchecked() };

    // SAFETY: The caller obtained this container from `placed()`, which initialized an event
    // into it. Releasing an event does not deinitialize the storage.
    unsafe { event.assume_init_ref() }
}
