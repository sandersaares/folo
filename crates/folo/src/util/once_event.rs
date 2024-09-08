use super::{PinnedSlabChain, RcSlabRc, RefSlabRc, SlabRcCell, SlabRcCellStorage, UnsafeSlabRc};
use negative_impl::negative_impl;
use std::{
    cell::RefCell,
    future::Future,
    mem,
    pin::Pin,
    rc::Rc,
    task::{self, Waker},
};

/// Shorthand type for defining the backing storage for OnceEvent instances. Use
/// `OnceEvent::new_storage()` to easily create a new instance without having to remember each
/// layer of types inside this type.
pub type OnceEventStorage<T> = SlabRcCellStorage<OnceEvent<T>>;

/// An event that can be triggered at most once to deliver a value of type T to at most
/// one listener awaiting that value.
///
/// # Efficiency
///
/// The event uses pooled backing storage provided by the caller, so can typically be used in ways
/// that do not allocate memory, making it suitable for rapid creation and destruction.
///
/// Event notifications are triggered instantly via waker if a listener is already awaiting, and
/// the result is delivered instantly if the listener starts after the result is set.
///
/// # Thread safety
///
/// The event is single-threaded.
pub struct OnceEvent<T> {
    state: RefCell<EventState<T>>,
}

impl<T> OnceEvent<T> {
    fn set(&self, result: T) {
        let mut state = self.state.borrow_mut();

        match &*state {
            EventState::NotSet => {
                *state = EventState::Set(result);
            }
            EventState::Awaiting(_) => {
                let previous_state = mem::replace(&mut *state, EventState::Set(result));

                match previous_state {
                    EventState::Awaiting(waker) => waker.wake(),
                    _ => unreachable!("we are re-matching an already matched pattern"),
                }
            }
            EventState::Set(_) => {
                panic!("result already set");
            }
            EventState::Consumed => {
                panic!("result already consumed");
            }
        }
    }

    // We are intended to be polled via Future::poll, so we have an equivalent signature here.
    fn poll(&self, waker: &Waker) -> Option<T> {
        let mut state = self.state.borrow_mut();

        match &*state {
            EventState::NotSet => {
                *state = EventState::Awaiting(waker.clone());
                None
            }
            EventState::Awaiting(_) => {
                // This is permitted by the Future API contract, in which case only the waker
                // from the most recent poll should be woken up when the result is available.
                *state = EventState::Awaiting(waker.clone());
                None
            }
            EventState::Set(_) => {
                let previous_state = mem::replace(&mut *state, EventState::Consumed);

                match previous_state {
                    EventState::Set(result) => Some(result),
                    _ => unreachable!("we are re-matching an already matched pattern"),
                }
            }
            EventState::Consumed => {
                // We do not want to keep a copy of the result around, so we can only return it once.
                // The futures API contract allows us to panic in this situation.
                panic!("event polled after result was already consumed");
            }
        }
    }

    fn new() -> Self {
        Self {
            state: RefCell::new(EventState::NotSet),
        }
    }

    /// Creates a new instance of the backing storage for OnceEvent instances. You may need to
    /// further wrap this depending on which storage-referencing mode you are using.
    pub fn new_storage() -> OnceEventStorage<T> {
        SlabRcCellStorage::new(PinnedSlabChain::new())
    }

    pub fn new_in_ref<'storage>(
        storage: &'storage OnceEventStorage<T>,
    ) -> (RefSender<'storage, T>, RefReceiver<'storage, T>) {
        let event = SlabRcCell::new(Self::new()).insert_into_ref(storage);

        (
            RefSender {
                event: event.clone(),
            },
            RefReceiver { event },
        )
    }

    pub fn new_in_rc(storage: Rc<OnceEventStorage<T>>) -> (RcSender<T>, RcReceiver<T>) {
        let event = SlabRcCell::new(Self::new()).insert_into_rc(storage);

        (
            RcSender {
                event: event.clone(),
            },
            RcReceiver { event },
        )
    }

    /// # Safety
    ///
    /// The caller is responsible for ensuring that the event does not outlive the storage.
    pub unsafe fn new_in_unsafe(
        storage: Pin<&OnceEventStorage<T>>,
    ) -> (UnsafeSender<T>, UnsafeReceiver<T>) {
        let event = SlabRcCell::new(Self::new()).insert_into_unsafe(storage);

        (
            UnsafeSender {
                event: event.clone(),
            },
            UnsafeReceiver { event },
        )
    }
}

enum EventState<T> {
    /// The event has not been set and nobody is listening for a result.
    NotSet,

    /// The event has not been set and someone is listening for a result.
    Awaiting(Waker),

    /// The event has been set but nobody has yet started listening.
    Set(T),

    /// The event has been set and the result has been consumed.
    Consumed,
}

#[negative_impl]
impl<T> !Send for OnceEvent<T> {}
#[negative_impl]
impl<T> !Sync for OnceEvent<T> {}

// ############## Ref ##############

pub struct RefSender<'storage, T> {
    event: RefSlabRc<'storage, OnceEvent<T>>,
}

impl<'storage, T> RefSender<'storage, T> {
    pub fn set(self, result: T) {
        self.event.deref_pin().set(result);
    }
}

pub struct RefReceiver<'storage, T> {
    event: RefSlabRc<'storage, OnceEvent<T>>,
}

impl<T> Future for RefReceiver<'_, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let result = self.event.deref_pin().poll(&cx.waker());

        match result {
            Some(result) => task::Poll::Ready(result),
            None => task::Poll::Pending,
        }
    }
}

// ############## Rc ##############

pub struct RcSender<T> {
    event: RcSlabRc<OnceEvent<T>>,
}

impl<T> RcSender<T> {
    pub fn set(self, result: T) {
        self.event.deref_pin().set(result);
    }
}

pub struct RcReceiver<T> {
    event: RcSlabRc<OnceEvent<T>>,
}

impl<T> Future for RcReceiver<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let result = self.event.deref_pin().poll(&cx.waker());

        match result {
            Some(result) => task::Poll::Ready(result),
            None => task::Poll::Pending,
        }
    }
}

// ############## Unsafe ##############

pub struct UnsafeSender<T> {
    event: UnsafeSlabRc<OnceEvent<T>>,
}

impl<T> UnsafeSender<T> {
    pub fn set(self, result: T) {
        self.event.deref_pin().set(result);
    }
}

pub struct UnsafeReceiver<T> {
    event: UnsafeSlabRc<OnceEvent<T>>,
}

impl<T> Future for UnsafeReceiver<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let result = self.event.deref_pin().poll(&cx.waker());

        match result {
            Some(result) => task::Poll::Ready(result),
            None => task::Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{task::noop_waker_ref, FutureExt};

    #[test]
    fn get_after_set_ref() {
        let storage = OnceEvent::new_storage();
        let (sender, mut receiver) = OnceEvent::new_in_ref(&storage);

        sender.set(42);

        let cx = &mut task::Context::from_waker(noop_waker_ref());
        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Ready(42));
    }

    #[test]
    fn get_before_set_ref() {
        let storage = OnceEvent::new_storage();
        let (sender, mut receiver) = OnceEvent::new_in_ref(&storage);

        let cx = &mut task::Context::from_waker(noop_waker_ref());
        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Pending);

        sender.set(42);

        let cx = &mut task::Context::from_waker(noop_waker_ref());
        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Ready(42));
    }

    #[test]
    fn get_after_set_rc() {
        let storage = Rc::new(OnceEvent::new_storage());
        let (sender, mut receiver) = OnceEvent::new_in_rc(Rc::clone(&storage));

        sender.set(42);

        let cx = &mut task::Context::from_waker(noop_waker_ref());
        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Ready(42));
    }

    #[test]
    fn get_before_set_rc() {
        let storage = Rc::new(OnceEvent::new_storage());
        let (sender, mut receiver) = OnceEvent::new_in_rc(Rc::clone(&storage));

        let cx = &mut task::Context::from_waker(noop_waker_ref());
        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Pending);

        sender.set(42);

        let cx = &mut task::Context::from_waker(noop_waker_ref());
        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Ready(42));
    }

    #[test]
    fn get_after_set_unsafe() {
        let storage = Box::pin(OnceEvent::new_storage());
        let (sender, mut receiver) = unsafe { OnceEvent::new_in_unsafe(storage.as_ref()) };

        sender.set(42);

        let cx = &mut task::Context::from_waker(noop_waker_ref());
        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Ready(42));
    }

    #[test]
    fn get_before_set_unsafe() {
        let storage = Box::pin(OnceEvent::new_storage());
        let (sender, mut receiver) = unsafe { OnceEvent::new_in_unsafe(storage.as_ref()) };

        let cx = &mut task::Context::from_waker(noop_waker_ref());
        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Pending);

        sender.set(42);

        let cx = &mut task::Context::from_waker(noop_waker_ref());
        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Ready(42));
    }
}
