use crate::mem::{RcSlabRc, RefSlabRc, SlabRcBox, SlabRcStorage, UnsafeSlabRc};
use crate::util::WithRefCount;
use std::marker::PhantomPinned;
use std::{
    cell::UnsafeCell,
    future::Future,
    mem,
    pin::Pin,
    rc::Rc,
    task::{self, Waker},
};

/// Shorthand type for defining the slab-based backing storage for OnceEvent instances. Use
/// `OnceEvent::new_storage()` to easily create a new instance without having to remember each
/// layer of types inside this type.
pub type OnceEventSlabStorage<T> = SlabRcStorage<OnceEvent<T>>;

/// An asynchronous event that can be triggered at most once to deliver a value of type T to at most
/// one listener awaiting that value.
///
/// Usage:
///
/// 1. Allocate the storage for the event using one of the `new*_storage*()` methods.
/// 2. Create the event in the allocated storage using one of the `new_in_*()` methods. This will
///    return a sender and receiver pair.
/// 3. Call `set()` or `poll()` at most once to read or write the value through the sender/receiver.
///
/// # Efficiency
///
/// The event uses either pooled backing storage provided by the caller or is embedded inline into
/// another data structure owned by the caller, so it can typically be used in ways that do not
/// allocate memory, making it suitable for rapid creation and destruction.
///
/// Event notifications are triggered instantly via waker if a listener is already awaiting, and
/// the result is delivered instantly if the listener starts after the result is set.
///
/// # Thread safety
///
/// This type is `?Send + !Sync`. It is safe to send across threads as long as `T` supports that.
/// Methods on this type may not be called across threads.
///
/// See `OnceEventShared` for a thread-safe version.
#[derive(Debug)]
pub struct OnceEvent<T> {
    // We only have a get() and a set() that access the state and we guarantee this happens on the
    // same thread (because UnsafeCell is !Sync), so there is no point in wasting cycles on borrow
    // counting at runtime with RefCell.
    state: UnsafeCell<EventState<T>>,
}

impl<T> OnceEvent<T> {
    fn set(&self, result: T) {
        // SAFETY: See comments on field.
        let state = unsafe { &mut *self.state.get() };

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
        // SAFETY: See comments on field.
        let state = unsafe { &mut *self.state.get() };

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
            state: UnsafeCell::new(EventState::NotSet),
        }
    }

    /// Creates a new instance of the backing storage for OnceEvent instances. The storage will be
    /// borrow checked at runtime using `RefCell` for safety.
    ///
    /// You may need to further wrap this depending on which storage-referencing mode you are using.
    /// For example:
    ///
    /// * If referencing the storage by reference (`new_in_ref()`), nothing more is needed.
    /// * If referencing the storage via `Rc` (`new_in_rc()`), wrap this in an `Rc`.
    /// * If referencing the storage via unsafe memory access (`new_in_unsafe()`), the slab chain
    ///   must be pinned.
    pub fn new_slab_storage() -> OnceEventSlabStorage<T> {
        SlabRcBox::new_storage_ref()
    }

    /// This embeds a single event directly into a data structure owned by the caller. It is the
    /// caller's responsibility to ensure that the storage outlives the event inserted into it.
    /// The storage must be pinned at all times once an event is created in it.
    pub fn new_embedded_storage_single() -> OnceEventEmbeddedStorage<T> {
        OnceEventEmbeddedStorage::default()
    }

    /// Creates an event in storage that is referenced by a direct shared reference. This is a cheap
    /// and efficient mechanism but requires the caller to track lifetimes across the type graph.
    pub fn new_in_ref(storage: &OnceEventSlabStorage<T>) -> (RefSender<'_, T>, RefReceiver<'_, T>) {
        let event = SlabRcBox::new(Self::new()).insert_into_ref(storage);

        (
            RefSender {
                event: event.clone(),
            },
            RefReceiver { event },
        )
    }

    /// Creates an event in storage that is referenced by `Rc`. This is relatively cheap, though
    /// incurs minor reference counting overhead. At the same time, it saves you from lifetimes
    /// while ensuring safety.
    pub fn new_in_rc(storage: Rc<OnceEventSlabStorage<T>>) -> (RcSender<T>, RcReceiver<T>) {
        let event = SlabRcBox::new(Self::new()).insert_into_rc(storage);

        (
            RcSender {
                event: event.clone(),
            },
            RcReceiver { event },
        )
    }

    /// Creates an event in storage that is referenced via unsafe memory access. This avoids any
    /// reference counting overhead and the need to track lifetimes but requires the caller to
    /// guarantee safety.
    ///
    /// # Safety
    ///
    /// The caller is responsible for ensuring that the storage is not dropped until both the
    /// sender and receiver have been dropped (once an event has been created in the storage).
    ///
    /// The storage must be pinned at all times during the lifetime of the event.
    pub unsafe fn new_in_unsafe(
        storage: Pin<&OnceEventSlabStorage<T>>,
    ) -> (UnsafeSender<T>, UnsafeReceiver<T>) {
        let event = SlabRcBox::new(Self::new()).insert_into_unsafe(storage);

        (
            UnsafeSender {
                event: event.clone(),
            },
            UnsafeReceiver { event },
        )
    }

    /// Creates an event in embedded storage that is referenced via unsafe memory access. This
    /// avoids any reference counting overhead and the need to track lifetimes but requires the
    /// caller to guarantee safety.
    ///
    /// Embedding the storage directly into a data structure owned by the caller can be more
    /// efficient than managing a pool of events, if a suitable data structure is already available.
    ///
    /// # Safety
    ///
    /// The caller is responsible for ensuring that the event does not outlive the storage.
    ///
    /// The storage must be pinned at all times during the lifetime of the event.
    pub unsafe fn new_embedded(
        storage: Pin<&OnceEventEmbeddedStorage<T>>,
    ) -> (EmbeddedSender<T>, EmbeddedReceiver<T>) {
        let mut with_ref_count = WithRefCount::new(Some(Self::new()));

        // Sender + Receiver
        with_ref_count.inc_ref();
        with_ref_count.inc_ref();

        let storage_ref = &mut *storage.inner.get();
        *storage_ref = with_ref_count;

        let event = Pin::into_inner_unchecked(storage);
        (EmbeddedSender { event }, EmbeddedReceiver { event })
    }
}

#[derive(Debug)]
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

// ############## Ref ##############

#[derive(Debug)]
pub struct RefSender<'storage, T> {
    event: RefSlabRc<'storage, OnceEvent<T>>,
}

impl<'storage, T> RefSender<'storage, T> {
    pub fn set(self, result: T) {
        self.event.deref_pin().set(result);
    }
}

#[derive(Debug)]
pub struct RefReceiver<'storage, T> {
    event: RefSlabRc<'storage, OnceEvent<T>>,
}

impl<T> Future for RefReceiver<'_, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let result = self.event.deref_pin().poll(cx.waker());

        match result {
            Some(result) => task::Poll::Ready(result),
            None => task::Poll::Pending,
        }
    }
}

// ############## Rc ##############

#[derive(Debug)]
pub struct RcSender<T> {
    event: RcSlabRc<OnceEvent<T>>,
}

impl<T> RcSender<T> {
    pub fn set(self, result: T) {
        self.event.deref_pin().set(result);
    }
}

#[derive(Debug)]
pub struct RcReceiver<T> {
    event: RcSlabRc<OnceEvent<T>>,
}

impl<T> Future for RcReceiver<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let result = self.event.deref_pin().poll(cx.waker());

        match result {
            Some(result) => task::Poll::Ready(result),
            None => task::Poll::Pending,
        }
    }
}

// ############## Unsafe ##############

#[derive(Debug)]
pub struct UnsafeSender<T> {
    event: UnsafeSlabRc<OnceEvent<T>>,
}

impl<T> UnsafeSender<T> {
    pub fn set(self, result: T) {
        self.event.deref_pin().set(result);
    }
}

#[derive(Debug)]
pub struct UnsafeReceiver<T> {
    event: UnsafeSlabRc<OnceEvent<T>>,
}

impl<T> Future for UnsafeReceiver<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let result = self.event.deref_pin().poll(cx.waker());

        match result {
            Some(result) => task::Poll::Ready(result),
            None => task::Poll::Pending,
        }
    }
}

// ############## Embedded ##############

/// Shorthand type for defining inline backing storage for OnceEvent instances embedded into custom
/// data structures owned by the caller. The caller must guarantee that this storage is pinned.
///
/// # Safety
///
/// This uses UnsafeCell because we only ever have one sender and one receiver and they both execute
/// on the same thread, and each only performs a logically atomic operation on the event. There is
/// no need to pay for the runtime borrow counting of RefCell, so UnsafeCell gives us some extra
/// performance for this commonly used primitive.
#[derive(Debug)]
pub struct OnceEventEmbeddedStorage<T> {
    inner: UnsafeCell<WithRefCount<Option<OnceEvent<T>>>>,

    _must_pin: PhantomPinned,
}

impl<T> OnceEventEmbeddedStorage<T> {
    pub fn is_inert(&self) -> bool {
        // SAFETY: See comments on type.
        let storage = unsafe { &*self.inner.get() };

        !storage.is_referenced()
    }

    pub fn ref_count(&self) -> usize {
        // SAFETY: See comments on type.
        let storage = unsafe { &*self.inner.get() };

        storage.ref_count()
    }
}

impl<T> Default for OnceEventEmbeddedStorage<T> {
    fn default() -> Self {
        Self {
            inner: UnsafeCell::new(WithRefCount::new(None)),
            _must_pin: PhantomPinned,
        }
    }
}

#[derive(Debug)]
pub struct EmbeddedSender<T> {
    // The owner of the event is responsible for ensuring that we reference pinned memory that
    // outlives the event.
    event: *const OnceEventEmbeddedStorage<T>,
}

impl<T> EmbeddedSender<T> {
    pub fn set(self, result: T) {
        // SAFETY: We rely on the owner of the event to guarantee that the backing storage remains
        // alive for at least as long as the event itself.
        let storage = unsafe { &*self.event };

        // SAFETY: See comments on storage type alias.
        let storage = unsafe { &mut *storage.inner.get() };

        storage
            .get()
            .as_ref()
            .expect("OnceEvent must still exist because sender exists")
            .set(result);

        // There is no sender anymore, so we can drop a reference.
        storage.dec_ref();
    }
}

#[derive(Debug)]
pub struct EmbeddedReceiver<T> {
    // The owner of the event is responsible for ensuring that we reference pinned memory that
    // outlives the event.
    event: *const OnceEventEmbeddedStorage<T>,
}

impl<T> Future for EmbeddedReceiver<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        // SAFETY: We rely on the owner of the event to guarantee that the backing storage remains
        // alive for at least as long as the event itself.
        let storage = unsafe { &*self.event };

        // SAFETY: See comments on storage type alias.
        let storage = unsafe { &*storage.inner.get() };

        let result = storage
            .get()
            .as_ref()
            .expect("OnceEvent must still exist because receiver exists")
            .poll(cx.waker());

        match result {
            Some(result) => task::Poll::Ready(result),
            None => task::Poll::Pending,
        }
    }
}

impl<T> Drop for EmbeddedReceiver<T> {
    fn drop(&mut self) {
        // SAFETY: We rely on the owner of the event to guarantee that the backing storage remains
        // alive for at least as long as the event itself.
        let storage = unsafe { &*self.event };

        // SAFETY: See comments on storage type alias.
        let storage = unsafe { &mut *storage.inner.get() };

        // There is no receiver anymore, so we can drop a reference.
        storage.dec_ref();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{task::noop_waker_ref, FutureExt};

    #[test]
    fn get_after_set_ref() {
        let storage = OnceEvent::new_slab_storage();
        let (sender, mut receiver) = OnceEvent::new_in_ref(&storage);

        sender.set(42);

        let cx = &mut task::Context::from_waker(noop_waker_ref());
        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Ready(42));
    }

    #[test]
    fn get_before_set_ref() {
        let storage = OnceEvent::new_slab_storage();
        let (sender, mut receiver) = OnceEvent::new_in_ref(&storage);

        let cx = &mut task::Context::from_waker(noop_waker_ref());

        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Pending);

        sender.set(42);

        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Ready(42));
    }

    #[test]
    fn get_after_set_rc() {
        let storage = Rc::new(OnceEvent::new_slab_storage());
        let (sender, mut receiver) = OnceEvent::new_in_rc(Rc::clone(&storage));

        sender.set(42);

        let cx = &mut task::Context::from_waker(noop_waker_ref());

        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Ready(42));
    }

    #[test]
    fn get_before_set_rc() {
        let storage = Rc::new(OnceEvent::new_slab_storage());
        let (sender, mut receiver) = OnceEvent::new_in_rc(Rc::clone(&storage));

        let cx = &mut task::Context::from_waker(noop_waker_ref());

        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Pending);

        sender.set(42);

        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Ready(42));
    }

    #[test]
    fn get_after_set_unsafe() {
        let storage = Box::pin(OnceEvent::new_slab_storage());
        let (sender, mut receiver) = unsafe { OnceEvent::new_in_unsafe(storage.as_ref()) };

        sender.set(42);

        let cx = &mut task::Context::from_waker(noop_waker_ref());

        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Ready(42));
    }

    #[test]
    fn get_before_set_unsafe() {
        let storage = Box::pin(OnceEvent::new_slab_storage());
        let (sender, mut receiver) = unsafe { OnceEvent::new_in_unsafe(storage.as_ref()) };

        let cx = &mut task::Context::from_waker(noop_waker_ref());

        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Pending);

        sender.set(42);

        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Ready(42));
    }

    #[test]
    fn get_after_set_embedded() {
        let storage = Box::pin(OnceEvent::new_embedded_storage_single());
        let (sender, mut receiver) = unsafe { OnceEvent::new_embedded(storage.as_ref()) };

        sender.set(42);

        let cx = &mut task::Context::from_waker(noop_waker_ref());

        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Ready(42));
    }

    #[test]
    fn get_before_set_embedded() {
        let storage = Box::pin(OnceEvent::new_embedded_storage_single());
        let (sender, mut receiver) = unsafe { OnceEvent::new_embedded(storage.as_ref()) };

        let cx = &mut task::Context::from_waker(noop_waker_ref());

        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Pending);

        sender.set(42);

        let result = receiver.poll_unpin(cx);
        assert_eq!(result, task::Poll::Ready(42));
    }
}
