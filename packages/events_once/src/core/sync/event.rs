use std::any::type_name;
#[cfg(all(test, debug_assertions))]
use std::backtrace::Backtrace;
use std::cell::UnsafeCell;
use std::fmt;
use std::hint::spin_loop;
use std::marker::PhantomPinned;
use std::mem::{MaybeUninit, offset_of};
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::pin::Pin;
use std::ptr::NonNull;
#[cfg(all(test, debug_assertions))]
use std::sync::Arc;
#[cfg(debug_assertions)]
use std::sync::Mutex;
use std::sync::atomic::{self, AtomicU8};
use std::task::Waker;

#[cfg(debug_assertions)]
use crate::NEVER_POISONED;
#[cfg(debug_assertions)]
use crate::{BacktraceType, capture_backtrace};
use crate::{
    BoxedReceiver, BoxedRef, BoxedSender, Cancellation, Disconnected, EVENT_AWAITING, EVENT_BOUND,
    EVENT_DISCONNECTED, EVENT_SET, EVENT_SIGNALING, EmbeddedEvent, PtrRef, RawReceiver, RawSender,
    ReceiverCore, SenderCore,
};

/// Coordinates delivery of a `T` at most once from a sender to a receiver on any thread.
///
/// # Reentrancy
///
/// Completing or cancelling an event runs the awaiting task's waker, either waking it or dropping
/// it. Such a callback may operate on the endpoints of the event it belongs to: it may poll the
/// receiver to completion, and it may drop an endpoint - including the last one, which releases
/// the event storage.
///
/// A poll that registers the task for later notification runs the waker's clone operation, which
/// is likewise user-supplied code. Such a callback may operate on the sender endpoint of the event
/// being polled: it may send a value and it may drop the sender. The poll observes the resulting
/// state.
///
/// Destruction of a registered waker or discarded payload may also run arbitrary user code. The
/// event completes any endpoint and storage cleanup that must survive unwinding before invoking
/// such a destructor.
pub struct Event<T> {
    /// The logical state of the event; see constants in `state.rs`.
    pub(crate) state: AtomicU8,

    /// If `state` is [`EVENT_AWAITING`] or [`EVENT_SIGNALING`], this field is initialized with the
    /// waker of whoever most recently awaited the receiver. In other states, this field is not
    /// initialized.
    ///
    /// We use `MaybeUninit` to minimize the storage and avoid an `Option` or enum overhead,
    /// as we already track the presence via `state`.
    ///
    /// We use `UnsafeCell` because we are a synchronization primitive and
    /// do our own synchronization of reads/writes.
    awaiter: UnsafeCell<MaybeUninit<Waker>>,

    /// If `state` is `EVENT_SET`, this field is initialized with the value that was sent by
    /// the sender. In other states, this field is not initialized.
    ///
    /// We use `MaybeUninit` to minimize the storage and avoid an `Option` or enum overhead,
    /// as we already track the presence via `state`.
    ///
    /// We use `UnsafeCell` because we are a synchronization primitive and
    /// do our own synchronization of reads/writes.
    value: UnsafeCell<MaybeUninit<T>>,

    // In debug builds, we save the backtrace of the most recent awaiter here. The value will be
    // retained for the entire lifetime of the event, even after the awaiter has been woken up,
    // to allow inspection after the fact.
    #[cfg(debug_assertions)]
    backtrace: Mutex<Option<BacktraceType>>,

    // It is invalid to move this type once it has been pinned because
    // the sender and receiver connect via raw pointers to the event.
    _requires_pinning: PhantomPinned,
}

// The `UnsafeCell` fields (`awaiter`, `value`) cause auto-trait inference to mark `Event` as
// `!RefUnwindSafe`, and a payload that is not itself `UnwindSafe` would additionally make it
// `!UnwindSafe`. The event supplies both regardless of the payload because every mutation it
// performs is panic-atomic:
//
// * The payload and the waker are moved into and out of their cells as whole values. The event
//   never hands out a reference through which user code could leave either of them partially
//   modified, so the only user code that can unwind while a cell is live is a destructor, which
//   runs when the state machine has already given that cell up.
// * The `state` that records which cell is initialized is published by a single atomic operation
//   ordered after the corresponding move, so a panic lands either before that publication (state
//   and storage agree that the cell is uninitialized) or after it (they agree that it is
//   initialized).
// * The transient `EVENT_SIGNALING` window, in which the sender owns the awaiter, contains no
//   user-controlled code: the sender takes the waker and publishes the terminal state before it
//   invokes any callback. An unwind can therefore never strand the event in `EVENT_SIGNALING`
//   and leave another endpoint waiting for a transition that will not come.
// * User-controlled code that can unwind - waker clones, wakes, waker drops and payload drops -
//   therefore always runs where state and storage agree, so a caught panic leaves an event that
//   the remaining endpoint can still complete or clean up. The unwinding regression tests exercise
//   this.
impl<T: Send + 'static> UnwindSafe for Event<T> {}
impl<T: Send + 'static> RefUnwindSafe for Event<T> {}

impl<T> Event<T>
where
    T: Send + 'static,
{
    /// In-place initializes a new instance in the `BOUND` state.
    ///
    /// This is for internal use only and is wrapped by public methods that also
    /// wire up the sender and receiver after doing the initialization. An event
    /// without a sender and receiver is invalid.
    pub(crate) fn new_in_inner(place: &mut UnsafeCell<MaybeUninit<Self>>) {
        // The key here is that we can skip initializing the MaybeUninit fields because
        // they start uninitialized by design and the UnsafeCell wrapper is transparent,
        // only affecting accesses and not the contents.
        let base_ptr = place.get_mut().as_mut_ptr();

        // SAFETY: We are making a pointer to a known field at a compiler-guaranteed offset.
        let state_ptr = unsafe { base_ptr.byte_add(offset_of!(Self, state)) }.cast::<AtomicU8>();

        // SAFETY: `place` is an exclusive reference to storage laid out and aligned as
        // `MaybeUninit<Self>`, `get_mut()` preserves that exclusive access and `as_mut_ptr()`
        // yields its base address, so `offset_of!` selects the in-bounds `state` field with the
        // alignment of that field. The event lifecycle guarantees that no endpoint of an earlier
        // event in this storage is still active, so nothing else accesses these bytes. `write`
        // initializes the field without reading or dropping the previous bytes, which is what
        // lets us leave `awaiter` and `value` uninitialized.
        unsafe {
            state_ptr.write(AtomicU8::new(EVENT_BOUND));
        }

        #[cfg(debug_assertions)]
        {
            // SAFETY: We are making a pointer to a known field at a compiler-guaranteed offset.
            let backtrace_ptr = unsafe { base_ptr.byte_add(offset_of!(Self, backtrace)) }
                .cast::<Mutex<Option<BacktraceType>>>();

            // SAFETY: Same as the `state` write above - the exclusive `MaybeUninit<Self>` storage
            // is aligned writable storage for `Self` that no active event aliases, `offset_of!`
            // selects the in-bounds `backtrace` field with that field's alignment, and `write`
            // initializes it without reading or dropping the previous bytes.
            unsafe {
                backtrace_ptr.write(Mutex::new(None));
            }
        }
    }

    #[must_use]
    pub(crate) fn boxed_core() -> (SenderCore<BoxedRef<T>, T>, ReceiverCore<BoxedRef<T>, T>) {
        let (sender_event_ref, receiver_event_ref) = BoxedRef::new_pair();

        (
            SenderCore::new(sender_event_ref),
            ReceiverCore::new(receiver_event_ref),
        )
    }

    /// Heap-allocates a new instance and returns the endpoints.
    ///
    /// The memory used is released when both endpoints are dropped.
    ///
    /// For more efficiency, consider using [`placed`][Self::placed], which allows you to
    /// initialize the event in preallocated storage as part of a larger structure.
    ///
    /// # Examples
    ///
    /// ```
    /// use events_once::Event;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let (sender, receiver) = Event::<String>::boxed();
    ///
    /// sender.send("Hello, world!".to_string());
    ///
    /// let message = receiver.await.unwrap();
    /// assert_eq!(message, "Hello, world!");
    /// # }
    /// ```
    #[must_use]
    pub fn boxed() -> (BoxedSender<T>, BoxedReceiver<T>) {
        let (sender_core, receiver_core) = Self::boxed_core();

        (
            BoxedSender::new(sender_core),
            BoxedReceiver::new(receiver_core),
        )
    }

    /// # Safety
    ///
    /// The caller must guarantee that:
    ///
    /// * The referenced place remains valid for writes for the entire lifetime of the returned
    ///   sender and receiver cores.
    /// * The referenced place remains pinned for the entire lifetime of the returned sender and
    ///   receiver cores.
    /// * The referenced place is not already in use by another instance of the event.
    #[must_use]
    pub(crate) unsafe fn placed_core(
        place: Pin<&mut UnsafeCell<MaybeUninit<Self>>>,
    ) -> (SenderCore<PtrRef<T>, T>, ReceiverCore<PtrRef<T>, T>) {
        // SAFETY: Nothing is getting moved, we just temporarily unwrap the Pin wrapper.
        let place_mut = unsafe { place.get_unchecked_mut() };

        Self::new_in_inner(place_mut);

        // We cast away the MaybeUninit wrapper because it is now initialized.
        let event = NonNull::from_mut(place_mut).cast::<UnsafeCell<Self>>();

        // SAFETY: `new_in_inner` just initialized the event in this place and the exclusive
        // borrow of the place ends here, so nothing accesses the storage through an exclusive
        // reference while the endpoints exist. The caller guarantees that the place stays valid
        // and pinned until both endpoints have released it and that no other event uses it.
        let sender_event_ref = unsafe { PtrRef::new(event) };

        // SAFETY: Same as for the sender's reference above - the contract is discharged
        // independently for each endpoint from the same caller guarantees.
        let receiver_event_ref = unsafe { PtrRef::new(event) };

        (
            SenderCore::new(sender_event_ref),
            ReceiverCore::new(receiver_event_ref),
        )
    }

    /// Initializes the event in-place, returning the endpoints.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that:
    ///
    /// * The referenced place remains valid for writes for the entire lifetime of
    ///   the sender and receiver returned by this function.
    /// * The referenced place remains pinned for the entire lifetime of
    ///   the sender and receiver returned by this function.
    /// * The referenced place is not already in use by another instance of the event.
    ///
    /// # Examples
    ///
    /// ```
    /// use events_once::{EmbeddedEvent, Event};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mut place = Box::pin(EmbeddedEvent::<String>::new());
    ///
    /// // SAFETY: `place` is a freshly created container, so it holds no other event, and the
    /// // box keeps its storage allocated and writable at a stable address. It is box-pinned, so
    /// // it cannot move while the endpoints below hold pointers to it, and it stays in scope
    /// // until after both endpoints have been consumed. We do not touch the container itself
    /// // while the endpoints are alive.
    /// let (sender, receiver) = unsafe { Event::placed(place.as_mut()) };
    ///
    /// sender.send("Hello from embedded event!".to_string());
    ///
    /// let message = receiver.await.unwrap();
    /// assert_eq!(message, "Hello from embedded event!");
    /// # }
    /// ```
    #[must_use]
    pub unsafe fn placed(place: Pin<&mut EmbeddedEvent<T>>) -> (RawSender<T>, RawReceiver<T>) {
        // SAFETY: `inner` is an inline field of `EmbeddedEvent`, so its address is fixed relative
        // to the container and pinning the container pins the field - this projection is
        // structural. The closure returns that field directly, without moving or replacing it,
        // and no operation on the container replaces `inner` while an event lives there. The raw
        // endpoint pointers derived from the projection outlive this temporary `Pin<&mut _>`;
        // their validity comes from the caller's obligation to keep the container pinned and
        // valid for as long as the endpoints exist.
        let place = unsafe { place.map_unchecked_mut(|container| &mut container.inner) };

        // SAFETY: Forwarding safety guarantees from the caller.
        let (sender_core, receiver_core) = unsafe { Self::placed_core(place) };

        (RawSender::new(sender_core), RawReceiver::new(receiver_core))
    }

    /// Returns the address of this event's type-independent diagnostic cell.
    ///
    /// # Safety
    ///
    /// `event` must point to an initialized event at a stable address. Until the returned pointer
    /// is discarded, the event must remain alive at that address and no exclusive reference to it
    /// may exist.
    #[cfg(debug_assertions)]
    pub(crate) unsafe fn awaiter_backtrace_cell(
        event: NonNull<UnsafeCell<Self>>,
    ) -> NonNull<Mutex<Option<BacktraceType>>> {
        // SAFETY: The caller guarantees validity, initialization and shared-only aliasing for the
        // event. `UnsafeCell` permits this shared access to its initialized contents.
        let event_cell = unsafe { event.as_ref() };

        // SAFETY: The same caller guarantee excludes an exclusive reference while this shared
        // reference exists, and the event outlives this function call.
        let event = unsafe { event_cell.get().as_ref_unchecked() };

        NonNull::from(&event.backtrace)
    }

    /// Returns a snapshot of the most recent awaiter backtrace.
    #[cfg(all(test, debug_assertions))]
    pub(crate) fn awaiter_backtrace(&self) -> Option<Arc<Backtrace>> {
        let backtrace = self.backtrace.lock().expect(NEVER_POISONED);

        backtrace.as_ref().map(Arc::clone)
    }

    /// Releases the backtrace of the most recent awaiter of the event.
    ///
    /// The storage an event occupies may be reused or freed without dropping the event, so
    /// whoever releases an event calls this first, to release the memory that the backtrace
    /// occupies. Snapshots taken earlier remain valid because they are shared owners of the
    /// backtrace.
    #[cfg(debug_assertions)]
    pub(crate) fn clear_awaiter_backtrace(event_cell: &UnsafeCell<Self>) {
        // SAFETY: The cell's pointer is always valid and points to an initialized event because
        // the caller is still an endpoint of it. We only ever create shared references to the
        // event through this cell, so no exclusive reference can alias this one.
        let event = unsafe { event_cell.get().as_ref_unchecked() };

        let mut backtrace = event.backtrace.lock().expect(NEVER_POISONED);

        *backtrace = None;
    }

    /// Sets the value of the event and notifies the receiver's awaiter, if there is one.
    ///
    /// Returns the payload to the caller if the receiver has already disconnected. The caller
    /// owns cleanup in that case and must release the event before dropping the payload.
    #[inline]
    pub(crate) fn set(event_cell: &UnsafeCell<Self>, value: T) -> Result<(), T> {
        // SAFETY: We only ever create shared references to the event, so no aliasing conflicts.
        // The event lives until both sender and receiver are dropped or inert, so we know it must
        // still exist because something was able to call this method.
        let event_maybe = unsafe { event_cell.get().as_ref() };
        // SAFETY: UnsafeCell pointer is never null.
        let event = unsafe { event_maybe.unwrap_unchecked() };

        let value_cell = event.value.get();

        // We can start by setting the value - this has to happen no matter what.
        // Everything else we do here is just to get the awaiter to come pick it up.
        //
        // SAFETY: It is valid for the sender to write here because we know that nobody else will
        // be accessing this field at this time. This is guaranteed by:
        // * There is only one sender and it is !Sync, so it cannot be used in parallel.
        // * The receiver will only access this field in the "Set" state, which can only be entered
        //   from later on in this method.
        unsafe {
            value_cell.write(MaybeUninit::new(value));
        }

        // A "set" operation is always a state increment. See `state.rs`.
        // We use `Release` ordering for the write because we are
        // releasing the synchronization block of `value`.
        //
        // It is legal to enter SET state here, permitting dealloc, even though we still
        // hold an &event reference here, which ordinarily means it is still illegal to
        // deallocate the object. However, the dangling reference in this method is an
        // `UnsafeCell` which is allowed to dangle, so we are fine even if the object is
        // immediately destroyed after this line.
        let previous_state = event.state.fetch_add(1, atomic::Ordering::Release);

        match previous_state {
            EVENT_BOUND => {
                // Current state: EVENT_SET
                // There was nobody polling via the receiver - our work here is done.
                // The receiver was still connected, so it will clean up the event.
                Ok(())
            }
            EVENT_AWAITING => {
                // Current state: EVENT_SIGNALING
                // There was someone polling via the receiver. We need to
                // notify the awaiter that they can come back for the value now.

                // We need to acquire the synchronization block for the `awaiter`.
                atomic::fence(atomic::Ordering::Acquire);

                // SAFETY: The only other potential references to the field are other short-lived
                // references in this type, which cannot exist at the moment because
                // we are in the `EVENT_SIGNALING` state that acts as a mutex to block access
                // to the `awaiter` field.
                let awaiter_cell_maybe = unsafe { event.awaiter.get().as_mut() };
                // SAFETY: UnsafeCell pointer is never null.
                let awaiter_cell = unsafe { awaiter_cell_maybe.unwrap_unchecked() };

                // We extract the waker and consider the field uninitialized again.
                // SAFETY: We were in EVENT_AWAITING which guarantees there is a waker in there.
                let waker = unsafe { awaiter_cell.assume_init_read() };

                // Test hook: pause here while the sender is in SIGNALING state, so a
                // concurrent test thread can observe and act on the SIGNALING state.
                #[cfg(test)]
                test_hooks::set_in_signaling();

                // Before we send the wake signal we must transition into the `EVENT_SET` state
                // so that the receiver can directly pick up the result when it comes back.
                //
                // We use Release ordering because we are releasing the synchronization block of
                // the `awaiter`. Note that `value` was already released by `fetch_add()` above.
                //
                // It is legal to enter SET state here, permitting dealloc, even though we still
                // hold an &event reference here, which ordinarily means it is still illegal to
                // deallocate the object. However, the dangling reference in this method is an
                // `UnsafeCell` which is allowed to dangle, so we are fine even if the object is
                // immediately destroyed after this line.
                event.state.store(EVENT_SET, atomic::Ordering::Release);

                // Come and get it.
                //
                // As the event is multithreaded, the receiver may already have returned to us
                // before we send this wake signal - that is fine. If that happens, this signal
                // is simply a no-op.
                waker.wake();

                // The receiver was still connected, so it will clean up the event.
                Ok(())
            }
            EVENT_DISCONNECTED => {
                // The receiver has already been dropped, so we need to clean up the event.
                // Move the value back to the sender core so it can release the event before the
                // payload destructor invokes user code.

                // SAFETY: The receiver is gone - there is nobody else who might be touching
                // the event anymore, we are essentially in a single-threaded mode now.
                // We also just inserted the value, so it must still be there because we never
                // entered a state where the receiver had the permission to extract the value.
                let value = unsafe { event.take_value() };

                // Before it is safe to destroy the event, we need to synchronize with whatever
                // writes the receiver may have done into its state (e.g. it may have removed
                // its waker before it marked the event as disconnected).
                atomic::fence(atomic::Ordering::Acquire);

                // The sender (the caller) needs to clean up the event.
                Err(value)
            }
            // Defensive: state machine guarantees this is unreachable.
            _ => {
                unreachable!(
                    "unreachable {} state on set: {previous_state}",
                    type_name::<Self>()
                );
            }
        }
    }

    /// Marks the event as having been disconnected early from the sender side.
    ///
    /// Returns `Err` if the receiver has already disconnected and we must clean up the event now.
    #[inline]
    pub(crate) fn sender_dropped_without_set(
        event_cell: &UnsafeCell<Self>,
    ) -> Result<(), Disconnected> {
        // SAFETY: We only ever create shared references to the event, so no aliasing conflicts.
        // The event lives until both sender and receiver are dropped or inert, so we know it must
        // still exist because something was able to call this method.
        let event_maybe = unsafe { event_cell.get().as_ref() };
        // SAFETY: UnsafeCell pointer is never null.
        let event = unsafe { event_maybe.unwrap_unchecked() };

        // We first need to switch into the SIGNALING state, which acquires exclusive access
        // of the awaiter field, so we can send the wake signal if there is an awaiter.
        // Only after that can we transition into the DISCONNECTED state (because that must
        // be our last action - the receiver may clean up the event at any point after we do that).

        let previous_state = event.state.swap(EVENT_SIGNALING, atomic::Ordering::Relaxed);

        match previous_state {
            EVENT_BOUND => {
                // There was nobody polling via the receiver - our work here is done.

                // It is legal to set DISCONNECTED here, permitting dealloc, even though we still
                // hold an &event reference here, which ordinarily means it is still illegal to
                // deallocate the object. However, the dangling reference in this method is an
                // `UnsafeCell` which is allowed to dangle, so we are fine even if the object is
                // immediately destroyed after this line.
                event
                    .state
                    .store(EVENT_DISCONNECTED, atomic::Ordering::Release);

                // The receiver is the last endpoint remaining, so it will clean up.
                Ok(())
            }
            EVENT_AWAITING => {
                // There is an awaiter that we need to wake up.

                // We need to acquire the synchronization block for the `awaiter`.
                atomic::fence(atomic::Ordering::Acquire);

                // SAFETY: The only other potential references to the field are other short-lived
                // references in this type, which cannot exist at the moment because
                // we are in the `EVENT_SIGNALING` state that acts as a mutex to block access
                // to the `awaiter` field.
                let awaiter_cell_maybe = unsafe { event.awaiter.get().as_mut() };
                // SAFETY: UnsafeCell pointer is never null.
                let awaiter_cell = unsafe { awaiter_cell_maybe.unwrap_unchecked() };

                // We extract the waker and consider the field uninitialized again.
                // SAFETY: We were in EVENT_AWAITING which guarantees there is a waker in there.
                let waker = unsafe { awaiter_cell.assume_init_read() };

                // Transition out of `EVENT_SIGNALING` before the wake so that a synchronously
                // reentrant waker (one that polls the receiver inline) observes a terminal
                // state instead of spinning in `poll_signaling`. This matches the ordering
                // used by `set()` on the `EVENT_SET` path and prevents same-thread reentrancy
                // deadlocks.
                //
                // It is legal to set DISCONNECTED here, permitting dealloc, even though we still
                // hold an &event reference here, which ordinarily means it is still illegal to
                // deallocate the object. However, the dangling reference in this method is an
                // `UnsafeCell` which is allowed to dangle, so we are fine even if the object is
                // immediately destroyed after this line.
                event
                    .state
                    .store(EVENT_DISCONNECTED, atomic::Ordering::Release);

                // Come and get it.
                //
                // As the event is multithreaded, the receiver may already have returned to us
                // before we send this wake signal - that is fine. If that happens, this signal
                // is simply a no-op.
                waker.wake();

                // The receiver is the last endpoint remaining, so it will clean up.
                Ok(())
            }
            EVENT_DISCONNECTED => {
                // We are the last endpoint remaining, so we will clean up.
                Err(Disconnected)
            }
            // Defensive: state machine guarantees this is unreachable.
            _ => {
                unreachable!(
                    "unreachable {} state on sender disconnect: {previous_state}",
                    type_name::<Self>()
                );
            }
        }
    }

    /// We are intended to be polled via `Future::poll`, so we have an equivalent signature here.
    ///
    /// If `Some` is returned, the caller is the last remaining endpoint and responsible
    /// for cleaning up the event.
    #[inline]
    #[must_use]
    pub(crate) fn poll(&self, waker: &Waker) -> Option<Result<T, Disconnected>> {
        #[cfg(debug_assertions)]
        self.backtrace
            .lock()
            .expect(NEVER_POISONED)
            .replace(capture_backtrace());

        // We use Acquire because we are (depending on the state) acquiring the synchronization
        // block for `value` and/or `awaiter`.
        match self.state.load(atomic::Ordering::Acquire) {
            EVENT_BOUND => self.poll_bound(waker),
            EVENT_SET => Some(Ok(self.poll_set())),
            EVENT_AWAITING => self.poll_awaiting(waker),
            EVENT_SIGNALING => Some(self.poll_signaling()),
            EVENT_DISCONNECTED => {
                // There is no result coming, ever! This is the end.
                Some(Err(Disconnected))
            }
            // Defensive: state machine guarantees this is unreachable.
            state => {
                unreachable!("unreachable {} state on poll: {state}", type_name::<Self>());
            }
        }
    }

    /// `poll()` impl for `EVENT_BOUND` state.
    ///
    /// Assumes acquired synchronization block for `awaiter`.
    #[must_use]
    fn poll_bound(&self, waker: &Waker) -> Option<Result<T, Disconnected>> {
        // The sender has not yet set any value, so we will have to wait.

        // SAFETY: The only other potential references to the field are other short-lived
        // references in this type, which cannot exist at the moment because the receiver
        // is !Sync so cannot be used in parallel, while the sender is only allowed to
        // access this field in states that explicitly allow it, which we can only be
        // entered by the receiver in this method.
        let awaiter_cell_maybe = unsafe { self.awaiter.get().as_mut() };
        // SAFETY: UnsafeCell pointer is never null.
        let awaiter_cell = unsafe { awaiter_cell_maybe.unwrap_unchecked() };

        awaiter_cell.write(waker.clone());

        // Test hook: pause here so a concurrent test thread can change the event
        // state before we attempt the CAS below.
        #[cfg(test)]
        test_hooks::poll_bound_pre_cas();

        // The sender is concurrently racing us to either EVENT_SET or EVENT_DISCONNECTED
        // or EVENT_SIGNALING. Note that it is legal for the sender to enter EVENT_SIGNALING
        // at any time - it does not require there to be an awaiter present.
        //
        // We use Release ordering on success because we are releasing the synchronization
        // block for `awaiter`.
        // We use Acquire ordering in failure because on state transition, we acquire the
        // synchronization block for `awaiter` and `value`.
        match self.state.compare_exchange(
            EVENT_BOUND,
            EVENT_AWAITING,
            atomic::Ordering::Release,
            atomic::Ordering::Acquire,
        ) {
            Ok(_) => {
                // We successfully transitioned to the EVENT_AWAITING state.
                // The sender will wake us up when it sets the value.
                None
            }
            Err(EVENT_SET) => {
                // The sender has set the value while we were doing our thing.
                // We know that the sender will have gone away by this point.
                // We need to clean up our awaiter and pick up the value.

                // SAFETY: We wrote the cloned waker into `awaiter` above, so it is initialized.
                // The sender reached `EVENT_SET` from `EVENT_BOUND` (its `fetch_add` observed
                // `EVENT_BOUND`, which is the only predecessor of `EVENT_SET`), and that branch
                // publishes the value without reading or taking the awaiter. The receiver is
                // !Sync, so there is no other accessor and we have exclusive access here.
                unsafe {
                    self.destroy_awaiter();
                }

                Some(Ok(self.poll_set()))
            }
            Err(EVENT_SIGNALING) => {
                // The sender entered SIGNALING via `swap` in `sender_dropped_without_set`.
                // Since we were in `poll_bound`, the state was BOUND when the sender did
                // the swap, so the sender saw `previous = BOUND` and will not touch the
                // awaiter field. We must clean up the waker we just wrote ourselves.

                // SAFETY: We wrote the cloned waker into `awaiter` above, so it is initialized.
                // The sender's swap observed `EVENT_BOUND`, and that branch disconnects without
                // accessing the awaiter. The receiver is !Sync, so there is no other accessor
                // and we have exclusive access here.
                unsafe {
                    self.destroy_awaiter();
                }

                Some(self.poll_signaling())
            }
            Err(EVENT_DISCONNECTED) => {
                // The sender was dropped without setting the event.
                // We need to clean up our awaiter and return an error.

                // SAFETY: We wrote the cloned waker into `awaiter` above, so it is initialized.
                // The sender reached `EVENT_DISCONNECTED` from `EVENT_BOUND` (its swap observed
                // `EVENT_BOUND`), and that branch publishes disconnection without accessing the
                // awaiter. The receiver is !Sync, so there is no other accessor and we have
                // exclusive access here.
                unsafe {
                    self.destroy_awaiter();
                }

                Some(Err(Disconnected))
            }
            // Defensive: state machine guarantees this is unreachable.
            Err(state) => {
                unreachable!(
                    "unreachable {} state on poll transition from EVENT_BOUND: {state}",
                    type_name::<Self>()
                );
            }
        }
    }

    /// `poll()` impl for `EVENT_SET` state.
    ///
    /// Assumes acquired synchronization block for `value`.
    #[must_use]
    fn poll_set(&self) -> T {
        // The sender has delivered a value and we can complete the event.
        // We know that the sender will have gone away by this point.

        // SAFETY: The sender is gone - there is nobody else who might be touching
        // the event anymore, we are essentially in a single-threaded mode now.
        let value_cell_maybe = unsafe { self.value.get().as_mut() };
        // SAFETY: UnsafeCell pointer is never null.
        let value_cell = unsafe { value_cell_maybe.unwrap_unchecked() };

        // We extract the value and consider the cell uninitialized.
        //
        // SAFETY: We were in EVENT_SET which guarantees there is a value in there.
        unsafe { value_cell.assume_init_read() }
    }

    /// `poll()` impl for `EVENT_AWAITING` state.
    ///
    /// Assumes acquired synchronization block for `awaiter`.
    #[must_use]
    fn poll_awaiting(&self, waker: &Waker) -> Option<Result<T, Disconnected>> {
        // We are re-polling after previously starting a wait. This is fine
        // and we just need to clean up the previous waker, replacing it with
        // a new one.

        // The danger here is that the sender may be, at the same time, taking the old
        // waiter and using it. We cannot touch the `awaiter` field right now, as we do
        // not have exclusive access. Instead, we must first transition back into the
        // `EVENT_BOUND` state, then replace the awaiter, then transition back into the
        // `EVENT_AWAITING` state. This way the sender will not do anything invalid.
        // Each state transition is, of course, a potential race that we need to care for.

        // We use Relaxed on both success and failure because we do not yet change
        // externally visible state, merely continue to use our already acquired `awaiter`
        // field that purely sender-side transitions cannot acquire on their own.
        // For the transitions where we do care about synchronization, we use fences.

        // Test hook: pause here so a concurrent test thread can change the event
        // state before we attempt the CAS below.
        #[cfg(test)]
        test_hooks::poll_awaiting_pre_cas();

        match self.state.compare_exchange(
            EVENT_AWAITING,
            EVENT_BOUND,
            atomic::Ordering::Relaxed,
            atomic::Ordering::Relaxed,
        ) {
            Ok(_) => {
                // We have successfully transitioned into EVENT_BOUND.
                // We must destroy the old awaiter and then act as if we were never in
                // EVENT_AWAITING in the first place, going through the EVENT_BOUND path.

                // We are about to touch the `awaiter`, so we need to acquire its synchronization
                // block - the load above was relaxed, so we may not yet have access to the awaiter.
                atomic::fence(atomic::Ordering::Acquire);

                // SAFETY: We have entered the `EVENT_BOUND` state which makes it invalid
                // for the sender to touch `awaiter`. The receiver is !Sync, so we have
                // the only reference here.
                // We also just came from `EVENT_AWAITING` which guarantees there is an
                // awaiter in the cell as we are the only one allowed to touch the field.
                unsafe {
                    self.destroy_awaiter();
                }

                // Now we go back into the normal `EVENT_BOUND` path.
                self.poll_bound(waker)
            }
            Err(EVENT_SET) => {
                // The sender has transitioned the event into EVENT_SET while we were
                // doing our thing. As part of this transition, the sender took ownership
                // of the awaiter and destroyed it. We can treat this as just EVENT_SET now.

                // We entered this state unsynchronized, so we need to acquire the synchronization
                // block for the `value` now.
                atomic::fence(atomic::Ordering::Acquire);

                Some(Ok(self.poll_set()))
            }
            Err(EVENT_SIGNALING) => {
                // The sender is in the middle of a state transition. It is using the old
                // waker, so we cannot touch it any more. We really cannot do anything here
                // except wait for the sender to complete its state transition into
                // EVENT_SET or EVENT_DISCONNECTED.
                Some(self.poll_signaling())
            }
            Err(EVENT_DISCONNECTED) => {
                // The sender was dropped without setting the event, while we were awaiting.
                // This already consumed the waker, so all we need to do is handle the disconnect.

                // As the last surviving party, we are responsible for cleanup.
                // We must also ensure that we observe any writes done by the sender
                // before we destroy the event, so we synchronize here.
                atomic::fence(atomic::Ordering::Acquire);

                Some(Err(Disconnected))
            }
            // Defensive: state machine guarantees this is unreachable.
            Err(state) => {
                unreachable!(
                    "unreachable {} state on poll transition from EVENT_AWAITING: {state}",
                    type_name::<Self>()
                );
            }
        }
    }

    /// `poll()` impl for `EVENT_SIGNALING` state.
    fn poll_signaling(&self) -> Result<T, Disconnected> {
        // This is pretty much a mutex - we are locked out of touching the event state until
        // the sender completes its state transition into either EVENT_SET or EVENT_DISCONNECTED.

        let state = loop {
            let state = self.state.load(atomic::Ordering::Relaxed);

            if state != EVENT_SIGNALING {
                break state;
            }

            // The sender-side transition is just a few instructions,
            // which should be near-instantaneous so we just spin.
            spin_loop();
        };

        // The store that brings us out of the SIGNALING state has Release semantics,
        // so we must have an Acquire fence here to ensure we observe all its effects.
        atomic::fence(atomic::Ordering::Acquire);

        // After the sender-side transition, we continue, knowing now that the event must
        // either by in SET or DISCONNECTED state, as those are the only valid sender-side
        // transitions from SIGNALING.
        match state {
            EVENT_SET => {
                let value = self.poll_set();

                // The receiver will clean up.
                Ok(value)
            }
            EVENT_DISCONNECTED => {
                // The receiver is the last endpoint remaining, so it will clean up.
                Err(Disconnected)
            }
            // Defensive: state machine guarantees this is unreachable.
            _ => {
                unreachable!(
                    "unreachable {} post-signaling state: {state}",
                    type_name::<Self>()
                )
            }
        }
    }

    /// Whether the event has reached a terminal state, meaning `EVENT_SET` or
    /// `EVENT_DISCONNECTED` (see `state.rs`). A terminal state is what makes an outcome - a value
    /// or a disconnect - immediately retrievable.
    #[must_use]
    pub(crate) fn is_set(&self) -> bool {
        // We use Relaxed ordering because this is independent of any other data.
        // If something wishes to actually obtain the value from the event, that
        // logic will perform its own synchronization.
        //
        // Only terminal states count as ready. `EVENT_SIGNALING` is the transient window
        // during which the sender is mid-`set()`: the value is not yet retrievable, so
        // `into_value()` still reports `Pending` there. Reporting readiness in that window
        // would disagree with immediate value retrieval (issue #462). This matches the
        // single-threaded variant, whose `is_set()` also admits only terminal states.
        matches!(
            self.state.load(atomic::Ordering::Relaxed),
            EVENT_SET | EVENT_DISCONNECTED
        )
    }

    /// Extracts the terminal result after the receiver has observed a completed event.
    ///
    /// The receiver owns event cleanup after either result.
    ///
    /// # Panics
    ///
    /// Panics if the event is not in a terminal state.
    // Callgrind and disassembly show that terminal extraction must remain inline with
    // `into_value()` to keep cancellation machinery off that hot path.
    #[inline]
    pub(crate) fn take_result(event_cell: &UnsafeCell<Self>) -> Result<T, Disconnected> {
        // SAFETY: The event reference contract guarantees shared access through this outer
        // UnsafeCell for as long as the receiver still owns its endpoint reference.
        let event = unsafe { event_cell.get().as_ref_unchecked() };

        #[cfg(debug_assertions)]
        event
            .backtrace
            .lock()
            .expect(NEVER_POISONED)
            .replace(capture_backtrace());

        // Terminal states are stable, so no sender can race this exchange. AcqRel both acquires
        // the sender's payload/disconnection writes and publishes receiver cleanup.
        let previous_state = event
            .state
            .swap(EVENT_DISCONNECTED, atomic::Ordering::AcqRel);

        match previous_state {
            EVENT_SET => {
                // SAFETY: EVENT_SET guarantees that the payload is initialized and that this
                // receiver acquired its synchronization block through the exchange above.
                Ok(unsafe { event.take_value() })
            }
            EVENT_DISCONNECTED => Err(Disconnected),
            // Defensive: the caller must have observed a terminal state.
            _ => {
                unreachable!("invalid {} state: {previous_state}", type_name::<Self>());
            }
        }
    }

    /// Cancels the receiver, returning the final event observation and any deferred callback.
    ///
    /// Returns `Ok(None)` if the sender has not yet sent a value. In this case, the sender will
    /// eventually clean up the event.
    ///
    /// Returns `Ok(Some(value))` if the sender sender has already sent a value.
    /// Returns `Err` if the sender has already disconnected without sending a value.
    /// In both of these cases, the receiver must clean up the event now.
    #[inline]
    pub(crate) fn cancel(event_cell: &UnsafeCell<Self>) -> Cancellation<T> {
        // SAFETY: We only ever create shared references to the event, so no aliasing conflicts.
        // The event lives until both sender and receiver are dropped or inert, so we know it must
        // still exist because something was able to call this method.
        let event_maybe = unsafe { event_cell.get().as_ref() };
        // SAFETY: UnsafeCell pointer is never null.
        let event = unsafe { event_maybe.unwrap_unchecked() };

        #[cfg(debug_assertions)]
        event
            .backtrace
            .lock()
            .expect(NEVER_POISONED)
            .replace(capture_backtrace());

        // The receiver may still own the waker if the waker has not been used. If this is the case,
        // move it out and defer its destruction until the state handoff and any storage release
        // have completed.
        // We implement this check by attempting to revert ourselves back to the BOUND state.
        // If successful, we know we were in AWAITING and can extract the waker.
        let awaiter = if event
            .state
            .compare_exchange(
                EVENT_AWAITING,
                EVENT_BOUND,
                atomic::Ordering::Acquire,
                atomic::Ordering::Relaxed,
            )
            .is_ok()
        {
            // SAFETY: The `awaiter` is guaranteed to be present because we just came from
            // `EVENT_AWAITING`. The sender will only touch the awaiter in `EVENT_SIGNALING`,
            // which never entered. Therefore, we are the only one who can touch the awaiter now.
            Some(unsafe { event.take_awaiter() })
        } else {
            None
        };

        // We must transition the event into DISCONNECTED, but we cannot do so while the
        // sender is in the middle of a SIGNALING transition — writing DISCONNECTED via swap
        // would clobber SIGNALING and the sender would then write SET/DISCONNECTED into
        // memory that the receiver has already released (use-after-free). Instead, we use a
        // CAS loop that spins on SIGNALING, waiting for the sender to finish its transition
        // before we write DISCONNECTED.
        //
        // It is legal to set DISCONNECTED here, permitting dealloc, even though we still
        // hold an &event reference here, which ordinarily means it is still illegal to
        // deallocate the object. However, the dangling reference in this method is an
        // `UnsafeCell` which is allowed to dangle, so we are fine even if the object is
        // immediately destroyed after this line.
        let previous_state = loop {
            let current = event.state.load(atomic::Ordering::Relaxed);

            if current == EVENT_SIGNALING {
                // The sender is mid-transition. It will complete in just a few instructions,
                // so we spin until it finishes and writes the final state.
                spin_loop();
                continue;
            }

            // We use Release because we are releasing the synchronization block of the event.
            match event.state.compare_exchange(
                current,
                EVENT_DISCONNECTED,
                atomic::Ordering::Release,
                atomic::Ordering::Relaxed,
            ) {
                Ok(_) => break current,
                // Another thread changed the state between our load and the CAS. Retry.
                Err(_) => continue,
            }
        };

        let result = match previous_state {
            EVENT_BOUND => {
                // The sender had not yet set any value. It will clean up the event later.
                Ok(None)
            }
            EVENT_SET => {
                // The sender has already set a value but we disconnected before we received it.
                // We need to clean up the value and then later clean up the event, as well.

                // We need to acquire the synchronization block for the `value`.
                atomic::fence(atomic::Ordering::Acquire);

                let value = event.poll_set();

                // The receiver (the caller) will clean up.
                Ok(Some(value))
            }
            EVENT_DISCONNECTED => {
                // We need to ensure we see any writes the sender made before it disconnected.
                atomic::fence(atomic::Ordering::Acquire);

                // The receiver (the caller) is the last endpoint remaining, so it will clean up.
                Err(Disconnected)
            }
            // Defensive: state machine guarantees this is unreachable because the CAS loop
            // spins on SIGNALING and only breaks on BOUND, SET, or DISCONNECTED.
            _ => {
                unreachable!(
                    "unreachable {} state on receiver disconnect: {previous_state}",
                    type_name::<Self>()
                );
            }
        };

        Cancellation {
            result,
            _awaiter: awaiter,
        }
    }

    /// Extracts the waker registered in `awaiter`, leaving that cell uninitialized.
    ///
    /// # Safety
    ///
    /// The caller must have acquired the synchronization block for `awaiter` and `awaiter` must
    /// hold an initialized waker.
    unsafe fn take_awaiter(&self) -> Waker {
        // SAFETY: Forwarding guarantees from the caller.
        let awaiter_cell_maybe = unsafe { self.awaiter.get().as_mut() };
        // SAFETY: UnsafeCell pointer is never null.
        let awaiter_cell = unsafe { awaiter_cell_maybe.unwrap_unchecked() };

        // SAFETY: Forwarding guarantees from the caller.
        unsafe { awaiter_cell.assume_init_read() }
    }

    /// Drops the waker registered in `awaiter`, leaving that cell uninitialized.
    ///
    /// # Safety
    ///
    /// The caller must have acquired the synchronization block for `awaiter` and `awaiter` must
    /// hold an initialized waker.
    unsafe fn destroy_awaiter(&self) {
        // SAFETY: Forwarding guarantees from the caller.
        drop(unsafe { self.take_awaiter() });
    }

    /// Extracts the payload stored in `value`, leaving that cell uninitialized.
    ///
    /// # Safety
    ///
    /// The caller must have acquired the synchronization block for `value` and `value` must hold
    /// an initialized payload.
    unsafe fn take_value(&self) -> T {
        // SAFETY: Forwarding guarantees from the caller.
        let value_cell_maybe = unsafe { self.value.get().as_mut() };
        // SAFETY: UnsafeCell pointer is never null.
        let value_cell = unsafe { value_cell_maybe.unwrap_unchecked() };

        // SAFETY: Forwarding guarantees from the caller.
        unsafe { value_cell.assume_init_read() }
    }
}

// SAFETY: Shared references to an event may be used from any thread because every access to the
// `UnsafeCell` fields is arbitrated by the atomic `state`. There is at most one sender and one
// receiver, and their core types are !Sync, so neither endpoint is used from two threads at once.
// The receiver writes `awaiter` only while the event is `EVENT_BOUND` and publishes it with the
// Release transition to `EVENT_AWAITING`; a sender that wants the awaiter first enters
// `EVENT_SIGNALING`, which locks the receiver out of the field, and reads it behind an Acquire
// fence. The sender writes `value` before the Release transition that publishes `EVENT_SET`, and
// the receiver reads it only after an Acquire load or fence has observed that state. `T: Send`
// is what permits the payload to be handed from the sender's thread to the receiver's thread.
// We do not repeat the `'static` bound of the inherent impls here: this proof does not need it
// and an unnecessary bound on an unsafe impl triggers a rustc bug (rust-lang/rust#110338) in
// async generator Send inference with trait object type params.
unsafe impl<T: Send> Sync for Event<T> {}

#[cfg_attr(coverage_nightly, coverage(off))] // No API contract to test.
impl<T: Send + 'static> fmt::Debug for Event<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct(type_name::<Self>());

        debug.field("state", &self.state);
        debug.field("awaiter", &self.awaiter);
        debug.field("value", &self.value);

        #[cfg(debug_assertions)]
        {
            debug.field("backtrace", &self.backtrace);
        }

        debug.finish()
    }
}

/// Synchronization points that let a test pause an [`Event`] operation between two steps that
/// normally execute without interruption, making the race-condition branches of the state
/// machine deterministically reachable.
///
/// The storage and selection logic lives here instead of inline in the [`Event`] methods so that
/// the test harness does not become part of the coverage signal of the state machine - each
/// method only calls the matching helper. Ref: docs/testing.md, "Test coverage".
///
/// A hook only fires on threads that have marked themselves as participants via
/// [`HOOK_PARTICIPANT`], so a test that happens to run concurrently cannot wander into another
/// test's barrier. Tests that install hooks hold [`HOOK_SERIALIZATION_MUTEX`] for the duration.
#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
pub(crate) mod test_hooks {
    use std::cell::Cell;
    use std::sync::{Arc, Mutex};

    use crate::NEVER_POISONED;

    /// A hook closure, shared between the test that installs it and the thread that runs it.
    pub(crate) type HookFn = dyn Fn() + Send + Sync;

    /// Held by a test for as long as it has any hook installed, so that hook-based tests cannot
    /// observe each other's hooks.
    pub(crate) static HOOK_SERIALIZATION_MUTEX: Mutex<()> = Mutex::new(());

    pub(crate) static HOOK_POLL_BOUND_PRE_CAS: Mutex<Option<Arc<HookFn>>> = Mutex::new(None);
    pub(crate) static HOOK_POLL_AWAITING_PRE_CAS: Mutex<Option<Arc<HookFn>>> = Mutex::new(None);
    pub(crate) static HOOK_SET_IN_SIGNALING: Mutex<Option<Arc<HookFn>>> = Mutex::new(None);

    thread_local! {
        /// Marks the current thread as a participant in a hook-based test. Only threads with
        /// this flag set to `true` trigger hooks when they reach a hook callsite.
        pub(crate) static HOOK_PARTICIPANT: Cell<bool> = const { Cell::new(false) };
    }

    /// Runs in `Event::poll_bound()` after the waker has been written into the event but before
    /// the state transition that publishes it to the sender.
    pub(crate) fn poll_bound_pre_cas() {
        run(&HOOK_POLL_BOUND_PRE_CAS);
    }

    /// Runs in `Event::poll_awaiting()` before the state transition that takes the previously
    /// registered waker back from the sender.
    pub(crate) fn poll_awaiting_pre_cas() {
        run(&HOOK_POLL_AWAITING_PRE_CAS);
    }

    /// Runs in `Event::set()` while the event is in the transient `EVENT_SIGNALING` state, after
    /// the sender has taken the awaiter and before it publishes `EVENT_SET`.
    pub(crate) fn set_in_signaling() {
        run(&HOOK_SET_IN_SIGNALING);
    }

    fn run(hook: &Mutex<Option<Arc<HookFn>>>) {
        if !HOOK_PARTICIPANT.get() {
            return;
        }

        // We take a copy of the hook so that no lock is held while the hook runs - a hook
        // typically blocks on a barrier until another thread has done its part.
        let hook = hook.lock().expect(NEVER_POISONED).clone();

        if let Some(hook) = hook {
            hook();
        }
    }
}
