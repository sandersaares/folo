use std::any::type_name;
#[cfg(all(test, debug_assertions))]
use std::backtrace::Backtrace;
#[cfg(debug_assertions)]
use std::cell::RefCell;
use std::cell::{Cell, UnsafeCell};
use std::fmt;
use std::marker::{PhantomData, PhantomPinned};
use std::mem::{MaybeUninit, offset_of};
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::pin::Pin;
use std::ptr::NonNull;
#[cfg(all(test, debug_assertions))]
use std::sync::Arc;
use std::task::Waker;

#[cfg(debug_assertions)]
use crate::{BacktraceType, capture_backtrace};
use crate::{
    BoxedLocalReceiver, BoxedLocalRef, BoxedLocalSender, Cancellation, Disconnected,
    EVENT_AWAITING, EVENT_BOUND, EVENT_DISCONNECTED, EVENT_SET, EmbeddedLocalEvent,
    LocalReceiverCore, LocalSenderCore, PtrLocalRef, RawLocalReceiver, RawLocalSender,
};

/// Coordinates delivery of a `T` at most once from a sender to a receiver on the same thread.
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
pub struct LocalEvent<T> {
    /// The logical state of the event; see constants in `state.rs`.
    pub(crate) state: Cell<u8>,

    /// Holds the waker of whoever most recently awaited the receiver. The `state` field is what
    /// records whether this field is initialized; see `state.rs`.
    ///
    /// We use `MaybeUninit` to minimize the storage and avoid an `Option` or enum overhead,
    /// as we already track the presence via `state`.
    ///
    /// We use `UnsafeCell` because we are a synchronization primitive and
    /// do our own synchronization of reads/writes.
    awaiter: UnsafeCell<MaybeUninit<Waker>>,

    /// Holds the value that was sent by the sender. The `state` field is what records whether
    /// this field is initialized; see `state.rs`.
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
    backtrace: RefCell<Option<BacktraceType>>,

    // Everything to do with this event is single-threaded,
    // even if T is thread-mobile or thread-safe.
    _single_threaded: PhantomData<*const ()>,

    // It is invalid to move this type once it has been pinned because
    // the sender and receiver connect via raw pointers to the event.
    _requires_pinning: PhantomPinned,
}

// The Cell and UnsafeCell fields (state, awaiter, value) cause auto-trait
// inference to mark LocalEvent as !RefUnwindSafe, and a payload that is itself
// !UnwindSafe would additionally make it !UnwindSafe. The event is unwind-safe
// regardless of the payload because every mutation is panic-atomic: the payload
// and the waker are moved into and out of their cells in single moves, the state
// that records which of those cells is initialized is published by a single store
// ordered after the move, and the user-controlled code that can unwind (payload,
// waker and destructor code) only runs where state and storage agree. A caught
// panic therefore leaves the event in a state that the remaining endpoint can
// complete or clean up, which the unwinding-mid-poll regression tests below
// exercise.
impl<T: 'static> UnwindSafe for LocalEvent<T> {}
impl<T: 'static> RefUnwindSafe for LocalEvent<T> {}

impl<T: 'static> LocalEvent<T> {
    /// In-place initializes a new instance in the `BOUND` state.
    ///
    /// This is for internal use only and is wrapped by public methods that also
    /// wire up the sender and receiver after doing the initialization. An event
    /// without a sender and receiver is invalid.
    pub(crate) fn new_in_inner(place: &mut UnsafeCell<MaybeUninit<Self>>) {
        // We can skip initializing the MaybeUninit fields because they start uninitialized
        // by design and the UnsafeCell wrapper is transparent, only affecting accesses and
        // not the contents.
        let base_ptr = place.get_mut().as_mut_ptr();

        // SAFETY: We are making a pointer to a known field at a compiler-guaranteed offset.
        let state_ptr = unsafe { base_ptr.byte_add(offset_of!(Self, state)) }.cast::<Cell<u8>>();

        // SAFETY: This is the matching field of the type we are initializing, so valid for writes.
        unsafe {
            state_ptr.write(Cell::new(EVENT_BOUND));
        }

        #[cfg(debug_assertions)]
        {
            // SAFETY: We are making a pointer to a known field at a compiler-guaranteed offset.
            let backtrace_ptr = unsafe { base_ptr.byte_add(offset_of!(Self, backtrace)) }
                .cast::<RefCell<Option<BacktraceType>>>();

            // SAFETY: This is the matching field of the type we are initializing, so valid for writes.
            unsafe {
                backtrace_ptr.write(RefCell::new(None));
            }
        }
    }

    #[must_use]
    pub(crate) fn boxed_core() -> (
        LocalSenderCore<BoxedLocalRef<T>, T>,
        LocalReceiverCore<BoxedLocalRef<T>, T>,
    ) {
        let (sender_event, receiver_event) = BoxedLocalRef::new_pair();

        (
            LocalSenderCore::new(sender_event),
            LocalReceiverCore::new(receiver_event),
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
    /// use events_once::LocalEvent;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let (sender, receiver) = LocalEvent::<String>::boxed();
    ///
    /// sender.send("Hello, world!".to_string());
    ///
    /// let message = receiver.await.unwrap();
    /// assert_eq!(message, "Hello, world!");
    /// # }
    /// ```
    #[must_use]
    pub fn boxed() -> (BoxedLocalSender<T>, BoxedLocalReceiver<T>) {
        let (sender_core, receiver_core) = Self::boxed_core();

        (
            BoxedLocalSender::new(sender_core),
            BoxedLocalReceiver::new(receiver_core),
        )
    }

    #[must_use]
    pub(crate) unsafe fn placed_core(
        place: Pin<&mut UnsafeCell<MaybeUninit<Self>>>,
    ) -> (
        LocalSenderCore<PtrLocalRef<T>, T>,
        LocalReceiverCore<PtrLocalRef<T>, T>,
    ) {
        // SAFETY: Nothing is getting moved, we just temporarily unwrap the Pin wrapper.
        let place_mut = unsafe { place.get_unchecked_mut() };

        Self::new_in_inner(place_mut);

        // We cast away the MaybeUninit wrapper because it is now initialized.
        let event = NonNull::from_mut(place_mut).cast::<UnsafeCell<Self>>();

        // SAFETY: The event was just initialized into this storage, the caller of this function
        // guaranteed that the storage stays valid and pinned at this address for the entire
        // lifetime of the endpoints, and the endpoints only ever reach the event through the
        // shared references that the reference policy hands out.
        let sender_event = unsafe { PtrLocalRef::new(event) };

        // SAFETY: Same as above - the second endpoint references the same initialized event in
        // the same caller-guaranteed storage.
        let receiver_event = unsafe { PtrLocalRef::new(event) };

        (
            LocalSenderCore::new(sender_event),
            LocalReceiverCore::new(receiver_event),
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
    /// use events_once::{EmbeddedLocalEvent, LocalEvent};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let mut place = Box::pin(EmbeddedLocalEvent::<String>::new());
    ///
    /// // SAFETY: `place` is a freshly created container that no other event is using, it stays
    /// // alive and writable until both endpoints below are consumed, and `Box::pin` keeps the
    /// // event at a stable address for that entire time.
    /// let (sender, receiver) = unsafe { LocalEvent::placed(place.as_mut()) };
    ///
    /// sender.send("Hello from embedded event!".to_string());
    ///
    /// let message = receiver.await.unwrap();
    /// assert_eq!(message, "Hello from embedded event!");
    /// # }
    /// ```
    #[must_use]
    pub unsafe fn placed(
        place: Pin<&mut EmbeddedLocalEvent<T>>,
    ) -> (RawLocalSender<T>, RawLocalReceiver<T>) {
        // SAFETY: Not moving anything, just breaking through the public wrapper API.
        let place = unsafe { place.map_unchecked_mut(|container| &mut container.inner) };

        // SAFETY: Forwarding safety guarantees from the caller.
        let (sender_core, receiver_core) = unsafe { Self::placed_core(place) };

        (
            RawLocalSender::new(sender_core),
            RawLocalReceiver::new(receiver_core),
        )
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
    ) -> NonNull<RefCell<Option<BacktraceType>>> {
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
        let backtrace = self.backtrace.borrow();

        backtrace.as_ref().map(Arc::clone)
    }

    /// Releases the backtrace of the most recent awaiter of this event.
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

        let mut backtrace = event.backtrace.borrow_mut();

        *backtrace = None;
    }

    /// Sets the value of the event and notifies the awaiter, if there is one.
    ///
    /// Returns `Err` if the receiver has already disconnected and the caller must clean up the
    /// event now.
    ///
    /// The event arrives as an `&UnsafeCell<Self>` rather than as `&self` because the wake
    /// callback performed here is free to release the event storage. A `&self` argument stays
    /// strongly protected by the aliasing model for the whole call, which makes such a release
    /// undefined behavior, whereas references derived from an `UnsafeCell` carry no protector and
    /// may dangle. The event is therefore not touched after the callback runs.
    ///
    /// Returns the payload to the caller if the receiver has already disconnected. The caller
    /// owns cleanup in that case and must release the event before dropping the payload.
    /// Ref: docs/callback-safety.md.
    #[inline]
    pub(crate) fn set(event_cell: &UnsafeCell<Self>, value: T) -> Result<(), T> {
        // SAFETY: We only ever create shared references to the event, so no aliasing conflicts.
        // The event lives until both sender and receiver are dropped or inert, so we know it must
        // still exist because something was able to call this method.
        let event = unsafe { event_cell.get().as_ref_unchecked() };

        let value_cell = event.value.get();

        // We can start by setting the value - this has to happen no matter what.
        // Everything else we do here is just to get the awaiter to come pick it up.
        //
        // SAFETY: It is valid for the sender to write here because we know that nobody else will
        // be accessing this field at this time. This is guaranteed by:
        // * There is only one sender and it is single-threaded, so it cannot be used in parallel.
        // * The receiver will only access this field in the "Set" state, which can only be entered
        //   from later on in this method.
        unsafe {
            value_cell.write(MaybeUninit::new(value));
        }

        // We publish the terminal state with a single store, which also collapses the two state
        // writes that the thread-safe variant needs on the awaiting branch (`EVENT_SIGNALING`
        // then `EVENT_SET`). Only the thread-safe variant needs that intermediate; see
        // `state.rs`. In the disconnected branch the state we store here is a don't-care because
        // the event is about to be released, and no external observer can witness it.
        let previous_state = event.state.replace(EVENT_SET);

        match previous_state {
            EVENT_BOUND => {
                // There was nobody listening via the receiver - our work here is done.
                // The receiver is still connected, so it will clean up.
                Ok(())
            }
            EVENT_AWAITING => {
                // There was someone listening via the receiver. We need to
                // notify the awaiter that they can come back for the value now.
                //
                // The state is already EVENT_SET (set by the replace above), so
                // the reentrant receiver poll triggered by `wake()` will observe
                // a terminal state and proceed straight to value extraction.

                // Extract the waker in a tight scope so the `&mut MaybeUninit<Waker>`
                // borrow is released before the wake callback fires, matching the
                // callback-safety guidance in docs/callback-safety.md.
                let waker = {
                    // SAFETY: The only other potential references to the field are other
                    // short-lived references in this type, which cannot exist at the moment
                    // because the type is single-threaded and does not let any references
                    // escape.
                    let awaiter_cell_maybe = unsafe { event.awaiter.get().as_mut() };
                    // SAFETY: UnsafeCell pointer is never null.
                    let awaiter_cell = unsafe { awaiter_cell_maybe.unwrap_unchecked() };

                    // We extract the waker and consider the field uninitialized again.
                    // SAFETY: We were in EVENT_AWAITING which guarantees there is a waker
                    // in there.
                    unsafe { awaiter_cell.assume_init_read() }
                };

                // Come and get it. The event is in a terminal state, so the woken receiver may
                // reentrantly complete and release the event storage. We touch nothing in the
                // event after this point.
                waker.wake();

                // The receiver is still connected, so it will clean up.
                Ok(())
            }
            EVENT_DISCONNECTED => {
                // The receiver has already disconnected, so we can clean up the event now.
                // Move the value back to the sender core so it can release the event before the
                // payload destructor invokes user code.

                // SAFETY: The only other potential references to the field are other short-lived
                // references in this type, which cannot exist at the moment because
                // the type is single-threaded and does not let any references escape.
                let value_cell_maybe = unsafe { event.value.get().as_mut() };
                // SAFETY: UnsafeCell pointer is never null.
                let value_cell = unsafe { value_cell_maybe.unwrap_unchecked() };

                // We extract the value and consider the cell uninitialized.
                //
                // SAFETY: We were in EVENT_SET which guarantees there is a value in there.
                let value = unsafe { value_cell.assume_init_read() };

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

    /// We are intended to be polled via `Future::poll`, so we have a similar signature here,
    /// with `None` equating to `Poll::Pending`.
    ///
    /// If `Some` result is returned, the caller is the last remaining endpoint and responsible
    /// for cleaning up the event. It must do so immediately, without running any user-controlled
    /// code in between and without any further state-based cleanup, because the event may be in
    /// the transient state described on [`poll_set()`][Self::poll_set].
    #[inline]
    #[must_use]
    pub(crate) fn poll(&self, waker: &Waker) -> Option<Result<T, Disconnected>> {
        #[cfg(debug_assertions)]
        self.backtrace.replace(Some(capture_backtrace()));

        match self.state.get() {
            EVENT_BOUND => self.poll_bound(waker),
            EVENT_SET => Some(Ok(self.poll_set())),
            EVENT_AWAITING => self.poll_awaiting(waker),
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

    /// `poll()` impl for the `EVENT_BOUND` state.
    #[must_use]
    fn poll_bound(&self, waker: &Waker) -> Option<Result<T, Disconnected>> {
        // The sender has not yet set any value, so we will have to wait.

        // `Waker::clone` runs arbitrary user code, which is free to operate on the sender endpoint
        // of this same event and thereby move the event into a terminal state. We therefore clone
        // before touching the event and consult the state again afterwards. This mirrors the
        // compare-exchange in the sync `Event::poll_bound`: reentrancy hands the single-threaded
        // variant the same interleavings that concurrency hands the thread-safe one.
        // Ref: docs/callback-safety.md.
        let new_waker = waker.clone();

        match self.state.get() {
            EVENT_BOUND => {
                // SAFETY: The only other potential references to the field are other short-lived
                // references in this type, which cannot exist at the moment because
                // the type is single-threaded and does not let any references escape.
                let awaiter_cell_maybe = unsafe { self.awaiter.get().as_mut() };
                // SAFETY: UnsafeCell pointer is never null.
                let awaiter_cell = unsafe { awaiter_cell_maybe.unwrap_unchecked() };

                awaiter_cell.write(new_waker);

                // The sender will wake us up when it has set the value.
                self.state.set(EVENT_AWAITING);
                None
            }
            EVENT_SET => {
                // A reentrant sender delivered the value while we were cloning the waker. Nobody
                // would ever consume a registration made now, so we release the clone instead.
                // Releasing it runs user code, which must not see the event mid-extraction: the
                // value comes out only once no callback can run, so an unwinding destructor leaves
                // `EVENT_SET` still backed by an initialized value for the receiver to clean up.
                // Ref: docs/callback-safety.md.
                drop(new_waker);

                Some(Ok(self.poll_set()))
            }
            EVENT_DISCONNECTED => {
                // A reentrant sender drop disconnected while we were cloning the waker, so there
                // is no result coming, ever. We release the clone that nobody would consume.
                drop(new_waker);

                Some(Err(Disconnected))
            }
            // Defensive: only the receiver enters EVENT_AWAITING and it is right here, so the
            // state machine guarantees this is unreachable.
            state => {
                unreachable!(
                    "unreachable {} state on poll of bound event: {state}",
                    type_name::<Self>()
                );
            }
        }
    }

    /// `poll()` impl for the `EVENT_SET` state.
    ///
    /// Extracts the payload and leaves the value cell uninitialized while `state` still says
    /// `EVENT_SET`. That is the window which `state.rs` ("Field initialization") requires the
    /// caller to close, so two obligations follow: every user-controlled callback (waker clone,
    /// drop or wake) must have finished before entry, and the caller must release the event on
    /// return without any further state-driven cleanup. Otherwise a callback that unwinds into
    /// receiver cancellation reaches `cancel()`, which trusts `EVENT_SET` and would read the
    /// moved-out payload a second time. Ref: docs/callback-safety.md.
    #[must_use]
    fn poll_set(&self) -> T {
        // The sender has delivered a value and we can complete the event.

        // SAFETY: The only other potential references to the field are other short-lived
        // references in this type, which cannot exist at the moment because
        // the type is single-threaded and does not let any references escape.
        let value_cell_maybe = unsafe { self.value.get().as_ref() };
        // SAFETY: UnsafeCell pointer is never null.
        let value_cell = unsafe { value_cell_maybe.unwrap_unchecked() };

        // We extract the value and consider the cell uninitialized.
        //
        // SAFETY: We were in EVENT_SET which guarantees there is a value in there.
        unsafe { value_cell.assume_init_read() }
    }

    /// `poll()` impl for the `EVENT_AWAITING` state.
    #[must_use]
    fn poll_awaiting(&self, waker: &Waker) -> Option<Result<T, Disconnected>> {
        // We are re-polling after previously starting a wait. This is fine
        // and we just need to clean up the previous waker, replacing it with
        // a new one. The previous registration must be released exactly once;
        // overwriting it without dropping would leak a `Waker` reference.

        // Clone the incoming waker before touching the cell. `Waker::clone` runs arbitrary user
        // code, so it must not observe the cell in the transiently-uninitialized window between
        // the read and the write below. That code is also free to operate on the sender endpoint
        // of this same event, which moves the event into a terminal state and takes the
        // previously registered waker along the way, so we consult the state again afterwards.
        // Ref: docs/callback-safety.md.
        let new_waker = waker.clone();

        match self.state.get() {
            EVENT_AWAITING => {
                // Extract the previous waker in a tight scope so the
                // `&mut MaybeUninit<Waker>` borrow is released before we drop it below:
                // a waker's drop runs arbitrary vtable code that must not observe an
                // active borrow of shared state.
                // Ref: docs/callback-safety.md, "No callbacks under borrows of shared state".
                let previous_waker = {
                    // SAFETY: The only other potential references to the field are other
                    // short-lived references in this type, which cannot exist at the moment
                    // because the type is single-threaded and does not let any references
                    // escape.
                    let awaiter_cell_maybe = unsafe { self.awaiter.get().as_mut() };
                    // SAFETY: UnsafeCell pointer is never null.
                    let awaiter_cell = unsafe { awaiter_cell_maybe.unwrap_unchecked() };

                    // We extract the previous waker and immediately overwrite the cell with
                    // the new one, keeping the field initialized and the state consistent for
                    // any reentrant poll triggered while the previous waker is dropped below.
                    // SAFETY: We were in EVENT_AWAITING which guarantees there is a waker
                    // in there.
                    let previous_waker = unsafe { awaiter_cell.assume_init_read() };
                    awaiter_cell.write(new_waker);
                    previous_waker
                };

                // Release the previous registration exactly once, now that the borrow is gone.
                // A reentrant sender that completes the event from this destructor takes the
                // registration we just made and wakes it, so the caller is still told to come
                // back even though we report being pending. We touch nothing in the event after
                // this point.
                drop(previous_waker);

                None
            }
            EVENT_SET => {
                // A reentrant sender delivered the value while we were cloning the waker, taking
                // the previous registration with it. See the equivalent arm of `poll_bound()` for
                // why the unused clone is released before the value comes out.
                drop(new_waker);

                Some(Ok(self.poll_set()))
            }
            EVENT_DISCONNECTED => {
                // A reentrant sender drop disconnected while we were cloning the waker, taking
                // the previous registration with it. There is no result coming, ever.
                drop(new_waker);

                Some(Err(Disconnected))
            }
            // Defensive: only the receiver leaves EVENT_AWAITING for a non-terminal state and it
            // is right here, so the state machine guarantees this is unreachable.
            state => {
                unreachable!(
                    "unreachable {} state on poll of awaited event: {state}",
                    type_name::<Self>()
                );
            }
        }
    }

    /// Marks the event as having been disconnected early from the sender side.
    ///
    /// Returns `Err` if the receiver has already disconnected and we must clean up the event now.
    ///
    /// See [`set()`][Self::set] for why the event arrives as an `&UnsafeCell<Self>`.
    #[inline]
    pub(crate) fn sender_dropped_without_set(
        event_cell: &UnsafeCell<Self>,
    ) -> Result<(), Disconnected> {
        // SAFETY: We only ever create shared references to the event, so no aliasing conflicts.
        // The event lives until both sender and receiver are dropped or inert, so we know it must
        // still exist because something was able to call this method.
        let event = unsafe { event_cell.get().as_ref_unchecked() };

        let previous_state = event.state.get();

        // We can immediately set this because this is a single-threaded event, so there cannot
        // be any race condition causing us issues with the receiver seeing this too early.
        event.state.set(EVENT_DISCONNECTED);

        match previous_state {
            EVENT_BOUND => {
                // There was nobody listening via the receiver - our work here is done.
                // The receiver still exists, so it will clean up.
                Ok(())
            }
            EVENT_AWAITING => {
                // There was someone listening via the receiver. We need to notify
                // the awaiter that they can come back for another check now.

                // SAFETY: The only other potential references to the field are other short-lived
                // references in this type, which cannot exist at the moment because
                // the type is single-threaded and does not let any references escape.
                let awaiter_cell_maybe = unsafe { event.awaiter.get().as_mut() };
                // SAFETY: UnsafeCell pointer is never null.
                let awaiter_cell = unsafe { awaiter_cell_maybe.unwrap_unchecked() };

                // We extract the waker and consider the field uninitialized again.
                // SAFETY: We were in EVENT_AWAITING which guarantees there is a waker in there.
                let waker = unsafe { awaiter_cell.assume_init_read() };

                // Come and get it. The event is in a terminal state, so the woken receiver may
                // reentrantly complete and release the event storage. We touch nothing in the
                // event after this point.
                waker.wake();

                // The receiver is the last endpoint remaining, so it will clean up.
                Ok(())
            }
            EVENT_DISCONNECTED => {
                // The sender is the last endpoint remaining, so it will clean up.
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

    /// Whether the event has reached a terminal state, meaning `EVENT_SET` or
    /// `EVENT_DISCONNECTED` (see `state.rs`). A terminal state is what makes an outcome - a value
    /// or a disconnect - immediately retrievable.
    #[must_use]
    pub(crate) fn is_set(&self) -> bool {
        matches!(self.state.get(), EVENT_SET | EVENT_DISCONNECTED)
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

        let previous_state = event.state.replace(EVENT_DISCONNECTED);

        match previous_state {
            EVENT_SET => {
                // SAFETY: EVENT_SET guarantees that the payload cell is initialized, and this
                // terminal transition gives the receiver exclusive extraction rights.
                let value_cell_maybe = unsafe { event.value.get().as_mut() };
                // SAFETY: UnsafeCell pointer is never null.
                let value_cell = unsafe { value_cell_maybe.unwrap_unchecked() };

                // SAFETY: The state guarantee above proves the cell is initialized.
                Ok(unsafe { value_cell.assume_init_read() })
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
    ///
    /// See [`set()`][Self::set] for why the event arrives as an `&UnsafeCell<Self>`; here it is
    /// the waker's destructor rather than `wake()` that may release the event storage.
    #[inline]
    pub(crate) fn cancel(event_cell: &UnsafeCell<Self>) -> Cancellation<T> {
        // SAFETY: We only ever create shared references to the event, so no aliasing conflicts.
        // The event lives until both sender and receiver are dropped or inert, so we know it must
        // still exist because something was able to call this method.
        let event = unsafe { event_cell.get().as_ref_unchecked() };

        // If we are still awaiting, the receiver owns the stored waker. Move it out before
        // disconnecting, but defer its destruction to the receiver core so no user code runs until
        // the state transition and any storage release have completed.
        //
        // This mirrors the sync `Event::cancel`, which also reverts AWAITING -> BOUND before
        // extracting the awaiter. Ref: docs/callback-safety.md.
        let awaiter = if event.state.get() == EVENT_AWAITING {
            event.state.set(EVENT_BOUND);

            // SAFETY: The only other potential references to the field are other short-lived
            // references in this type, which cannot exist at the moment because
            // the type is single-threaded and does not let any references escape.
            let awaiter_cell_maybe = unsafe { event.awaiter.get().as_mut() };
            // SAFETY: UnsafeCell pointer is never null.
            let awaiter_cell = unsafe { awaiter_cell_maybe.unwrap_unchecked() };

            // We extract the waker and consider the field uninitialized again.
            // SAFETY: We were in EVENT_AWAITING which guarantees there is a waker in there.
            Some(unsafe { awaiter_cell.assume_init_read() })
        } else {
            None
        };

        let previous_state = event.state.get();

        // We can immediately set this because this is a single-threaded event, so there cannot
        // be any race condition causing us issues with the receiver seeing this too early.
        event.state.set(EVENT_DISCONNECTED);

        let result = match previous_state {
            EVENT_BOUND => {
                // The sender had not yet set any value. It will clean up the event later.
                Ok(None)
            }
            EVENT_SET => {
                // The sender has already set a value but we disconnected before we received it.
                // We need to clean up the value and then later clean up the event, as well.

                // SAFETY: The only other potential references to the field are other short-lived
                // references in this type, which cannot exist at the moment because
                // the type is single-threaded and does not let any references escape.
                let value_cell_maybe = unsafe { event.value.get().as_mut() };
                // SAFETY: UnsafeCell pointer is never null.
                let value_cell = unsafe { value_cell_maybe.unwrap_unchecked() };

                // We extract the value and consider the cell uninitialized.
                //
                // SAFETY: We were in EVENT_SET which guarantees there is a value in there.
                let value = unsafe { value_cell.assume_init_read() };

                // The receiver will clean up.
                Ok(Some(value))
            }
            EVENT_DISCONNECTED => {
                // The receiver is the last endpoint remaining, so it will clean up.
                Err(Disconnected)
            }
            // Defensive: state machine guarantees this is unreachable, since we already handled
            // and cleared EVENT_AWAITING above.
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
}

#[cfg_attr(coverage_nightly, coverage(off))] // No API contract to test.
impl<T: 'static> fmt::Debug for LocalEvent<T> {
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
