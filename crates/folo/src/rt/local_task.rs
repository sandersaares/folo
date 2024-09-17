use crate::{
    rt::erased_async_task::ErasedResultAsyncTask,
    rt::LocalJoinHandle,
    sync::once_event::{self, OnceEvent, OnceEventEmbeddedStorage},
};
use negative_impl::negative_impl;
use pin_project::pin_project;
use std::{cell::RefCell, future::Future, pin::Pin, task};

/// This is the core essence of a task, relating a future to some result where everything up to and
/// including consuming the result takes place on a single thread.
///
/// The task is created as soon as its scheduling is requested. After initialization, details of the
/// task type are erased and it is exposed only as a `dyn Future<Output = ()>` used to progress the
/// task, pinned as soon as it reaches the async task engine of the worker selected to execute it.
///
/// Compare with `RemoteTask` which is the multithreaded variant of this.
#[pin_project]
#[derive(Debug)]
pub(crate) struct LocalTask<F, R>
where
    F: Future<Output = R> + 'static,
    R: 'static,
{
    // We drop this on `ErasedResultAsyncTask::clear()` to ensure that any captured state in the
    // future is dropped, releasing critical references that may be blocking runtime shutdown.
    future: RefCell<Option<F>>,

    // Value is consumed after the result is set or when the task itself is dropped.
    result_tx: Option<once_event::EmbeddedSender<R>>,

    // Cleared after the join handle has been acquired for the first time.
    // There can only be one join handle for one task.
    result_rx: Option<once_event::EmbeddedReceiver<R>>,

    /// This is the backing storage used by result_tx and result_rx. The owner of the LocalTask must
    /// ensure that this storage is not dropped while any references still exist.
    ///
    /// NB! This is declared below the result_rx and result_tx to ensure that it gets dropped after
    /// those, in case we are still holding on to the tx/rx when the task is dropped.
    #[pin]
    result: OnceEventEmbeddedStorage<R>,
}

impl<F, R> LocalTask<F, R>
where
    F: Future<Output = R> + 'static,
    R: 'static,
{
    /// # Safety
    ///
    /// The caller is responsible for not dropping the LocalTask as long as there may be someone
    /// awaiting its result. You can verify this by calling `.is_inert()` - dropping is safe only
    /// when this is true.
    pub unsafe fn new(future: F) -> Pin<Box<Self>> {
        // A LocalTask is always pinned, as this is required by the OnceEvent embedded into it.

        // We initialize in two steps, initializing the OnceEvent after we are pinned.
        let mut instance = Box::pin(LocalTask {
            future: RefCell::new(Some(future)),
            result_tx: None,
            result_rx: None,
            result: OnceEvent::new_embedded_storage_single(),
        });

        let (tx, rx) = {
            let instance = instance.as_ref();
            OnceEvent::new_embedded(instance.project_ref().result)
        };

        {
            let instance = instance.as_mut().project();

            *instance.result_tx = Some(tx);
            *instance.result_rx = Some(rx);
        }

        instance
    }

    pub fn join_handle(self: Pin<&mut Self>) -> LocalJoinHandle<R> {
        LocalJoinHandle::new(
            self.project()
                .result_rx
                .take()
                .expect("join handle for task can only be acquired once"),
        )
    }

    pub fn is_inert(&self) -> bool {
        // We are inert if the only references to the OnceEvent are those we hold ourselves.
        let mut self_references = 0;

        if self.result_tx.is_some() {
            self_references += 1;
        }

        if self.result_rx.is_some() {
            self_references += 1;
        }

        let ref_count = self.result.ref_count();

        assert!(
            ref_count >= self_references,
            "ref count should never be less than the number of references we hold"
        );

        ref_count == self_references
    }
}

impl<F, R> Future for LocalTask<F, R>
where
    F: Future<Output = R> + 'static,
    R: 'static,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let poll_result = {
            let self_as_mut = self.as_mut();
            let mut borrowed_future = self_as_mut.future.borrow_mut();
            let future = borrowed_future
                .as_mut()
                .expect("a task is never going to be polled after its future is removed");

            // SAFETY: It is actually pinned, the RefCell layer just makes it hard to preserve the
            // annotation, so we add it back manually.
            let future = unsafe { Pin::new_unchecked(future) };
            future.poll(cx)
        };

        match poll_result {
            task::Poll::Ready(result) => {
                let tx = self
                    .as_mut()
                    .project()
                    .result_tx
                    .take()
                    .expect("result tx can only be sent once");

                tx.set(result);
                task::Poll::Ready(())
            }
            task::Poll::Pending => task::Poll::Pending,
        }
    }
}

impl<F, R> ErasedResultAsyncTask for LocalTask<F, R>
where
    F: Future<Output = R> + 'static,
    R: 'static,
{
    fn is_inert(&self) -> bool {
        LocalTask::is_inert(self)
    }

    fn clear(&self) {
        *self.future.borrow_mut() = None;
    }
}

// Perhaps already implied but let's be super explicit here.
#[negative_impl]
impl<F, R> !Send for LocalTask<F, R>
where
    F: Future<Output = R> + 'static,
    R: 'static,
{
}
#[negative_impl]
impl<F, R> !Sync for LocalTask<F, R>
where
    F: Future<Output = R> + 'static,
    R: 'static,
{
}
