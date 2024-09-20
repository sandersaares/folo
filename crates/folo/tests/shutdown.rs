use folo::rt::{RemoteJoinHandle, RuntimeBuilder};
use folo_testing::init_test_worker;
use futures::{task::noop_waker, FutureExt};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{self, Waker},
    thread,
    time::Duration,
};

#[folo::test(worker_init_fn = init_test_worker)]
async fn runtime_stops_with_deadlocked_tasks() {
    // It is possible to create a situation where two tasks are waiting for each other to complete.
    // This is a deadlock and while we cannot do much to help the user if they create such a
    // situation, the runtime must still be able to shut down and do so fast and clean.
    // Note: if there is a defect, this test may time out.

    // We give task 1 a join handle for task 2, and vice versa.
    let (init_tx, init_rx) = oneshot::channel::<RemoteJoinHandle<()>>();

    // We wait for the deadlock to be set up nice and proper.
    let (tx, rx) = oneshot::channel();

    let task1 = folo::rt::spawn_on_any(move || async {
        let task2 = init_rx.await.unwrap();
        _ = tx.send(());
        task2.await;
    });
    let task2 = folo::rt::spawn_on_any(move || async {
        task1.await;
    });

    init_tx.send(task2).unwrap();

    // The task must receive the start signal or something else went wrong.
    rx.await.unwrap();

    // Just in case it takes it a few moments to achieve deadlock. Extra precaution to avoid
    // making the test too easy.
    thread::sleep(Duration::from_millis(10));
}

#[folo::test(worker_init_fn = init_test_worker)]
async fn runtime_stops_with_infinitely_sleeping_tasks() {
    // What if a future never completes? It is fine if it just sits around in the inactive queue
    // (after all, that is what the caller ordered) but the runtime must still be able to shut down
    // and do so fast and clean.
    // Note: if there is a defect, this test may time out.

    // We need to wait a bit to ensure that a worker picks up the task, otherwise the test would
    // be too easy because maybe the task did not even start yet when we try to shut down.
    let (tx, rx) = oneshot::channel();

    let join_task = folo::rt::spawn_on_any(move || ForeverSleepFuture::new(tx));

    // We create a dependent task to add extra complexity in the form of cross-task relationships.
    // This utilizes the waker of the original task, exploring additional potential for defects.
    _ = folo::rt::spawn_on_any(move || join_task);

    // We must receive the start signal or something else went wrong.
    rx.await.unwrap();
}

struct ForeverSleepFuture {
    // Cleared after the future is polled once.
    start_tx: Option<oneshot::Sender<()>>,
}

impl ForeverSleepFuture {
    fn new(start_tx: oneshot::Sender<()>) -> Self {
        Self {
            start_tx: Some(start_tx),
        }
    }
}

impl Future for ForeverSleepFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        if let Some(start_tx) = self.start_tx.take() {
            _ = start_tx.send(());
        }

        task::Poll::Pending
    }
}

#[folo::test(worker_init_fn = init_test_worker)]
async fn runtime_stops_with_infinitely_active_tasks() {
    // What if a future never completes? It is fine if it just sits around in the inactive queue
    // (after all, that is what the caller ordered) but the runtime must still be able to shut down
    // and do so fast and clean.
    // Note: if there is a defect, this test may time out.

    // We need to wait a bit to ensure that a worker picks up the task, otherwise the test would
    // be too easy because maybe the task did not even start yet when we try to shut down.
    let (tx, rx) = oneshot::channel();

    // We grab a join handle here to add complexity. We do not await it, as it would hang forever.
    let join_task = folo::rt::spawn_on_any(move || yield_loop(tx));

    // We create a dependent task to add extra complexity in the form of cross-task relationships.
    // This utilizes the waker of the original task, exploring additional potential for defects.
    _ = folo::rt::spawn_on_any(move || join_task);

    // We must receive the start signal or something else went wrong.
    rx.await.unwrap();
}

async fn yield_loop(started_tx: oneshot::Sender<()>) {
    _ = started_tx.send(());

    loop {
        folo::rt::yield_now().await;
    }
}

#[test]
fn runtime_stops_with_external_observer() {
    // The universe in the app is larger than a Folo runtime - results of its work may be needed
    // by other entities! For example, other threads or even different runtimes (whether Folo or
    // something entirely different like Tokio). This is very common for interop scenarios, as well.

    // We manually create the runtime here because we need to also operate outside of it.
    let folo = RuntimeBuilder::new()
        .worker_init(folo_testing::init_test_worker)
        .build().unwrap();
    let folo_clone = folo.clone();

    let (tx, rx) = oneshot::channel();

    let observer_thread = thread::spawn(move || {
        // First, wait for the signal to arrive that the infinite task has started.
        let mut task: RemoteJoinHandle<()> = rx.recv().unwrap();

        // Tell the runtime to stop and wait for this to happen.
        folo_clone.stop();
        folo_clone.wait();

        let waker = noop_waker();

        // We keep polling the task for a little bit. We do not expect it to complete
        // but the main thing is that it must not crash/panic or otherwise corrupt the universe.
        for _ in 0..20 {
            thread::sleep(Duration::from_millis(10));
            let mut cx = task::Context::from_waker(&waker);
            let poll_result = task.poll_unpin(&mut cx);
            assert_eq!(poll_result, task::Poll::Pending);
        }
    });

    let (started_tx, started_rx) = oneshot::channel();

    let task = folo.spawn_on_any(|| yield_loop(started_tx));

    // Wait for the start signal before we give the task to the observer, to ensure it is running.
    // This prevents "stop before task even starts" which might make it too easy to pass the test.
    started_rx.recv().unwrap();
    tx.send(task).unwrap();

    observer_thread.join().unwrap();
}

#[test]
fn runtime_stops_with_external_dependency() {
    // The universe in the app is larger than a Folo runtime - it may depends on the work of other
    // entities! For example, it may have given a waker of itself to some other entity, and that
    // entity might still be using our waker after the Folo runtime shuts down. This is very common
    // in interop scenarios.

    // We manually create the runtime here because we need to also operate outside of it.
    let folo = RuntimeBuilder::new()
        .worker_init(folo_testing::init_test_worker)
        .build().unwrap();

    let nexus = Arc::new(ManualFuture::new());
    let nexus_folo = Arc::clone(&nexus);

    // This will give its waker to the nexus task once it starts.
    folo.spawn_on_any(move || async move { nexus_folo.as_ref().await });

    // Give it a moment to set the waker.
    for _ in 0..100 {
        if nexus.has_waker() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }

    assert!(nexus.has_waker());

    folo.stop();

    // The Folo runtime will keep running in shutdown mode until we call `nexus.complete()` because
    // the nexus task is holding the last waker. Give it a moment to make any mistakes and corrupt
    // any state before we allow it to complete.

    thread::sleep(Duration::from_millis(10));

    assert!(!folo.is_stopped());

    // Allow it to complete the shutdown.
    nexus.complete();

    folo.wait();
}

/// A future that progresses or completes (and wakes up the last poller) when manually commanded.
struct ManualFuture {
    state: Mutex<ManualFutureState>,
}

struct ManualFutureState {
    waker: Option<Waker>,
    is_completed: bool,
}

impl ManualFuture {
    fn new() -> Self {
        Self {
            state: Mutex::new(ManualFutureState {
                waker: None,
                is_completed: false,
            }),
        }
    }

    fn has_waker(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.waker.is_some()
    }

    fn complete(&self) {
        let mut state = self.state.lock().unwrap();

        state.is_completed = true;

        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }
}

impl Future for &ManualFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let mut state = self.state.lock().unwrap();

        if state.is_completed {
            task::Poll::Ready(())
        } else {
            state.waker = Some(cx.waker().clone());
            task::Poll::Pending
        }
    }
}
