use folo::rt::{spawn, spawn_on_any, yield_now, RuntimeBuilder};
use std::rc::Rc;

#[test]
fn spawning() {
    // We use a manually started runtime here to avoid any potential
    // interference from the logic in the entrypoint macro.
    let folo = RuntimeBuilder::new().build().unwrap();
    let folo_clone = folo.clone();

    folo.spawn_on_any(|| async move {
        // These take a future directly.
        spawn(async { thread_safe_logic().await }).await.unwrap();
        spawn(async { single_threaded_logic().await })
            .await
            .unwrap();

        // These take a future-returning closure, so the call style is a bit different.
        spawn_on_any(thread_safe_logic).await.unwrap();
        spawn_on_any(single_threaded_logic).await.unwrap();

        folo_clone.stop();
    });

    folo.wait();
}

async fn thread_safe_logic() -> Option<()> {
    yield_now().await;
    Some(())
}

async fn single_threaded_logic() -> Option<()> {
    let rc = Rc::new(42);
    yield_now().await;
    assert_eq!(42, *rc);
    Some(())
}
