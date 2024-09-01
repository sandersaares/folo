use folo::RemoteJoinHandle;

#[test]
fn remote_join() {
    // The entrypoint macro itself relies on joins, so we test things "raw" here.
    let folo = folo::ExecutorBuilder::new().build().unwrap();
    let folo_clone = folo.clone();

    folo.spawn_on_any(async move {
        let handle = folo::spawn_on_any(some_logic());
        handle.await;

        folo_clone.stop();
    });

    folo.wait();
}

#[test]
fn local_join() {
    // The entrypoint macro itself relies on joins, so we test things "raw" here.
    let folo = folo::ExecutorBuilder::new().build().unwrap();
    let folo_clone = folo.clone();

    folo.spawn_on_any(async move {
        // `spawn_on_any` cannot accept anything that is not thread-safe in its state machine
        // so we need to wrap the local join handle in a remote join handle.
        let wrapper: RemoteJoinHandle<_> = folo::spawn(async move {
            // We have a non-wrapped local join handle as 2nd layer, to test without wrapping, too.
            folo::spawn(some_logic()).await;
        })
        .into();
    
        wrapper.await;

        folo_clone.stop();
    });

    folo.wait();
}

async fn some_logic() -> usize {
    folo::yield_now().await;
    42
}
