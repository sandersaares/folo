use super::*;

#[test]
fn a_client_that_leaves_before_it_is_acknowledged_leaves_the_slot_empty() {
    let transport = MemoryTransport::new();
    let pty_host = MemoryPseudoconsole::new();
    let shared = shared_session(&transport, &pty_host);
    let (supervisor, client) = connected_pair(&transport, "pipe");

    transport.send(client, &ORDINARY_ATTACH).unwrap();
    transport.disconnect(client);

    let (flags, recorder) = attach_recorder();
    client_loop(&shared, supervisor, &recorder);

    // The session is never left believing a client that has gone still owns
    // it, however far into the attach that client got.
    assert!(client_conn(&shared).is_none());
    assert_eq!(
        flags.lock().unwrap().last().copied(),
        Some(false),
        "the last thing published about a departed client must be that it left"
    );
}

#[test]
fn a_stalled_attach_acknowledgement_does_not_hold_up_the_attach() {
    with_watchdog_phases("setting up the client relay", |phase_reporter| {
        let transport = MemoryTransport::new();
        let pty_host = MemoryPseudoconsole::new();
        let shared = Arc::new(shared_session(&transport, &pty_host));
        let (supervisor, client) = connected_pair(&transport, "pipe");

        let (attached_tx, attached_rx) = mpsc::channel();
        transport.send(client, &ORDINARY_ATTACH).unwrap();
        transport.stall(supervisor);
        let relay = thread::spawn({
            let shared = Arc::clone(&shared);
            move || {
                client_loop(&shared, supervisor, &|_generation, attached| {
                    attached_tx.send(attached).unwrap();
                });
            }
        });

        phase_reporter.report("waiting for the attach acknowledgement to stall");
        transport.wait_for_stalled_send(supervisor);
        // The acknowledgement is queued, so a client that has stopped
        // reading holds up only its own delivery: ownership has already
        // transferred and the session is claimed.
        phase_reporter.report("waiting for the attached-flag update");
        assert!(attached_rx.recv().unwrap());
        assert!(shared.first_attach().claimed);

        transport.resume(supervisor);
        phase_reporter.report("waiting for the attach acknowledgement");
        assert!(matches!(
            transport.recv(client).unwrap(),
            Message::Attached { .. }
        ));

        transport.send(client, &Message::StartupErr).unwrap();
        phase_reporter.report("waiting for the client relay to stop");
        relay.join().unwrap();
        assert!(!attached_rx.recv().unwrap());
    });
}

#[test]
fn a_stalled_detach_update_does_not_block_or_overwrite_a_steal() {
    with_watchdog_phases("setting up the client relays", |phase_reporter| {
        let transport = MemoryTransport::new();
        let pty_host = MemoryPseudoconsole::new();
        let shared = Arc::new(shared_session(&transport, &pty_host));
        let store = MemorySessionStore::new();
        let owner = ProcessIdentity::for_test(1);
        let id = store.allocate_id(&owner).unwrap();
        assert_eq!(id, shared.session_id);
        store
            .publish(&SessionRecord {
                id,
                supervisor: owner,
                pipe_name: "pipe".to_string(),
                launch_directory: PathBuf::from("/work"),
                command: AppCommand::for_test(&["app.exe"]),
                started_at_unix_ms: 1,
                attached: false,
                protocol_version: PROTOCOL_VERSION,
            })
            .unwrap();
        let record_live = Arc::new(Mutex::new(true));
        let set_attached = store_attached_flag(
            &store,
            id,
            record_live,
            Arc::clone(&shared.attached_generation),
        );
        let (updated_tx, updated_rx) = mpsc::channel();
        let observe_update = move |generation, attached| {
            set_attached(generation, attached);
            updated_tx.send(attached).unwrap();
        };

        let (first_supervisor, first_client) = connected_pair(&transport, "first");
        let first_relay = thread::spawn({
            let shared = Arc::clone(&shared);
            let observe_update = observe_update.clone();
            move || client_loop(&shared, first_supervisor, &observe_update)
        });
        transport.send(first_client, &ORDINARY_ATTACH).unwrap();
        phase_reporter.report("waiting for the first attach acknowledgement");
        assert!(matches!(
            transport.recv(first_client).unwrap(),
            Message::Attached { .. }
        ));
        phase_reporter.report("waiting for the first attached-flag update");
        assert!(updated_rx.recv().unwrap());

        store.stall_publishes();
        transport.disconnect(first_client);
        phase_reporter.report("waiting for the detached-flag update to stall");
        store.wait_for_stalled_publish();

        let (second_supervisor, second_client) = connected_pair(&transport, "second");
        let second_relay = thread::spawn({
            let shared = Arc::clone(&shared);
            move || client_loop(&shared, second_supervisor, &observe_update)
        });
        transport.send(second_client, &ORDINARY_ATTACH).unwrap();
        // The first relay is blocked in store I/O. Receiving this proves it
        // released the client slot before publishing the advisory flag.
        phase_reporter.report("waiting for the stealing attach acknowledgement");
        assert!(matches!(
            transport.recv(second_client).unwrap(),
            Message::Attached { .. }
        ));

        store.resume_publishes();
        phase_reporter.report("waiting for the stalled detach and newer attach updates");
        let completed = [updated_rx.recv().unwrap(), updated_rx.recv().unwrap()];
        assert!(completed.contains(&false));
        assert!(completed.contains(&true));
        first_relay.join().unwrap();
        assert!(store.read(id).unwrap().unwrap().attached);

        transport.disconnect(second_client);
        phase_reporter.report("waiting for the final detach update");
        assert!(!updated_rx.recv().unwrap());
        second_relay.join().unwrap();
        assert!(!store.read(id).unwrap().unwrap().attached);
    });
}

#[test]
fn an_attach_that_arrives_after_teardown_claims_the_slot_is_refused() {
    let transport = MemoryTransport::new();
    let pty_host = MemoryPseudoconsole::new();
    let shared = shared_session(&transport, &pty_host);
    let (supervisor, client) = connected_pair(&transport, "pipe");
    // Teardown has already routed the exit status to whoever owned the
    // slot, so there is nothing left for a new client to be given.
    shared.stopping.store(true, Ordering::SeqCst);

    transport.send(client, &ORDINARY_ATTACH).unwrap();

    let (flags, recorder) = attach_recorder();
    client_loop(&shared, supervisor, &recorder);

    // Refused before the acknowledgement, so the client sees a session that
    // is gone rather than one that broke mid-relay.
    assert!(client_conn(&shared).is_none());
    assert!(flags.lock().unwrap().is_empty());
    transport.recv(client).unwrap_err();
}

#[test]
fn a_client_that_does_not_attach_first_is_dropped() {
    let transport = MemoryTransport::new();
    let pty_host = MemoryPseudoconsole::new();
    let shared = shared_session(&transport, &pty_host);
    let (supervisor, client) = connected_pair(&transport, "pipe");

    transport
        .send(client, &Message::Input(b"x".to_vec()))
        .unwrap();

    let (flags, recorder) = attach_recorder();
    client_loop(&shared, supervisor, &recorder);

    assert!(client_conn(&shared).is_none());
    assert!(flags.lock().unwrap().is_empty());
    assert!(pty_host.take_input(shared.pty).is_empty());
    transport.recv(client).unwrap_err();
}

#[test]
fn the_relay_forwards_input_and_resize_until_the_client_stops() {
    let transport = MemoryTransport::new();
    let pty_host = MemoryPseudoconsole::new();
    let shared = shared_session(&transport, &pty_host);
    let (supervisor, client) = connected_pair(&transport, "pipe");

    transport.send(client, &ORDINARY_ATTACH).unwrap();
    transport
        .send(
            client,
            &Message::Resize {
                size: WindowSize::new(120, 40).expect("a fixture size is not empty"),
            },
        )
        .unwrap();
    transport
        .send(client, &Message::Input(b"hi".to_vec()))
        .unwrap();
    // Anything the supervisor does not relay ends the client loop.
    transport.send(client, &Message::StartupErr).unwrap();

    let (flags, recorder) = attach_recorder();
    client_loop(&shared, supervisor, &recorder);

    assert_eq!(pty_host.size(shared.pty), WindowSize::new(120, 40));
    assert_eq!(pty_host.take_input(shared.pty), b"hi");
    assert!(client_conn(&shared).is_none());
    assert_eq!(*flags.lock().unwrap(), vec![true, false]);
}

#[test]
fn a_displaced_relay_leaves_the_new_client_installed() {
    let transport = MemoryTransport::new();
    let pty_host = MemoryPseudoconsole::new();
    let shared = Arc::new(shared_session(&transport, &pty_host));
    let (supervisor, client) = connected_pair(&transport, "pipe");
    let (successor, _successor_client) = connected_pair(&transport, "successor");
    let successor_outbox = Outbox::start(transport.clone(), successor);
    let displaced = Arc::new(Mutex::new(None));
    // Stands in for a steal that lands between the acknowledgement and the
    // first relayed message. The displaced relay must not reach the app.
    let steal = {
        let shared = Arc::clone(&shared);
        let successor_outbox = Arc::clone(&successor_outbox);
        let displaced = Arc::clone(&displaced);
        move |_generation: u64, attached: bool| {
            if attached {
                let previous = shared.client().replace(Client {
                    conn: successor,
                    outbox: Arc::clone(&successor_outbox),
                });
                // What the real steal does to the client it replaces.
                if let Some(previous) = previous {
                    previous.outbox.finish();
                    *displaced.lock().unwrap() = Some(previous.outbox);
                }
            }
        }
    };

    transport.send(client, &ORDINARY_ATTACH).unwrap();
    transport
        .send(client, &Message::Input(b"x".to_vec()))
        .unwrap();

    client_loop(&shared, supervisor, &steal);

    assert!(pty_host.take_input(shared.pty).is_empty());
    assert_eq!(client_conn(&shared), Some(successor));

    displaced
        .lock()
        .unwrap()
        .take()
        .expect("the steal displaced the first client")
        .wait_for_writer();
    successor_outbox.finish();
    successor_outbox.wait_for_writer();
}

#[test]
fn output_is_relayed_to_the_installed_client() {
    let transport = MemoryTransport::new();
    let pty_host = MemoryPseudoconsole::new();
    let shared = shared_session(&transport, &pty_host);
    let (supervisor, client) = connected_pair(&transport, "pipe");
    let outbox = Outbox::start(transport.clone(), supervisor);
    *shared.client() = Some(Client {
        conn: supervisor,
        outbox: Arc::clone(&outbox),
    });

    pty_host.push_output(shared.pty, b"out");
    // Ends the loop once the output has been drained.
    pty_host.finish(shared.pty);

    pty_output_loop(&shared);
    outbox.finish();
    outbox.wait_for_writer();

    assert!(matches!(
        transport.recv(client).unwrap(),
        Message::Output(bytes) if bytes == b"out",
    ));
}

#[test]
fn output_for_a_client_that_is_gone_is_discarded() {
    let transport = MemoryTransport::new();
    let pty_host = MemoryPseudoconsole::new();
    let shared = shared_session(&transport, &pty_host);
    let (supervisor, client) = connected_pair(&transport, "pipe");
    let outbox = Outbox::start(transport.clone(), supervisor);
    *shared.client() = Some(Client {
        conn: supervisor,
        outbox: Arc::clone(&outbox),
    });
    transport.disconnect(client);

    pty_host.push_output(shared.pty, b"out");
    // Ends the loop once the undeliverable output has been drained.
    pty_host.finish(shared.pty);

    // The pump must not be the thread that notices, so it completes even
    // though nothing can be delivered.
    pty_output_loop(&shared);
    outbox.wait_for_writer();
}

#[test]
fn attached_flag_publishes_only_the_current_generation_while_the_record_lives() {
    let store = MemorySessionStore::new();
    let id = store.allocate_id(&ProcessIdentity::for_test(1)).unwrap();
    store
        .publish(&SessionRecord {
            id,
            supervisor: ProcessIdentity {
                pid: 10,
                creation_time: 100,
            },
            pipe_name: "pipe".to_string(),
            launch_directory: PathBuf::from("/work"),
            command: AppCommand::for_test(&["app.exe"]),
            started_at_unix_ms: 1,
            attached: false,
            protocol_version: PROTOCOL_VERSION,
        })
        .unwrap();

    let record_live = Arc::new(Mutex::new(true));
    let current_generation = Arc::new(AtomicU64::new(2));
    let set_attached = store_attached_flag(
        &store,
        id,
        Arc::clone(&record_live),
        Arc::clone(&current_generation),
    );
    set_attached(1, true);
    assert!(!store.read(id).unwrap().unwrap().attached);

    set_attached(2, true);
    assert!(store.read(id).unwrap().unwrap().attached);

    *record_live.lock().expect("record_live lock") = false;
    current_generation.store(3, Ordering::SeqCst);
    set_attached(3, false);
    assert!(store.read(id).unwrap().unwrap().attached);
}
