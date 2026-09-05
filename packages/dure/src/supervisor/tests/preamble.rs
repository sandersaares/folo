use super::*;
#[test]
fn output_produced_before_the_first_attach_reaches_that_client() {
    with_watchdog_phases("setting up the supervisor", |phase_reporter| {
        let transport = MemoryTransport::new();
        let pty = MemoryPseudoconsole::new();
        let store = MemorySessionStore::new();
        let exit = Arc::new((Mutex::new(false), Condvar::new()));
        let processes = mock_processes(Arc::clone(&exit));

        let startup = transport.listen("startup").unwrap();
        let supervisor = thread::spawn({
            let transport = transport.clone();
            let pty = pty.clone();
            move || {
                run_supervisor(
                    &processes,
                    &store,
                    &transport,
                    &pty,
                    "startup",
                    sample_spec(),
                )
            }
        });

        let started = commit_startup(&transport, startup, &phase_reporter);

        // The app speaks before anyone has attached, which is the window
        // `dure run` spends spawning the supervisor and connecting to it.
        pty.push_output(pty.only_pty(), b"hello");
        transport.disconnect(started.startup_conn);

        let client = transport
            .connect(&started.pipe_name, CONNECT_TIMEOUT)
            .unwrap();
        transport.send(client, &ORDINARY_ATTACH).unwrap();
        phase_reporter.report("waiting for the attach acknowledgement");
        assert!(matches!(
            transport.recv(client).unwrap(),
            Message::Attached { .. }
        ));

        {
            let (lock, cvar) = &*exit;
            *lock.lock().expect("exit lock") = true;
            cvar.notify_all();
        }

        // Reading until the app's exit status rather than expecting the held
        // bytes as the very next message keeps this test terminating: a
        // supervisor that never delivers them still reaches `AppExited`, so
        // the failure is an assertion rather than a wait with no end.
        let mut received = Vec::new();
        loop {
            phase_reporter.report("waiting for held output or the app exit status");
            match transport.recv(client).unwrap() {
                Message::Output(bytes) => received.extend(bytes),
                Message::AppExited { status } => {
                    assert_eq!(status, SAMPLE_APP_EXIT);
                    break;
                }
                other => panic!("unexpected message {other:?}"),
            }
        }
        assert_eq!(received, b"hello");

        phase_reporter.report("waiting for supervisor shutdown");
        assert_eq!(supervisor.join().unwrap().unwrap(), SAMPLE_APP_EXIT);
    });
}

#[test]
fn only_the_first_client_receives_what_was_held_for_it() {
    let transport = MemoryTransport::new();
    let pty = MemoryPseudoconsole::new();
    let shared = shared_session(&transport, &pty);
    shared.hold_for_first_client(b"hello");
    assert_eq!(shared.take_preamble(), Some(b"hello".to_vec()));
    // A client that attaches later starts on an empty screen, so output
    // produced while nobody was attached is not kept for it.
    shared.hold_for_first_client(b"unheard");
    assert_eq!(shared.take_preamble(), None);
}

#[test]
fn nothing_held_is_nothing_to_deliver() {
    let transport = MemoryTransport::new();
    let pty = MemoryPseudoconsole::new();
    let shared = shared_session(&transport, &pty);
    assert_eq!(shared.take_preamble(), None);
}

#[test]
// Moves megabytes through pure byte handling, so Miri has no unsafe code to
// check here and its interpreter makes the volume impractically slow.
#[cfg_attr(miri, ignore)]
fn what_is_held_for_the_first_client_is_capped() {
    let transport = MemoryTransport::new();
    let pty = MemoryPseudoconsole::new();
    let shared = shared_session(&transport, &pty);
    let chunk = vec![b'x'; 64 * 1024];
    let rounds = MAX_CLIENT_BACKLOG_BYTES.div_euclid(chunk.len()) + 2;
    for _ in 0..rounds {
        shared.hold_for_first_client(&chunk);
    }
    assert_eq!(
        shared.take_preamble().map(|held| held.len()),
        Some(MAX_CLIENT_BACKLOG_BYTES)
    );
}

#[test]
// Moves megabytes through pure byte handling, so Miri has no unsafe code to
// check here and its interpreter makes the volume impractically slow.
#[cfg_attr(miri, ignore)]
fn a_preamble_too_large_for_one_frame_is_split_into_frames_a_receiver_accepts() {
    // A hold the transport could not carry in one frame, which is reachable
    // because the hold cap is several frames' worth.
    let held = vec![b'x'; MAX_OUTPUT_CHUNK_BYTES.get().saturating_add(1)];
    const {
        assert!(
            MAX_CLIENT_BACKLOG_BYTES > MAX_OUTPUT_CHUNK_BYTES.get(),
            "a hold that cannot outgrow one frame would make this test vacuous"
        );
    }

    let messages: Vec<Message> = preamble_messages(&held).collect();

    assert!(messages.len() > 1, "the hold was not split");
    let mut rejoined = Vec::new();
    for message in &messages {
        let frame = encode(message);
        let prefix: [u8; 4] = frame
            .get(..4)
            .expect("a frame carries a length prefix")
            .try_into()
            .unwrap();
        assert!(
            payload_len_ok(u32::from_le_bytes(prefix)),
            "a receiver would reject this frame"
        );
        match message {
            Message::Output(bytes) => rejoined.extend_from_slice(bytes),
            other => panic!("the hold must be relayed as output, got {other:?}"),
        }
    }
    // Splitting must not lose or reorder what the app said.
    assert_eq!(rejoined, held);
}
