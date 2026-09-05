use super::*;
/// Client behavior that rejects a provisional startup transaction.
enum RejectedStartup {
    Disconnect,
    Message(Message),
    Timeout,
}

fn assert_rejected_startup_rolls_back(rejection: RejectedStartup) {
    with_watchdog_phases("setting up the supervisor", |phase_reporter| {
        let transport = MemoryTransport::new();
        let pty = MemoryPseudoconsole::new();
        let store = MemorySessionStore::new();
        let exit = Arc::new((Mutex::new(true), Condvar::new()));
        let processes = mock_processes(exit);

        let startup = transport.listen("startup").unwrap();
        let supervisor = thread::spawn({
            let transport = transport.clone();
            let store = store.clone();
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

        phase_reporter.report("waiting for the supervisor startup connection");
        let startup_conn = transport.accept(startup).unwrap();
        phase_reporter.report("waiting for the supervisor startup response");
        assert!(matches!(
            transport.recv(startup_conn).unwrap(),
            Message::StartupOk { .. }
        ));
        let records = store.list().unwrap();
        let [record] = records.as_slice() else {
            panic!("expected exactly one record");
        };
        let session_pipe = record.pipe_name.clone();
        match rejection {
            RejectedStartup::Disconnect => transport.disconnect(startup_conn),
            RejectedStartup::Message(acknowledgement) => {
                transport.send(startup_conn, &acknowledgement).unwrap();
            }
            RejectedStartup::Timeout => transport.expire_next_recv("startup"),
        }

        phase_reporter.report("waiting for startup rollback");
        let error = supervisor.join().unwrap().unwrap_err();
        assert!(error.find_source::<StartupFailedError>().is_some());
        assert!(store.list().unwrap().is_empty());
        transport
            .connect(&session_pipe, CONNECT_TIMEOUT)
            .unwrap_err();
    });
}

#[test]
fn a_session_without_startup_commit_is_rolled_back() {
    assert_rejected_startup_rolls_back(RejectedStartup::Disconnect);
}

#[test]
fn a_session_with_an_invalid_startup_commit_is_rolled_back() {
    assert_rejected_startup_rolls_back(RejectedStartup::Message(Message::StartupErr));
}

#[test]
fn a_startup_commit_timeout_is_rolled_back() {
    assert_rejected_startup_rolls_back(RejectedStartup::Timeout);
}

#[test]
fn failure_to_send_startup_ok_rolls_back() {
    with_watchdog_phases("running the rejected startup", |_phase_reporter| {
        let transport = MemoryTransport::new();
        transport.fail_next_send("startup");
        let pty = MemoryPseudoconsole::new();
        let store = MemorySessionStore::new();
        let exit = Arc::new((Mutex::new(true), Condvar::new()));
        let processes = mock_processes(exit);

        let _startup = transport.listen("startup").unwrap();
        let error = run_supervisor(
            &processes,
            &store,
            &transport,
            &pty,
            "startup",
            sample_spec(),
        )
        .unwrap_err();

        assert!(error.find_source::<StartupFailedError>().is_some());
        assert!(store.list().unwrap().is_empty());
    });
}

#[test]
fn init_failure_sends_startup_err_and_closes_job() {
    with_watchdog_phases("setting up the supervisor", |phase_reporter| {
        let transport = MemoryTransport::new();
        let pty = MemoryPseudoconsole::new();
        let store = MemorySessionStore::new();
        let mut processes = MockProcesses::new();
        processes
            .expect_launcher_tie()
            .returning(|| LauncherTie::NoneDetected);
        processes
            .expect_create_lifetime_job()
            .returning(|| Ok(JobId::for_test(1)));
        // Spawn fails before initialization constructs the session record,
        // so this path never reads the system clock.
        processes
            .expect_spawn_app()
            .returning(|_| Err(PalError::new(PalErrorKind::Other)));
        processes.expect_close_job().times(1).returning(|_| ());

        let startup = transport.listen("startup").unwrap();
        let supervisor = thread::spawn({
            let transport = transport.clone();
            let store = store.clone();
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

        phase_reporter.report("waiting for the supervisor startup connection");
        let startup_conn = transport.accept(startup).unwrap();
        phase_reporter.report("waiting for the startup error");
        assert_eq!(transport.recv(startup_conn).unwrap(), Message::StartupErr);
        phase_reporter.report("waiting for startup rollback");
        supervisor.join().unwrap().unwrap_err();
        assert!(store.list().unwrap().is_empty());
    });
}

#[test]
fn a_supervisor_that_cannot_outlive_its_launcher_says_so_on_the_startup_pipe() {
    with_watchdog_phases("setting up the supervisor", |phase_reporter| {
        let exit = Arc::new((Mutex::new(false), Condvar::new()));
        let transport = MemoryTransport::new();
        let pty = MemoryPseudoconsole::new();
        let store = MemorySessionStore::new();
        let processes =
            mock_processes_with(Arc::clone(&exit), LauncherTie::Confirmed, AppWait::Reports);

        let startup = transport.listen("startup").unwrap();
        let supervisor = thread::spawn({
            let transport = transport.clone();
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

        // Only the client has a console to report this on.
        let (startup_conn, _session_id, launcher_tie) =
            commit_startup(&transport, startup, &phase_reporter);
        assert_eq!(launcher_tie, LauncherTie::Confirmed);
        transport.disconnect(startup_conn);
        {
            let (lock, cvar) = &*exit;
            *lock.lock().expect("exit lock") = true;
            cvar.notify_all();
        }
        phase_reporter.report("waiting for supervisor shutdown");
        supervisor.join().unwrap().unwrap();
    });
}

#[test]
fn a_wait_that_fails_still_takes_the_session_off_the_host() {
    with_watchdog_phases("setting up the supervisor", |phase_reporter| {
        let exit = Arc::new((Mutex::new(false), Condvar::new()));
        let transport = MemoryTransport::new();
        let pty = MemoryPseudoconsole::new();
        let store = MemorySessionStore::new();
        let processes =
            mock_processes_with(Arc::clone(&exit), LauncherTie::NoneDetected, AppWait::Fails);

        let startup = transport.listen("startup").unwrap();
        let supervisor = thread::spawn({
            let transport = transport.clone();
            let store = store.clone();
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

        let (startup_conn, _session_id, _durability) =
            commit_startup(&transport, startup, &phase_reporter);
        assert_eq!(store.list().unwrap().len(), 1, "the session was published");
        transport.disconnect(startup_conn);
        {
            let (lock, cvar) = &*exit;
            *lock.lock().expect("exit lock") = true;
            cvar.notify_all();
        }

        phase_reporter.report("waiting for failed-wait cleanup");
        supervisor.join().unwrap().unwrap_err();
        // The wait is the only thing that failed, so everything the session
        // put on the host is still the session's to take back.
        assert!(
            store.list().unwrap().is_empty(),
            "the record outlived the session"
        );
    });
}

#[test]
fn breakaway_denial_keeps_its_identity() {
    let denied = map_startup(&PalError::new(PalErrorKind::BreakawayDenied));
    assert!(denied.find_source::<BreakawayDeniedError>().is_some());
    let other = map_startup(&PalError::new(PalErrorKind::Other));
    assert!(other.find_source::<StartupFailedError>().is_some());
}

#[test]
fn a_failure_after_id_allocation_releases_the_id() {
    let store = MemorySessionStore::new();
    store.fail_next_publish();
    let transport = MemoryTransport::new();
    let pty = MemoryPseudoconsole::new();
    let exit = Arc::new((Mutex::new(true), Condvar::new()));
    let processes = mock_processes(exit);

    {
        let mut guard = InitGuard {
            processes: &processes,
            store: &store,
            transport: &transport,
            pty_host: &pty,
            job: None,
            pty: None,
            listener: None,
            session: None,
            committed: false,
        };
        let error = initialize(
            &mut guard,
            &processes,
            &store,
            &transport,
            &pty,
            sample_spec(),
        )
        .err()
        .expect("record publication fails");
        assert!(error.find_source::<StoreError>().is_some());
    }

    assert!(store.list_reservations().unwrap().is_empty());
}
