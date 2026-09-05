use super::*;
#[test]
fn steal_displaces_first_client() {
    with_watchdog_phases("setting up the supervisor", |phase_reporter| {
        let transport = MemoryTransport::new();
        let pty = MemoryPseudoconsole::new();
        let store = MemorySessionStore::new();
        let exit = Arc::new((Mutex::new(false), Condvar::new()));
        let processes = mock_processes(Arc::clone(&exit));

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

        let started = commit_startup(&transport, startup, &phase_reporter);
        transport.disconnect(started.startup_conn);

        let pipe = started.pipe_name;
        let first = transport.connect(&pipe, CONNECT_TIMEOUT).unwrap();
        transport.send(first, &ORDINARY_ATTACH).unwrap();
        phase_reporter.report("waiting for the first attach acknowledgement");
        assert!(matches!(
            transport.recv(first).unwrap(),
            Message::Attached { session_id: id } if id == started.session_id
        ));

        let second = transport.connect(&pipe, CONNECT_TIMEOUT).unwrap();
        transport.send(second, &ORDINARY_ATTACH).unwrap();
        phase_reporter.report("waiting for the second attach acknowledgement");
        assert!(matches!(
            transport.recv(second).unwrap(),
            Message::Attached { .. }
        ));
        phase_reporter.report("waiting for the displacement notice");
        assert!(matches!(transport.recv(first).unwrap(), Message::Displaced));

        {
            let (lock, cvar) = &*exit;
            *lock.lock().expect("exit lock") = true;
            cvar.notify_all();
        }
        phase_reporter.report("waiting for supervisor shutdown");
        assert_eq!(supervisor.join().unwrap().unwrap(), SAMPLE_APP_EXIT);
        assert!(store.list().unwrap().is_empty());
    });
}

#[test]
fn final_output_arrives_before_the_exit_status() {
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

        // Withholding puts the bytes past the pump's reach, modelling output
        // still in flight when the app exits. Only an orderly shutdown can
        // deliver them, so a teardown that abandons the console instead
        // fails here rather than intermittently.
        let app_console = pty.only_pty();
        pty.withhold_output(app_console);
        pty.push_output(app_console, b"bye");
        {
            let (lock, cvar) = &*exit;
            *lock.lock().expect("exit lock") = true;
            cvar.notify_all();
        }

        phase_reporter.report("waiting for final app output");
        assert_eq!(
            transport.recv(client).unwrap(),
            Message::Output(b"bye".to_vec())
        );
        phase_reporter.report("waiting for the app exit status");
        assert_eq!(
            transport.recv(client).unwrap(),
            Message::AppExited {
                status: SAMPLE_APP_EXIT
            }
        );
        phase_reporter.report("waiting for supervisor shutdown");
        assert_eq!(supervisor.join().unwrap().unwrap(), SAMPLE_APP_EXIT);
    });
}

#[test]
fn teardown_ends_the_job_before_it_closes_the_console() {
    with_watchdog_phases("setting up the supervisor", |phase_reporter| {
        let transport = MemoryTransport::new();
        let pty = MemoryPseudoconsole::new();
        let store = MemorySessionStore::new();
        let exit = Arc::new((Mutex::new(true), Condvar::new()));
        let teardown = Arc::new(Mutex::new(Vec::new()));
        pty.on_close({
            let teardown = Arc::clone(&teardown);
            move |_| teardown.lock().expect("teardown log").push(Torn::Console)
        });
        let processes = mock_processes_with_close_job(
            Arc::clone(&exit),
            LauncherTie::NoneDetected,
            AppWait::Reports,
            {
                let teardown = Arc::clone(&teardown);
                move |_| teardown.lock().expect("teardown log").push(Torn::Job)
            },
        );

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

        let started = commit_startup(&transport, startup, &phase_reporter);
        let client = transport
            .connect(&started.pipe_name, CONNECT_TIMEOUT)
            .unwrap();
        transport.send(client, &ORDINARY_ATTACH).unwrap();
        phase_reporter.report("waiting for the attach acknowledgement");
        _ = transport.recv(client).unwrap();
        transport.disconnect(started.startup_conn);

        phase_reporter.report("waiting for supervisor shutdown");
        assert_eq!(supervisor.join().unwrap().unwrap(), SAMPLE_APP_EXIT);

        // The same order rollback uses, for the same reason: descendants stay
        // attached to the console until their job is gone, and closing a
        // console waits for whoever is attached.
        // Ref: docs/supervisor.md, "Teardown".
        assert_eq!(*teardown.lock().unwrap(), [Torn::Job, Torn::Console]);
    });
}

#[test]
fn an_app_that_exits_before_anyone_attaches_still_reports_its_status() {
    with_watchdog_phases("setting up the supervisor", |phase_reporter| {
        let transport = MemoryTransport::new();
        let pty = MemoryPseudoconsole::new();
        let store = MemorySessionStore::new();
        // The app is already gone by the time the supervisor waits on it.
        let exit = Arc::new((Mutex::new(true), Condvar::new()));
        let processes = mock_processes(Arc::clone(&exit));

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

        let started = commit_startup(&transport, startup, &phase_reporter);

        // The startup connection stays open, which is what holds the
        // session up for the attach that `dure run` is about to make.
        let client = transport
            .connect(&started.pipe_name, CONNECT_TIMEOUT)
            .unwrap();
        transport.send(client, &ORDINARY_ATTACH).unwrap();
        phase_reporter.report("waiting for the attach acknowledgement");
        assert!(matches!(
            transport.recv(client).unwrap(),
            Message::Attached { .. }
        ));
        phase_reporter.report("waiting for the app exit status");
        assert_eq!(
            transport.recv(client).unwrap(),
            Message::AppExited {
                status: SAMPLE_APP_EXIT
            }
        );
        phase_reporter.report("waiting for supervisor shutdown");
        assert_eq!(supervisor.join().unwrap().unwrap(), SAMPLE_APP_EXIT);
        transport.disconnect(started.startup_conn);
    });
}

#[test]
fn a_stalled_attached_flag_does_not_delay_the_exit_status() {
    with_watchdog_phases("setting up the supervisor", |phase_reporter| {
        let transport = MemoryTransport::new();
        let pty = MemoryPseudoconsole::new();
        let store = MemorySessionStore::new();
        // The app is already gone, so attaching must immediately release
        // the supervisor to report its status.
        let exit = Arc::new((Mutex::new(true), Condvar::new()));
        let processes =
            mock_processes_with_close_job(exit, LauncherTie::NoneDetected, AppWait::Reports, {
                let store = store.clone();
                // Hold teardown after the first-attach lifetime gate opens
                // but before it claims the client slot and invalidates the
                // record, so the advisory write reaches its injected stall.
                move |_| store.wait_for_stalled_publish()
            });

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

        let started = commit_startup(&transport, startup, &phase_reporter);
        store.stall_publishes();

        let client = transport
            .connect(&started.pipe_name, CONNECT_TIMEOUT)
            .unwrap();
        transport.send(client, &ORDINARY_ATTACH).unwrap();
        phase_reporter.report("waiting for the attach acknowledgement");
        assert!(matches!(
            transport.recv(client).unwrap(),
            Message::Attached { .. }
        ));

        phase_reporter.report("waiting for the attached-flag publication to stall");
        store.wait_for_stalled_publish();
        // Reaching the exit status proves both that the first-attach signal
        // preceded the advisory write and that teardown acquired the attach
        // lock while the write remained stalled.
        phase_reporter.report("waiting for the app exit status");
        assert_eq!(
            transport.recv(client).unwrap(),
            Message::AppExited {
                status: SAMPLE_APP_EXIT
            }
        );

        store.resume_publishes();
        transport.disconnect(client);
        phase_reporter.report("waiting for supervisor shutdown");
        assert_eq!(supervisor.join().unwrap().unwrap(), SAMPLE_APP_EXIT);
        transport.disconnect(started.startup_conn);
        assert!(store.list().unwrap().is_empty());
    });
}

#[test]
fn a_session_nobody_comes_for_ends_when_its_initiator_gives_up() {
    with_watchdog_phases("setting up the supervisor", |phase_reporter| {
        let transport = MemoryTransport::new();
        let pty = MemoryPseudoconsole::new();
        let store = MemorySessionStore::new();
        let exit = Arc::new((Mutex::new(true), Condvar::new()));
        let processes = mock_processes(Arc::clone(&exit));

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

        let started = commit_startup(&transport, startup, &phase_reporter);
        // Nobody will ever attach, so the gate must open on this instead.
        transport.disconnect(started.startup_conn);
        phase_reporter.report("waiting for supervisor shutdown");
        assert_eq!(supervisor.join().unwrap().unwrap(), SAMPLE_APP_EXIT);
        assert!(store.list().unwrap().is_empty());
    });
}
