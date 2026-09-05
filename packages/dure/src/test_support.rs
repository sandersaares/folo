//! Helpers for Windows integration tests. Not part of the product.

use std::path::Path;
use std::sync::mpsc::{self, Receiver};
use std::thread;

use crate::app_command::AppCommand;
use crate::constants::{DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS};
use crate::pal::ids::{AppId, JobId, PtyId};
use crate::pal::processes::{
    AppSpawn, Breakaway, BuildTargetProcesses, Processes, ProcessesFacade,
};
use crate::pal::pseudoconsole::{Pseudoconsole, PseudoconsoleFacade, WindowSize};

/// A process started inside a test-owned pseudoconsole.
///
/// Integration tests use this so they do not depend on the runner having an
/// interactive console (implementation.md, "Integration tests").
#[derive(Debug)]
pub struct ConsoleProcess {
    processes: ProcessesFacade,
    pty_host: PseudoconsoleFacade,
    app: AppId,
    pty: PtyId,
    job: JobId,
    closed: bool,
}

impl ConsoleProcess {
    /// Spawn `exe` with `args` in `cwd`, attached to a new `ConPTY`.
    ///
    /// The surrounding job permits breakaway, which models the shell an SSH
    /// session provides (implementation.md, "Job breakaway").
    #[must_use]
    pub fn spawn(exe: &Path, args: &[String], cwd: &Path) -> Self {
        Self::spawn_in_jobs(exe, args, cwd, &[Breakaway::Permitted])
    }

    /// Spawn `exe` the way a launcher that confines its children would.
    ///
    /// The surrounding job forbids breakaway, which models wrappers such as
    /// `cargo run` that `dure run` must refuse to detach from
    /// (implementation.md, "Job breakaway").
    #[must_use]
    pub fn spawn_confined(exe: &Path, args: &[String], cwd: &Path) -> Self {
        Self::spawn_in_jobs(exe, args, cwd, &[Breakaway::Forbidden])
    }

    /// Spawn `exe` inside a permissive job that itself sits in a confining one.
    ///
    /// Breakaway is evaluated against the immediate job only, so `CreateProcessW`
    /// succeeds here and leaves the supervisor a member of the outer job. This
    /// models the case `dure run` can only detect after the spawn
    /// (implementation.md, "Job breakaway").
    #[must_use]
    pub fn spawn_confined_by_ancestor(exe: &Path, args: &[String], cwd: &Path) -> Self {
        Self::spawn_in_jobs(
            exe,
            args,
            cwd,
            &[Breakaway::Forbidden, Breakaway::Permitted],
        )
    }

    fn spawn_in_jobs(exe: &Path, args: &[String], cwd: &Path, jobs: &[Breakaway]) -> Self {
        let processes = ProcessesFacade::target();
        let pty_host = PseudoconsoleFacade::target();
        let job = BuildTargetProcesses::create_job_chain(jobs).expect("create test job");
        let pty = pty_host
            .create(WindowSize {
                cols: DEFAULT_PTY_COLS,
                rows: DEFAULT_PTY_ROWS,
            })
            .expect("create test pseudoconsole");
        let mut argv = Vec::with_capacity(args.len().saturating_add(1));
        argv.push(exe.to_string_lossy().into_owned());
        argv.extend(args.iter().cloned());
        let command = AppCommand::from_argv(argv).expect("test argv names an executable");
        let app = processes
            .spawn_app(&AppSpawn {
                command,
                launch_directory: cwd.to_path_buf(),
                pty,
                job,
            })
            .expect("spawn test client in pseudoconsole");
        Self {
            processes,
            pty_host,
            app,
            pty,
            job,
            closed: false,
        }
    }

    /// Write bytes to the child's console input.
    pub fn write_input(&self, data: &[u8]) {
        self.pty_host
            .write_input(self.pty, data)
            .expect("write test console input");
    }

    /// Console output as it arrives, ending once the child has exited.
    ///
    /// A pseudoconsole keeps its read side open for as long as this process
    /// holds it, so a caller waiting for a phrase the child never printed would
    /// wait forever, including under mutation testing where the workspace
    /// watchdog is disabled. Ending the pseudoconsole once the child is gone
    /// ends the stream instead, turning that wait into a failed assertion,
    /// after delivering everything the child did write.
    /// Ref: docs/testing.md, "Tests must not hang".
    #[must_use]
    pub fn output_until_exit(&self) -> Receiver<Vec<u8>> {
        let (sender, receiver) = mpsc::channel();
        thread::spawn({
            let pty_host = self.pty_host.clone();
            let pty = self.pty;
            move || {
                loop {
                    match pty_host.read_output(pty) {
                        Ok(bytes) if bytes.is_empty() => break,
                        Ok(bytes) => {
                            if sender.send(bytes).is_err() {
                                break;
                            }
                        }
                        Err(_error) => break,
                    }
                }
            }
        });
        thread::spawn({
            let processes = self.processes.clone();
            let pty_host = self.pty_host.clone();
            let app = self.app;
            let pty = self.pty;
            move || {
                _ = processes.wait_app(app);
                pty_host.finish(pty);
            }
        });
        receiver
    }

    /// Wait for the child to exit and tear down the job and pseudoconsole.
    ///
    /// Output is drained on a helper thread so a child that writes to the
    /// pseudoconsole cannot block on a full pipe while this wait runs.
    #[must_use]
    pub fn wait(mut self) -> i32 {
        let drain = thread::spawn({
            let pty_host = self.pty_host.clone();
            let pty = self.pty;
            move || loop {
                match pty_host.read_output(pty) {
                    Ok(bytes) if bytes.is_empty() => break,
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });
        let status = self.processes.wait_app(self.app).expect("wait test child");
        self.shutdown();
        _ = drain.join();
        status
    }

    fn shutdown(&mut self) {
        if self.closed {
            return;
        }
        // Same ordering the supervisor's teardown relies on: the child stays
        // attached to the pseudoconsole until the job that owns its lifetime is
        // closed, and closing a pseudoconsole waits for its attached clients. A
        // drop while the child is still running would otherwise never reach
        // `close_job`.
        self.processes.close_job(self.job);
        self.pty_host.close(self.pty);
        self.closed = true;
    }
}

impl Drop for ConsoleProcess {
    fn drop(&mut self) {
        self.shutdown();
    }
}
