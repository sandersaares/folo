//! Helpers for Windows integration tests. Not part of the product.

use std::path::Path;
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};

use windows::Win32::Foundation::{HLOCAL, LocalFree};
use windows::Win32::Security::Authorization::{
    ConvertSecurityDescriptorToStringSecurityDescriptorW,
    ConvertStringSecurityDescriptorToSecurityDescriptorW, GetNamedSecurityInfoW, SDDL_REVISION_1,
    SE_FILE_OBJECT,
};
use windows::Win32::Security::{DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR};
use windows::core::{PCWSTR, PWSTR};

use crate::AppCommand;
use crate::constants::{DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS};
use crate::pal::ids::{AppId, JobId, PtyId};
use crate::pal::processes::{
    AppSpawn, Breakaway, BuildTargetProcesses, Processes, ProcessesFacade,
};
use crate::pal::pseudoconsole::{Pseudoconsole, PseudoconsoleFacade, WindowSize};
use crate::pal::transport::current_user_sid_string;

/// The SID of the user this process is running as, in string form.
///
/// # Panics
///
/// Panics when the platform will not say who is running, which a test cannot
/// meaningfully continue past.
#[must_use]
pub fn current_user_sid() -> String {
    current_user_sid_string().expect("the current user has a SID")
}

/// The canonical protected file-object DACL that grants only this user full access.
///
/// Windows can render well-known SIDs through SDDL aliases, so tests compare
/// descriptors after asking Windows to canonicalize both sides.
#[must_use]
pub fn current_user_file_dacl_sddl() -> String {
    let sid = current_user_sid();
    let sddl = format!("D:P(A;;FA;;;{sid})");
    let wide: Vec<u16> = sddl.encode_utf16().chain(std::iter::once(0)).collect();
    let mut descriptor = PSECURITY_DESCRIPTOR::default();
    // SAFETY: `wide` is a NUL-terminated SDDL string. On success `descriptor`
    // points to the LocalAlloc block released below.
    unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            PCWSTR(wide.as_ptr()),
            SDDL_REVISION_1,
            &raw mut descriptor,
            None,
        )
    }
    .expect("the expected DACL is valid SDDL");
    let text = descriptor_dacl_sddl(descriptor);
    // SAFETY: `descriptor` is the unique LocalAlloc pointer obtained above.
    unsafe {
        _ = LocalFree(Some(HLOCAL(descriptor.0.cast())));
    }
    text
}

/// The discretionary access control list of `object`, in SDDL form.
///
/// Named so a test can pass a session pipe path. Lets a test read back what a
/// session actually permits rather than trusting the code that set it.
///
/// # Panics
///
/// Panics when the object cannot be queried, which for something a test just
/// caused to exist means the test itself is wrong.
#[must_use]
pub fn dacl_sddl(object: &str) -> String {
    let wide: Vec<u16> = object.encode_utf16().chain(std::iter::once(0)).collect();
    let mut descriptor = PSECURITY_DESCRIPTOR::default();
    // SAFETY: `wide` is a NUL-terminated object name. On success `descriptor`
    // points into a LocalAlloc block this call allocates and the caller frees.
    let queried = unsafe {
        GetNamedSecurityInfoW(
            PCWSTR(wide.as_ptr()),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            None,
            None,
            None,
            None,
            &raw mut descriptor,
        )
    };
    assert!(
        queried.is_ok(),
        "reading the security of {object:?}: {queried:?}"
    );

    let text = descriptor_dacl_sddl(descriptor);
    // SAFETY: `descriptor` is the unique LocalAlloc pointer obtained above.
    unsafe {
        _ = LocalFree(Some(HLOCAL(descriptor.0.cast())));
    }
    text
}

fn descriptor_dacl_sddl(descriptor: PSECURITY_DESCRIPTOR) -> String {
    let mut sddl = PWSTR::null();
    // SAFETY: `descriptor` is the descriptor returned above. On success `sddl`
    // is a LocalAlloc string this function owns.
    let converted = unsafe {
        ConvertSecurityDescriptorToStringSecurityDescriptorW(
            descriptor,
            SDDL_REVISION_1,
            DACL_SECURITY_INFORMATION,
            &raw mut sddl,
            None,
        )
    };
    // SAFETY: `sddl` is a NUL-terminated string that call allocated.
    let text = converted
        .is_ok()
        .then(|| String::from_utf16_lossy(unsafe { sddl.as_wide() }));
    // SAFETY: `sddl` is the unique LocalAlloc string obtained above, if the
    // conversion produced one.
    unsafe {
        if !sddl.is_null() {
            _ = LocalFree(Some(HLOCAL(sddl.0.cast())));
        }
    }
    text.expect("a security descriptor converts to SDDL")
}

/// A process started inside a test-owned pseudoconsole.
///
/// Integration tests use this so they do not depend on the runner having an
/// interactive console (docs/implementation.md, "Integration tests").
///
/// This is an owning handle: dropping it closes the job and pseudoconsole that
/// own the child's lifetime, which ends a child that is still running.
/// [`ConsoleProcess::wait`] is the explicit way out, and is also what reports
/// the child's exit status.
#[derive(Debug)]
pub struct ConsoleProcess {
    pty_host: PseudoconsoleFacade,
    pty: PtyId,
    job: JobId,
    /// The single wait on the child, which also ends its console.
    ///
    /// Started at spawn so there is exactly one wait per child: waiting twice
    /// would consume a process handle that only the first wait owns.
    waiter: Option<JoinHandle<i32>>,
    closed: bool,
}

impl ConsoleProcess {
    /// Spawn `exe` with `args` in `cwd`, attached to a new pseudoconsole.
    ///
    /// The surrounding job permits breakaway, which models the shell an SSH
    /// session provides (docs/job-breakaway.md).
    #[must_use]
    pub fn spawn(exe: &Path, args: &[String], cwd: &Path) -> Self {
        Self::spawn_in_jobs(exe, args, cwd, &[Breakaway::Permitted])
    }

    /// Spawn `exe` the way a launcher that confines its children would.
    ///
    /// The surrounding job forbids breakaway, which models wrappers such as
    /// `cargo run` that `dure run` must refuse to detach from
    /// (docs/job-breakaway.md).
    #[must_use]
    pub fn spawn_confined(exe: &Path, args: &[String], cwd: &Path) -> Self {
        Self::spawn_in_jobs(exe, args, cwd, &[Breakaway::Forbidden])
    }

    /// Spawn `exe` inside a permissive job that itself sits in a confining one.
    ///
    /// Breakaway is evaluated against the immediate job only, so `CreateProcessW`
    /// succeeds here and leaves the supervisor a member of the outer job. This
    /// models the case `dure run` can only detect after the spawn
    /// (docs/job-breakaway.md).
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
            pty_host: pty_host.clone(),
            pty,
            job,
            waiter: Some(spawn_waiter(&processes, &pty_host, app, pty)),
            closed: false,
        }
    }

    /// Write bytes to the child's console input.
    pub fn write_input(&self, data: &[u8]) {
        self.pty_host
            .write_input(self.pty, data)
            .expect("write test console input");
    }

    /// Resize the console the child is running in, as a user resizing their
    /// terminal window would.
    ///
    /// # Panics
    ///
    /// Panics on a size no console can have, which is a mistake in the test
    /// rather than a condition the product has to handle.
    pub fn resize(&self, cols: u16, rows: u16) {
        let size =
            WindowSize::new(cols, rows).expect("a test resizes to a size a console can have");
        self.pty_host
            .resize(self.pty, size)
            .expect("resize test console");
    }

    /// Console output as it arrives, ending once the child has exited.
    ///
    /// These are raw console bytes in whatever chunks the reads happened to
    /// produce, so a caller has to assemble them and strip the terminal control
    /// sequences the console host emits before asserting on text.
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
                        Ok(Some(bytes)) => {
                            if sender.send(bytes).is_err() {
                                break;
                            }
                        }
                        // The child has said everything it is going to say.
                        Ok(None) => break,
                        // A failed read is not the child finishing, and ending
                        // the stream quietly here would turn it into a missing
                        // phrase in some unrelated assertion.
                        Err(error) => panic!("reading test console output: {error}"),
                    }
                }
            }
        });
        receiver
    }

    /// Wait for the child to exit and tear down the job and pseudoconsole.
    ///
    /// Output is drained here so a child that writes to the pseudoconsole
    /// cannot block on a full pipe while this wait runs.
    #[must_use]
    pub fn wait(mut self) -> i32 {
        let drain = self.output_until_exit();
        let status = self
            .waiter
            .take()
            .expect("a console process waits for its child exactly once")
            .join()
            .expect("wait test child");
        // Drained to the end, so the child is never blocked writing while this
        // waits and teardown below is not racing a live reader.
        for _chunk in drain {}
        self.shutdown();
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
        ProcessesFacade::target().close_job(self.job);
        self.pty_host.close(self.pty);
        self.closed = true;
    }
}

/// Waits for the child, then ends its console so readers stop.
fn spawn_waiter(
    processes: &ProcessesFacade,
    pty_host: &PseudoconsoleFacade,
    app: AppId,
    pty: PtyId,
) -> JoinHandle<i32> {
    thread::spawn({
        let processes = processes.clone();
        let pty_host = pty_host.clone();
        move || {
            let status = processes.wait_app(app).expect("wait test child");
            pty_host.finish(pty);
            status
        }
    })
}

impl Drop for ConsoleProcess {
    fn drop(&mut self) {
        self.shutdown();
    }
}
