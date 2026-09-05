//! Windows process, job, and spawn implementation.

use std::collections::HashMap;
use std::ffi::OsString;
use std::fmt::Write;
use std::mem::size_of;
use std::os::windows::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::{env, iter, ptr};

use rand::RngExt;
use windows::Win32::Foundation::{
    CloseHandle, ERROR_ACCESS_DENIED, ERROR_INVALID_PARAMETER, FILETIME, GetLastError, HANDLE,
    INVALID_HANDLE_VALUE, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows::Win32::System::Console::HPCON;
use windows::Win32::System::JobObjects::{
    CreateJobObjectW, IsProcessInJob, JOB_OBJECT_LIMIT, JOB_OBJECT_LIMIT_BREAKAWAY_OK,
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
};
use windows::Win32::System::Threading::{
    CREATE_BREAKAWAY_FROM_JOB, CREATE_UNICODE_ENVIRONMENT, CreateProcessW, DETACHED_PROCESS,
    DeleteProcThreadAttributeList, EXTENDED_STARTUPINFO_PRESENT, GetCurrentProcess,
    GetCurrentProcessId, GetExitCodeProcess, GetProcessId, GetProcessTimes, INFINITE,
    InitializeProcThreadAttributeList, LPPROC_THREAD_ATTRIBUTE_LIST, OpenProcess,
    PROC_THREAD_ATTRIBUTE_JOB_LIST, PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, PROCESS_ACCESS_RIGHTS,
    PROCESS_INFORMATION, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE, PROCESS_TERMINATE,
    STARTF_USESTDHANDLES, STARTUPINFOEXW, STARTUPINFOW, TerminateProcess,
    UpdateProcThreadAttribute, WaitForSingleObject,
};
use windows::core::{BOOL, PCWSTR, PWSTR};

use crate::constants::TERMINATE_TIMEOUT;
use crate::durability::LauncherTie;
use crate::pal::error::{PalError, PalErrorKind};
use crate::pal::ids::{AppId, JobId};
use crate::pal::processes::command_line::windows_command_line;
use crate::pal::processes::resolve::resolve_executable;
use crate::pal::processes::{AppSpawn, ProcessLiveness, Processes, ResolvedCommand, SupervisorSpawn};
use crate::pal::pseudoconsole::windows::hpcon_for;
use crate::pal::raw_handle::RawHandle;
use crate::session_record::ProcessIdentity;

/// Real Windows process control.
#[derive(Debug, Default)]
pub(crate) struct BuildTargetProcesses;

/// Whether a job object lets its members create processes that escape it.
///
/// Job topology is a Windows concept that the `Processes` contract does not
/// express: production logic only ever asks for the standard lifetime job. The
/// policy is therefore named here, alongside the job helpers that consume it,
/// rather than in the slice-wide abstraction.
///
/// Ref: docs/implementation.md, "Job breakaway".
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Breakaway {
    /// A member may escape the job by asking for breakaway at creation time.
    Permitted,
    /// Members and everything they spawn stay confined to the job.
    ///
    /// `dure` never confines a session this way. Only the integration harness
    /// builds such a job, to model the launchers `dure run` must refuse.
    #[cfg(any(test, feature = "private-test-util"))]
    Forbidden,
}

struct HandleTable {
    jobs: HashMap<u64, Vec<RawHandle>>,
    apps: HashMap<u64, RawHandle>,
}

fn table() -> &'static Mutex<HandleTable> {
    static TABLE: OnceLock<Mutex<HandleTable>> = OnceLock::new();
    TABLE.get_or_init(|| {
        Mutex::new(HandleTable {
            jobs: HashMap::new(),
            apps: HashMap::new(),
        })
    })
}

fn next_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// Pseudoconsole plus lifetime job, both applied at `CreateProcessW`.
const APP_SPAWN_ATTRIBUTE_COUNT: u32 = 2;

fn filetime_u64(time: FILETIME) -> u64 {
    let high = u64::from(time.dwHighDateTime);
    let low = u64::from(time.dwLowDateTime);
    high.checked_shl(32)
        .expect("shifting a u32 into the high half of a u64 cannot overflow")
        | low
}

fn identity_of(handle: HANDLE) -> Result<ProcessIdentity, PalError> {
    let pid = {
        // SAFETY: `handle` is a valid process handle owned by the caller.
        unsafe { GetProcessId(handle) }
    };
    if pid == 0 {
        return Err(PalError::new(PalErrorKind::InspectFailed));
    }
    let mut creation = FILETIME::default();
    let mut exit = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut user = FILETIME::default();
    // SAFETY: all FILETIME pointers refer to stack values that outlive the call.
    unsafe {
        GetProcessTimes(
            handle,
            &raw mut creation,
            &raw mut exit,
            &raw mut kernel,
            &raw mut user,
        )
    }
    .map_err(|error| PalError::with_source(PalErrorKind::InspectFailed, error))?;
    Ok(ProcessIdentity {
        pid,
        creation_time: filetime_u64(creation),
    })
}

fn open_verified(
    identity: &ProcessIdentity,
    access: PROCESS_ACCESS_RIGHTS,
) -> Result<HANDLE, PalError> {
    // SAFETY: pid is a numeric process id; OpenProcess does not retain aliasing
    // of Rust references.
    let opened = unsafe { OpenProcess(access, false, identity.pid) };
    let Ok(handle) = opened else {
        // SAFETY: immediately after the failed OpenProcess.
        let err = unsafe { GetLastError() };
        // A missing pid is NotFound (stale record). Access-denied and other
        // failures are InspectFailed so GC does not drop a live supervisor.
        if err == ERROR_INVALID_PARAMETER {
            return Err(PalError::new(PalErrorKind::NotFound));
        }
        return Err(PalError::new(PalErrorKind::InspectFailed));
    };
    match identity_of(handle) {
        Ok(actual) if actual.creation_time == identity.creation_time => Ok(handle),
        Ok(_) => {
            close(handle);
            Err(PalError::new(PalErrorKind::NotFound))
        }
        Err(error) => {
            close(handle);
            Err(error)
        }
    }
}

fn close(handle: HANDLE) {
    // SAFETY: `handle` is a process or job handle we own and never use again.
    _ = unsafe { CloseHandle(handle) };
}

pub(super) fn wide(s: &str) -> Vec<u16> {
    OsString::from(s)
        .encode_wide()
        .chain(iter::once(0))
        .collect()
}

/// Waits for a process to exit and reports the status it exited with.
#[cfg_attr(coverage_nightly, coverage(off))]
fn exit_status_of(handle: HANDLE) -> Result<i32, PalError> {
    // SAFETY: `handle` is a process handle this call keeps alive throughout.
    let wait = unsafe { WaitForSingleObject(handle, INFINITE) };
    if wait != WAIT_OBJECT_0 {
        return Err(PalError::new(PalErrorKind::Other));
    }
    let mut code = 0_u32;
    // SAFETY: the process has exited; `code` is a stack u32.
    unsafe { GetExitCodeProcess(handle, &raw mut code) }
        .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;
    // Windows process statuses are `u32`; NTSTATUS-style failure codes use
    // the high bit and do not fit in a non-negative `i32`.
    Ok(code.cast_signed())
}

/// Whether `process` belongs to any job object.
fn process_in_a_job(process: HANDLE) -> Result<bool, PalError> {
    let mut in_job = BOOL::default();
    // SAFETY: `process` is a valid process handle, a null job asks about any
    // job, and `in_job` outlives the call.
    unsafe { IsProcessInJob(process, None, &raw mut in_job) }
        .map_err(|error| PalError::with_source(PalErrorKind::InspectFailed, error))?;
    Ok(in_job.as_bool())
}

/// What is known about the limits of the job this process is directly in.
///
/// Membership and limits are separate questions with separate failure modes, and
/// callers answer an unreadable job differently from no job at all.
enum JobLimits {
    /// The process belongs to no job object.
    None,
    /// The process belongs to a job whose limits could not be read.
    Unknown,
    /// The limit flags of the job the process is directly in.
    Known(JOB_OBJECT_LIMIT),
}

/// Read the limits of the job this process is directly in.
///
/// Windows reports only the immediate job, and only to the process itself, so
/// this says nothing about any ancestor job.
/// Ref: docs/implementation.md, "Job breakaway".
#[cfg_attr(coverage_nightly, coverage(off))]
fn immediate_job_limits() -> JobLimits {
    // SAFETY: the pseudo-handle this returns needs no state and is always valid.
    let current = unsafe { GetCurrentProcess() };
    match process_in_a_job(current) {
        Ok(false) => return JobLimits::None,
        Ok(true) => {}
        Err(_error) => return JobLimits::Unknown,
    }
    let mut info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    // SAFETY: a null job handle asks about the job this process is directly in,
    // and `info` is a stack structure of the size the information class expects.
    let queried = unsafe {
        QueryInformationJobObject(
            None,
            JobObjectExtendedLimitInformation,
            ptr::from_mut(&mut info).cast(),
            u32::try_from(size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>())
                .expect("job info size fits in u32"),
            None,
        )
    };
    if queried.is_err() {
        return JobLimits::Unknown;
    }
    JobLimits::Known(info.BasicLimitInformation.LimitFlags)
}

/// Whether the job this process is directly in would refuse it a breakaway.
///
/// Windows reports a refused breakaway as a plain access-denied error, and an
/// unreadable image reports the same thing. Only a job that withholds
/// `JOB_OBJECT_LIMIT_BREAKAWAY_OK` can have refused one, so this is what tells
/// the two apart. A job whose limits cannot be read is assumed to be the cause,
/// because a process outside a job never sees this question at all.
#[cfg_attr(coverage_nightly, coverage(off))]
fn breakaway_forbidden() -> bool {
    match immediate_job_limits() {
        JobLimits::None => false,
        JobLimits::Unknown => true,
        JobLimits::Known(flags) => (flags & JOB_OBJECT_LIMIT_BREAKAWAY_OK).0 == 0,
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl BuildTargetProcesses {
    /// Create an unnamed kill-on-close job with the requested breakaway policy.
    ///
    /// Ref: docs/implementation.md, "Job breakaway".
    pub(crate) fn create_job(breakaway: Breakaway) -> Result<JobId, PalError> {
        Self::create_job_chain(&[breakaway])
    }

    /// Create a chain of nested jobs, outermost first.
    ///
    /// A process spawned into the chain joins every job in it, so the last
    /// policy is the one its breakaway is evaluated against and the earlier ones
    /// stay behind as ancestors.
    ///
    /// Ref: docs/implementation.md, "Job breakaway".
    pub(crate) fn create_job_chain(policies: &[Breakaway]) -> Result<JobId, PalError> {
        let mut handles = Vec::with_capacity(policies.len());
        for breakaway in policies {
            match create_job_handle(*breakaway) {
                Ok(handle) => handles.push(RawHandle::from_handle(handle)),
                Err(error) => {
                    // A partly built chain owns handles no id names yet, so it
                    // can only be released here.
                    for handle in handles {
                        close(handle.as_handle());
                    }
                    return Err(error);
                }
            }
        }
        let id = next_id();
        table()
            .lock()
            .expect("the handle table is only inserted into and looked up, never held across a panic")
            .jobs
            .insert(id, handles);
        Ok(JobId(id))
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn create_job_handle(breakaway: Breakaway) -> Result<HANDLE, PalError> {
    // SAFETY: a null name creates an unnamed job object.
    let handle = unsafe { CreateJobObjectW(None, PCWSTR::null()) }
        .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;
    let mut info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    info.BasicLimitInformation.LimitFlags = match breakaway {
        Breakaway::Permitted => JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE | JOB_OBJECT_LIMIT_BREAKAWAY_OK,
        #[cfg(any(test, feature = "private-test-util"))]
        Breakaway::Forbidden => JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    };
    // SAFETY: `info` is a stack structure of the size SetInformationJobObject
    // expects for JobObjectExtendedLimitInformation.
    let configured = unsafe {
        SetInformationJobObject(
            handle,
            JobObjectExtendedLimitInformation,
            ptr::from_mut(&mut info).cast(),
            u32::try_from(size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>())
                .expect("job info size fits in u32"),
        )
    };
    if configured.is_err() {
        // The job exists but does not have the limits the caller asked for, so
        // it is never returned and nothing else can close it.
        // SAFETY: `handle` is the job just created and is not used again.
        unsafe {
            _ = CloseHandle(handle);
        }
        return Err(PalError::new(PalErrorKind::Other));
    }
    Ok(handle)
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl Processes for BuildTargetProcesses {
    fn current_exe(&self) -> Result<PathBuf, PalError> {
        env::current_exe().map_err(PalError::from_io)
    }

    fn spawn_supervisor(&self, request: &SupervisorSpawn) -> Result<ProcessIdentity, PalError> {
        let mut cmd_wide = wide(&windows_command_line(
            &request.exe.to_string_lossy(),
            &request.args,
        ));
        let mut exe_wide = wide(&request.exe.to_string_lossy());
        let si = STARTUPINFOW {
            cb: u32::try_from(size_of::<STARTUPINFOW>()).expect("STARTUPINFOW fits in u32"),
            ..Default::default()
        };
        let mut pi = PROCESS_INFORMATION::default();
        let flags = CREATE_BREAKAWAY_FROM_JOB | DETACHED_PROCESS | CREATE_UNICODE_ENVIRONMENT;
        // SAFETY: `exe_wide` and `cmd_wide` are NUL-terminated. `si`/`pi` are
        // valid stack structures. No handle inherit list is passed because the
        // startup channel is a named pipe connected by name after spawn.
        let created = unsafe {
            CreateProcessW(
                PCWSTR(exe_wide.as_mut_ptr()),
                Some(PWSTR(cmd_wide.as_mut_ptr())),
                None,
                None,
                false,
                flags,
                None,
                None,
                &raw const si,
                &raw mut pi,
            )
        };
        created.map_err(|error| {
            // Windows reports a refused breakaway as plain access-denied, which
            // an unreadable image produces too. The job's breakaway policy
            // separates the two, so `run` can name the cause instead of
            // guessing at it.
            if error.code() == ERROR_ACCESS_DENIED.to_hresult() && breakaway_forbidden() {
                PalError::new(PalErrorKind::BreakawayDenied)
            } else {
                PalError::new(PalErrorKind::Other)
            }
        })?;
        close(pi.hThread);
        // Closed on both paths: an early return here would leave the detached
        // supervisor holding an unreferenced handle in this process.
        let identity = identity_of(pi.hProcess);
        close(pi.hProcess);
        identity
    }

    fn launcher_tie(&self) -> LauncherTie {
        match immediate_job_limits() {
            JobLimits::None => LauncherTie::NoneDetected,
            // Nothing was established, so nothing is claimed: the client warns
            // about the uncertainty rather than naming a cause.
            JobLimits::Unknown => LauncherTie::Unknown,
            // Only kill-on-close ties this process's lifetime to the launcher's
            // job. Membership in a job without it is harmless, and terminals and
            // remote session hosts routinely impose such a job on everything
            // they start.
            JobLimits::Known(flags) => {
                if (flags & JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE).0 == 0 {
                    LauncherTie::NoneDetected
                } else {
                    LauncherTie::Confirmed
                }
            }
        }
    }

    fn probe(&self, identity: &ProcessIdentity) -> ProcessLiveness {
        let access = PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE;
        let handle = match open_verified(identity, access) {
            Ok(handle) => handle,
            Err(error) if error.kind() == PalErrorKind::NotFound => return ProcessLiveness::Dead,
            Err(_) => return ProcessLiveness::InspectFailed,
        };
        // SAFETY: `handle` is a process handle opened with SYNCHRONIZE. Zero
        // timeout distinguishes still-running (`WAIT_TIMEOUT`) from already
        // exited (`WAIT_OBJECT_0`). Exit code 259 is a valid exited status and
        // must not be treated as live.
        let wait = unsafe { WaitForSingleObject(handle, 0) };
        close(handle);
        if wait == WAIT_TIMEOUT {
            ProcessLiveness::Live
        } else if wait == WAIT_OBJECT_0 {
            ProcessLiveness::Dead
        } else {
            ProcessLiveness::InspectFailed
        }
    }

    fn terminate(&self, identity: &ProcessIdentity) -> Result<(), PalError> {
        let access = PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE | PROCESS_TERMINATE;
        let handle = open_verified(identity, access).map_err(|error| {
            if error.kind() == PalErrorKind::NotFound {
                PalError::new(PalErrorKind::NotFound)
            } else {
                PalError::new(PalErrorKind::InspectFailed)
            }
        })?;
        // SAFETY: `handle` is a process handle opened with PROCESS_TERMINATE
        // and verified to be the recorded process.
        let result = unsafe { TerminateProcess(handle, 1) };
        let settled = if result.is_ok() {
            // Termination is asynchronous. Reporting success while the process
            // still runs would let `kill` delete the record and reuse the id
            // while the old supervisor can still publish an attached flag and
            // recreate it, so wait for the process object to signal.
            // SAFETY: `handle` is a process handle opened with SYNCHRONIZE.
            let wait = unsafe {
                WaitForSingleObject(
                    handle,
                    u32::try_from(TERMINATE_TIMEOUT.as_millis())
                        .expect("terminate timeout fits in u32 milliseconds"),
                )
            };
            wait == WAIT_OBJECT_0
        } else {
            false
        };
        close(handle);
        result.map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;
        if settled {
            Ok(())
        } else {
            Err(PalError::new(PalErrorKind::Other))
        }
    }

    fn create_lifetime_job(&self) -> Result<JobId, PalError> {
        // The session job permits breakaway so a nested `dure run` inside the
        // app can still create an independent inner supervisor.
        // Ref: docs/implementation.md, "Job breakaway".
        Self::create_job(Breakaway::Permitted)
    }

    fn close_job(&self, job: JobId) {
        if let Some(handles) = table().lock().expect("the handle table is only inserted into and looked up, never held across a panic").jobs.remove(&job.0) {
            // Innermost first, so a kill-on-close ancestor never tears down a
            // job this still holds a handle to.
            for handle in handles.into_iter().rev() {
                close(handle.as_handle());
            }
        }
    }

    fn spawn_app(&self, request: &AppSpawn) -> Result<AppId, PalError> {
        let hpcon = hpcon_for(request.pty).ok_or_else(|| PalError::new(PalErrorKind::NotFound))?;
        let exe = self
            .resolve_executable(request.command.exe(), &request.launch_directory)
            .path;
        let mut cmd_wide = wide(&windows_command_line(
            &exe.to_string_lossy(),
            request.command.args(),
        ));
        let mut exe_wide = wide(&exe.to_string_lossy());
        let mut dir_wide = wide(&request.launch_directory.to_string_lossy());

        // Outermost first: a process is assigned to the jobs in the order the
        // attribute lists them, which is what nests them.
        let mut job_list = table()
            .lock()
            .expect("the handle table is only inserted into and looked up, never held across a panic")
            .jobs
            .get(&request.job.0)
            .ok_or_else(|| PalError::new(PalErrorKind::NotFound))?
            .iter()
            .map(|handle| handle.as_handle())
            .collect::<Vec<_>>();

        // Pseudoconsole and lifetime job, both applied at CreateProcessW so the
        // app cannot run outside the job. CREATE_SUSPENDED plus a later
        // AssignProcessToJobObject delays console initialization and can leave
        // the child with pipes instead of a console.
        let attribute_count = APP_SPAWN_ATTRIBUTE_COUNT;
        let mut attr_size: usize = 0;
        // SAFETY: querying the required size; the null list pointer is allowed
        // when discovering the buffer length.
        _ = unsafe {
            InitializeProcThreadAttributeList(None, attribute_count, None, &raw mut attr_size)
        };
        let mut attr_buf = vec![0_u8; attr_size];
        let attr_list = LPPROC_THREAD_ATTRIBUTE_LIST(attr_buf.as_mut_ptr().cast());
        // SAFETY: `attr_buf` is the size reported by the previous call and lives
        // for the rest of this function.
        unsafe {
            InitializeProcThreadAttributeList(
                Some(attr_list),
                attribute_count,
                None,
                &raw mut attr_size,
            )
        }
        .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;

        // PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE stores `lpValue` as the HPCON
        // itself. The Microsoft sample passes `hPC`, not `&hPC`.
        // Ref: https://learn.microsoft.com/windows/console/creating-a-pseudoconsole-session
        let hpcon_value = hpcon.0 as *const core::ffi::c_void;
        // SAFETY: the attribute list was initialized for two attributes. `hpcon`
        // is a live HPCON owned by the pseudoconsole PAL for `request.pty`.
        unsafe {
            UpdateProcThreadAttribute(
                attr_list,
                0,
                usize::try_from(PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE)
                    .expect("PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE fits in usize"),
                Some(hpcon_value),
                size_of::<HPCON>(),
                None,
                None,
            )
        }
        .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;

        // SAFETY: the list still has a free slot. `job_list` holds the lifetime
        // job handles and lives until CreateProcessW returns.
        unsafe {
            UpdateProcThreadAttribute(
                attr_list,
                0,
                usize::try_from(PROC_THREAD_ATTRIBUTE_JOB_LIST)
                    .expect("PROC_THREAD_ATTRIBUTE_JOB_LIST fits in usize"),
                Some(job_list.as_mut_ptr().cast()),
                size_of::<HANDLE>()
                    .checked_mul(job_list.len())
                    .expect("job handle list size fits in usize"),
                None,
                None,
            )
        }
        .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;

        let mut si = STARTUPINFOEXW::default();
        si.StartupInfo.cb =
            u32::try_from(size_of::<STARTUPINFOEXW>()).expect("STARTUPINFOEXW fits in u32");
        // Parent stdio is often redirected (SSH, `cargo test`). CreateProcess
        // copies those handle values into the child unless STARTF_USESTDHANDLES
        // overrides them, and the pseudoconsole attribute does not displace an
        // inherited value. Invalid std handles are what leaves the pseudoconsole
        // free to install console handles instead: without this the child sees
        // pipes, which the `helper_sees_a_console` integration test observes
        // directly.
        si.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
        si.StartupInfo.hStdInput = INVALID_HANDLE_VALUE;
        si.StartupInfo.hStdOutput = INVALID_HANDLE_VALUE;
        si.StartupInfo.hStdError = INVALID_HANDLE_VALUE;
        si.lpAttributeList = attr_list;
        let mut pi = PROCESS_INFORMATION::default();
        let flags = EXTENDED_STARTUPINFO_PRESENT | CREATE_UNICODE_ENVIRONMENT;
        // SAFETY: wide strings are NUL-terminated. Attribute list is initialized
        // with the pseudoconsole and job. Std handles are invalid so they are
        // not the parent's redirected pipes.
        let created = unsafe {
            CreateProcessW(
                PCWSTR(exe_wide.as_mut_ptr()),
                Some(PWSTR(cmd_wide.as_mut_ptr())),
                None,
                None,
                false,
                flags,
                None,
                PCWSTR(dir_wide.as_mut_ptr()),
                ptr::from_ref(&si.StartupInfo),
                &raw mut pi,
            )
        };
        // SAFETY: the list was initialized above and is no longer referenced by
        // the kernel after CreateProcessW returns.
        unsafe {
            DeleteProcThreadAttributeList(attr_list);
        }
        created.map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;

        close(pi.hThread);
        let id = next_id();
        table()
            .lock()
            .expect("the handle table is only inserted into and looked up, never held across a panic")
            .apps
            .insert(id, RawHandle::from_handle(pi.hProcess));
        Ok(AppId(id))
    }

    fn wait_app(&self, app: AppId) -> Result<i32, PalError> {
        // Taken out of the table rather than borrowed: this call consumes the
        // app, so the handle is this call's to close whatever the wait says.
        let handle = table()
            .lock()
            .expect("the handle table is only inserted into and looked up, never held across a panic")
            .apps
            .remove(&app.0)
            .ok_or_else(|| PalError::new(PalErrorKind::NotFound))?
            .as_handle();
        let status = exit_status_of(handle);
        close(handle);
        status
    }

    fn current_identity(&self) -> Result<ProcessIdentity, PalError> {
        // SAFETY: GetCurrentProcess returns a pseudo-handle that does not need
        // to be closed and is valid for the calling process.
        let handle = unsafe { GetCurrentProcess() };
        let mut identity = identity_of(handle)?;
        identity.pid = {
            // SAFETY: no preconditions.
            unsafe { GetCurrentProcessId() }
        };
        Ok(identity)
    }

    fn resolve_executable(&self, command: &str, launch_directory: &Path) -> ResolvedCommand {
        resolve_executable(command, launch_directory)
    }

    fn random_nonce(&self) -> String {
        // 128 bits of CSPRNG output, encoded as hex. This only has to be unique
        // among concurrent sessions for this user, not unguessable to other
        // users (the pipe ACL already restricts the creating user).
        let bytes: [u8; 16] = rand::rng().random();
        let mut nonce = String::with_capacity(32);
        for byte in bytes {
            write!(nonce, "{byte:02x}").expect("writing to String cannot fail");
        }
        nonce
    }
}
