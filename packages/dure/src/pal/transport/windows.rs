//! Windows named-pipe transport.

use std::collections::HashMap;
use std::iter;
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use windows::Win32::Foundation::{
    CloseHandle, ERROR_BROKEN_PIPE, ERROR_IO_PENDING, ERROR_NO_DATA, ERROR_PIPE_BUSY,
    ERROR_PIPE_CONNECTED, ERROR_PIPE_NOT_CONNECTED, ERROR_SEM_TIMEOUT, GetLastError, HANDLE,
    HLOCAL, LocalFree, WAIT_OBJECT_0, WAIT_TIMEOUT, WIN32_ERROR,
};
use windows::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
};
use windows::Win32::Security::{
    GetTokenInformation, PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER,
    TokenUser,
};
use windows::Win32::Storage::FileSystem::{
    CreateFileW, FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED, FILE_FLAGS_AND_ATTRIBUTES,
    FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_SHARE_NONE, OPEN_EXISTING, PIPE_ACCESS_DUPLEX,
    ReadFile, SECURITY_IDENTIFICATION, SECURITY_SQOS_PRESENT, WriteFile,
};
use windows::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};
use windows::Win32::System::Pipes::{
    ConnectNamedPipe, CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_REJECT_REMOTE_CLIENTS,
    PIPE_TYPE_BYTE, PIPE_UNLIMITED_INSTANCES, PIPE_WAIT, WaitNamedPipeW,
};
use windows::Win32::System::Threading::{
    CreateEventW, GetCurrentProcess, INFINITE, OpenProcessToken, WaitForSingleObject,
};
use windows::core::{PCWSTR, PWSTR};

use crate::pal::error::{PalError, PalErrorKind};
use crate::pal::ids::{ConnId, ListenerId};
use crate::pal::raw_handle::PipeHandle;
use crate::pal::transport::Transport;
use crate::protocol::{Message, decode_body, encode, payload_len_ok};

struct PipeTable {
    listeners: HashMap<ListenerId, Listener>,
    conns: HashMap<ConnId, Conn>,
}

struct Listener {
    name: Vec<u16>,
    pending: Arc<PipeHandle>,
}

/// One accepted or connected pipe end.
///
/// `write` serializes complete frames so output, displacement, and app-exit
/// cannot interleave length prefixes on this byte-mode pipe.
struct Conn {
    handle: Arc<PipeHandle>,
    write: Arc<Mutex<()>>,
}

/// Tracks one total timeout across a multi-step overlapped operation.
#[derive(Clone, Copy)]
struct Deadline {
    started: Instant,
    timeout: Duration,
}

impl Deadline {
    fn after(timeout: Duration) -> Self {
        Self {
            started: Instant::now(),
            timeout,
        }
    }

    fn wait_millis(self) -> Result<u32, PalError> {
        /// `WaitForSingleObject` reserves zero for polling.
        const MIN_FINITE_WAIT_MILLIS: u32 = 1;
        /// `WaitForSingleObject` reserves `u32::MAX` for an infinite wait.
        const MAX_FINITE_WAIT_MILLIS: u32 = u32::MAX.saturating_sub(1);

        let remaining = self.timeout.saturating_sub(self.started.elapsed());
        if remaining.is_zero() {
            return Err(PalError::new(PalErrorKind::Timeout));
        }
        Ok(u32::try_from(ceil_millis(remaining))
            .unwrap_or(MAX_FINITE_WAIT_MILLIS)
            .clamp(MIN_FINITE_WAIT_MILLIS, MAX_FINITE_WAIT_MILLIS))
    }
}

/// Convert to the whole milliseconds Windows accepts without shortening a deadline.
fn ceil_millis(duration: Duration) -> u128 {
    duration
        .as_nanos()
        .div_ceil(Duration::from_millis(1).as_nanos())
}

fn table() -> &'static Mutex<PipeTable> {
    static TABLE: OnceLock<Mutex<PipeTable>> = OnceLock::new();
    TABLE.get_or_init(|| {
        Mutex::new(PipeTable {
            listeners: HashMap::new(),
            conns: HashMap::new(),
        })
    })
}

fn next_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

fn io_error_kind(err: WIN32_ERROR) -> PalErrorKind {
    if err == ERROR_BROKEN_PIPE || err == ERROR_NO_DATA || err == ERROR_PIPE_NOT_CONNECTED {
        PalErrorKind::Disconnected
    } else {
        PalErrorKind::Other
    }
}

fn close(handle: HANDLE) {
    if handle.is_invalid() {
        return;
    }
    // SAFETY: `handle` is a pipe or event handle we own and never use again.
    _ = unsafe { CloseHandle(handle) };
}

fn wide_z(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(iter::once(0)).collect()
}

// Kernel buffer for each direction. Sized for a typical console burst so a
// slow client does not immediately stall ConPTY output. Not a protocol bound.
const PIPE_BUFFER: u32 = 65_536;

/// Process-lifetime DACL used for every session pipe.
///
/// Permits only the creating user (docs/transport.md).
struct UserPipeSecurity {
    descriptor: PSECURITY_DESCRIPTOR,
    attrs: SECURITY_ATTRIBUTES,
}

impl UserPipeSecurity {
    fn new() -> Result<Self, PalError> {
        let sid = current_user_sid_string()?;
        let sddl = format!("D:P(A;;GA;;;{sid})");
        let wide = wide_z(&sddl);
        let mut descriptor = PSECURITY_DESCRIPTOR::default();
        // SAFETY: `wide` is a NUL-terminated SDDL string. On success `descriptor`
        // is a LocalAlloc security descriptor we own and must LocalFree.
        let converted = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                PCWSTR(wide.as_ptr()),
                SDDL_REVISION_1,
                &raw mut descriptor,
                None,
            )
        };
        converted.map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;
        let attrs = SECURITY_ATTRIBUTES {
            nLength: u32::try_from(size_of::<SECURITY_ATTRIBUTES>())
                .expect("SECURITY_ATTRIBUTES fits in u32"),
            lpSecurityDescriptor: descriptor.0,
            bInheritHandle: false.into(),
        };
        Ok(Self { descriptor, attrs })
    }
}

impl Drop for UserPipeSecurity {
    fn drop(&mut self) {
        if !self.descriptor.0.is_null() {
            // SAFETY: `descriptor` is the unique LocalAlloc pointer from
            // ConvertStringSecurityDescriptorToSecurityDescriptorW.
            _ = unsafe { LocalFree(Some(HLOCAL(self.descriptor.0))) };
            self.descriptor = PSECURITY_DESCRIPTOR::default();
        }
    }
}

pub(crate) fn current_user_sid_string() -> Result<String, PalError> {
    let mut token = HANDLE::default();
    // SAFETY: a pseudo-handle to this process; it is not closed.
    let process = unsafe { GetCurrentProcess() };
    // SAFETY: `token` is an out-handle on the stack. `process` is the
    // current-process pseudo-handle.
    unsafe { OpenProcessToken(process, TOKEN_QUERY, &raw mut token) }
        .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;
    let mut len = 0_u32;
    // SAFETY: size query; a null information buffer is allowed.
    _ = unsafe { GetTokenInformation(token, TokenUser, None, 0, &raw mut len) };
    let words = usize::try_from(len)
        .expect("token info size fits in usize")
        .div_ceil(size_of::<u64>());
    let mut buf = vec![0_u64; words];
    let byte_len = u32::try_from(buf.len().saturating_mul(size_of::<u64>()))
        .expect("token info buffer fits in u32");
    // SAFETY: `buf` is exclusive, 8-byte aligned, and large enough for `len`.
    let queried = unsafe {
        GetTokenInformation(
            token,
            TokenUser,
            Some(buf.as_mut_ptr().cast()),
            byte_len,
            &raw mut len,
        )
    };
    close(token);
    queried.map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;
    let mut sid_str = PWSTR::null();
    {
        // SAFETY: `buf` holds a TOKEN_USER written by GetTokenInformation. No
        // other exclusive borrow of `buf` exists. `User.Sid` points into `buf`.
        let user = unsafe { buf.as_ptr().cast::<TOKEN_USER>().as_ref_unchecked() };
        // SAFETY: `user.User.Sid` is a valid SID inside `buf`. On success
        // `sid_str` is a LocalAlloc string we own.
        unsafe { ConvertSidToStringSidW(user.User.Sid, &raw mut sid_str) }
            .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;
    }
    // SAFETY: `sid_str` is the unique owner of a NUL-terminated SID string.
    // Copy before LocalFree so conversion errors cannot leak the allocation.
    let wide = unsafe { sid_str.as_wide() }.to_vec();
    // SAFETY: `sid_str` is the ConvertSidToStringSidW allocation we copied.
    _ = unsafe { LocalFree(Some(HLOCAL(sid_str.0.cast()))) };
    String::from_utf16(&wide).map_err(|error| PalError::with_source(PalErrorKind::Other, error))
}

fn create_instance(name: &[u16], first: bool) -> Result<HANDLE, PalError> {
    let mut open_mode = PIPE_ACCESS_DUPLEX.0 | FILE_FLAG_OVERLAPPED.0;
    if first {
        open_mode |= FILE_FLAG_FIRST_PIPE_INSTANCE.0;
    }
    let security = UserPipeSecurity::new()?;
    // SAFETY: `name` is a NUL-terminated pipe path. `security.attrs` is a valid
    // SECURITY_ATTRIBUTES whose descriptor lives until after this call. The
    // created handle is owned by the caller. Remote clients are rejected.
    let handle = unsafe {
        CreateNamedPipeW(
            PCWSTR(name.as_ptr()),
            FILE_FLAGS_AND_ATTRIBUTES(open_mode),
            PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
            PIPE_UNLIMITED_INSTANCES,
            PIPE_BUFFER,
            PIPE_BUFFER,
            0,
            Some(&raw const security.attrs),
        )
    };
    if handle.is_invalid() {
        return Err(PalError::new(PalErrorKind::Other));
    }
    Ok(handle)
}

fn create_event() -> Result<HANDLE, PalError> {
    // SAFETY: a manual-reset event used only as an overlapped completion event.
    unsafe { CreateEventW(None, true, false, None) }
        .map_err(|error| PalError::with_source(PalErrorKind::Other, error))
}

fn wait_event(event: HANDLE, timeout_ms: u32) -> Result<(), PalError> {
    // SAFETY: `event` is a live event handle created for this overlapped wait.
    let wait = unsafe { WaitForSingleObject(event, timeout_ms) };
    if wait == WAIT_TIMEOUT {
        return Err(PalError::new(PalErrorKind::Timeout));
    }
    if wait != WAIT_OBJECT_0 {
        return Err(PalError::new(PalErrorKind::Other));
    }
    Ok(())
}

/// Gives up on an overlapped operation and waits for the kernel to let go of it.
///
/// `CancelIoEx` only asks for cancellation; it does not wait for one. Until the operation
/// actually completes the kernel may still write its result into the `OVERLAPPED` and signal
/// the event, both of which this thread is about to reclaim, so the blocking
/// `GetOverlappedResult` is what makes reclaiming them sound. It cannot block indefinitely
/// because every caller holds the `Arc<PipeHandle>` the operation was issued on, so the pipe
/// outlives the cancellation and the cancellation always completes.
fn abandon_operation(handle: HANDLE, overlapped: &OVERLAPPED) {
    // SAFETY: `handle` is the pipe this operation was issued on and `overlapped` addresses
    // that operation, which no other thread is waiting on.
    _ = unsafe { CancelIoEx(handle, Some(&raw const *overlapped)) };
    let mut transferred = 0_u32;
    // SAFETY: as above. Waiting is the point: it returns only once `overlapped` is the
    // caller's again. The outcome is irrelevant because the operation is being discarded.
    _ = unsafe { GetOverlappedResult(handle, &raw const *overlapped, &raw mut transferred, true) };
}

/// Waits for a pending operation without returning while the kernel still owns its storage.
fn wait_pending(
    handle: HANDLE,
    event: HANDLE,
    overlapped: &OVERLAPPED,
    deadline: Option<Deadline>,
) -> Result<(), PalError> {
    let wait_millis = match deadline.map_or(Ok(INFINITE), Deadline::wait_millis) {
        Ok(wait_millis) => wait_millis,
        Err(error) => {
            abandon_operation(handle, overlapped);
            return Err(error);
        }
    };
    if let Err(error) = wait_event(event, wait_millis) {
        abandon_operation(handle, overlapped);
        return Err(error);
    }
    Ok(())
}

fn connect_instance(pipe: &PipeHandle, deadline: Option<Deadline>) -> Result<(), PalError> {
    let handle = pipe.as_handle();
    let event = create_event()?;
    let mut overlapped = OVERLAPPED {
        hEvent: event,
        ..Default::default()
    };
    // SAFETY: `handle` is a listening pipe instance. `overlapped` is valid for
    // the duration of this wait.
    let Some(result) =
        pipe.issue(|handle| unsafe { ConnectNamedPipe(handle, Some(&raw mut overlapped)) })
    else {
        close(event);
        return Err(PalError::new(PalErrorKind::Disconnected));
    };
    if result.is_ok() {
        close(event);
        return Ok(());
    }
    let err = {
        // SAFETY: GetLastError is called immediately after the failed API.
        unsafe { GetLastError() }
    };
    if err == ERROR_PIPE_CONNECTED {
        close(event);
        return Ok(());
    }
    if err != ERROR_IO_PENDING {
        close(event);
        return Err(PalError::new(PalErrorKind::Other));
    }
    if let Err(error) = wait_pending(handle, event, &overlapped, deadline) {
        close(event);
        return Err(error);
    }
    let mut transferred = 0_u32;
    // SAFETY: the wait succeeded; `overlapped` still addresses this connect. Waiting rather
    // than polling means a result that is somehow not in yet is awaited instead of abandoned.
    let completed =
        unsafe { GetOverlappedResult(handle, &raw const overlapped, &raw mut transferred, true) };
    close(event);
    completed.map_err(|error| PalError::with_source(PalErrorKind::Disconnected, error))
}

fn read_exact_until(
    pipe: &PipeHandle,
    buf: &mut [u8],
    deadline: Option<Deadline>,
) -> Result<(), PalError> {
    let handle = pipe.as_handle();
    let mut filled = 0_usize;
    while filled < buf.len() {
        let event = create_event()?;
        let mut overlapped = OVERLAPPED {
            hEvent: event,
            ..Default::default()
        };
        let mut transferred = 0_u32;
        let dest = buf
            .get_mut(filled..)
            .ok_or_else(|| PalError::new(PalErrorKind::Other))?;
        // SAFETY: `handle` is a connected overlapped pipe. `dest` is exclusive
        // for the duration of this call.
        let Some(ok) = pipe.issue(|handle| unsafe {
            ReadFile(
                handle,
                Some(dest),
                Some(&raw mut transferred),
                Some(&raw mut overlapped),
            )
        }) else {
            close(event);
            return Err(PalError::new(PalErrorKind::Disconnected));
        };
        if ok.is_err() {
            let err = {
                // SAFETY: immediately after the failed ReadFile.
                unsafe { GetLastError() }
            };
            if err != ERROR_IO_PENDING {
                close(event);
                return Err(PalError::new(io_error_kind(err)));
            }
            if let Err(error) = wait_pending(handle, event, &overlapped, deadline) {
                close(event);
                return Err(error);
            }
            // SAFETY: the wait succeeded; `overlapped` still addresses this read. Waiting rather
            // than polling means a result that is somehow not in yet is awaited, not abandoned.
            if unsafe {
                GetOverlappedResult(handle, &raw const overlapped, &raw mut transferred, true)
            }
            .is_err()
            {
                let err = {
                    // SAFETY: immediately after the failed GetOverlappedResult.
                    unsafe { GetLastError() }
                };
                close(event);
                return Err(PalError::new(io_error_kind(err)));
            }
        }
        close(event);
        if transferred == 0 {
            return Err(PalError::new(PalErrorKind::Disconnected));
        }
        filled = filled
            .checked_add(transferred as usize)
            .ok_or_else(|| PalError::new(PalErrorKind::Other))?;
    }
    Ok(())
}

fn write_all(pipe: &PipeHandle, mut buf: &[u8]) -> Result<(), PalError> {
    let handle = pipe.as_handle();
    while !buf.is_empty() {
        let event = create_event()?;
        let mut overlapped = OVERLAPPED {
            hEvent: event,
            ..Default::default()
        };
        let mut transferred = 0_u32;
        // SAFETY: `handle` is a connected overlapped pipe. `buf` is exclusive
        // for the duration of this call.
        let Some(ok) = pipe.issue(|handle| unsafe {
            WriteFile(
                handle,
                Some(buf),
                Some(&raw mut transferred),
                Some(&raw mut overlapped),
            )
        }) else {
            close(event);
            return Err(PalError::new(PalErrorKind::Other));
        };
        if ok.is_err() {
            let err = {
                // SAFETY: immediately after the failed WriteFile.
                unsafe { GetLastError() }
            };
            if err != ERROR_IO_PENDING {
                close(event);
                return Err(PalError::new(PalErrorKind::Other));
            }
            if let Err(error) = wait_event(event, INFINITE) {
                abandon_operation(handle, &overlapped);
                close(event);
                return Err(error);
            }
            // SAFETY: the wait succeeded; `overlapped` still addresses this write. Waiting rather
            // than polling means a result that is somehow not in yet is awaited, not abandoned.
            if unsafe {
                GetOverlappedResult(handle, &raw const overlapped, &raw mut transferred, true)
            }
            .is_err()
            {
                let err = {
                    // SAFETY: immediately after the failed GetOverlappedResult.
                    unsafe { GetLastError() }
                };
                close(event);
                return Err(PalError::new(io_error_kind(err)));
            }
        }
        close(event);
        if transferred == 0 {
            return Err(PalError::new(PalErrorKind::Other));
        }
        buf = buf
            .get(transferred as usize..)
            .ok_or_else(|| PalError::new(PalErrorKind::Other))?;
    }
    Ok(())
}

fn conn_handle(conn: ConnId) -> Result<Arc<PipeHandle>, PalError> {
    table()
        .lock()
        .expect("the pipe table is only inserted into and looked up, never held across a panic")
        .conns
        .get(&conn)
        .map(|conn| Arc::clone(&conn.handle))
        .ok_or_else(|| PalError::new(PalErrorKind::NotFound))
}

fn conn_write(conn: ConnId) -> Result<(Arc<PipeHandle>, Arc<Mutex<()>>), PalError> {
    table()
        .lock()
        .expect("the pipe table is only inserted into and looked up, never held across a panic")
        .conns
        .get(&conn)
        .map(|conn| (Arc::clone(&conn.handle), Arc::clone(&conn.write)))
        .ok_or_else(|| PalError::new(PalErrorKind::NotFound))
}

fn accept_connection(listener: ListenerId, timeout: Option<Duration>) -> Result<ConnId, PalError> {
    let deadline = timeout.map(Deadline::after);
    let (pending, name) = {
        let table = table().lock().expect(
            "the pipe table is only inserted into and looked up, never held across a panic",
        );
        let listener = table
            .listeners
            .get(&listener)
            .ok_or_else(|| PalError::new(PalErrorKind::NotFound))?;
        (Arc::clone(&listener.pending), listener.name.clone())
    };
    let connected = connect_instance(&pending, deadline);
    let mut table = table()
        .lock()
        .expect("the pipe table is only inserted into and looked up, never held across a panic");
    // If close_listener already removed the listener, this handle must not be
    // published; dropping the last reference closes it.
    if !table.listeners.contains_key(&listener) {
        return Err(PalError::new(PalErrorKind::Disconnected));
    }
    connected?;
    // Published before the next listener instance is created. A client already
    // owns the other end of this pipe and may have sent its `Attach`, so the
    // handle has to become a connection someone can answer on rather than sit
    // in the listener slot describing a pipe nobody is listening on.
    let id = ConnId(next_id());
    table.conns.insert(
        id,
        Conn {
            handle: pending,
            write: Arc::new(Mutex::new(())),
        },
    );
    // After each accept, create the next server instance so another client
    // can connect while this connection is still live (steal).
    let next = match create_instance(&name, false) {
        Ok(next) => next,
        Err(error) => {
            // Nothing will serve this connection, so it is closed rather than
            // left for a client to wait on: a dropped pipe tells that client to
            // try again, where silence would hold it until teardown.
            table.conns.remove(&id);
            return Err(error);
        }
    };
    let Some(listener_state) = table.listeners.get_mut(&listener) else {
        close(next);
        table.conns.remove(&id);
        return Err(PalError::new(PalErrorKind::Disconnected));
    };
    listener_state.pending = PipeHandle::new(next);
    Ok(id)
}

fn recv_message(conn: ConnId, timeout: Option<Duration>) -> Result<Message, PalError> {
    let deadline = timeout.map(Deadline::after);
    let handle = conn_handle(conn)?;
    let mut header = [0_u8; 4];
    read_exact_until(&handle, &mut header, deadline)?;
    let len = u32::from_le_bytes(header);
    if !payload_len_ok(len) {
        return Err(PalError::new(PalErrorKind::Other));
    }
    // The kind byte is read on its own so the body arrives in an allocation
    // the message can simply take. Ref: docs/transport.md.
    let mut kind = [0_u8; 1];
    read_exact_until(&handle, &mut kind, deadline)?;
    let body_len = (len as usize)
        .checked_sub(kind.len())
        .ok_or_else(|| PalError::new(PalErrorKind::Other))?;
    let mut body = vec![0_u8; body_len];
    read_exact_until(&handle, &mut body, deadline)?;
    decode_body(kind[0], body).map_err(|_error| PalError::new(PalErrorKind::Other))
}

/// Real Windows named-pipe transport.
#[derive(Debug, Default)]
pub(crate) struct BuildTargetTransport;

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl Transport for BuildTargetTransport {
    fn listen(&self, name: &str) -> Result<ListenerId, PalError> {
        let name = wide_z(name);
        let pending = create_instance(&name, true)?;
        let id = ListenerId(next_id());
        table()
            .lock()
            .expect("the pipe table is only inserted into and looked up, never held across a panic")
            .listeners
            .insert(
                id,
                Listener {
                    name,
                    pending: PipeHandle::new(pending),
                },
            );
        Ok(id)
    }

    fn accept(&self, listener: ListenerId) -> Result<ConnId, PalError> {
        accept_connection(listener, None)
    }

    fn accept_timeout(&self, listener: ListenerId, timeout: Duration) -> Result<ConnId, PalError> {
        accept_connection(listener, Some(timeout))
    }

    fn connect(&self, name: &str, timeout: Duration) -> Result<ConnId, PalError> {
        let name = wide_z(name);
        let access = FILE_GENERIC_READ.0 | FILE_GENERIC_WRITE.0;
        let started = Instant::now();
        loop {
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(PalError::new(PalErrorKind::Timeout));
            }
            // `WaitNamedPipeW` reserves three values: 0 asks for the server's
            // default timeout, 1 (`NMPWAIT_NOWAIT`) asks for no wait at all,
            // and `u32::MAX` (`NMPWAIT_WAIT_FOREVER`) waits without a deadline.
            // Clamping into the interval between them keeps a live deadline from
            // collapsing into "do not wait" or expanding into "wait forever".
            let timeout_ms = u32::try_from(ceil_millis(remaining))
                .unwrap_or(u32::MAX)
                .clamp(2, u32::MAX - 1);
            // SAFETY: `name` is a NUL-terminated pipe path. WaitNamedPipeW does not
            // retain the pointer after it returns.
            let ready = unsafe { WaitNamedPipeW(PCWSTR(name.as_ptr()), timeout_ms) };
            if !ready.as_bool() {
                // SAFETY: immediately after the failed WaitNamedPipeW.
                let err = unsafe { GetLastError() };
                // The wait fails both when it was spent and when there is no
                // such pipe at all, and only the first is a timeout: the
                // command layer turns that into "timed out connecting", which
                // is the wrong thing to tell someone whose session is gone.
                if err == ERROR_SEM_TIMEOUT {
                    // Whether the caller's own deadline is spent is decided by
                    // the loop, which retries while it has time left.
                    continue;
                }
                return Err(PalError::new(PalErrorKind::NotFound));
            }
            // SAFETY: WaitNamedPipeW reported an instance. CreateFile opens a new
            // client handle we own. FILE_FLAG_OVERLAPPED matches the server end,
            // and the SQOS flags cap what the server on the other end may do
            // with this client's identity.
            let handle = unsafe {
                CreateFileW(
                    PCWSTR(name.as_ptr()),
                    access,
                    FILE_SHARE_NONE,
                    None,
                    OPEN_EXISTING,
                    FILE_FLAG_OVERLAPPED | SECURITY_SQOS_PRESENT | SECURITY_IDENTIFICATION,
                    None,
                )
            };
            let handle = match handle {
                Ok(handle) => handle,
                Err(_error) => {
                    let err = {
                        // SAFETY: immediately after the failed CreateFileW.
                        unsafe { GetLastError() }
                    };
                    if err != ERROR_PIPE_BUSY {
                        return Err(PalError::new(PalErrorKind::NotFound));
                    }
                    // Another client took the instance the wait reported. The
                    // supervisor posts the next one as soon as it accepts, so
                    // keep trying for as long as the caller allows.
                    continue;
                }
            };
            let id = ConnId(next_id());
            table()
                .lock()
                .expect(
                    "the pipe table is only inserted into and looked up, never held across a panic",
                )
                .conns
                .insert(
                    id,
                    Conn {
                        handle: PipeHandle::new(handle),
                        write: Arc::new(Mutex::new(())),
                    },
                );
            return Ok(id);
        }
    }

    fn send(&self, conn: ConnId, message: &Message) -> Result<(), PalError> {
        let frame = encode(message);
        let (handle, write) = conn_write(conn)?;
        let _guard = write.lock().expect(
            "a pipe write holds the lock only across the write itself, which does not panic",
        );
        write_all(&handle, &frame)
    }

    fn recv(&self, conn: ConnId) -> Result<Message, PalError> {
        recv_message(conn, None)
    }

    fn recv_timeout(&self, conn: ConnId, timeout: Duration) -> Result<Message, PalError> {
        recv_message(conn, Some(timeout))
    }

    fn disconnect(&self, conn: ConnId) {
        let removed = table()
            .lock()
            .expect("the pipe table is only inserted into and looked up, never held across a panic")
            .conns
            .remove(&conn);
        if let Some(conn) = removed {
            // Aborts a read or write another thread is blocked in, so it fails
            // and releases its reference; the handle closes with the last one.
            conn.handle.cancel();
        }
    }

    fn close_listener(&self, listener: ListenerId) {
        let removed = table()
            .lock()
            .expect("the pipe table is only inserted into and looked up, never held across a panic")
            .listeners
            .remove(&listener);
        if let Some(listener) = removed {
            // Aborts the connect an `accept` is blocked in; see `disconnect`.
            listener.pending.cancel();
        }
    }

    fn pipe_name(&self, nonce: &str) -> String {
        format!(r"\\.\pipe\dure-{nonce}")
    }
}
