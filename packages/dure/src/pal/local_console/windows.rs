//! Windows local console PAL.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use std::{io, thread};

use windows::Win32::Foundation::{CloseHandle, ERROR_NOT_FOUND, HANDLE, WAIT_EVENT, WAIT_OBJECT_0};
use windows::Win32::Globalization::CP_UTF8;
use windows::Win32::Storage::FileSystem::{ReadFile, WriteFile};
use windows::Win32::System::Console::{
    CONSOLE_MODE, CONSOLE_SCREEN_BUFFER_INFO, CTRL_BREAK_EVENT, CTRL_C_EVENT, ENABLE_ECHO_INPUT,
    ENABLE_LINE_INPUT, ENABLE_PROCESSED_INPUT, ENABLE_PROCESSED_OUTPUT,
    ENABLE_VIRTUAL_TERMINAL_INPUT, ENABLE_VIRTUAL_TERMINAL_PROCESSING, ENABLE_WINDOW_INPUT,
    ENABLE_WRAP_AT_EOL_OUTPUT, GetConsoleCP, GetConsoleMode, GetConsoleOutputCP,
    GetConsoleScreenBufferInfo, GetStdHandle, INPUT_RECORD, KEY_EVENT, PeekConsoleInputW,
    ReadConsoleInputW, STD_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, SetConsoleCP,
    SetConsoleCtrlHandler, SetConsoleMode, SetConsoleOutputCP, WINDOW_BUFFER_SIZE_EVENT,
};
use windows::Win32::System::IO::CancelIoEx;
use windows::Win32::System::Threading::{CreateEventW, INFINITE, SetEvent, WaitForMultipleObjects};
use windows::core::{BOOL, HRESULT};

use crate::pal::error::{PalError, PalErrorKind};
use crate::pal::ids::RelayLeaseId;
use crate::pal::local_console::{ConsoleInput, LocalConsole};
use crate::pal::pseudoconsole::WindowSize;
use crate::pal::raw_handle::RawHandle;

/// Real Windows console attached to this process.
#[derive(Debug, Default)]
pub(crate) struct BuildTargetConsole;

/// One console `ReadFile` burst. Larger than a typical key or CSI sequence;
/// `ReadFile` may return less. Not a protocol bound.
const INPUT_READ_BUF: usize = 4096;

/// Input records inspected per `PeekConsoleInputW` call.
///
/// Bounds how many leading records one pass can classify and discard; the queue
/// is re-inspected until it starts with a key, so a smaller batch costs extra
/// passes rather than losing events. It also sizes the stack buffer the discard
/// path reads into, which is why the discard path allocates nothing.
const PEEK_INPUT_RECORDS: usize = 16;

/// Wait result for the input handle, which follows cancellation in the wait set.
const WAIT_INPUT: WAIT_EVENT = WAIT_EVENT(WAIT_OBJECT_0.0 + 1);

/// Maximum time allowed for the reader to cross into `ReadFile`.
///
/// The ordinary handoff needs only one scheduler turn. A larger bound absorbs
/// machine contention while ensuring a host that cannot cancel console reads
/// reports failure instead of spinning indefinitely.
const CANCEL_READ_HANDOFF_TIMEOUT: Duration = Duration::from_secs(1);

/// Console state one relay takeover replaced, kept so the console can be handed
/// back the way it was found. Ref: docs/console.md, "Modes".
///
/// Each field records a change that succeeded, so restoring undoes exactly what
/// was done rather than assuming the whole takeover completed.
#[derive(Debug, Default)]
struct TakenConsole {
    in_mode: Option<CONSOLE_MODE>,
    out_mode: Option<CONSOLE_MODE>,
    code_pages: Option<(u32, u32)>,
    ctrl_handler_installed: bool,
    cancel_event: Option<Arc<CancelEvent>>,
}

/// Cancellation state shared by the relay and its console-input reader.
#[derive(Debug)]
struct CancelEvent {
    handle: RawHandle,
    cancelled: AtomicBool,
    read_active: AtomicBool,
}

impl CancelEvent {
    fn new() -> Result<Arc<Self>, PalError> {
        // SAFETY: creates an unnamed, initially unset manual-reset event.
        let handle = unsafe { CreateEventW(None, true, false, None) }
            .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;
        Ok(Arc::new(Self {
            handle: RawHandle::from_handle(handle),
            cancelled: AtomicBool::new(false),
            read_active: AtomicBool::new(false),
        }))
    }

    fn as_handle(&self) -> HANDLE {
        self.handle.as_handle()
    }

    /// Registers the transition into a potentially blocking `ReadFile`.
    ///
    /// The reader publishes this before checking cancellation. A concurrent
    /// canceller therefore either makes this return false or observes an active
    /// read and keeps retrying `CancelIoEx` until the operation exists.
    fn begin_read(&self) -> bool {
        self.read_active.store(true, Ordering::SeqCst);
        if self.cancelled.load(Ordering::SeqCst) {
            self.read_active.store(false, Ordering::SeqCst);
            return false;
        }
        true
    }

    fn end_read(&self) {
        self.read_active.store(false, Ordering::SeqCst);
    }

    fn await_read_end(&self, deadline: Instant) -> Result<(), PalError> {
        while self.read_active.load(Ordering::SeqCst) {
            if Instant::now() >= deadline {
                return Err(PalError::new(PalErrorKind::Other));
            }
            thread::yield_now();
        }
        Ok(())
    }

    fn cancel(&self, input: HANDLE) -> Result<(), PalError> {
        self.cancelled.store(true, Ordering::SeqCst);
        // SAFETY: this value owns the live event handle for its whole call.
        unsafe { SetEvent(self.as_handle()) }
            .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;

        let deadline = Instant::now()
            .checked_add(CANCEL_READ_HANDOFF_TIMEOUT)
            .expect("the cancellation handoff timeout fits in Instant");
        while self.read_active.load(Ordering::SeqCst) {
            // SAFETY: `input` is the live console input handle. A null
            // OVERLAPPED cancels the synchronous read issued on it by this
            // process without changing the handle itself.
            match unsafe { CancelIoEx(input, None) } {
                Ok(()) => {
                    // Cancellation is asynchronous. Success means the request
                    // was accepted; wait until the reader confirms that the
                    // operation actually retired before promising it is safe
                    // for the caller to join.
                    return self.await_read_end(deadline);
                }
                Err(error) if error.code() == HRESULT::from_win32(ERROR_NOT_FOUND.0) => {
                    if Instant::now() >= deadline {
                        return Err(PalError::with_source(PalErrorKind::Other, error));
                    }
                    // No matching request may mean the reader published
                    // `read_active` just before entering ReadFile. Yield and
                    // retry until the call is pending or the reader retires.
                    thread::yield_now();
                }
                Err(error) => {
                    return Err(PalError::with_source(PalErrorKind::Other, error));
                }
            }
        }
        Ok(())
    }
}

impl Drop for CancelEvent {
    fn drop(&mut self) {
        // SAFETY: this is the last reference to the event handle, so no later
        // operation can wait on or signal it.
        _ = unsafe { CloseHandle(self.as_handle()) };
    }
}

/// The outstanding console takeover, if any.
///
/// A console is process-wide state, so at most one relay may hold it; a second
/// takeover is refused rather than sharing the first one's saved state and
/// restoring it out from under a live relay.
fn relay_lease() -> &'static Mutex<Option<(RelayLeaseId, TakenConsole)>> {
    static LEASE: OnceLock<Mutex<Option<(RelayLeaseId, TakenConsole)>>> = OnceLock::new();
    LEASE.get_or_init(|| Mutex::new(None))
}

fn active_cancel_event() -> Result<Arc<CancelEvent>, PalError> {
    relay_lease()
        .lock()
        .expect("the relay lease is only replaced, never held across a panic")
        .as_ref()
        .and_then(|(_id, taken)| taken.cancel_event.as_ref())
        .cloned()
        .ok_or_else(|| PalError::new(PalErrorKind::Other))
}

fn next_lease_id() -> RelayLeaseId {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    RelayLeaseId(NEXT.fetch_add(1, Ordering::Relaxed))
}

/// Consumes Ctrl+C and Ctrl+Break so the client does not act on a key that
/// belongs to the app.
///
/// An owned handler rather than the process-wide ignore flag, because a
/// takeover has to be reversible: removing this handler restores whatever
/// control-signal policy the caller had, which setting the ignore flag would
/// have overwritten permanently.
#[cfg_attr(coverage_nightly, coverage(off))]
unsafe extern "system" fn relay_ctrl_handler(ctrl_type: u32) -> BOOL {
    // Close, logoff, and shutdown are left to run their course: the session
    // ends with the logon session either way, and refusing them would only
    // delay a shutdown.
    BOOL::from(ctrl_type == CTRL_C_EVENT || ctrl_type == CTRL_BREAK_EVENT)
}

fn std_handle(kind: STD_HANDLE) -> Result<HANDLE, PalError> {
    // SAFETY: GetStdHandle returns a process-lifetime handle that this process
    // does not own or close.
    let handle = unsafe { GetStdHandle(kind) }
        .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;
    if handle.is_invalid() {
        return Err(PalError::new(PalErrorKind::Other));
    }
    Ok(handle)
}

fn console_mode(handle: HANDLE) -> Result<CONSOLE_MODE, PalError> {
    let mut mode = CONSOLE_MODE(0);
    // SAFETY: `handle` is a standard handle from `std_handle`; `mode` is a
    // stack value that outlives the call.
    unsafe { GetConsoleMode(handle, &raw mut mode) }
        .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;
    Ok(mode)
}

/// Restores one standard handle's console mode, best effort.
fn restore_mode(kind: STD_HANDLE, mode: CONSOLE_MODE) -> Result<(), PalError> {
    let handle = std_handle(kind)?;
    // SAFETY: `handle` is a standard console handle; `mode` was captured from
    // it before `enter_raw_relay` changed it.
    unsafe { SetConsoleMode(handle, mode) }
        .map_err(|error| PalError::with_source(PalErrorKind::Other, error))
}

/// Sets the code pages this console uses to interpret relayed bytes.
///
/// The relay carries a byte stream that a pseudoconsole produced and that a
/// pseudoconsole will consume, and those are UTF-8 in both directions. A
/// console applies its code page to the bytes crossing `WriteFile` and
/// `ReadFile`, and that code page defaults to the machine's OEM one, under
/// which every multi-byte UTF-8 sequence decodes as several unrelated glyphs.
/// Ref: docs/console.md, "Encoding".
///
/// Both are attempted even when the first fails, because a console left with
/// one side converted is worse than one left wholly unconverted.
fn set_code_pages(input: u32, output: u32) -> Result<(), PalError> {
    // SAFETY: both set process-wide console state to a documented code page
    // identifier and take no pointers.
    let input = unsafe { SetConsoleCP(input) };
    // SAFETY: as above, for the output direction.
    let output = unsafe { SetConsoleOutputCP(output) };
    input
        .and(output)
        .map_err(|error| PalError::with_source(PalErrorKind::Other, error))
}

fn read_window_size(output: HANDLE) -> Result<WindowSize, PalError> {
    let mut info = CONSOLE_SCREEN_BUFFER_INFO::default();
    // SAFETY: `output` is a console handle; `info` is a stack structure that
    // outlives the call.
    unsafe { GetConsoleScreenBufferInfo(output, &raw mut info) }
        .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;
    let width = info
        .srWindow
        .Right
        .checked_sub(info.srWindow.Left)
        .and_then(|delta| delta.checked_add(1))
        .unwrap_or(1);
    let height = info
        .srWindow
        .Bottom
        .checked_sub(info.srWindow.Top)
        .and_then(|delta| delta.checked_add(1))
        .unwrap_or(1);
    // A console host that reports an empty window is describing something no
    // app can paint into. One cell is the smallest thing that is still a
    // console, and is what the relay carries on with. The upper bound needs no
    // handling here: the host reports `i16` edges, so a width or height derived
    // from them cannot exceed what `WindowSize` accepts.
    WindowSize::new(
        u16::try_from(width.max(1)).unwrap_or(1),
        u16::try_from(height.max(1)).unwrap_or(1),
    )
    .ok_or_else(|| PalError::new(PalErrorKind::Other))
}

fn event_kind(record: &INPUT_RECORD) -> u32 {
    u32::from(record.EventType)
}

fn peek_input(handle: HANDLE) -> Result<([INPUT_RECORD; PEEK_INPUT_RECORDS], usize), PalError> {
    let mut peek = [INPUT_RECORD::default(); PEEK_INPUT_RECORDS];
    let mut count = 0_u32;
    // SAFETY: `handle` is stdin; `peek` is exclusive for this call.
    unsafe { PeekConsoleInputW(handle, &mut peek, &raw mut count) }
        .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;
    Ok((peek, count as usize))
}

/// Reads and throws away `count` leading records.
///
/// `count` is always a prefix of one peek, so the records fit on the stack and
/// this path — which runs before every blocking read — allocates nothing.
fn consume_records(handle: HANDLE, count: usize) -> Result<(), PalError> {
    if count == 0 {
        return Ok(());
    }
    let mut discarded = [INPUT_RECORD::default(); PEEK_INPUT_RECORDS];
    let discarded = discarded
        .get_mut(..count)
        .ok_or_else(|| PalError::new(PalErrorKind::Other))?;
    let mut read = 0_u32;
    // SAFETY: `discarded` is exclusive and exactly `count` records long.
    unsafe { ReadConsoleInputW(handle, discarded, &raw mut read) }
        .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;
    Ok(())
}

/// Consumes leading `WINDOW_BUFFER_SIZE_EVENT` records so a later `ReadFile`
/// is not blocked behind them. Window changes are console input records, not
/// VT bytes, which is why attach cannot learn resizes from `ReadFile` alone.
/// Ref: docs/console.md, "Window size".
fn take_leading_resize(handle: HANDLE) -> Result<Option<WindowSize>, PalError> {
    let (peek, count) = peek_input(handle)?;
    let leading_resizes = peek
        .iter()
        .take(count)
        .take_while(|record| event_kind(record) == WINDOW_BUFFER_SIZE_EVENT)
        .count();
    if leading_resizes == 0 {
        return Ok(None);
    }
    consume_records(handle, leading_resizes)?;
    let output = std_handle(STD_OUTPUT_HANDLE)?;
    read_window_size(output).map(Some)
}

/// Drops focus/menu/mouse records so they cannot hide a later resize or key.
/// This is what excludes mouse reporting from pass-through.
/// Ref: docs/console.md, "Window size".
fn discard_leading_noise(handle: HANDLE) -> Result<bool, PalError> {
    let (peek, count) = peek_input(handle)?;
    let leading_noise = peek
        .iter()
        .take(count)
        .take_while(|record| {
            let kind = event_kind(record);
            kind != WINDOW_BUFFER_SIZE_EVENT && kind != KEY_EVENT
        })
        .count();
    if leading_noise == 0 {
        return Ok(false);
    }
    consume_records(handle, leading_noise)?;
    Ok(true)
}

/// Whether a queued record exists and, if so, whether it is a key.
fn leading_record_is_key(handle: HANDLE) -> Result<Option<bool>, PalError> {
    let (peek, count) = peek_input(handle)?;
    Ok(peek
        .first()
        .filter(|_record| count != 0)
        .map(|record| event_kind(record) == KEY_EVENT))
}

/// Puts both console directions into the relay's modes, recording each success.
///
/// Ref: docs/console.md, "Modes".
#[cfg_attr(coverage_nightly, coverage(off))]
fn take_over_console(
    taken: &mut TakenConsole,
    input: HANDLE,
    output: HANDLE,
    in_mode: CONSOLE_MODE,
    out_mode: CONSOLE_MODE,
) -> Result<(), PalError> {
    // Disable cooked input so keystrokes reach the app immediately. Enable
    // VT input for CSI sequences and window-input so resizes appear as
    // `WINDOW_BUFFER_SIZE_EVENT` records rather than being dropped.
    let raw_in = CONSOLE_MODE(
        (in_mode.0 & !(ENABLE_ECHO_INPUT.0 | ENABLE_LINE_INPUT.0 | ENABLE_PROCESSED_INPUT.0))
            | ENABLE_VIRTUAL_TERMINAL_INPUT.0
            | ENABLE_WINDOW_INPUT.0,
    );
    // VT processing plus wrap so the local console host renders the same
    // sequences the app writes through its pseudoconsole.
    let raw_out = CONSOLE_MODE(
        out_mode.0
            | ENABLE_VIRTUAL_TERMINAL_PROCESSING.0
            | ENABLE_PROCESSED_OUTPUT.0
            | ENABLE_WRAP_AT_EOL_OUTPUT.0,
    );
    // SAFETY: `input` is the process stdin console handle; `raw_in` is a
    // combination of documented console mode flags.
    unsafe { SetConsoleMode(input, raw_in) }
        .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;
    taken.in_mode = Some(in_mode);
    // SAFETY: `output` is the process stdout console handle; `raw_out` is a
    // combination of documented console mode flags.
    unsafe { SetConsoleMode(output, raw_out) }
        .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;
    taken.out_mode = Some(out_mode);
    Ok(())
}

/// Undoes every change `taken` records, attempting all of them.
///
/// Stopping at the first failure would leave the console partly raw, which is
/// worse for the user than the error being reported one step later.
#[cfg_attr(coverage_nightly, coverage(off))]
fn hand_back_console(taken: &TakenConsole) -> Result<(), PalError> {
    let input = taken
        .in_mode
        .map_or(Ok(()), |mode| restore_mode(STD_INPUT_HANDLE, mode));
    let output = taken
        .out_mode
        .map_or(Ok(()), |mode| restore_mode(STD_OUTPUT_HANDLE, mode));
    let code_pages = taken.code_pages.map_or(Ok(()), |(in_page, out_page)| {
        set_code_pages(in_page, out_page)
    });
    let ctrl_handler = if taken.ctrl_handler_installed {
        // SAFETY: Add=FALSE removes the handler this process installed.
        unsafe { SetConsoleCtrlHandler(Some(relay_ctrl_handler), false) }
            .map_err(|error| PalError::with_source(PalErrorKind::Other, error))
    } else {
        Ok(())
    };
    input.and(output).and(code_pages).and(ctrl_handler)
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl LocalConsole for BuildTargetConsole {
    fn has_console(&self) -> bool {
        // The relay drives both directions, and raw-relay setup needs the mode
        // of each, so a console on only one of them cannot carry an attach.
        [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE]
            .into_iter()
            .all(|kind| {
                std_handle(kind)
                    .ok()
                    .and_then(|handle| console_mode(handle).ok())
                    .is_some()
            })
    }

    fn stdin_is_terminal(&self) -> bool {
        std_handle(STD_INPUT_HANDLE)
            .ok()
            .and_then(|handle| console_mode(handle).ok())
            .is_some()
    }

    fn begin_raw_relay(&self) -> Result<RelayLeaseId, PalError> {
        let mut lease = relay_lease()
            .lock()
            .expect("the relay lease is only replaced, never held across a panic");
        if lease.is_some() {
            return Err(PalError::new(PalErrorKind::Other));
        }
        let input = std_handle(STD_INPUT_HANDLE)?;
        let output = std_handle(STD_OUTPUT_HANDLE)?;
        let in_mode = console_mode(input)?;
        let out_mode = console_mode(output)?;
        // SAFETY: reads process-wide console state and takes no arguments.
        let in_code_page = unsafe { GetConsoleCP() };
        // SAFETY: reads process-wide console state and takes no arguments.
        let out_code_page = unsafe { GetConsoleOutputCP() };

        let mut taken = TakenConsole {
            cancel_event: Some(CancelEvent::new()?),
            ..TakenConsole::default()
        };
        // Each step records itself before the next is attempted, so a takeover
        // that fails halfway is handed back exactly as far as it got.
        let result = take_over_console(&mut taken, input, output, in_mode, out_mode)
            .and_then(|()| {
                taken.code_pages = Some((in_code_page, out_code_page));
                set_code_pages(CP_UTF8, CP_UTF8)
            })
            .and_then(|()| {
                // SAFETY: installs an owned handler that this process removes
                // again when the lease ends.
                unsafe { SetConsoleCtrlHandler(Some(relay_ctrl_handler), true) }
                    .map_err(|error| PalError::with_source(PalErrorKind::Other, error))
            })
            .inspect(|()| taken.ctrl_handler_installed = true);
        if let Err(error) = result {
            _ = hand_back_console(&taken);
            return Err(error);
        }
        let id = next_lease_id();
        *lease = Some((id, taken));
        Ok(id)
    }

    fn end_raw_relay(&self, lease: RelayLeaseId) -> Result<(), PalError> {
        let taken = {
            let mut held = relay_lease()
                .lock()
                .expect("the relay lease is only replaced, never held across a panic");
            match held.as_ref() {
                Some((id, _taken)) if *id == lease => held
                    .take()
                    .map(|(_id, taken)| taken)
                    .ok_or_else(|| PalError::new(PalErrorKind::Other))?,
                // A lease this console never issued, or one already handed
                // back, must not restore state a live relay owns.
                _ => return Err(PalError::new(PalErrorKind::Other)),
            }
        };
        hand_back_console(&taken)
    }

    fn window_size(&self) -> Result<WindowSize, PalError> {
        read_window_size(std_handle(STD_OUTPUT_HANDLE)?)
    }

    fn read_input(&self) -> Result<ConsoleInput, PalError> {
        let handle = std_handle(STD_INPUT_HANDLE)?;
        let cancel_event = active_cancel_event()?;
        loop {
            // Cancellation is a separate kernel event rather than a console
            // input record, so it cannot land between queue inspection and
            // `ReadFile` and become a record that blocks that read.
            // SAFETY: both handles remain live across this wait. The lease owns
            // the input handle, and `cancel_event` keeps the event alive.
            let wait = unsafe {
                WaitForMultipleObjects(&[cancel_event.as_handle(), handle], false, INFINITE)
            };
            if wait == WAIT_OBJECT_0 {
                return Err(PalError::new(PalErrorKind::Disconnected));
            }
            if wait != WAIT_INPUT {
                return Err(PalError::new(PalErrorKind::Other));
            }
            if let Some(size) = take_leading_resize(handle)? {
                return Ok(ConsoleInput::Resize(size));
            }
            if discard_leading_noise(handle)? {
                continue;
            }
            match leading_record_is_key(handle)? {
                Some(true) | None => {}
                Some(false) => continue,
            }
            if !cancel_event.begin_read() {
                return Err(PalError::new(PalErrorKind::Disconnected));
            }
            let mut buf = vec![0_u8; INPUT_READ_BUF];
            let mut transferred = 0_u32;
            // SAFETY: `handle` is stdin; `buf` is exclusive for this call.
            let read = unsafe {
                ReadFile(
                    handle,
                    Some(buf.as_mut_slice()),
                    Some(&raw mut transferred),
                    None,
                )
            };
            cancel_event.end_read();
            read.map_err(|error| PalError::with_source(PalErrorKind::Disconnected, error))?;
            if transferred == 0 {
                return Err(PalError::new(PalErrorKind::Disconnected));
            }
            buf.truncate(transferred as usize);
            return Ok(ConsoleInput::Bytes(buf));
        }
    }

    fn cancel_input(&self) -> Result<(), PalError> {
        let input = std_handle(STD_INPUT_HANDLE)?;
        active_cancel_event()?.cancel(input)
    }

    fn write_output(&self, data: &[u8]) -> Result<(), PalError> {
        let handle = std_handle(STD_OUTPUT_HANDLE)?;
        let mut remaining = data;
        while !remaining.is_empty() {
            let mut transferred = 0_u32;
            // SAFETY: `handle` is stdout; `remaining` is exclusive for this call.
            unsafe { WriteFile(handle, Some(remaining), Some(&raw mut transferred), None) }
                .map_err(|error| PalError::with_source(PalErrorKind::Other, error))?;
            if transferred == 0 {
                return Err(PalError::new(PalErrorKind::Other));
            }
            remaining = remaining
                .get(transferred as usize..)
                .ok_or_else(|| PalError::new(PalErrorKind::Other))?;
        }
        Ok(())
    }

    fn read_prompt_line(&self) -> Result<String, PalError> {
        let mut line = String::new();
        io::stdin()
            .read_line(&mut line)
            .map_err(PalError::from_io)?;
        Ok(line.trim_end_matches(['\r', '\n']).to_string())
    }
}
