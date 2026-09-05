//! In-memory pseudoconsole for unit tests.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use crate::pal::error::{PalError, PalErrorKind};
use crate::pal::ids::PtyId;
use crate::pal::pseudoconsole::{Pseudoconsole, WindowSize};

struct PtyState {
    size: WindowSize,
    input: VecDeque<u8>,
    output: VecDeque<u8>,
    closed: bool,
    /// Withholds output from readers until the pty is finished.
    ///
    /// A real app can write bytes that are still sitting in the pipe when the
    /// session tears down, which no ordering of test steps reproduces on its
    /// own: whatever a test pushes, the pump is free to read first. Holding
    /// reads lets a test put output beyond a reader's reach and then require
    /// that shutdown still delivers it.
    withheld: bool,
}

struct Inner {
    next_id: AtomicU64,
    ptys: Mutex<HashMap<PtyId, PtyState>>,
    cond: Condvar,
}

/// Byte-pump stand-in for a pseudoconsole.
#[derive(Clone)]
pub(crate) struct MemoryPseudoconsole {
    inner: Arc<Inner>,
}

impl fmt::Debug for MemoryPseudoconsole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryPseudoconsole").finish()
    }
}

impl MemoryPseudoconsole {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                next_id: AtomicU64::new(1),
                ptys: Mutex::new(HashMap::new()),
                cond: Condvar::new(),
            }),
        }
    }

    /// Push output as if the app wrote to its console.
    ///
    /// # Panics
    ///
    /// Panics when `pty` is not live. A test that pushes into a pseudoconsole
    /// that was never created, or was already closed, is not exercising the
    /// scenario it names, so the mistake is reported here rather than as an
    /// unexplained absence of output later.
    pub(crate) fn push_output(&self, pty: PtyId, data: &[u8]) {
        let mut ptys = self.inner.ptys.lock().expect("pty map lock");
        let state = ptys.get_mut(&pty).expect("pushing output into a live pty");
        state.output.extend(data.iter().copied());
        self.inner.cond.notify_all();
    }

    /// Withhold output from readers until this pty is finished.
    ///
    /// A reader already parked in `read_output` stays parked, so a test can put
    /// output out of reach of the pump and then require that shutdown still
    /// delivers it.
    /// # Panics
    ///
    /// Panics when `pty` is not live, for the same reason as `push_output`.
    pub(crate) fn withhold_output(&self, pty: PtyId) {
        let mut ptys = self.inner.ptys.lock().expect("pty map lock");
        let state = ptys
            .get_mut(&pty)
            .expect("withholding output from a live pty");
        state.withheld = true;
    }

    /// Take input the supervisor wrote to the app.
    ///
    /// # Panics
    ///
    /// Panics when `pty` is not live: an empty result would otherwise be
    /// indistinguishable from the supervisor having written nothing.
    pub(crate) fn take_input(&self, pty: PtyId) -> Vec<u8> {
        let mut ptys = self.inner.ptys.lock().expect("pty map lock");
        let state = ptys.get_mut(&pty).expect("taking input from a live pty");
        state.input.drain(..).collect()
    }

    /// Current size last applied to this pty.
    pub(crate) fn size(&self, pty: PtyId) -> Option<WindowSize> {
        let ptys = self.inner.ptys.lock().expect("pty map lock");
        ptys.get(&pty).map(|state| state.size)
    }

    /// The one live pseudoconsole on this host.
    ///
    /// A supervisor creates exactly one, so a test that drives it can name it
    /// without assuming which integer the allocator happened to hand out.
    ///
    /// # Panics
    ///
    /// Panics unless exactly one pseudoconsole is live, which means the test
    /// reached this point in a state it did not intend.
    pub(crate) fn only_pty(&self) -> PtyId {
        let ptys = self.inner.ptys.lock().expect("pty map lock");
        let mut live = ptys.keys();
        let only = *live.next().expect("exactly one live pty");
        assert!(live.next().is_none(), "exactly one live pty");
        only
    }
}

impl Default for MemoryPseudoconsole {
    fn default() -> Self {
        Self::new()
    }
}

impl Pseudoconsole for MemoryPseudoconsole {
    fn create(&self, size: WindowSize) -> Result<PtyId, PalError> {
        let id = PtyId(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
        self.inner.ptys.lock().expect("pty map lock").insert(
            id,
            PtyState {
                size,
                input: VecDeque::new(),
                output: VecDeque::new(),
                closed: false,
                withheld: false,
            },
        );
        Ok(id)
    }

    fn resize(&self, pty: PtyId, size: WindowSize) -> Result<(), PalError> {
        let mut ptys = self.inner.ptys.lock().expect("pty map lock");
        let state = ptys
            .get_mut(&pty)
            .ok_or_else(|| PalError::new(PalErrorKind::NotFound))?;
        state.size = size;
        Ok(())
    }

    fn write_input(&self, pty: PtyId, data: &[u8]) -> Result<(), PalError> {
        let mut ptys = self.inner.ptys.lock().expect("pty map lock");
        let state = ptys
            .get_mut(&pty)
            .ok_or_else(|| PalError::new(PalErrorKind::NotFound))?;
        if state.closed {
            return Err(PalError::new(PalErrorKind::NotFound));
        }
        state.input.extend(data.iter().copied());
        self.inner.cond.notify_all();
        Ok(())
    }

    // Blocking condvar wait. A mutation that drops the closed check or the
    // wake hangs tests because watchdogs are disabled under cargo-mutants.
    #[cfg_attr(test, mutants::skip)]
    fn read_output(&self, pty: PtyId) -> Result<Option<Vec<u8>>, PalError> {
        let mut ptys = self.inner.ptys.lock().expect("pty map lock");
        loop {
            let Some(state) = ptys.get_mut(&pty) else {
                return Err(PalError::new(PalErrorKind::NotFound));
            };
            // Withheld output becomes readable once the pty is finished, which
            // is what makes shutdown the only path that can deliver it.
            if !state.output.is_empty() && (state.closed || !state.withheld) {
                return Ok(Some(state.output.drain(..).collect()));
            }
            if state.closed {
                return Ok(None);
            }
            ptys = self.inner.cond.wait(ptys).expect("pty condvar");
        }
    }

    fn finish(&self, pty: PtyId) {
        let mut ptys = self.inner.ptys.lock().expect("pty map lock");
        if let Some(state) = ptys.get_mut(&pty) {
            state.closed = true;
        }
        self.inner.cond.notify_all();
    }

    fn close(&self, pty: PtyId) {
        self.inner.ptys.lock().expect("pty map lock").remove(&pty);
        self.inner.cond.notify_all();
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn pumps_bytes_and_tracks_size() {
        let host = MemoryPseudoconsole::new();
        let pty = host.create(WindowSize { cols: 80, rows: 24 }).unwrap();
        host.write_input(pty, b"in").unwrap();
        assert_eq!(host.take_input(pty), b"in");
        host.resize(pty, WindowSize { cols: 40, rows: 10 }).unwrap();
        assert_eq!(host.size(pty), Some(WindowSize { cols: 40, rows: 10 }));
        host.push_output(pty, b"out");
        assert_eq!(host.read_output(pty).unwrap().as_deref(), Some(b"out".as_slice()));
        host.close(pty);
    }

    #[test]
    fn a_finished_pty_reports_the_end_of_the_stream() {
        let host = MemoryPseudoconsole::new();
        let pty = host.create(WindowSize { cols: 80, rows: 24 }).unwrap();
        host.push_output(pty, b"tail");
        host.finish(pty);
        // Everything the app wrote is delivered first, and only then does the
        // stream end; neither is a read failure.
        assert_eq!(
            host.read_output(pty).unwrap().as_deref(),
            Some(b"tail".as_slice())
        );
        assert_eq!(host.read_output(pty).unwrap(), None);
        host.close(pty);
    }

    #[test]
    #[should_panic(expected = "live pty")]
    fn pushing_into_an_unknown_pty_is_a_mistake_the_test_hears_about() {
        MemoryPseudoconsole::new().push_output(PtyId(404), b"out");
    }
}
