//! Facade over pseudoconsole PAL implementations.

use std::fmt;

use crate::pal::error::PalError;
use crate::pal::ids::PtyId;
#[cfg(test)]
use crate::pal::pseudoconsole::MemoryPseudoconsole;
use crate::pal::pseudoconsole::windows::BuildTargetPseudoconsole;
use crate::pal::pseudoconsole::{Pseudoconsole, WindowSize};

/// Dispatches pseudoconsole calls to the real PAL or an in-memory test host.
#[derive(Clone)]
pub(crate) enum PseudoconsoleFacade {
    /// Real platform implementation.
    Target(&'static BuildTargetPseudoconsole),
    /// In-memory host for unit tests.
    #[cfg(test)]
    Memory(MemoryPseudoconsole),
}

static TARGET: BuildTargetPseudoconsole = BuildTargetPseudoconsole;

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl fmt::Debug for PseudoconsoleFacade {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Target(_) => f.debug_struct("PseudoconsoleFacade::Target").finish(),
            #[cfg(test)]
            Self::Memory(_) => f.debug_struct("PseudoconsoleFacade::Memory").finish(),
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl PseudoconsoleFacade {
    pub(crate) const fn target() -> Self {
        Self::Target(&TARGET)
    }

    #[cfg(test)]
    pub(crate) fn from_memory(pty: MemoryPseudoconsole) -> Self {
        Self::Memory(pty)
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl Pseudoconsole for PseudoconsoleFacade {
    fn create(&self, size: WindowSize) -> Result<PtyId, PalError> {
        match self {
            Self::Target(inner) => inner.create(size),
            #[cfg(test)]
            Self::Memory(inner) => inner.create(size),
        }
    }

    fn resize(&self, pty: PtyId, size: WindowSize) -> Result<(), PalError> {
        match self {
            Self::Target(inner) => inner.resize(pty, size),
            #[cfg(test)]
            Self::Memory(inner) => inner.resize(pty, size),
        }
    }

    fn write_input(&self, pty: PtyId, data: &[u8]) -> Result<(), PalError> {
        match self {
            Self::Target(inner) => inner.write_input(pty, data),
            #[cfg(test)]
            Self::Memory(inner) => inner.write_input(pty, data),
        }
    }

    fn read_output(&self, pty: PtyId) -> Result<Option<Vec<u8>>, PalError> {
        match self {
            Self::Target(inner) => inner.read_output(pty),
            #[cfg(test)]
            Self::Memory(inner) => inner.read_output(pty),
        }
    }

    fn finish(&self, pty: PtyId) {
        match self {
            Self::Target(inner) => inner.finish(pty),
            #[cfg(test)]
            Self::Memory(inner) => inner.finish(pty),
        }
    }

    fn close(&self, pty: PtyId) {
        match self {
            Self::Target(inner) => inner.close(pty),
            #[cfg(test)]
            Self::Memory(inner) => inner.close(pty),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn from_memory_creates_pty() {
        let pty = MemoryPseudoconsole::new();
        let facade = PseudoconsoleFacade::from_memory(pty);
        let id = facade
            .create(WindowSize::new(80, 24).expect("a fixture size is not empty"))
            .unwrap();
        facade.close(id);
        _ = format!("{facade:?}");
    }
}
