//! Facade over local console PAL implementations.

use std::fmt;
#[cfg(test)]
use std::sync::Arc;

use crate::pal::error::PalError;
use crate::pal::ids::RelayLeaseId;
#[cfg(test)]
use crate::pal::local_console::MockLocalConsole;
use crate::pal::local_console::windows::BuildTargetConsole;
use crate::pal::local_console::{ConsoleInput, LocalConsole};
use crate::pal::pseudoconsole::WindowSize;

/// Dispatches console operations to the real PAL or a test mock.
#[derive(Clone)]
pub(crate) enum LocalConsoleFacade {
    /// Real platform implementation.
    Target(&'static BuildTargetConsole),
    /// Mock for tests.
    #[cfg(test)]
    Mock(Arc<MockLocalConsole>),
}

static TARGET: BuildTargetConsole = BuildTargetConsole;

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl fmt::Debug for LocalConsoleFacade {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Target(_) => f.debug_tuple(stringify!(Target)).finish(),
            #[cfg(test)]
            Self::Mock(_) => f.debug_tuple(stringify!(Mock)).finish(),
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl LocalConsoleFacade {
    pub(crate) const fn target() -> Self {
        Self::Target(&TARGET)
    }

    #[cfg(test)]
    pub(crate) fn from_mock(mock: MockLocalConsole) -> Self {
        Self::Mock(Arc::new(mock))
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl LocalConsole for LocalConsoleFacade {
    fn has_console(&self) -> bool {
        match self {
            Self::Target(inner) => inner.has_console(),
            #[cfg(test)]
            Self::Mock(inner) => inner.has_console(),
        }
    }

    fn stdin_is_terminal(&self) -> bool {
        match self {
            Self::Target(inner) => inner.stdin_is_terminal(),
            #[cfg(test)]
            Self::Mock(inner) => inner.stdin_is_terminal(),
        }
    }

    fn begin_raw_relay(&self) -> Result<RelayLeaseId, PalError> {
        match self {
            Self::Target(inner) => inner.begin_raw_relay(),
            #[cfg(test)]
            Self::Mock(inner) => inner.begin_raw_relay(),
        }
    }

    fn end_raw_relay(&self, lease: RelayLeaseId) -> Result<(), PalError> {
        match self {
            Self::Target(inner) => inner.end_raw_relay(lease),
            #[cfg(test)]
            Self::Mock(inner) => inner.end_raw_relay(lease),
        }
    }

    fn window_size(&self) -> Result<WindowSize, PalError> {
        match self {
            Self::Target(inner) => inner.window_size(),
            #[cfg(test)]
            Self::Mock(inner) => inner.window_size(),
        }
    }

    fn read_input(&self) -> Result<ConsoleInput, PalError> {
        match self {
            Self::Target(inner) => inner.read_input(),
            #[cfg(test)]
            Self::Mock(inner) => inner.read_input(),
        }
    }

    fn cancel_input(&self) -> Result<(), PalError> {
        match self {
            Self::Target(inner) => inner.cancel_input(),
            #[cfg(test)]
            Self::Mock(inner) => inner.cancel_input(),
        }
    }

    fn write_output(&self, data: &[u8]) -> Result<(), PalError> {
        match self {
            Self::Target(inner) => inner.write_output(data),
            #[cfg(test)]
            Self::Mock(inner) => inner.write_output(data),
        }
    }

    fn read_prompt_line(&self) -> Result<String, PalError> {
        match self {
            Self::Target(inner) => inner.read_prompt_line(),
            #[cfg(test)]
            Self::Mock(inner) => inner.read_prompt_line(),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn from_mock_dispatches_has_console() {
        let mut mock = MockLocalConsole::new();
        mock.expect_has_console().return_const(true);
        let facade = LocalConsoleFacade::from_mock(mock);
        assert!(facade.has_console());
        _ = format!("{facade:?}");
    }
}
