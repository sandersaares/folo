//! Facade over process PAL implementations.

use std::fmt;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::Arc;

use crate::durability::LauncherTie;
use crate::pal::error::PalError;
use crate::pal::ids::{AppId, JobId};
#[cfg(test)]
use crate::pal::processes::MockProcesses;
use crate::pal::processes::{
    AppSpawn, BuildTargetProcesses, ProcessLiveness, Processes, ResolvedCommand, SupervisorSpawn,
};
use crate::session_record::ProcessIdentity;

/// Dispatches process operations to the real PAL or a test mock.
#[derive(Clone)]
pub(crate) enum ProcessesFacade {
    /// Real platform implementation.
    Target(&'static BuildTargetProcesses),
    /// Mock for tests.
    #[cfg(test)]
    Mock(Arc<MockProcesses>),
}

static TARGET: BuildTargetProcesses = BuildTargetProcesses;

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl fmt::Debug for ProcessesFacade {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Target(_) => f.debug_struct("ProcessesFacade::Target").finish(),
            #[cfg(test)]
            Self::Mock(_) => f.debug_struct("ProcessesFacade::Mock").finish(),
        }
    }
}

impl ProcessesFacade {
    pub(crate) const fn target() -> Self {
        Self::Target(&TARGET)
    }

    #[cfg(test)]
    // Facade pass-through logic is not worth testing.
    #[cfg_attr(coverage_nightly, coverage(off))]
    #[cfg_attr(test, mutants::skip)]
    pub(crate) fn from_mock(mock: MockProcesses) -> Self {
        Self::Mock(Arc::new(mock))
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl Processes for ProcessesFacade {
    fn current_exe(&self) -> Result<PathBuf, PalError> {
        match self {
            Self::Target(inner) => inner.current_exe(),
            #[cfg(test)]
            Self::Mock(inner) => inner.current_exe(),
        }
    }

    fn spawn_supervisor(&self, request: &SupervisorSpawn) -> Result<ProcessIdentity, PalError> {
        match self {
            Self::Target(inner) => inner.spawn_supervisor(request),
            #[cfg(test)]
            Self::Mock(inner) => inner.spawn_supervisor(request),
        }
    }

    fn launcher_tie(&self) -> LauncherTie {
        match self {
            Self::Target(inner) => inner.launcher_tie(),
            #[cfg(test)]
            Self::Mock(inner) => inner.launcher_tie(),
        }
    }

    fn probe(&self, identity: &ProcessIdentity) -> ProcessLiveness {
        match self {
            Self::Target(inner) => inner.probe(identity),
            #[cfg(test)]
            Self::Mock(inner) => inner.probe(identity),
        }
    }

    fn terminate(&self, identity: &ProcessIdentity) -> Result<(), PalError> {
        match self {
            Self::Target(inner) => inner.terminate(identity),
            #[cfg(test)]
            Self::Mock(inner) => inner.terminate(identity),
        }
    }

    fn create_lifetime_job(&self) -> Result<JobId, PalError> {
        match self {
            Self::Target(inner) => inner.create_lifetime_job(),
            #[cfg(test)]
            Self::Mock(inner) => inner.create_lifetime_job(),
        }
    }

    fn close_job(&self, job: JobId) {
        match self {
            Self::Target(inner) => inner.close_job(job),
            #[cfg(test)]
            Self::Mock(inner) => inner.close_job(job),
        }
    }

    fn spawn_app(&self, request: &AppSpawn) -> Result<AppId, PalError> {
        match self {
            Self::Target(inner) => inner.spawn_app(request),
            #[cfg(test)]
            Self::Mock(inner) => inner.spawn_app(request),
        }
    }

    fn wait_app(&self, app: AppId) -> Result<i32, PalError> {
        match self {
            Self::Target(inner) => inner.wait_app(app),
            #[cfg(test)]
            Self::Mock(inner) => inner.wait_app(app),
        }
    }

    fn current_identity(&self) -> Result<ProcessIdentity, PalError> {
        match self {
            Self::Target(inner) => inner.current_identity(),
            #[cfg(test)]
            Self::Mock(inner) => inner.current_identity(),
        }
    }

    fn resolve_executable(&self, command: &str, launch_directory: &Path) -> ResolvedCommand {
        match self {
            Self::Target(inner) => inner.resolve_executable(command, launch_directory),
            #[cfg(test)]
            Self::Mock(inner) => inner.resolve_executable(command, launch_directory),
        }
    }

    fn random_nonce(&self) -> String {
        match self {
            Self::Target(inner) => inner.random_nonce(),
            #[cfg(test)]
            Self::Mock(inner) => inner.random_nonce(),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::hint::black_box;

    use super::*;
    use crate::pal::processes::ProcessLiveness;
    use crate::session_record::ProcessIdentity;

    #[test]
    fn target_constructor_executes_at_runtime() {
        assert!(matches!(
            black_box(ProcessesFacade::target()),
            ProcessesFacade::Target(_)
        ));
    }

    #[test]
    fn from_mock_dispatches_probe() {
        let mut mock = MockProcesses::new();
        mock.expect_probe().return_const(ProcessLiveness::Live);
        let facade = ProcessesFacade::from_mock(mock);
        assert_eq!(
            facade.probe(&ProcessIdentity {
                pid: 1,
                creation_time: 1,
            }),
            ProcessLiveness::Live
        );
        _ = format!("{facade:?}");
    }
}
