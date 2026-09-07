use std::fmt::Debug;
use std::io;
use std::num::NonZero;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use crate::pal::linux::MockBindings;
use crate::pal::linux::{Bindings, BuildTargetBindings, CpuMask};

/// Enum to hide the real/mock choice behind a single wrapper type.
#[derive(Clone)]
pub(crate) enum BindingsFacade {
    Target(&'static BuildTargetBindings),

    #[cfg(test)]
    Mock(Arc<MockBindings>),
}

impl BindingsFacade {
    pub(crate) const fn target() -> Self {
        Self::Target(&BuildTargetBindings)
    }

    #[cfg(test)]
    // Facade pass-through logic is not worth testing.
    #[cfg_attr(coverage_nightly, coverage(off))]
    pub(crate) fn from_mock(mock: MockBindings) -> Self {
        Self::Mock(Arc::new(mock))
    }
}

// Facade pass-through logic is not worth testing.
#[cfg_attr(coverage_nightly, coverage(off))]
impl Bindings for BindingsFacade {
    fn sched_setaffinity_current(&self, mask: &CpuMask) -> Result<(), io::Error> {
        match self {
            Self::Target(bindings) => bindings.sched_setaffinity_current(mask),
            #[cfg(test)]
            Self::Mock(mock) => mock.sched_setaffinity_current(mask),
        }
    }

    fn sched_getcpu(&self) -> i32 {
        match self {
            Self::Target(bindings) => bindings.sched_getcpu(),
            #[cfg(test)]
            Self::Mock(mock) => mock.sched_getcpu(),
        }
    }

    fn sched_getaffinity_current(&self, words: NonZero<usize>) -> Result<CpuMask, io::Error> {
        match self {
            Self::Target(bindings) => bindings.sched_getaffinity_current(words),
            #[cfg(test)]
            Self::Mock(mock) => mock.sched_getaffinity_current(words),
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))] // No API contract to test.
impl Debug for BindingsFacade {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Target(inner) => inner.fmt(f),
            #[cfg(test)]
            Self::Mock(inner) => inner.fmt(f),
        }
    }
}
