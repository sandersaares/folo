use std::fmt::Debug;
#[cfg(test)]
use std::sync::Arc;
use std::time::Instant;

use crate::pal::windows::BuildTargetBindings;
#[cfg(test)]
use crate::pal::windows::MockBindings;
use crate::pal::windows::bindings::Bindings;

#[derive(Clone)]
pub(crate) enum BindingsFacade {
    Real(&'static BuildTargetBindings),

    #[cfg(test)]
    Mock(Arc<MockBindings>),
}

impl BindingsFacade {
    pub(crate) const fn real() -> Self {
        Self::Real(&BuildTargetBindings)
    }
}

// Facade types are trivial pass-through layers - not worth testing.
#[cfg_attr(coverage_nightly, coverage(off))]
impl Bindings for BindingsFacade {
    #[inline]
    fn get_tick_count_64(&self) -> u64 {
        match self {
            Self::Real(bindings) => bindings.get_tick_count_64(),
            #[cfg(test)]
            Self::Mock(bindings) => bindings.get_tick_count_64(),
        }
    }

    #[inline]
    fn now(&self) -> Instant {
        match self {
            Self::Real(bindings) => bindings.now(),
            #[cfg(test)]
            Self::Mock(bindings) => bindings.now(),
        }
    }
}

// Facade types are trivial pass-through layers - not worth testing.
#[cfg_attr(coverage_nightly, coverage(off))]
impl From<&'static BuildTargetBindings> for BindingsFacade {
    fn from(bindings: &'static BuildTargetBindings) -> Self {
        Self::Real(bindings)
    }
}

#[cfg(test)]
// Facade types are trivial pass-through layers - not worth testing.
#[cfg_attr(coverage_nightly, coverage(off))]
impl From<MockBindings> for BindingsFacade {
    fn from(bindings: MockBindings) -> Self {
        Self::Mock(Arc::new(bindings))
    }
}

#[cfg_attr(coverage_nightly, coverage(off))] // No API contract to test.
impl Debug for BindingsFacade {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Real(bindings) => bindings.fmt(f),
            #[cfg(test)]
            Self::Mock(bindings) => bindings.fmt(f),
        }
    }
}
