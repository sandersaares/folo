use crate::pal::windows::BindingsFacade;
use crate::pal::{Platform, TimeSourceImpl};

/// Singleton instance of `BuildTargetPlatform`, used by public API types
/// to hook up to the correct PAL implementation.
pub(crate) static BUILD_TARGET_PLATFORM: BuildTargetPlatform =
    BuildTargetPlatform::new(BindingsFacade::real());

#[derive(Debug)]
pub(crate) struct BuildTargetPlatform {
    bindings: BindingsFacade,
}

impl BuildTargetPlatform {
    pub(crate) const fn new(bindings: BindingsFacade) -> Self {
        Self { bindings }
    }
}

impl Platform for BuildTargetPlatform {
    type TimeSource = TimeSourceImpl;

    fn new_time_source(&self) -> Self::TimeSource {
        Self::TimeSource::new(self.bindings.clone())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::hint::black_box;
    use std::panic::{RefUnwindSafe, UnwindSafe};

    use super::*;

    static_assertions::assert_impl_all!(BuildTargetPlatform: UnwindSafe, RefUnwindSafe);

    #[test]
    fn constructors_execute_at_runtime() {
        let bindings = black_box(BindingsFacade::real());
        let platform = black_box(BuildTargetPlatform::new(bindings));

        assert!(matches!(platform.bindings, BindingsFacade::Real(_)));
    }
}
