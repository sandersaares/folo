use std::fmt::Debug;
#[cfg(test)]
use std::sync::Arc;

use windows::Win32::System::JobObjects::JOBOBJECT_CPU_RATE_CONTROL_INFORMATION;
use windows::Win32::System::Kernel::PROCESSOR_NUMBER;
use windows::Win32::System::SystemInformation::{
    GROUP_AFFINITY, LOGICAL_PROCESSOR_RELATIONSHIP, SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX,
};
use windows::core::Result;

#[cfg(test)]
use crate::pal::windows::MockBindings;
use crate::pal::windows::{Bindings, BuildTargetBindings};

/// Hide the real/mock bindings choice behind a single type.
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
    fn get_active_processor_count(&self, group_number: u16) -> u32 {
        match self {
            Self::Target(bindings) => bindings.get_active_processor_count(group_number),
            #[cfg(test)]
            Self::Mock(bindings) => bindings.get_active_processor_count(group_number),
        }
    }

    fn get_maximum_processor_count(&self, group_number: u16) -> u32 {
        match self {
            Self::Target(bindings) => bindings.get_maximum_processor_count(group_number),
            #[cfg(test)]
            Self::Mock(bindings) => bindings.get_maximum_processor_count(group_number),
        }
    }

    fn get_maximum_processor_group_count(&self) -> u16 {
        match self {
            Self::Target(bindings) => bindings.get_maximum_processor_group_count(),
            #[cfg(test)]
            Self::Mock(bindings) => bindings.get_maximum_processor_group_count(),
        }
    }

    fn get_current_processor_number_ex(&self) -> PROCESSOR_NUMBER {
        match self {
            Self::Target(bindings) => bindings.get_current_processor_number_ex(),
            #[cfg(test)]
            Self::Mock(bindings) => bindings.get_current_processor_number_ex(),
        }
    }

    unsafe fn get_logical_processor_information_ex(
        &self,
        relationship_type: LOGICAL_PROCESSOR_RELATIONSHIP,
        buffer: Option<*mut SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX>,
        returned_length: *mut u32,
    ) -> Result<()> {
        match self {
            // SAFETY: Forwarding safety requirements to caller.
            Self::Target(bindings) => unsafe {
                bindings.get_logical_processor_information_ex(
                    relationship_type,
                    buffer,
                    returned_length,
                )
            },
            #[cfg(test)]
            // SAFETY: Forwarding safety requirements to caller.
            Self::Mock(bindings) => unsafe {
                bindings.get_logical_processor_information_ex(
                    relationship_type,
                    buffer,
                    returned_length,
                )
            },
        }
    }

    fn get_numa_highest_node_number(&self) -> u32 {
        match self {
            Self::Target(bindings) => bindings.get_numa_highest_node_number(),
            #[cfg(test)]
            Self::Mock(bindings) => bindings.get_numa_highest_node_number(),
        }
    }

    fn get_current_process_default_cpu_set_masks(&self) -> Vec<GROUP_AFFINITY> {
        match self {
            Self::Target(bindings) => bindings.get_current_process_default_cpu_set_masks(),
            #[cfg(test)]
            Self::Mock(bindings) => bindings.get_current_process_default_cpu_set_masks(),
        }
    }

    fn get_current_thread_cpu_set_masks(&self) -> Vec<GROUP_AFFINITY> {
        match self {
            Self::Target(bindings) => bindings.get_current_thread_cpu_set_masks(),
            #[cfg(test)]
            Self::Mock(bindings) => bindings.get_current_thread_cpu_set_masks(),
        }
    }

    fn set_current_thread_cpu_set_masks(&self, masks: &[GROUP_AFFINITY]) {
        match self {
            Self::Target(bindings) => bindings.set_current_thread_cpu_set_masks(masks),
            #[cfg(test)]
            Self::Mock(bindings) => bindings.set_current_thread_cpu_set_masks(masks),
        }
    }

    fn get_current_job_cpu_set_masks(&self) -> Vec<GROUP_AFFINITY> {
        match self {
            Self::Target(bindings) => bindings.get_current_job_cpu_set_masks(),
            #[cfg(test)]
            Self::Mock(bindings) => bindings.get_current_job_cpu_set_masks(),
        }
    }

    fn get_current_thread_legacy_group_affinity(&self) -> GROUP_AFFINITY {
        match self {
            Self::Target(bindings) => bindings.get_current_thread_legacy_group_affinity(),
            #[cfg(test)]
            Self::Mock(bindings) => bindings.get_current_thread_legacy_group_affinity(),
        }
    }

    fn get_current_job_cpu_rate_control(&self) -> Option<JOBOBJECT_CPU_RATE_CONTROL_INFORMATION> {
        match self {
            Self::Target(bindings) => bindings.get_current_job_cpu_rate_control(),
            #[cfg(test)]
            Self::Mock(bindings) => bindings.get_current_job_cpu_rate_control(),
        }
    }

    fn get_processor_max_mhz(&self, max_processor_count: usize) -> Vec<u32> {
        match self {
            Self::Target(bindings) => bindings.get_processor_max_mhz(max_processor_count),
            #[cfg(test)]
            Self::Mock(bindings) => bindings.get_processor_max_mhz(max_processor_count),
        }
    }

    fn get_processor_name_strings(&self, max_processor_count: usize) -> Vec<Option<String>> {
        match self {
            Self::Target(bindings) => bindings.get_processor_name_strings(max_processor_count),
            #[cfg(test)]
            Self::Mock(bindings) => bindings.get_processor_name_strings(max_processor_count),
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
