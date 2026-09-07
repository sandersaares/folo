use std::fmt::Debug;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use crate::pal::linux::MockFilesystem;
use crate::pal::linux::{BuildTargetFilesystem, Filesystem};

/// Enum to hide the different filesystem implementations behind a single wrapper type.
#[derive(Clone)]
pub(crate) enum FilesystemFacade {
    Target(&'static BuildTargetFilesystem),

    #[cfg(test)]
    Mock(Arc<MockFilesystem>),
}

impl FilesystemFacade {
    pub(crate) const fn target() -> Self {
        Self::Target(&BuildTargetFilesystem)
    }

    #[cfg(test)]
    // Facade pass-through logic is not worth testing.
    #[cfg_attr(coverage_nightly, coverage(off))]
    pub(crate) fn from_mock(mock: MockFilesystem) -> Self {
        Self::Mock(Arc::new(mock))
    }
}

// Facade pass-through logic is not worth testing.
#[cfg_attr(coverage_nightly, coverage(off))]
impl Filesystem for FilesystemFacade {
    fn get_cpuinfo_contents(&self) -> String {
        match self {
            Self::Target(filesystem) => filesystem.get_cpuinfo_contents(),
            #[cfg(test)]
            Self::Mock(mock) => mock.get_cpuinfo_contents(),
        }
    }

    fn get_numa_node_cpulist_contents(&self, node_index: u32) -> Option<String> {
        match self {
            Self::Target(filesystem) => filesystem.get_numa_node_cpulist_contents(node_index),
            #[cfg(test)]
            Self::Mock(mock) => mock.get_numa_node_cpulist_contents(node_index),
        }
    }

    fn get_possible_cpus_contents(&self) -> Option<String> {
        match self {
            Self::Target(filesystem) => filesystem.get_possible_cpus_contents(),
            #[cfg(test)]
            Self::Mock(mock) => mock.get_possible_cpus_contents(),
        }
    }

    fn get_online_cpus_contents(&self) -> Option<String> {
        match self {
            Self::Target(filesystem) => filesystem.get_online_cpus_contents(),
            #[cfg(test)]
            Self::Mock(mock) => mock.get_online_cpus_contents(),
        }
    }

    fn get_cpu_online_contents(&self, cpu_index: u32) -> Option<String> {
        match self {
            Self::Target(filesystem) => filesystem.get_cpu_online_contents(cpu_index),
            #[cfg(test)]
            Self::Mock(mock) => mock.get_cpu_online_contents(cpu_index),
        }
    }

    fn get_numa_node_possible_contents(&self) -> Option<String> {
        match self {
            Self::Target(filesystem) => filesystem.get_numa_node_possible_contents(),
            #[cfg(test)]
            Self::Mock(mock) => mock.get_numa_node_possible_contents(),
        }
    }

    fn get_proc_self_status_contents(&self) -> String {
        match self {
            Self::Target(filesystem) => filesystem.get_proc_self_status_contents(),
            #[cfg(test)]
            Self::Mock(mock) => mock.get_proc_self_status_contents(),
        }
    }

    fn get_proc_self_cgroup(&self) -> Option<String> {
        match self {
            Self::Target(filesystem) => filesystem.get_proc_self_cgroup(),
            #[cfg(test)]
            Self::Mock(mock) => mock.get_proc_self_cgroup(),
        }
    }

    fn get_v1_cgroup_cpu_quota(&self, cgroup_name: &str) -> Option<String> {
        match self {
            Self::Target(filesystem) => filesystem.get_v1_cgroup_cpu_quota(cgroup_name),
            #[cfg(test)]
            Self::Mock(mock) => mock.get_v1_cgroup_cpu_quota(cgroup_name),
        }
    }

    fn get_v1_cgroup_cpu_period(&self, cgroup_name: &str) -> Option<String> {
        match self {
            Self::Target(filesystem) => filesystem.get_v1_cgroup_cpu_period(cgroup_name),
            #[cfg(test)]
            Self::Mock(mock) => mock.get_v1_cgroup_cpu_period(cgroup_name),
        }
    }

    fn get_v2_cgroup_cpu_quota_and_period(&self, cgroup_name: &str) -> Option<String> {
        match self {
            Self::Target(filesystem) => filesystem.get_v2_cgroup_cpu_quota_and_period(cgroup_name),
            #[cfg(test)]
            Self::Mock(mock) => mock.get_v2_cgroup_cpu_quota_and_period(cgroup_name),
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))] // No API contract to test.
impl Debug for FilesystemFacade {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Target(inner) => inner.fmt(f),
            #[cfg(test)]
            Self::Mock(inner) => inner.fmt(f),
        }
    }
}
