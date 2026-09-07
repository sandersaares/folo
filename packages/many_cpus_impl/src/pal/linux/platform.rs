use std::borrow::Cow;
use std::iter::{self, once};
use std::num::NonZero;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use foldhash::HashMap;
use itertools::Itertools;
use new_zealand::nz;
use nonempty::NonEmpty;

use crate::pal::linux::filesystem::FilesystemFacade;
use crate::pal::linux::{Bindings, BindingsFacade, CpuMask, Filesystem};
use crate::pal::{Platform, ProcessorFacade, ProcessorImpl};
use crate::{EfficiencyClass, MemoryRegionId, ProcessorId, RelativeSpeed};

/// Singleton instance of `BuildTargetPlatform`, used by public API types
/// to hook up to the correct PAL implementation.
pub(crate) static BUILD_TARGET_PLATFORM: BuildTargetPlatform =
    BuildTargetPlatform::new(BindingsFacade::target(), FilesystemFacade::target());

/// The platform that matches the crate's build target.
///
/// You would only use a different platform in unit tests that need to mock the platform.
/// Even then, whenever possible, unit tests should use the real platform for maximum realism.
#[derive(Debug)]
pub(crate) struct BuildTargetPlatform {
    bindings: BindingsFacade,
    fs: FilesystemFacade,

    // Including inactive.
    all_processors: OnceLock<NonEmpty<ProcessorImpl>>,
    max_processor_id: OnceLock<ProcessorId>,
    max_memory_region_id: OnceLock<MemoryRegionId>,

    // Only active.
    all_active_processors: OnceLock<NonEmpty<ProcessorFacade>>,

    // System-wide, so not narrowed by what the current process may use.
    active_processor_count: OnceLock<NonZero<usize>>,

    // The kernel's ID space masks, as published in /sys. Each is read and interpreted once, so
    // that every derivation made from a mask is made from the same reading of it - two
    // derivations that disagreed about whether a mask is usable would describe a machine that
    // does not exist (for example, more active processors than the ID space has room for).
    possible_processor_ids: OnceLock<Option<NonEmpty<ProcessorId>>>,
    online_processor_ids: OnceLock<Option<NonEmpty<ProcessorId>>>,
    possible_memory_region_ids: OnceLock<Option<NonEmpty<MemoryRegionId>>>,

    /// Width, in machine words, of the affinity mask that the operating system last accepted.
    ///
    /// This is a hint and not a conclusion: the required width can grow while the process runs,
    /// so a width that stops working merely sends the search widening again from there.
    affinity_mask_words: AtomicUsize,
}

/// The value of `affinity_mask_words` before the operating system has accepted any width.
///
/// A mask is never zero words wide, so zero cannot be mistaken for a width that worked.
const AFFINITY_MASK_WIDTH_UNKNOWN: usize = 0;

/// How many affinity mask widths to try before giving up on reading a thread's affinity.
///
/// Each attempt doubles the width of the previous one, starting from a mask that already covers
/// every processor that the platform's own fixed-size mask can describe, so the final attempt
/// describes a machine far larger than operating systems support. The limit exists to guarantee
/// that the search ends. Ref: `packages/many_cpus/docs/linux.md`, "Thread affinity masks".
const AFFINITY_MASK_ATTEMPTS: usize = 11;

/// Ratio between one affinity mask width that the operating system rejected and the next one to
/// try. Doubling keeps the number of attempts logarithmic in the size of the machine.
const AFFINITY_MASK_GROWTH: NonZero<usize> = nz!(2);

impl Platform for BuildTargetPlatform {
    fn get_all_processors(&self) -> NonEmpty<ProcessorFacade> {
        self.get_active_processors().clone()
    }

    fn pin_current_thread_to<P>(&self, processors: &NonEmpty<P>)
    where
        P: AsRef<ProcessorFacade>,
    {
        let mut mask = CpuMask::new();

        for processor in processors.iter() {
            mask.insert(processor.as_ref().as_target().id);
        }

        self.bindings
            .sched_setaffinity_current(&mask)
            .expect("failed to configure thread affinity");
    }

    #[expect(
        clippy::cast_sign_loss,
        reason = "negative processor IDs are not valid regardless, we do not expect to receive them"
    )]
    fn current_processor_id(&self) -> ProcessorId {
        self.bindings.sched_getcpu() as ProcessorId
    }

    fn max_processor_id(&self) -> ProcessorId {
        self.get_max_processor_id()
    }

    fn max_memory_region_id(&self) -> MemoryRegionId {
        self.get_max_memory_region_id()
    }

    fn current_thread_processors(&self) -> NonEmpty<ProcessorId> {
        let max_processor_id = self.get_max_processor_id();

        let affinity = self.get_current_thread_affinity();

        NonEmpty::from_vec(
            affinity
                .processor_ids()
                .filter(|processor_id| *processor_id <= max_processor_id)
                .collect_vec())
                .expect("current thread has no processors in its affinity mask - impossible because this code is running on an active processor")
    }

    fn max_processor_time(&self) -> f64 {
        // This is our ceiling - we cannot use more processor time than the number of processors.
        #[expect(
            clippy::cast_precision_loss,
            reason = "all realistic values are in safe bounds"
        )]
        let max_processor_time = self.get_all_processors().len() as f64;

        // If we are constrained by a cgroup, the ceiling may be lowered.
        if let Some(cgroup_max_processor_time) = self.cgroups_max_processor_time() {
            // We are allowed to use at most the minimum of the two.
            return max_processor_time.min(cgroup_max_processor_time);
        }

        max_processor_time
    }

    fn active_processor_count(&self) -> usize {
        self.get_active_processor_count().get()
    }
}

impl BuildTargetPlatform {
    pub(crate) const fn new(bindings: BindingsFacade, fs: FilesystemFacade) -> Self {
        Self {
            bindings,
            fs,
            all_processors: OnceLock::new(),
            all_active_processors: OnceLock::new(),
            max_processor_id: OnceLock::new(),
            max_memory_region_id: OnceLock::new(),
            active_processor_count: OnceLock::new(),
            possible_processor_ids: OnceLock::new(),
            online_processor_ids: OnceLock::new(),
            possible_memory_region_ids: OnceLock::new(),
            affinity_mask_words: AtomicUsize::new(AFFINITY_MASK_WIDTH_UNKNOWN),
        }
    }

    /// Reads the set of processors that the current thread is allowed to run on.
    ///
    /// The operating system refuses to fill an affinity mask that is too narrow to describe
    /// every processor that it knows of, without saying how wide the mask needs to be, so the
    /// only way to learn the required width is to offer wider and wider masks until one is
    /// accepted. Ref: `packages/many_cpus/docs/linux.md`, "Thread affinity masks".
    fn get_current_thread_affinity(&self) -> CpuMask {
        let mut last_error = None;

        for words in self.affinity_mask_widths() {
            match self.bindings.sched_getaffinity_current(words) {
                Ok(mask) => {
                    self.affinity_mask_words
                        .store(words.get(), Ordering::Relaxed);

                    return mask;
                }
                // A mask that is too narrow is rejected as an invalid argument. Other causes of
                // this error exist, so a wider mask is merely the most likely remedy and not a
                // certain one - which is why the error is preserved for the final report.
                Err(error) if error.raw_os_error() == Some(libc::EINVAL) => {
                    last_error = Some(error);
                }
                Err(error) => panic!("failed to get current thread processor affinity: {error}"),
            }
        }

        panic!(
            "failed to get current thread processor affinity, even with a mask wider than any operating system can fill: {}",
            last_error.expect("the search only ends without a mask once an attempt has failed")
        );
    }

    /// The affinity mask widths to offer the operating system, in the order to offer them.
    fn affinity_mask_widths(&self) -> impl Iterator<Item = NonZero<usize>> {
        // A width that worked before is likely to work again, so we start there. The machine can
        // grow while the process runs, so this is only a starting point.
        let first = NonZero::new(self.affinity_mask_words.load(Ordering::Relaxed))
            .unwrap_or_else(CpuMask::default_words);

        iter::successors(Some(first), |words| words.checked_mul(AFFINITY_MASK_GROWTH))
            .take(AFFINITY_MASK_ATTEMPTS)
    }

    fn get_all_processors_impl(&self) -> &NonEmpty<ProcessorImpl> {
        self.all_processors
            .get_or_init(|| self.load_all_processors())
    }

    fn get_active_processors(&self) -> &NonEmpty<ProcessorFacade> {
        self.all_active_processors.get_or_init(|| {
            NonEmpty::from_vec(
                self.get_all_processors_impl()
                    .iter()
                    .filter(|p| p.is_active)
                    .cloned()
                    .map(ProcessorFacade::Target)
                    .collect_vec())
                    .expect("found 0 active processors - impossible because this code is running on an active processor")
        })
    }

    fn get_max_memory_region_id(&self) -> MemoryRegionId {
        *self.max_memory_region_id.get_or_init(|| {
            // The ID space of memory regions is what the kernel says could possibly exist, which
            // is what `Platform::max_memory_region_id()` promises: a constant that covers offline
            // regions and regions this process may not use.
            //
            // A kernel that discloses no usable node mask describes a machine with no NUMA
            // topology for us to read, which `load_all_processors()` treats as the single memory
            // region that every processor belongs to. That region is then the whole ID space.
            // Both readings are made from the same interpretation of the mask, so they always
            // agree on whether there is a topology to read.
            self.get_possible_memory_region_ids()
                .map_or(SINGLE_MEMORY_REGION_ID, |ids| *ids.maximum())
        })
    }

    fn get_max_processor_id(&self) -> ProcessorId {
        *self.max_processor_id.get_or_init(|| {
            // The ID space of processors is what the kernel says could possibly exist. The
            // kernel fixes that set at boot, so the value stays constant as processors go
            // offline and regardless of which processors this process may use - which is what
            // `Platform::max_processor_id()` promises.
            if let Some(possible_processors) = self.get_possible_processor_ids() {
                return *possible_processors.maximum();
            }

            // A kernel that publishes no possible mask may still publish the online one, which
            // describes the machine just as system-wide a fact. It is the next best evidence
            // because it is also where the count of active processors comes from: deriving the
            // two from different sources could place the count outside the ID space, which is a
            // machine that cannot exist.
            if let Some(online_processors) = self.get_online_processor_ids() {
                return *online_processors.maximum();
            }

            // A kernel that publishes no readable mask at all leaves us with the machine we
            // managed to enumerate. That ID space is narrower than the contract asks for, but it
            // is the widest one we have evidence for and it still covers every processor a
            // caller can observe through this package.
            self.get_all_processors_impl()
                .iter()
                .map(|p| p.id)
                .max()
                .expect("NonEmpty always has at least one item")
        })
    }

    fn get_active_processor_count(&self) -> NonZero<usize> {
        *self.active_processor_count.get_or_init(|| {
            // The count is a fact about the machine, not about this process, so it comes from
            // the system-wide online mask rather than from the processors we enumerated (which
            // are narrowed to those the process may use).
            if let Some(online_processors) = self.get_online_processor_ids() {
                return NonZero::new(online_processors.len())
                    .expect("NonEmpty always has at least one item");
            }

            // A kernel that publishes no readable mask leaves us with the processors we
            // enumerated. They are all online, so counting them undercounts the machine at
            // worst, which beats reporting nothing at all.
            NonZero::new(self.get_active_processors().len())
                .expect("NonEmpty always has at least one item")
        })
    }

    /// The processors that could possibly exist in the system, or `None` when the kernel
    /// discloses no usable mask.
    fn get_possible_processor_ids(&self) -> Option<&NonEmpty<ProcessorId>> {
        self.possible_processor_ids
            .get_or_init(|| parse_kernel_id_list(self.fs.get_possible_cpus_contents()))
            .as_ref()
    }

    /// The processors that are currently online across the whole system, or `None` when the
    /// kernel discloses no usable mask.
    fn get_online_processor_ids(&self) -> Option<&NonEmpty<ProcessorId>> {
        self.online_processor_ids
            .get_or_init(|| parse_kernel_id_list(self.fs.get_online_cpus_contents()))
            .as_ref()
    }

    /// The memory regions that could possibly exist in the system, or `None` when the kernel
    /// discloses no usable mask - which is the machine that discloses no NUMA topology at all.
    fn get_possible_memory_region_ids(&self) -> Option<&NonEmpty<MemoryRegionId>> {
        self.possible_memory_region_ids
            .get_or_init(|| parse_kernel_id_list(self.fs.get_numa_node_possible_contents()))
            .as_ref()
    }

    fn load_all_processors(&self) -> NonEmpty<ProcessorImpl> {
        // There are two main ways to get processor information on Linux:
        // 1. Use various APIs to get the information as objects.
        // 2. Parse files in the /sys and /proc virtual filesystem.
        //
        // The former is "nicer" but requires more code and annoying FFI calls and working with
        // native Linux libraries, which is always troublesome because there is often a klunky
        // extra layer between the operating system and the app (e.g. libnuma, libcpuset, ...).
        //
        // To keep things simple, we will go with the latter.
        //
        // We need to combine multiple sources of information.
        // 1. /proc/cpuinfo gives us the set of processors available.
        // 2. /sys/devices/system/node/node*/cpulist gives us the processors in each NUMA node.
        // 3. /sys/devices/system/cpu/cpu*/online says whether a processor is online.
        // 4. /proc/self/status gives us the set of processors allowed for the current process.
        // Note: /sys/devices/system/node may be missing if there is only one NUMA node.
        //
        // The extent of the ID space is a separate question that none of these files answer -
        // see `get_max_processor_id()` and `get_max_memory_region_id()`.
        let cpu_infos = self.get_cpuinfo();
        let numa_nodes = self.get_numa_nodes();
        let allowed_processors = self.get_processors_allowed_for_current_process();

        // Just filter out disallowed processors right away.
        let cpu_infos = NonEmpty::from_vec(cpu_infos
            .into_iter()
            .filter(|info| allowed_processors.contains(&info.index))
            .collect_vec()).expect("found no allowed processors after filtering out forbidden processors - so how is this code even executing?");

        // If we did not get any NUMA node info, construct an imaginary NUMA node containing all.
        let numa_nodes = numa_nodes.unwrap_or_else(|| {
            once((
                SINGLE_MEMORY_REGION_ID,
                cpu_infos.clone().map(|info| info.index),
            ))
            .collect()
        });

        // We identify efficiency cores by comparing the bogomips of each processor to the maximum
        // bogomips of all processors. If the bogomips is less than the maximum, we consider it an
        // efficiency core. A processor whose bogomips the kernel does not disclose is not known to
        // be slower than any other, so it is never demoted on that basis - a kernel that discloses
        // none at all therefore reports a machine of a single efficiency class, which is what a
        // uniform machine looks like in any case.
        let max_bogomips = cpu_infos.iter().filter_map(|info| info.bogomips).max();

        let mut processors = cpu_infos.map(|info| {
            let memory_region = numa_nodes
                .iter()
                .find_map(|(node, node_processors)| {
                    if node_processors.contains(&info.index) {
                        return Some(*node);
                    }

                    None
                })
                // A processor that no node claims is not a machine we may refuse to describe, so
                // it belongs to the region that every processor belongs to on a machine with no
                // disclosed topology. Two situations reach here. The member list of each node
                // and /proc/cpuinfo both name only the online processors and are read at two
                // different instants, so a processor onlined or offlined in between is named by
                // one and not the other - letting an unrelated hotplug event take down a process
                // that merely asked about hardware is not an option. A kernel that lists an
                // offline processor in /proc/cpuinfo at all also reaches here, because no node
                // lists that processor either.
                .unwrap_or(SINGLE_MEMORY_REGION_ID);

            let is_slower_than_the_fastest = info
                .bogomips
                .zip(max_bogomips)
                .is_some_and(|(bogomips, max_bogomips)| bogomips < max_bogomips);

            let efficiency_class = if is_slower_than_the_fastest {
                EfficiencyClass::Efficiency
            } else {
                EfficiencyClass::Performance
            };

            // Mainstream kernels drop a processor from /proc/cpuinfo the moment it goes offline,
            // so this check normally finds every enumerated processor online. We keep it because
            // the per-processor file is the kernel's authoritative answer and a kernel that does
            // list an offline processor must not have it reported as usable.
            //
            // Some Linux flavors do not report this at all, and mainstream kernels omit it for
            // processor 0 because that processor cannot be taken offline, so an absent file
            // means online.
            let is_online = self
                .fs
                .get_cpu_online_contents(info.index)
                .is_none_or(|s| s.trim() == "1");

            ProcessorImpl {
                id: info.index,
                memory_region_id: memory_region,
                // An undisclosed metric is exactly what `UNDETERMINED` exists to report, matching
                // what Windows reports for a processor whose frequency it withholds.
                relative_speed: info
                    .bogomips
                    .map_or(RelativeSpeed::UNDETERMINED, RelativeSpeed::from_os_metric),
                efficiency_class,
                model: info.model,
                is_active: is_online,
            }
        });

        // We must return the processors sorted by global index. While the above logic may
        // already ensure this as a side-effect, we will sort here explicitly to be sure.
        processors.sort();

        processors
    }

    fn get_cpuinfo(&self) -> NonEmpty<CpuInfo> {
        let cpuinfo = self.fs.get_cpuinfo_contents();
        let lines = cpuinfo.lines();

        // Process groups of lines delimited by empty lines.
        NonEmpty::from_vec(
            lines
                .map(str::trim)
                .chunk_by(|l| l.is_empty())
                .into_iter()
                .filter_map(|(is_empty, lines)| {
                    if is_empty {
                        return None;
                    }

                    // This line gives us the processor index:
                    // processor       : 29
                    //
                    // This line gives us the processor bogomips:
                    // bogomips        : 4890.85
                    //
                    // We use bogomips instead of "cpu MHz" because cpu MHz reports the current
                    // dynamic frequency which fluctuates due to power management and thermal
                    // throttling, leading to unreliable efficiency class detection. Bogomips
                    // provides a stable measure of processor capability that remains consistent.
                    // The field is architecture-dependent and some kernels emit none at all, so
                    // its absence describes a machine we can still enumerate, not a failure.
                    //
                    // These lines identify the processor model, of which any subset may be
                    // present depending on the architecture:
                    // model name      : AMD EPYC 9V74 80-Core Processor
                    // CPU implementer : 0x41
                    // CPU part        : 0xd0c
                    //
                    // See `synthesize_model()` for how they combine into one model.
                    //
                    // All other lines we ignore.

                    let mut index = None;
                    let mut bogomips = None;
                    let mut model = None;
                    let mut implementer = None;
                    let mut part = None;

                    for line in lines {
                        let (key, value) = line
                            .split_once(':')
                            .map(|(key, value)| (key.trim(), value.trim()))
                            .expect("/proc/cpuinfo line was not a key:value pair");

                        // The Linux kernel may use different casing for keys depending on the processor
                        // architecture and kernel version. We normalize to lowercase for consistent matching.
                        //
                        // A blank value tells us nothing that an absent field does not already tell
                        // us, so every optional field below rejects blank values as if unset.
                        #[expect(clippy::cast_sign_loss, clippy::cast_possible_truncation, reason = "we expect small positive numbers for bogomips, which can have their integer part losslessly converted to u32")]
                        match key.to_ascii_lowercase().as_str() {
                            // Only the absence of this key may make us skip a record. A key that
                            // is present but unreadable identifies a processor we failed to
                            // understand, and dropping such a record would undercount the
                            // processors while looking exactly like a healthy read to the caller.
                            CPUINFO_KEY_PROCESSOR => {
                                index = Some(value.parse::<ProcessorId>().expect(
                                    "the kernel renders the processor index as a decimal number",
                                ));
                            }
                            CPUINFO_KEY_BOGOMIPS => {
                                bogomips = value.parse::<f32>().map(|f| f.round() as u32).ok();
                            }
                            CPUINFO_KEY_MODEL_NAME if !value.is_empty() => {
                                model = Some(Arc::from(value));
                            }
                            CPUINFO_KEY_IMPLEMENTER if !value.is_empty() => {
                                implementer = Some(value);
                            }
                            CPUINFO_KEY_PART if !value.is_empty() => part = Some(value),
                            _ => {}
                        }
                    }

                    // Some architectures close the file with a blank-line-separated block of
                    // machine-level facts (hardware name, board revision, serial number) that
                    // describes no processor at all. Such a block carries no processor index,
                    // which is how we recognize it and skip it. A record that does carry the key
                    // has already been resolved to an index or failed loudly above, so this is
                    // the only record we may drop.
                    let index = index?;

                    Some(CpuInfo {
                        index,
                        bogomips,
                        // A kernel-provided model is the most informative identification available,
                        // so we only assemble one ourselves when the kernel provides none.
                        model: model.or_else(|| synthesize_model(implementer, part)),
                    })
                })
                .collect_vec(),
        )
        .expect("must have at least one processor in /proc/cpuinfo to function")
    }

    fn get_processors_allowed_for_current_process(&self) -> NonEmpty<ProcessorId> {
        // On Linux, mechanisms like cgroups may limit what processors we are allowed to use.
        // Attempting to pin a thread to forbidden processors will fail. We want to avoid even
        // showing such processors, so we filter them out. The allowed list is in /proc/.../status.

        let status = self.fs.get_proc_self_status_contents();
        let lines = status.lines();

        let cpus_allowed_list = lines
            .into_iter()
            .map(str::trim)
            .filter_map(|line| {
                if line.is_empty() {
                    // There do not seem to be empty lines in this file but just in case.
                    return None;
                }

                // Example content:
                // Speculation_Store_Bypass:       thread vulnerable
                // SpeculationIndirectBranch:      conditional enabled
                // Cpus_allowed:   ffffffff
                // Cpus_allowed_list:      0-31
                // Mems_allowed:   1
                // Mems_allowed_list:      0
                // voluntary_ctxt_switches:        3
                // nonvoluntary_ctxt_switches:     0

                let (key, value) = line
                    .split_once(':')
                    .map(|(key, value)| (key.trim(), value.trim()))
                    .expect("/proc/self/status line was not a key:value pair");

                if key == "Cpus_allowed_list" {
                    return Some(value);
                }

                None
            })
            .take(1)
            .collect_vec();

        let cpus_allowed_list = cpus_allowed_list
            .first()
            .expect("Cpus_allowed_list not found in /proc/self/status");

        NonEmpty::from_vec(
            cpulist::parse(cpus_allowed_list)
                .expect("platform provided invalid cpulist in Cpus_allowed_list"),
        )
        .expect(
            "platform provided empty cpulist in Cpus_allowed_list - at least one must be allowed",
        )
    }

    /// The processors of each NUMA node the kernel says could possibly exist, or `None` when the
    /// kernel discloses no node mask we can read a single ID out of - the machine with no NUMA
    /// topology for us to read.
    ///
    /// A node that holds no online processor maps no processor to itself, so the result can be
    /// empty and need not claim every processor the machine enumerates.
    fn get_numa_nodes(&self) -> Option<HashMap<MemoryRegionId, NonEmpty<ProcessorId>>> {
        // The same interpretation of the node mask that `get_max_memory_region_id()` derives the
        // ID space from, so a mask that names no node we can read means "no topology" to both.
        let node_indexes = self.get_possible_memory_region_ids()?;

        Some(
            node_indexes
                .iter()
                .filter_map(|node| {
                    // A node that could possibly exist need not hold any online processor, in
                    // which case the kernel publishes no member list for it or an empty one.
                    // Such a node maps no processor to itself while still occupying an ID in the
                    // memory region ID space - see `get_max_memory_region_id()`.
                    let cpulist_str = self.fs.get_numa_node_cpulist_contents(*node)?;
                    let cpulist = NonEmpty::from_vec(
                        cpulist::parse(cpulist_str.trim())
                            .expect("platform provided invalid cpulist for NUMA node members"),
                    )?;

                    Some((*node, cpulist))
                })
                .collect(),
        )
    }

    /// Processor time limit in processor-seconds per second.
    fn cgroups_max_processor_time(&self) -> Option<f64> {
        let name = self.fs.get_proc_self_cgroup().and_then(parse_cgroup_name)?;

        #[expect(
            clippy::cast_precision_loss,
            reason = "unavoidable but also unlikely since typical values will be in safe bounds"
        )]
        self.get_cgroup_cpu_quota_and_period_us(&name)
            .map(|(quota, period)| {
                let quota = quota as f64;
                let period = period as f64;

                // If there is a zero in either field, we just accept what the platform is
                // telling us. It is nonsense but if the platform gives us nonsense, we
                // should eat it. A conversion down the line will probably convert this
                // to an integer count of processors (if used), which will be 0 either way
                // as NaN is converted to 0 on integer conversion. This 0 will presumbly
                // signal an error along the lines of "you cannot have 0 processors". Not
                // worth spending our code and tests on such bizarre lies from the platform.
                quota / period
            })
    }

    /// Gets the cgroup CPU quota and period for the given cgroup name.
    ///
    /// Probes both v1 and v2 cgroup APIs and returns data from the highest version available.
    /// Returns `None` if the cgroup does not exist or if a limit is not set.
    fn get_cgroup_cpu_quota_and_period_us(&self, name: &str) -> Option<(u64, u64)> {
        self.get_v2_cgroup_cpu_quota_and_period_us(name)
            .or_else(|| self.get_v1_cgroup_cpu_quota_and_period_us(name))
    }

    fn get_v2_cgroup_cpu_quota_and_period_us(&self, name: &str) -> Option<(u64, u64)> {
        let contents = self.fs.get_v2_cgroup_cpu_quota_and_period(name)?;
        parse_v2_cgroup_cpu_quota_and_period_us(&contents)
    }

    fn get_v1_cgroup_cpu_quota_and_period_us(&self, name: &str) -> Option<(u64, u64)> {
        let quota_contents = self.fs.get_v1_cgroup_cpu_quota(name)?;
        let period_contents = self.fs.get_v1_cgroup_cpu_period(name)?;

        parse_v1_cgroup_cpu_quota_and_period_us(&quota_contents, &period_contents)
    }
}

// One result from /proc/cpuinfo.
#[derive(Clone, Debug)]
struct CpuInfo {
    index: ProcessorId,

    /// CPU bogomips value, rounded to nearest integer. We use this to identify efficiency versus
    /// performance cores, where the processors with max bogomips are considered performance
    /// cores and any with lower bogomips are considered efficiency cores. `None` when the kernel
    /// discloses no readable value - the field is a Linux convenience whose presence depends on
    /// the processor architecture, and it feeds nothing but heuristics about relative core speed,
    /// so refusing to enumerate a machine over its absence would deny the caller every capability
    /// of the package to protect one heuristic.
    bogomips: Option<u32>,

    /// Best-effort model, either as reported by the kernel or synthesized from the identity
    /// fields the kernel reports instead. `None` when the record identifies the processor in
    /// no way we recognize.
    model: Option<Arc<str>>,
}

// Keys of the /proc/cpuinfo fields we read, in the lowercased form we normalize keys to before
// matching, because the kernel casing varies by architecture and kernel version.
const CPUINFO_KEY_PROCESSOR: &str = "processor";
const CPUINFO_KEY_BOGOMIPS: &str = "bogomips";
const CPUINFO_KEY_MODEL_NAME: &str = "model name";
const CPUINFO_KEY_IMPLEMENTER: &str = "cpu implementer";
const CPUINFO_KEY_PART: &str = "cpu part";

/// Marks a model string as assembled by us out of separate /proc/cpuinfo fields, as opposed to
/// being reported as a whole by the kernel.
const SYNTHESIZED_MODEL_PREFIX: &str = "cpuinfo";

/// The memory region every processor belongs to on a machine whose kernel discloses no NUMA
/// topology, which is also then the only ID in the memory region ID space. It is additionally
/// where a processor lands when the kernel does disclose a topology but no node claims that
/// processor - see `load_all_processors()`.
const SINGLE_MEMORY_REGION_ID: MemoryRegionId = 0;

// A `model name` field is not universal: a 64-bit ARM kernel emits one only when the reading
// process has a 32-bit personality, so a native 64-bit process sees none at all. Such kernels
// describe the processor through numeric identity fields instead, and reading `model name` alone
// therefore leaves us with no model on that hardware - every such machine then looks alike to
// consumers that use the model to tell hardware apart (for example, to decide whether two
// benchmark results describe the same silicon).
//
// `CPU implementer` (the vendor) and `CPU part` (the core design) are together the identity ARM
// defines for a core and are what tools such as `lscpu` translate into a human-readable name. That
// pair is exactly what separates one ARM core design from another, so it is what we synthesize
// from. We do not translate the values into vendor and core names ourselves: such a mapping is a
// large table that goes stale with every newly released core, and naming a core wrongly is worse
// than showing its raw identity, which is always faithful and always distinguishes what needs
// distinguishing.
//
// The same records also carry `CPU variant` and `CPU revision`, which we deliberately ignore. They
// identify the stepping of an individual chip - a finer distinction than an x86 `model name` draws,
// as an x86 model string carries no stepping. Including them would make two otherwise identical
// machines look like different hardware and would split consumers' per-model data sets more finely
// than the x86 equivalent does.
/// Builds a model string from the identity fields of one /proc/cpuinfo record.
///
/// Returns `None` when the record carries neither field, as there is then nothing to identify the
/// processor with. A single field is still worth reporting - partial discrimination between
/// different hardware beats none.
fn synthesize_model(implementer: Option<&str>, part: Option<&str>) -> Option<Arc<str>> {
    // Values are re-rendered from the number they carry rather than copied verbatim. The kernel
    // pads each field to a width of its own choosing, which is a formatting decision and not
    // something it promises us; were it to change, the model would move while the hardware stood
    // still and consumers that partition data by model would see their partitions fork. See
    // `canonical_identity_value()`.
    //
    // Naming the source field of each value keeps the origin traceable back to the file. It also
    // means the result cannot be mistaken for a kernel-provided `model name`, which is always
    // human prose such as `AMD EPYC 9V74 80-Core Processor`.
    let fields = [
        (CPUINFO_KEY_IMPLEMENTER, implementer),
        (CPUINFO_KEY_PART, part),
    ]
    .into_iter()
    .filter_map(|(key, value)| {
        value.map(|value| format!("{key}={}", canonical_identity_value(value)))
    })
    .join(", ");

    if fields.is_empty() {
        return None;
    }

    Some(Arc::from(format!("{SYNTHESIZED_MODEL_PREFIX}({fields})")))
}

/// Renders one identity field value in a form that depends only on the number it carries.
///
/// Every spelling of one value therefore yields one model string, so nothing about how the kernel
/// chose to write the value down can reach a consumer that tells hardware apart by model. The
/// canonical form keeps the base the kernel writes in, as that is what makes the value
/// recognizable against the kernel's own output, and drops the padding, as the digit count is
/// precisely the part of the rendering the kernel picks per field.
///
/// A value we cannot interpret is carried through as it came: an unfamiliar rendering still
/// distinguishes one core design from another, which beats discarding it.
fn canonical_identity_value(value: &str) -> Cow<'_, str> {
    const HEX_PREFIX: &str = "0x";
    const HEX_RADIX: u32 = 16;

    value
        .split_at_checked(HEX_PREFIX.len())
        .filter(|(prefix, _)| prefix.eq_ignore_ascii_case(HEX_PREFIX))
        .and_then(|(_, digits)| u64::from_str_radix(digits, HEX_RADIX).ok())
        .map_or(Cow::Borrowed(value), |number| {
            Cow::Owned(format!("{number:#x}"))
        })
}

/// Reads one of the kernel's cpulist-format ID masks into the IDs it names.
///
/// The kernel publishes the extent of the processor and memory region ID spaces as such masks
/// (for example `/sys/devices/system/cpu/possible`), which is where the values promised to be
/// constant and system-wide come from.
///
/// Returns `None` when the platform publishes no such file, publishes something we cannot read,
/// or names no ID at all. Every caller then falls back to describing the machine from whatever
/// other evidence it has, because a machine we can describe imperfectly is worth more to a caller
/// than a machine we refuse to describe.
fn parse_kernel_id_list(contents: Option<String>) -> Option<NonEmpty<u32>> {
    let ids = cpulist::parse(contents?.trim()).ok()?;

    NonEmpty::from_vec(ids)
}

/// This is the relative path of the cgroup the current process belongs to (e.g. `/foo/bar`)
/// or `None` if no cgroup is assigned.
///
/// The content a plaintest file with one line for each (sub)process visible to the process.
///
/// ```text
/// 17:cpuset:/docker/6a74f501e3b4c9d93ad440a7b73149cf2b5d56073c109a8d774c0793f7fe267f
/// 16:cpu:/docker/6a74f501e3b4c9d93ad440a7b73149cf2b5d56073c109a8d774c0793f7fe267f
/// 15:memory:/docker/6a74f501e3b4c9d93ad440a7b73149cf2b5d56073c109a8d774c0793f7fe267f
/// 0::/docker/6a74f501e3b4c9d93ad440a7b73149cf2b5d56073c109a8d774c0793f7fe267f
/// ```
///
/// This file may contain lines in both cgroups v1 and v2 format. To maintain implementation
/// simplicity, we are going to assume the cgroup name is the same between v1 and v2 and only look
/// for the v2 line (even if we try using the v1 API to access it later). In all tested
/// configurations so far this has been the case.
fn parse_cgroup_name(cgroup_contents: impl AsRef<str>) -> Option<String> {
    cgroup_contents.as_ref().lines().find_map(|line| {
        if !line.starts_with("0::") {
            return None;
        }

        Some(line.chars().skip(3).collect::<String>())
    })
}

fn parse_v2_cgroup_cpu_quota_and_period_us(contents: &str) -> Option<(u64, u64)> {
    let contents = contents.trim();

    // There are actual rules about what constitutes "unlimited" but we just treat anything
    // that does not successfully parse as "unlimited" because complaining about it will not help.
    let (quota_str, period_str) = contents.split_once(' ')?;
    let quota = quota_str.parse::<u64>().ok()?;
    let period = period_str.parse::<u64>().ok()?;

    Some((quota, period))
}

fn parse_v1_cgroup_cpu_quota_and_period_us(
    quota_contents: &str,
    period_contents: &str,
) -> Option<(u64, u64)> {
    let quota_contents = quota_contents.trim();
    let period_contents = period_contents.trim();

    // There are actual rules about what constitutes "unlimited" but we just treat anything
    // that does not successfully parse as "unlimited" because complaining about it will not help.
    let quota = quota_contents.parse::<u64>().ok()?;
    let period = period_contents.parse::<u64>().ok()?;

    Some((quota, period))
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(
        clippy::arithmetic_side_effects,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::indexing_slicing,
        reason = "we need not worry in tests"
    )]
    use std::fmt::Write;
    use std::hint::black_box;
    use std::sync::Barrier;
    use std::{io, thread};

    use testing::{assert_panics, f64_diff_abs, with_watchdog};

    use super::*;
    use crate::pal::linux::{MockBindings, MockFilesystem};

    const PROCESSOR_TIME_CLOSE_ENOUGH: f64 = 0.01;

    /// The processor that mainstream kernels cannot take offline and therefore publish no
    /// `/sys/devices/system/cpu/cpu{}/online` file for.
    const FIRST_PROCESSOR_ID: ProcessorId = 0;

    /// A machine with more processors than the operating system's own fixed-size mask can
    /// describe. The identifiers straddle the edge of that mask on purpose.
    const GIANT_MACHINE_PROCESSORS: [ProcessorId; 5] = [0, 1023, 1024, 1500, 2047];

    /// Memory regions of the processors in `GIANT_MACHINE_PROCESSORS`.
    const GIANT_MACHINE_MEMORY_REGIONS: [MemoryRegionId; 5] = [0; 5];

    /// Speed of the processors in `GIANT_MACHINE_PROCESSORS`, which the tests do not care about.
    const GIANT_MACHINE_BOGOMIPS: [f64; 5] = [2000.0; 5];

    #[test]
    fn constructors_execute_at_runtime() {
        let bindings = black_box(BindingsFacade::target());
        let filesystem = black_box(FilesystemFacade::target());
        let platform = black_box(BuildTargetPlatform::new(bindings, filesystem));

        assert!(matches!(platform.bindings, BindingsFacade::Target(_)));
        assert!(matches!(platform.fs, FilesystemFacade::Target(_)));
    }

    #[test]
    fn get_all_processors_smoke_test() {
        // We imagine a simple system with 2 physical cores, 4 logical processors, all in a
        // single processor group and a single memory region. Welcome to 2010!
        let mut fs = MockFilesystem::new();

        simulate_processor_layout(
            &mut fs,
            [0, 1, 2, 3],
            None,
            None,
            [0, 0, 0, 0],
            [99.9, 99.9, 99.9, 99.9],
        );

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        let processors = platform.get_all_processors();

        // We expect to see 4 logical processors. This API does not care about the physical cores.
        assert_eq!(processors.len(), 4);

        // All processors must be in the same memory region.
        assert_eq!(
            1,
            processors
                .iter()
                .map(|p| p.as_target().memory_region_id)
                .dedup()
                .count()
        );

        let p0 = &processors[0];
        assert_eq!(p0.as_target().id, 0);
        assert_eq!(p0.as_target().memory_region_id, 0);

        let p1 = &processors[1];
        assert_eq!(p1.as_target().id, 1);
        assert_eq!(p1.as_target().memory_region_id, 0);

        let p2 = &processors[2];
        assert_eq!(p2.as_target().id, 2);
        assert_eq!(p2.as_target().memory_region_id, 0);

        let p3 = &processors[3];
        assert_eq!(p3.as_target().id, 3);
        assert_eq!(p3.as_target().memory_region_id, 0);
    }

    #[test]
    fn forbidden_processors_are_ignored() {
        let mut fs = MockFilesystem::new();

        simulate_processor_layout(
            &mut fs,
            [0, 1, 2, 3],
            None,
            // We expect processor 2 to be absent from our results.
            Some([true, true, false, true]),
            [0, 0, 0, 0],
            [99.9, 99.9, 99.9, 99.9],
        );

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        let processors = platform.get_all_processors();

        assert_eq!(processors.len(), 3);

        // All processors must be in the same memory region.
        assert_eq!(
            1,
            processors
                .iter()
                .map(|p| p.as_target().memory_region_id)
                .dedup()
                .count()
        );

        let p0 = &processors[0];
        assert_eq!(p0.as_target().id, 0);
        assert_eq!(p0.as_target().memory_region_id, 0);

        let p1 = &processors[1];
        assert_eq!(p1.as_target().id, 1);
        assert_eq!(p1.as_target().memory_region_id, 0);

        let p2 = &processors[2];
        assert_eq!(p2.as_target().id, 3);
        assert_eq!(p2.as_target().memory_region_id, 0);
    }

    #[test]
    fn forbidden_memory_regions_are_ignored() {
        let mut fs = MockFilesystem::new();

        simulate_processor_layout(
            &mut fs,
            [0, 1, 2, 3],
            None,
            // Processors 2 and 3 are part of a memory region with 0 allowed processors.
            // We expect this memory region to be completely absent from any sort of results.
            Some([true, true, false, false]),
            [0, 0, 1, 1],
            [99.9, 99.9, 99.9, 99.9],
        );

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        let processors = platform.get_all_processors();

        assert_eq!(processors.len(), 2);

        // All processors must be in the same memory region.
        assert_eq!(
            1,
            processors
                .iter()
                .map(|p| p.as_target().memory_region_id)
                .dedup()
                .count()
        );

        let p0 = &processors[0];
        assert_eq!(p0.as_target().id, 0);
        assert_eq!(p0.as_target().memory_region_id, 0);

        let p1 = &processors[1];
        assert_eq!(p1.as_target().id, 1);
        assert_eq!(p1.as_target().memory_region_id, 0);
    }

    #[test]
    fn two_numa_nodes_efficiency_performance() {
        let mut fs = MockFilesystem::new();
        // Two nodes, each with 2 processors:
        // Node 0 -> [Performance, Efficiency], Node 1 -> [Efficiency, Performance].
        simulate_processor_layout(
            &mut fs,
            [0, 1, 2, 3],
            None,
            None,
            [0, 0, 1, 1],
            [3400.0, 2000.0, 2000.0, 3400.0],
        );

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );
        let processors = platform.get_all_processors();
        assert_eq!(processors.len(), 4);

        // Node 0
        let p0 = &processors[0];
        assert_eq!(p0.as_target().id, 0);
        assert_eq!(p0.as_target().memory_region_id, 0);
        assert_eq!(
            p0.as_target().efficiency_class,
            EfficiencyClass::Performance
        );
        assert_eq!(
            p0.as_target().relative_speed,
            RelativeSpeed::from_os_metric(3400)
        );
        // The layout simulator reports a `model name` for every processor, which surfaces as the
        // processor model.
        assert_eq!(
            p0.as_target().model.as_deref(),
            Some("Test Processor Model")
        );

        let p1 = &processors[1];
        assert_eq!(p1.as_target().id, 1);
        assert_eq!(p1.as_target().memory_region_id, 0);
        assert_eq!(p1.as_target().efficiency_class, EfficiencyClass::Efficiency);
        assert_eq!(
            p1.as_target().relative_speed,
            RelativeSpeed::from_os_metric(2000)
        );

        // Node 1
        let p2 = &processors[2];
        assert_eq!(p2.as_target().id, 2);
        assert_eq!(p2.as_target().memory_region_id, 1);
        assert_eq!(p2.as_target().efficiency_class, EfficiencyClass::Efficiency);
        assert_eq!(
            p2.as_target().relative_speed,
            RelativeSpeed::from_os_metric(2000)
        );

        let p3 = &processors[3];
        assert_eq!(p3.as_target().id, 3);
        assert_eq!(p3.as_target().memory_region_id, 1);
        assert_eq!(
            p3.as_target().efficiency_class,
            EfficiencyClass::Performance
        );
        assert_eq!(
            p3.as_target().relative_speed,
            RelativeSpeed::from_os_metric(3400)
        );
    }

    #[test]
    fn one_big_numa_two_small_nodes() {
        let mut fs = MockFilesystem::new();
        // Three nodes: node 0 -> 4 Performance, node 1 -> 2 Efficiency, node 2 -> 2 Efficiency
        simulate_processor_layout(
            &mut fs,
            [0, 1, 2, 3, 4, 5, 6, 7],
            None,
            None,
            [0, 0, 0, 0, 1, 1, 2, 2],
            [
                3400.0, 3400.0, 3400.0, 3400.0, 2000.0, 2000.0, 2000.0, 2000.0,
            ],
        );

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );
        let processors = platform.get_all_processors();
        assert_eq!(processors.len(), 8);

        // First 4 in node 0 => Performance
        for i in 0..4 {
            let p = &processors[i];
            assert_eq!(p.as_target().id, i as ProcessorId);
            assert_eq!(p.as_target().memory_region_id, 0);
            assert_eq!(p.as_target().efficiency_class, EfficiencyClass::Performance);
        }
        // Next 2 in node 1 => Efficiency
        for i in 4..6 {
            let p = &processors[i];
            assert_eq!(p.as_target().id, i as ProcessorId);
            assert_eq!(p.as_target().memory_region_id, 1);
            assert_eq!(p.as_target().efficiency_class, EfficiencyClass::Efficiency);
        }
        // Last 2 in node 2 => Efficiency
        for i in 6..8 {
            let p = &processors[i];
            assert_eq!(p.as_target().id, i as ProcessorId);
            assert_eq!(p.as_target().memory_region_id, 2);
            assert_eq!(p.as_target().efficiency_class, EfficiencyClass::Efficiency);
        }
    }

    #[test]
    fn numa_node_without_online_processors_stays_in_the_id_space() {
        let mut fs = MockFilesystem::new();
        // Node 1 has no online processor, Node 0 -> [Performance, Efficiency, Performance]
        simulate_processor_layout(
            &mut fs,
            [0, 1, 2, 3, 4, 5],
            Some([false, false, false, true, true, true]),
            None,
            [1, 1, 1, 0, 0, 0],
            [3400.0, 2000.0, 3400.0, 3400.0, 2000.0, 3400.0],
        );

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );
        let processors = platform.get_all_processors();
        assert_eq!(processors.len(), 3);

        // Node 0 => [Perf, Eff, Perf]
        let p0 = &processors[0];
        assert_eq!(p0.as_target().id, 3);
        assert_eq!(p0.as_target().memory_region_id, 0);
        assert_eq!(
            p0.as_target().efficiency_class,
            EfficiencyClass::Performance
        );

        let p1 = &processors[1];
        assert_eq!(p1.as_target().id, 4);
        assert_eq!(p1.as_target().memory_region_id, 0);
        assert_eq!(p1.as_target().efficiency_class, EfficiencyClass::Efficiency);

        let p2 = &processors[2];
        assert_eq!(p2.as_target().id, 5);
        assert_eq!(p2.as_target().memory_region_id, 0);
        assert_eq!(
            p2.as_target().efficiency_class,
            EfficiencyClass::Performance
        );

        // Node 1 holds no processor a caller can observe but remains part of the ID space, so
        // both nodes are still described by the extent of that space.
        assert_eq!(platform.max_memory_region_id(), 1);

        // The processors of node 1 are offline, not absent from the machine.
        assert_eq!(platform.max_processor_id(), 5);
        assert_eq!(platform.active_processor_count(), 3);
    }

    #[test]
    fn numa_node_that_was_never_onlined_publishes_no_member_list() {
        // A node named in the possible set need not have been onlined at all, in which case the
        // kernel publishes no directory for the node and therefore no member list either.
        let cpuinfo = "processor       : 0
bogomips        : 50.00
";

        let mut fs = MockFilesystem::new();

        fs.expect_get_cpuinfo_contents()
            .times(1)
            .return_const(cpuinfo.to_string());
        fs.expect_get_possible_cpus_contents()
            .return_const(Some("0\n".to_string()));
        fs.expect_get_online_cpus_contents()
            .return_const(Some("0\n".to_string()));
        fs.expect_get_numa_node_possible_contents()
            .times(1)
            .return_const(Some("0-1\n".to_string()));
        fs.expect_get_numa_node_cpulist_contents()
            .withf(|n| *n == 0)
            .times(1)
            .return_const(Some("0\n".to_string()));
        fs.expect_get_numa_node_cpulist_contents()
            .withf(|n| *n == 1)
            .times(1)
            .return_const(None);
        fs.expect_get_cpu_online_contents()
            .withf(|p| *p == 0)
            .times(1)
            .return_const(None);
        fs.expect_get_proc_self_status_contents()
            .times(1)
            .return_const("Cpus_allowed_list: 0".to_string());

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        let processors = platform.get_all_processors();

        assert_eq!(processors.len(), 1);
        assert_eq!(processors[0].as_target().memory_region_id, 0);
        assert_eq!(platform.max_memory_region_id(), 1);
    }

    #[test]
    fn offline_processors_stay_in_the_processor_id_space() {
        let mut fs = MockFilesystem::new();

        simulate_processor_layout(
            &mut fs,
            [0, 1, 2, 3, 4, 5, 6, 7],
            Some([true, true, true, true, false, false, false, false]),
            None,
            [0; 8],
            [2000.0; 8],
        );

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        // Taking a processor offline does not remove it from the machine, so the ID space keeps
        // its extent while only the online processors can be observed and used.
        assert_eq!(platform.max_processor_id(), 7);
        assert_eq!(platform.active_processor_count(), 4);
        assert_eq!(platform.get_all_processors().len(), 4);
    }

    #[test]
    fn forbidden_processors_stay_in_the_processor_id_space() {
        let mut fs = MockFilesystem::new();

        simulate_processor_layout(
            &mut fs,
            [0, 1, 2, 3, 4, 5, 6, 7],
            None,
            Some([true, true, false, false, false, false, false, false]),
            [0; 8],
            [2000.0; 8],
        );

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        // Both the extent of the ID space and the count of online processors describe the
        // machine, so a constraint on what this process may use leaves them untouched.
        assert_eq!(platform.max_processor_id(), 7);
        assert_eq!(platform.active_processor_count(), 8);
        assert_eq!(platform.get_all_processors().len(), 2);
    }

    #[test]
    fn machine_without_id_space_masks_is_described_by_its_processors() {
        let mut fs = MockFilesystem::new();

        simulate_processor_layout_without_id_space_masks(
            &mut fs,
            [0, 1, 2, 3],
            Some([true, true, false, true]),
            [2000.0; 4],
        );

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        // With no mask to read, the machine we enumerated is all the evidence we have.
        assert_eq!(platform.max_processor_id(), 3);
        assert_eq!(platform.max_memory_region_id(), 0);
        assert_eq!(platform.active_processor_count(), 3);
        assert_eq!(platform.get_all_processors().len(), 3);
    }

    #[test]
    fn machine_with_only_the_online_mask_derives_the_id_space_from_it() {
        let mut fs = MockFilesystem::new();

        // A machine of 8 processors whose kernel discloses which of them are online but not
        // which of them could possibly exist, with the process narrowed to two of them.
        simulate_processor_layout_with_only_the_online_mask(
            &mut fs,
            [0, 1, 2, 3, 4, 5, 6, 7],
            Some([true, true, false, false, false, false, false, false]),
            [2000.0; 8],
        );

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        // The online mask describes the machine and not this process, so the extent of the ID
        // space follows it rather than the two processors this process may use.
        assert_eq!(platform.max_processor_id(), 7);
        assert_eq!(platform.active_processor_count(), 8);
        assert_eq!(platform.get_all_processors().len(), 2);

        // Every active processor occupies an ID, so an ID space that cannot name them all
        // describes a machine that cannot exist. Deriving both from the same mask is what
        // guarantees this.
        assert!(
            usize::try_from(platform.max_processor_id()).unwrap() + 1
                >= platform.active_processor_count()
        );
    }

    #[test]
    fn cpuinfo_listing_offline_processor_excludes_the_processor() {
        // Mainstream kernels drop an offline processor from /proc/cpuinfo, but a kernel that
        // lists one anyway must not have it reported as a processor a caller may use. The node
        // member list is what the kernel really publishes for such a machine: it names only the
        // online processor, leaving the offline one claimed by no node at all.
        let cpuinfo = "processor       : 0
bogomips        : 50.00

processor       : 1
bogomips        : 50.00
";

        let mut fs = MockFilesystem::new();

        fs.expect_get_cpuinfo_contents()
            .times(1)
            .return_const(cpuinfo.to_string());
        fs.expect_get_possible_cpus_contents()
            .return_const(Some("0-1\n".to_string()));
        fs.expect_get_online_cpus_contents()
            .return_const(Some("0\n".to_string()));
        fs.expect_get_numa_node_possible_contents()
            .times(1)
            .return_const(Some("0\n".to_string()));
        fs.expect_get_numa_node_cpulist_contents()
            .withf(|n| *n == 0)
            .times(1)
            .return_const(Some("0\n".to_string()));
        fs.expect_get_cpu_online_contents()
            .withf(|p| *p == 0)
            .times(1)
            .return_const(None);
        fs.expect_get_cpu_online_contents()
            .withf(|p| *p == 1)
            .times(1)
            .return_const(Some("0\n".to_string()));
        fs.expect_get_proc_self_status_contents()
            .times(1)
            .return_const("Cpus_allowed_list: 0-1".to_string());

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        let processors = platform.get_all_processors();

        assert_eq!(processors.len(), 1);
        assert_eq!(processors[0].as_target().id, 0);
    }

    #[test]
    fn processor_claimed_by_no_numa_node_is_still_reported() {
        // A processor can be onlined between the moment we read /proc/cpuinfo and the moment we
        // read the member list of each node, which leaves it named by the former and claimed by
        // no node. Losing the machine to that race is not an option, so the processor belongs to
        // the memory region that a machine without a disclosed topology uses.
        let cpuinfo = "processor       : 0
bogomips        : 50.00

processor       : 1
bogomips        : 50.00
";

        let mut fs = MockFilesystem::new();

        fs.expect_get_cpuinfo_contents()
            .times(1)
            .return_const(cpuinfo.to_string());
        fs.expect_get_possible_cpus_contents()
            .return_const(Some("0-1\n".to_string()));
        fs.expect_get_online_cpus_contents()
            .return_const(Some("0-1\n".to_string()));
        fs.expect_get_numa_node_possible_contents()
            .times(1)
            .return_const(Some("0\n".to_string()));
        fs.expect_get_numa_node_cpulist_contents()
            .withf(|n| *n == 0)
            .times(1)
            .return_const(Some("0\n".to_string()));
        fs.expect_get_cpu_online_contents()
            .withf(|p| *p == 0)
            .times(1)
            .return_const(None);
        fs.expect_get_cpu_online_contents()
            .withf(|p| *p == 1)
            .times(1)
            .return_const(Some("1\n".to_string()));
        fs.expect_get_proc_self_status_contents()
            .times(1)
            .return_const("Cpus_allowed_list: 0-1".to_string());

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        let processors = platform.get_all_processors();

        assert_eq!(processors.len(), 2);
        assert_eq!(processors[1].as_target().id, 1);
        assert_eq!(
            processors[1].as_target().memory_region_id,
            SINGLE_MEMORY_REGION_ID
        );
    }

    #[test]
    fn numa_node_mask_that_names_nothing_is_a_machine_without_topology() {
        // A node mask we cannot read a single ID out of tells us nothing about the topology,
        // which must leave the machine described exactly as one whose kernel publishes no mask
        // at all - the alternative is a topology that claims no processor, which describes no
        // machine that could exist.
        let cpuinfo = "processor       : 0
bogomips        : 50.00
";

        for node_mask in ["", "\n", ",", "this is not a cpulist"] {
            let mut fs = MockFilesystem::new();

            fs.expect_get_cpuinfo_contents()
                .times(1)
                .return_const(cpuinfo.to_string());
            fs.expect_get_possible_cpus_contents()
                .return_const(Some("0\n".to_string()));
            fs.expect_get_online_cpus_contents()
                .return_const(Some("0\n".to_string()));
            fs.expect_get_numa_node_possible_contents()
                .times(1)
                .return_const(Some(node_mask.to_string()));
            fs.expect_get_cpu_online_contents()
                .withf(|p| *p == 0)
                .times(1)
                .return_const(None);
            fs.expect_get_proc_self_status_contents()
                .times(1)
                .return_const("Cpus_allowed_list: 0".to_string());

            let platform = BuildTargetPlatform::new(
                BindingsFacade::from_mock(MockBindings::new()),
                FilesystemFacade::from_mock(fs),
            );

            let processors = platform.get_all_processors();

            assert_eq!(processors.len(), 1);
            assert_eq!(
                processors[0].as_target().memory_region_id,
                SINGLE_MEMORY_REGION_ID
            );
            assert_eq!(platform.max_memory_region_id(), SINGLE_MEMORY_REGION_ID);
        }
    }

    #[test]
    fn two_numa_nodes_some_inactive_processors() {
        let mut fs = MockFilesystem::new();
        // Node 0 -> Efficiency, Node 1 -> Performance, with gaps in indexes
        simulate_processor_layout(
            &mut fs,
            [0, 1, 2, 3, 4, 5, 6, 7],
            Some([true, false, true, false, true, false, true, false]),
            None,
            [0, 0, 0, 0, 1, 1, 1, 1],
            [
                2000.0, 2000.0, 2000.0, 2000.0, 3400.0, 3400.0, 3400.0, 3400.0,
            ],
        );

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );
        let processors = platform.get_all_processors();
        assert_eq!(processors.len(), 4);

        // Node 0 => [Eff, Eff]
        let p0 = &processors[0];
        assert_eq!(p0.as_target().id, 0);
        assert_eq!(p0.as_target().memory_region_id, 0);
        assert_eq!(p0.as_target().efficiency_class, EfficiencyClass::Efficiency);

        let p1 = &processors[1];
        assert_eq!(p1.as_target().id, 2);
        assert_eq!(p1.as_target().memory_region_id, 0);
        assert_eq!(p1.as_target().efficiency_class, EfficiencyClass::Efficiency);

        // Node 1 => [Perf, Perf]
        let p2 = &processors[2];
        assert_eq!(p2.as_target().id, 4);
        assert_eq!(p2.as_target().memory_region_id, 1);
        assert_eq!(
            p2.as_target().efficiency_class,
            EfficiencyClass::Performance
        );

        let p3 = &processors[3];
        assert_eq!(p3.as_target().id, 6);
        assert_eq!(p3.as_target().memory_region_id, 1);
        assert_eq!(
            p3.as_target().efficiency_class,
            EfficiencyClass::Performance
        );
    }

    /// Configures the mock filesystem to simulate a particular machine.
    ///
    /// The processors named here are the processors the machine could possibly have, which is
    /// what the kernel publishes in `/sys/devices/system/cpu/possible` and what defines the
    /// processor ID space. A processor that is offline is absent from `/proc/cpuinfo` and from
    /// `/sys/devices/system/cpu/online`, matching what mainstream kernels publish, so the
    /// simulated machine can have an ID space wider than the set of processors it currently
    /// runs. The same holds for memory regions: every region named here is published in
    /// `/sys/devices/system/node/possible`, while a region whose every processor is offline
    /// publishes an empty member list.
    fn simulate_processor_layout<const PROCESSOR_COUNT: usize>(
        fs: &mut MockFilesystem,
        processor_index: [ProcessorId; PROCESSOR_COUNT],
        // If None, all are online.
        processor_is_online: Option<[bool; PROCESSOR_COUNT]>,
        // If None, all are allowed.
        processor_is_allowed: Option<[bool; PROCESSOR_COUNT]>,
        memory_region_index: [MemoryRegionId; PROCESSOR_COUNT],
        bogomips_per_processor: [f64; PROCESSOR_COUNT],
    ) {
        simulate_machine(
            fs,
            processor_index,
            processor_is_online.unwrap_or([true; PROCESSOR_COUNT]),
            processor_is_allowed.unwrap_or([true; PROCESSOR_COUNT]),
            memory_region_index,
            bogomips_per_processor,
            IdSpaceMasks::Published,
        );
    }

    /// Configures the mock filesystem to simulate a machine whose kernel publishes none of the
    /// masks that describe the ID space, which is what the fallback derivations exist for.
    ///
    /// Such a kernel discloses no NUMA topology either, so every processor of the simulated
    /// machine belongs to one memory region and every processor is online.
    fn simulate_processor_layout_without_id_space_masks<const PROCESSOR_COUNT: usize>(
        fs: &mut MockFilesystem,
        processor_index: [ProcessorId; PROCESSOR_COUNT],
        // If None, all are allowed.
        processor_is_allowed: Option<[bool; PROCESSOR_COUNT]>,
        bogomips_per_processor: [f64; PROCESSOR_COUNT],
    ) {
        simulate_machine(
            fs,
            processor_index,
            [true; PROCESSOR_COUNT],
            processor_is_allowed.unwrap_or([true; PROCESSOR_COUNT]),
            [SINGLE_MEMORY_REGION_ID; PROCESSOR_COUNT],
            bogomips_per_processor,
            IdSpaceMasks::Absent,
        );
    }

    /// Configures the mock filesystem to simulate a machine whose kernel publishes the online
    /// processor mask but not the possible one, which is the mixed state where one system-wide
    /// mask is available to describe the machine and the other is not.
    ///
    /// Such a kernel discloses no NUMA topology either, so every processor of the simulated
    /// machine belongs to one memory region and every processor is online.
    fn simulate_processor_layout_with_only_the_online_mask<const PROCESSOR_COUNT: usize>(
        fs: &mut MockFilesystem,
        processor_index: [ProcessorId; PROCESSOR_COUNT],
        // If None, all are allowed.
        processor_is_allowed: Option<[bool; PROCESSOR_COUNT]>,
        bogomips_per_processor: [f64; PROCESSOR_COUNT],
    ) {
        simulate_machine(
            fs,
            processor_index,
            [true; PROCESSOR_COUNT],
            processor_is_allowed.unwrap_or([true; PROCESSOR_COUNT]),
            [SINGLE_MEMORY_REGION_ID; PROCESSOR_COUNT],
            bogomips_per_processor,
            IdSpaceMasks::OnlyOnlineProcessors,
        );
    }

    /// Which of the masks that describe the extent of the ID space the simulated kernel
    /// publishes.
    ///
    /// A mainstream kernel publishes all of them, so the other variants exist to reach the
    /// derivations we fall back to when a kernel discloses less.
    #[derive(Clone, Copy)]
    enum IdSpaceMasks {
        Published,
        Absent,
        OnlyOnlineProcessors,
    }

    fn simulate_machine<const PROCESSOR_COUNT: usize>(
        fs: &mut MockFilesystem,
        processor_index: [ProcessorId; PROCESSOR_COUNT],
        processor_is_online: [bool; PROCESSOR_COUNT],
        processor_is_allowed: [bool; PROCESSOR_COUNT],
        memory_region_index: [MemoryRegionId; PROCESSOR_COUNT],
        bogomips_per_processor: [f64; PROCESSOR_COUNT],
        id_space_masks: IdSpaceMasks,
    ) {
        let online_positions = || (0..PROCESSOR_COUNT).filter(|index| processor_is_online[*index]);

        let mut cpuinfo = String::new();

        for position in online_positions() {
            let processor_id = processor_index[position];
            let bogomips = bogomips_per_processor[position];

            writeln!(cpuinfo, "processor       : {processor_id}").unwrap();
            writeln!(cpuinfo, "model name      : Test Processor Model").unwrap();
            writeln!(cpuinfo, "bogomips        : {bogomips}").unwrap();
            writeln!(cpuinfo, "whatever        : 123").unwrap();
            writeln!(cpuinfo, "other           : ignored").unwrap();
            writeln!(cpuinfo).unwrap();
        }

        // Each file below answers a question the platform derives once and remembers, so asking
        // the platform repeatedly must not read it again - asserting the exact count is what
        // keeps that memoization honest. The processor ID space masks are pinned by the tests
        // that read them rather than here, because most simulated scenarios ask no question
        // that reaches them.
        fs.expect_get_cpuinfo_contents()
            .times(1)
            .return_const(cpuinfo);

        let node_indexes = memory_region_index.iter().copied().unique().collect_vec();

        // A trailing newline might or might not be present in a real file, so we add one
        // everywhere to verify that it gets trimmed.
        let possible_processors = format!("{}\n", cpulist::emit(processor_index));
        let online_processor_ids = online_positions()
            .map(|position| processor_index[position])
            .collect_vec();
        let online_processors = format!("{}\n", cpulist::emit(online_processor_ids));
        let possible_nodes = format!("{}\n", cpulist::emit(node_indexes.iter().copied()));

        let (possible_processors, possible_nodes) = match id_space_masks {
            IdSpaceMasks::Published => {
                fs.expect_get_online_cpus_contents()
                    .return_const(Some(online_processors));

                (Some(possible_processors), Some(possible_nodes))
            }
            IdSpaceMasks::Absent => {
                fs.expect_get_online_cpus_contents().return_const(None);

                (None, None)
            }
            IdSpaceMasks::OnlyOnlineProcessors => {
                // Both the extent of the ID space and the count of active processors come from
                // this one mask on such a machine, so reading it exactly once is what says the
                // two are derived from the same reading of it.
                fs.expect_get_online_cpus_contents()
                    .times(1)
                    .return_const(Some(online_processors));

                (None, None)
            }
        };

        let publishes_numa_topology = possible_nodes.is_some();

        fs.expect_get_possible_cpus_contents()
            .return_const(possible_processors);
        fs.expect_get_numa_node_possible_contents()
            .times(1)
            .return_const(possible_nodes);

        for position in online_positions() {
            if !processor_is_allowed[position] {
                // Forbidden processors are not probed.
                continue;
            }

            let processor_id = processor_index[position];

            fs.expect_get_cpu_online_contents()
                .withf(move |p| *p == processor_id)
                .times(1)
                .return_const(if processor_id == FIRST_PROCESSOR_ID {
                    // Mainstream kernels publish no such file for the first processor because
                    // that processor cannot be taken offline.
                    None
                } else {
                    Some("1\n".to_string())
                });
        }

        // The kernel publishes a directory per node only for a machine whose topology it
        // discloses at all, so a machine without the node mask publishes no member list either.
        if publishes_numa_topology {
            for node in node_indexes {
                let members = online_positions()
                    .filter(|position| memory_region_index[*position] == node)
                    .map(|position| processor_index[position])
                    .collect_vec();

                // A node that holds no online processor publishes an empty member list.
                let members = format!("{}\n", cpulist::emit(members));

                fs.expect_get_numa_node_cpulist_contents()
                    .withf(move |n| *n == node)
                    .times(1)
                    .return_const(Some(members));
            }
        }

        let allowed_processors = (0..PROCESSOR_COUNT)
            .filter(|position| processor_is_allowed[*position])
            .map(|position| processor_index[position])
            .collect_vec();

        assert!(!allowed_processors.is_empty());

        let allowed_cpus = cpulist::emit(allowed_processors);

        fs.expect_get_proc_self_status_contents()
            .times(1)
            .return_const(format!("Cpus_allowed_list: {allowed_cpus}"));
    }

    /// Set quota to -1 for infinity (transformed for v2).
    fn simulate_cgroup_time_limit(
        fs: &mut MockFilesystem,
        quota: i64,
        period: i64,
        v1: bool,
        v2: bool,
    ) {
        const CGROUP_NAME: &str = "/foo/bar";

        let cgroup_file_contents = format!(
            "17:cpuset:{CGROUP_NAME}
16:cpu:{CGROUP_NAME}
15:memory:{CGROUP_NAME}
0::{CGROUP_NAME}
"
        );

        fs.expect_get_proc_self_cgroup()
            .times(1)
            .return_const(cgroup_file_contents);

        if v1 {
            fs.expect_get_v1_cgroup_cpu_period()
                .withf(move |name| name == CGROUP_NAME)
                .times(1)
                .return_const(period.to_string());
            fs.expect_get_v1_cgroup_cpu_quota()
                .withf(move |name| name == CGROUP_NAME)
                .times(1)
                .return_const(quota.to_string());
        }

        if v2 {
            if quota == -1 {
                fs.expect_get_v2_cgroup_cpu_quota_and_period()
                    .withf(move |name| name == CGROUP_NAME)
                    .times(1)
                    .return_const("max".to_string());
            } else {
                fs.expect_get_v2_cgroup_cpu_quota_and_period()
                    .withf(move |name| name == CGROUP_NAME)
                    .times(1)
                    .return_const(format!("{quota} {period}"));
            }
        } else {
            // v2 is always checked first, so if only v1 we still
            // need to return None here.
            fs.expect_get_v2_cgroup_cpu_quota_and_period()
                .withf(move |name| name == CGROUP_NAME)
                .times(1)
                .return_const(None);
        }

        // If neither is requested, we also need to return None for v1 quota,
        // as that is probed to check if data v1 is available.
        if !v1 && !v2 {
            fs.expect_get_v1_cgroup_cpu_quota()
                .withf(move |name| name == CGROUP_NAME)
                .times(1)
                .return_const(None);
        }
    }

    #[test]
    fn pin_current_thread_to_single_processor() {
        let mut bindings = MockBindings::new();

        let expected_mask = mask_from([0]);

        bindings
            .expect_sched_setaffinity_current()
            .withf(move |mask| *mask == expected_mask)
            .times(1)
            .returning(|_| Ok(()));

        let mut fs = MockFilesystem::new();
        simulate_processor_layout(&mut fs, [0], None, None, [0], [2000.0]);

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(bindings),
            FilesystemFacade::from_mock(fs),
        );
        let processors = platform.get_all_processors();
        platform.pin_current_thread_to(&processors);
    }

    #[test]
    fn pin_current_thread_to_multiple_processors() {
        let mut bindings = MockBindings::new();

        let expected_mask = mask_from([0, 1]);

        bindings
            .expect_sched_setaffinity_current()
            .withf(move |mask| *mask == expected_mask)
            .times(1)
            .returning(|_| Ok(()));

        let mut fs = MockFilesystem::new();
        simulate_processor_layout(&mut fs, [0, 1], None, None, [0, 0], [2000.0, 2000.0]);

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(bindings),
            FilesystemFacade::from_mock(fs),
        );
        let processors = platform.get_all_processors();
        platform.pin_current_thread_to(&processors);
    }

    #[test]
    fn pin_current_thread_to_multiple_memory_regions() {
        let mut bindings = MockBindings::new();

        let expected_mask = mask_from([0, 1]);

        bindings
            .expect_sched_setaffinity_current()
            .withf(move |mask| *mask == expected_mask)
            .times(1)
            .returning(|_| Ok(()));

        let mut fs = MockFilesystem::new();
        simulate_processor_layout(&mut fs, [0, 1], None, None, [0, 1], [2000.0, 2000.0]);

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(bindings),
            FilesystemFacade::from_mock(fs),
        );
        let processors = platform.get_all_processors();
        platform.pin_current_thread_to(&processors);
    }

    #[test]
    fn pin_current_thread_to_efficiency_processors() {
        let mut bindings = MockBindings::new();

        let expected_mask = mask_from([1, 2]);

        bindings
            .expect_sched_setaffinity_current()
            .withf(move |mask| *mask == expected_mask)
            .times(1)
            .returning(|_| Ok(()));

        let mut fs = MockFilesystem::new();
        // Node 0 -> [Performance, Efficiency], Node 1 -> [Efficiency, Performance]
        simulate_processor_layout(
            &mut fs,
            [0, 1, 2, 3],
            None,
            None,
            [0, 0, 2, 2],
            [3400.0, 2000.0, 2000.0, 3400.0],
        );

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(bindings),
            FilesystemFacade::from_mock(fs),
        );
        let processors = platform.get_all_processors();
        let efficiency_processors = NonEmpty::from_vec(
            processors
                .iter()
                .filter(|p| p.as_target().efficiency_class == EfficiencyClass::Efficiency)
                .collect_vec(),
        )
        .unwrap();
        platform.pin_current_thread_to(&efficiency_processors);
    }

    fn mask_from<const PROCESSOR_COUNT: usize>(
        processors: [ProcessorId; PROCESSOR_COUNT],
    ) -> CpuMask {
        let mut mask = CpuMask::new();

        for processor in processors {
            mask.insert(processor);
        }

        mask
    }

    /// A mask as the operating system would have filled it: exactly as wide as it was asked to be.
    ///
    /// The real bindings hand over a buffer of the requested width and the operating system fills
    /// that buffer, so it can never answer with a processor that the requested width cannot
    /// describe. A mock that answers otherwise describes something that cannot happen, which
    /// would let a test pass on an impossible response, so the width is enforced here.
    fn mask_of_width<const PROCESSOR_COUNT: usize>(
        words: NonZero<usize>,
        processors: [ProcessorId; PROCESSOR_COUNT],
    ) -> CpuMask {
        let mut mask = CpuMask::with_words(words);

        for processor in processors {
            mask.insert(processor);
            assert_eq!(mask.words(), words);
        }

        mask
    }

    #[test]
    fn current_thread_processors_smoke_test() {
        let mut bindings = MockBindings::new();

        bindings
            .expect_sched_getaffinity_current()
            .times(1)
            .returning(|words| Ok(mask_of_width(words, [0, 1])));

        bindings
            .expect_sched_getaffinity_current()
            .times(1)
            .returning(|words| Ok(mask_of_width(words, [2])));

        let mut fs = MockFilesystem::new();

        // The affinity mask and the extent of the processor ID space are all this operation
        // reads - it never enumerates the machine - and the ID space is remembered after the
        // first reading, so the mask that describes it is read once for both calls below.
        fs.expect_get_possible_cpus_contents()
            .times(1)
            .return_const(Some("0-2\n".to_string()));

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(bindings),
            FilesystemFacade::from_mock(fs),
        );

        let current_thread_processors = platform.current_thread_processors();
        assert_eq!(current_thread_processors.len(), 2);
        assert_eq!(current_thread_processors[0], 0);
        assert_eq!(current_thread_processors[1], 1);

        let current_thread_processors = platform.current_thread_processors();
        assert_eq!(current_thread_processors.len(), 1);
        assert_eq!(current_thread_processors[0], 2);
    }

    #[test]
    fn current_thread_processors_widens_mask_until_operating_system_accepts_it() {
        let mut bindings = MockBindings::new();

        let narrow = CpuMask::default_words();
        let wide = narrow.checked_mul(AFFINITY_MASK_GROWTH).unwrap();

        // A mask too narrow to describe every processor is rejected as an invalid argument.
        bindings
            .expect_sched_getaffinity_current()
            .withf(move |words| *words == narrow)
            .times(1)
            .returning(|_| Err(io::Error::from_raw_os_error(libc::EINVAL)));

        let affinity = [1024, 1500, 2047];

        // Both reads are expected to use the wider mask - the second one because the first one
        // already established the width that this operating system wants.
        bindings
            .expect_sched_getaffinity_current()
            .withf(move |words| *words == wide)
            .times(2)
            .returning(move |words| Ok(mask_of_width(words, affinity)));

        let mut fs = MockFilesystem::new();

        // Reading the affinity reads only the extent of the processor ID space, which comes from
        // the possible-processor mask; the machine is never enumerated, so that mask is the only
        // file this scenario sets up.
        fs.expect_get_possible_cpus_contents()
            .times(1)
            .return_const(Some(format!(
                "{}\n",
                cpulist::emit(GIANT_MACHINE_PROCESSORS)
            )));

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(bindings),
            FilesystemFacade::from_mock(fs),
        );

        for _ in 0..2 {
            let current_thread_processors = platform.current_thread_processors();

            assert_eq!(
                current_thread_processors.iter().copied().collect_vec(),
                vec![1024, 1500, 2047]
            );
        }
    }

    #[test]
    fn current_thread_processors_widens_again_when_the_remembered_width_stops_working() {
        let mut bindings = MockBindings::new();

        let narrow = CpuMask::default_words();
        let wide = narrow.checked_mul(AFFINITY_MASK_GROWTH).unwrap();

        // Every processor here fits in the narrower mask, which is what makes the first read
        // succeed at that width.
        let before = [0, 1023];
        let after = [1024, 1500, 2047];

        // The first read settles on a width and it is remembered.
        bindings
            .expect_sched_getaffinity_current()
            .withf(move |words| *words == narrow)
            .times(1)
            .returning(move |words| Ok(mask_of_width(words, before)));

        // The machine can grow while the process runs, so a remembered width is a hint and not a
        // conclusion - once it stops working, the search must widen again rather than give up.
        bindings
            .expect_sched_getaffinity_current()
            .withf(move |words| *words == narrow)
            .times(1)
            .returning(|_| Err(io::Error::from_raw_os_error(libc::EINVAL)));

        bindings
            .expect_sched_getaffinity_current()
            .withf(move |words| *words == wide)
            .times(1)
            .returning(move |words| Ok(mask_of_width(words, after)));

        let mut fs = MockFilesystem::new();

        // See the first widening test: only the possible-processor mask is read.
        fs.expect_get_possible_cpus_contents()
            .times(1)
            .return_const(Some(format!(
                "{}\n",
                cpulist::emit(GIANT_MACHINE_PROCESSORS)
            )));

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(bindings),
            FilesystemFacade::from_mock(fs),
        );

        assert_eq!(
            platform
                .current_thread_processors()
                .iter()
                .copied()
                .collect_vec(),
            vec![0, 1023]
        );

        assert_eq!(
            platform
                .current_thread_processors()
                .iter()
                .copied()
                .collect_vec(),
            vec![1024, 1500, 2047]
        );
    }

    #[test]
    fn current_thread_processors_shares_the_remembered_width_across_threads() {
        // The remembered width is the one piece of shared mutable state in the platform, so it
        // is read and written concurrently in practice. Every thread must come away with the
        // same answer no matter how their reads interleave.
        const READER_COUNT: usize = 8;
        const READS_PER_READER: usize = 16;

        let mut bindings = MockBindings::new();

        let narrow = CpuMask::default_words();
        let wide = narrow.checked_mul(AFFINITY_MASK_GROWTH).unwrap();
        let affinity = [1024, 1500, 2047];

        // How many threads try the narrower width before one of them establishes the wider one
        // depends on how the threads interleave, so no count is expected here.
        bindings
            .expect_sched_getaffinity_current()
            .withf(move |words| *words == narrow)
            .returning(|_| Err(io::Error::from_raw_os_error(libc::EINVAL)));

        bindings
            .expect_sched_getaffinity_current()
            .withf(move |words| *words == wide)
            .returning(move |words| Ok(mask_of_width(words, affinity)));

        let mut fs = MockFilesystem::new();

        // See the first widening test: only the possible-processor mask is read, and the platform
        // remembers it after the first reading even under concurrent readers.
        fs.expect_get_possible_cpus_contents()
            .times(1)
            .return_const(Some(format!(
                "{}\n",
                cpulist::emit(GIANT_MACHINE_PROCESSORS)
            )));

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(bindings),
            FilesystemFacade::from_mock(fs),
        );

        // Releasing every thread at once is what makes them contend for the remembered width.
        let start = Barrier::new(READER_COUNT);

        // The barrier and thread joins block, so a scheduling or mutation defect could hang the
        // test rather than fail it; the watchdog turns such a hang into a failure instead.
        with_watchdog(move || {
            thread::scope(|scope| {
                for _ in 0..READER_COUNT {
                    scope.spawn(|| {
                        start.wait();

                        for _ in 0..READS_PER_READER {
                            assert_eq!(
                                platform
                                    .current_thread_processors()
                                    .iter()
                                    .copied()
                                    .collect_vec(),
                                affinity.to_vec()
                            );
                        }
                    });
                }
            });
        });
    }

    #[test]
    fn current_thread_processors_ignores_processors_beyond_the_known_hardware() {
        // The operating system sizes its answer by the processors it could ever have, which can
        // exceed the processors that the hardware inventory lists, so it may name a processor
        // that the inventory knows nothing about.
        const UNKNOWN_PROCESSOR: ProcessorId = 3000;

        let mut bindings = MockBindings::new();

        // Naming a processor beyond the known hardware takes a mask wide enough to describe one,
        // so the operating system here is one that demands a mask two widenings out.
        let accepted = CpuMask::default_words()
            .checked_mul(AFFINITY_MASK_GROWTH)
            .unwrap()
            .checked_mul(AFFINITY_MASK_GROWTH)
            .unwrap();

        bindings
            .expect_sched_getaffinity_current()
            .withf(move |words| *words < accepted)
            .times(2)
            .returning(|_| Err(io::Error::from_raw_os_error(libc::EINVAL)));

        bindings
            .expect_sched_getaffinity_current()
            .withf(move |words| *words == accepted)
            .times(1)
            .returning(|words| Ok(mask_of_width(words, [1024, UNKNOWN_PROCESSOR])));

        let mut fs = MockFilesystem::new();

        // See the first widening test: only the possible-processor mask is read, and it is what
        // bounds the ID space so that a named processor beyond it is left out.
        fs.expect_get_possible_cpus_contents()
            .times(1)
            .return_const(Some(format!(
                "{}\n",
                cpulist::emit(GIANT_MACHINE_PROCESSORS)
            )));

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(bindings),
            FilesystemFacade::from_mock(fs),
        );

        let current_thread_processors = platform.current_thread_processors();

        assert_eq!(
            current_thread_processors.iter().copied().collect_vec(),
            vec![1024]
        );
    }

    #[test]
    fn current_thread_processors_panics_on_unexpected_error() {
        let mut bindings = MockBindings::new();

        // Only an invalid argument suggests that a wider mask might help.
        bindings
            .expect_sched_getaffinity_current()
            .times(1)
            .returning(|_| Err(io::Error::from_raw_os_error(libc::EPERM)));

        let mut fs = MockFilesystem::new();

        // Reading the affinity reads only the possible-processor mask before it consults the
        // operating system, so that mask is the one file this scenario sets up.
        fs.expect_get_possible_cpus_contents()
            .times(1)
            .return_const(Some("0\n".to_string()));

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(bindings),
            FilesystemFacade::from_mock(fs),
        );

        assert_panics(|| platform.current_thread_processors());
    }

    #[test]
    fn current_thread_processors_gives_up_when_no_mask_is_accepted() {
        let mut bindings = MockBindings::new();

        // An operating system that rejects every mask must not send us searching forever.
        bindings
            .expect_sched_getaffinity_current()
            .times(AFFINITY_MASK_ATTEMPTS)
            .returning(|_| Err(io::Error::from_raw_os_error(libc::EINVAL)));

        let mut fs = MockFilesystem::new();

        // See the unexpected-error test: only the possible-processor mask is read.
        fs.expect_get_possible_cpus_contents()
            .times(1)
            .return_const(Some("0\n".to_string()));

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(bindings),
            FilesystemFacade::from_mock(fs),
        );

        assert_panics(|| platform.current_thread_processors());
    }

    #[test]
    fn pin_current_thread_to_processor_beyond_fixed_size_mask() {
        let mut bindings = MockBindings::new();

        let expected_mask = mask_from([1500]);

        bindings
            .expect_sched_setaffinity_current()
            .withf(move |mask| {
                // A processor that the operating system's own fixed-size mask cannot describe
                // must arrive in a mask that is wider than that.
                *mask == expected_mask && mask.words() > CpuMask::default_words()
            })
            .times(1)
            .returning(|_| Ok(()));

        let mut fs = MockFilesystem::new();
        simulate_processor_layout(
            &mut fs,
            GIANT_MACHINE_PROCESSORS,
            None,
            None,
            GIANT_MACHINE_MEMORY_REGIONS,
            GIANT_MACHINE_BOGOMIPS,
        );

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(bindings),
            FilesystemFacade::from_mock(fs),
        );

        let processors = platform.get_all_processors();
        let target = NonEmpty::from_vec(
            processors
                .iter()
                .filter(|processor| processor.as_target().id == 1500)
                .collect_vec(),
        )
        .unwrap();

        platform.pin_current_thread_to(&target);
    }

    #[test]
    fn max_processor_time_without_cgroup() {
        let mut fs = MockFilesystem::new();

        simulate_processor_layout(
            &mut fs,
            [0, 1, 2, 3, 4, 5],
            // 2 inactive processors.
            Some([true, true, true, true, false, false]),
            None,
            [0, 0, 0, 0, 0, 0],
            [99.9, 99.9, 99.9, 99.9, 99.9, 99.9],
        );

        fs.expect_get_proc_self_cgroup().times(1).return_const(None);

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        let max_processor_time = platform.max_processor_time();

        #[expect(
            clippy::float_cmp,
            reason = "we use absolute error, which is the right way to compare"
        )]
        {
            assert_eq!(
                f64_diff_abs(max_processor_time, 4.0, PROCESSOR_TIME_CLOSE_ENOUGH),
                0.0
            );
        }
    }

    #[test]
    fn max_processor_time_below_available_v1() {
        // If the limit is less than the number of available processors,
        // we should use the cgroup limit as max processor time.
        let mut fs = MockFilesystem::new();

        simulate_processor_layout(
            &mut fs,
            [0, 1, 2, 3, 4, 5],
            // 2 inactive processors.
            Some([true, true, true, true, false, false]),
            None,
            [0, 0, 0, 0, 0, 0],
            [99.9, 99.9, 99.9, 99.9, 99.9, 99.9],
        );

        simulate_cgroup_time_limit(&mut fs, 20, 10, true, false);

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        let max_processor_time = platform.max_processor_time();

        #[expect(
            clippy::float_cmp,
            reason = "we use absolute error, which is the right way to compare"
        )]
        {
            assert_eq!(
                f64_diff_abs(max_processor_time, 2.0, PROCESSOR_TIME_CLOSE_ENOUGH),
                0.0
            );
        }
    }

    #[test]
    fn max_processor_time_below_available_v2() {
        // If the limit is less than the number of available processors,
        // we should use the cgroup limit as max processor time.
        let mut fs = MockFilesystem::new();

        simulate_processor_layout(
            &mut fs,
            [0, 1, 2, 3, 4, 5],
            // 2 inactive processors.
            Some([true, true, true, true, false, false]),
            None,
            [0, 0, 0, 0, 0, 0],
            [99.9, 99.9, 99.9, 99.9, 99.9, 99.9],
        );

        simulate_cgroup_time_limit(&mut fs, 20, 10, false, true);

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        let max_processor_time = platform.max_processor_time();

        #[expect(
            clippy::float_cmp,
            reason = "we use absolute error, which is the right way to compare"
        )]
        {
            assert_eq!(
                f64_diff_abs(max_processor_time, 2.0, PROCESSOR_TIME_CLOSE_ENOUGH),
                0.0
            );
        }
    }

    #[test]
    fn max_processor_time_above_available() {
        // If the limit is more than the number of available processors,
        // we should use the available processor count as max processor time.
        let mut fs = MockFilesystem::new();

        simulate_processor_layout(
            &mut fs,
            [0, 1, 2, 3, 4, 5],
            // 2 inactive processors.
            Some([true, true, true, true, false, false]),
            None,
            [0, 0, 0, 0, 0, 0],
            [99.9, 99.9, 99.9, 99.9, 99.9, 99.9],
        );

        simulate_cgroup_time_limit(&mut fs, 99999, 100, false, true);

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        let max_processor_time = platform.max_processor_time();

        #[expect(
            clippy::float_cmp,
            reason = "we use absolute error, which is the right way to compare"
        )]
        {
            assert_eq!(
                f64_diff_abs(max_processor_time, 4.0, PROCESSOR_TIME_CLOSE_ENOUGH),
                0.0
            );
        }
    }

    #[test]
    fn max_processor_time_with_infinite_limit_v1() {
        // If the limit is infinity,
        // we should use the available processor count as max processor time.
        let mut fs = MockFilesystem::new();

        simulate_processor_layout(
            &mut fs,
            [0, 1, 2, 3, 4, 5],
            // 2 inactive processors.
            Some([true, true, true, true, false, false]),
            None,
            [0, 0, 0, 0, 0, 0],
            [99.9, 99.9, 99.9, 99.9, 99.9, 99.9],
        );

        simulate_cgroup_time_limit(&mut fs, -1, 100, true, false);

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        let max_processor_time = platform.max_processor_time();

        #[expect(
            clippy::float_cmp,
            reason = "we use absolute error, which is the right way to compare"
        )]
        {
            assert_eq!(
                f64_diff_abs(max_processor_time, 4.0, PROCESSOR_TIME_CLOSE_ENOUGH),
                0.0
            );
        }
    }

    #[test]
    fn max_processor_time_with_no_limit() {
        // If there is no data in the limit file,
        // we should use the available processor count as max processor time.
        let mut fs = MockFilesystem::new();

        simulate_processor_layout(
            &mut fs,
            [0, 1, 2, 3, 4, 5],
            // 2 inactive processors.
            Some([true, true, true, true, false, false]),
            None,
            [0, 0, 0, 0, 0, 0],
            [99.9, 99.9, 99.9, 99.9, 99.9, 99.9],
        );

        simulate_cgroup_time_limit(&mut fs, 50, 100, false, false);

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        let max_processor_time = platform.max_processor_time();

        #[expect(
            clippy::float_cmp,
            reason = "we use absolute error, which is the right way to compare"
        )]
        {
            assert_eq!(
                f64_diff_abs(max_processor_time, 4.0, PROCESSOR_TIME_CLOSE_ENOUGH),
                0.0
            );
        }
    }

    #[test]
    fn parse_cgroup_name_typical() {
        let input =
            "17:cpuset:/docker/6a74f501e3b4c9d93ad440a7b73149cf2b5d56073c109a8d774c0793f7fe267f
16:cpu:/docker/6a74f501e3b4c9d93ad440a7b73149cf2b5d56073c109a8d774c0793f7fe267f
15:memory:/docker/6a74f501e3b4c9d93ad440a7b73149cf2b5d56073c109a8d774c0793f7fe267f
0::/docker/6a74f501e3b4c9d93ad440a7b73149cf2b5d56073c109a8d774c0793f7fe267f";

        let expected = "/docker/6a74f501e3b4c9d93ad440a7b73149cf2b5d56073c109a8d774c0793f7fe267f";

        let result = parse_cgroup_name(input).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn parse_cgroup_name_v2_only() {
        let input = "0::/docker/6a74f501e3b4c9d93ad440a7b73149cf2b5d56073c109a8d774c0793f7fe267f";

        let expected = "/docker/6a74f501e3b4c9d93ad440a7b73149cf2b5d56073c109a8d774c0793f7fe267f";

        let result = parse_cgroup_name(input).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn parse_cgroup_name_v1_only() {
        let input =
            "17:cpuset:/docker/6a74f501e3b4c9d93ad440a7b73149cf2b5d56073c109a8d774c0793f7fe267f
        16:cpu:/docker/6a74f501e3b4c9d93ad440a7b73149cf2b5d56073c109a8d774c0793f7fe267f
        15:memory:/docker/6a74f501e3b4c9d93ad440a7b73149cf2b5d56073c109a8d774c0793f7fe267f";

        let result = parse_cgroup_name(input);

        // We do not today support v1-only configurations. In theory, we could add support for this
        // but we will hold off on supporting a legacy API version until there is a customer need
        // because all test systems use at least a hybrid v1/v2 configuration where even if the data
        // is configured via v1 API, the name is still published via v2 API.
        assert!(result.is_none());
    }

    #[test]
    fn parse_cgroup_name_garbage() {
        let input = "this does not appear to be a valid cgroup file";

        let result = parse_cgroup_name(input);
        assert!(result.is_none());
    }

    #[test]
    fn parse_v2_cgroup_cpu_quota_and_period_us_typical() {
        let input = "100000 100000";
        let expected = (100_000, 100_000);

        let result = parse_v2_cgroup_cpu_quota_and_period_us(input).unwrap();
        assert_eq!(result, expected);

        let input = "3333 1000";
        let expected = (3333, 1000);

        let result = parse_v2_cgroup_cpu_quota_and_period_us(input).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn parse_v2_cgroup_cpu_quota_and_period_us_unlimited() {
        let input = "max";

        let result = parse_v2_cgroup_cpu_quota_and_period_us(input);
        assert!(result.is_none());
    }

    #[test]
    fn parse_v2_cgroup_cpu_quota_and_period_us_garbage() {
        let input = "12345 this is complete garbage";

        let result = parse_v2_cgroup_cpu_quota_and_period_us(input);
        // We treat errors as missing data and ignore it, little point complaining here.
        assert!(result.is_none());
    }

    #[test]
    fn parse_v1_cgroup_cpu_quota_and_period_us_typical() {
        let quota = "100000";
        let period = "100000";
        let expected = (100_000, 100_000);

        let result = parse_v1_cgroup_cpu_quota_and_period_us(quota, period).unwrap();
        assert_eq!(result, expected);

        let quota = "3333";
        let period = "1000";
        let expected = (3333, 1000);

        let result = parse_v1_cgroup_cpu_quota_and_period_us(quota, period).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn parse_v1_cgroup_cpu_quota_and_period_us_unlimited() {
        let quota = "-1";
        let period = "100000";

        let result = parse_v1_cgroup_cpu_quota_and_period_us(quota, period);
        assert!(result.is_none());
    }

    #[test]
    fn parse_v1_cgroup_cpu_quota_and_period_us_garbage() {
        let quota = "this is garbage";
        let period = "there is no data here";

        let result = parse_v1_cgroup_cpu_quota_and_period_us(quota, period);
        // We treat errors as missing data and ignore it, little point complaining here.
        assert!(result.is_none());
    }

    #[test]
    fn basic_facts_are_represented() {
        let mut fs = MockFilesystem::new();

        //  3 memory regions, each containing 3 processors.
        simulate_processor_layout(
            &mut fs,
            [0, 1, 2, 3, 4, 5, 6, 7, 8],
            None,
            None,
            [0, 1, 2, 0, 1, 2, 0, 1, 2],
            [2000.0; 9],
        );

        let mut bindings = MockBindings::new();

        bindings.expect_sched_getcpu().times(1).return_const(5);

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(bindings),
            FilesystemFacade::from_mock(fs),
        );

        assert_eq!(platform.current_processor_id(), 5);
        assert_eq!(platform.max_processor_id(), 8);
        assert_eq!(platform.max_memory_region_id(), 2);
        assert_eq!(platform.get_all_processors().len(), 9);
    }

    #[test]
    fn cpuinfo_with_nonstandard_key_casing() {
        let mut fs = MockFilesystem::new();

        // The Linux kernel may report keys with different casing depending on the processor
        // architecture and kernel version. This test uses capital "BogoMIPS" instead of lowercase
        // "bogomips" to verify case-insensitive key matching.
        let cpuinfo = "processor       : 0
BogoMIPS        : 50.00
Features        : fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics fphp asimdhp cpuid asimdrdm lrcpc dcpop asimddp
CPU implementer : 0x41
CPU architecture: 8
CPU variant     : 0x3
CPU part        : 0xd0c
CPU revision    : 1

processor       : 1
BogoMIPS        : 50.00
Features        : fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics fphp asimdhp cpuid asimdrdm lrcpc dcpop asimddp
CPU implementer : 0x41
CPU architecture: 8
CPU variant     : 0x3
CPU part        : 0xd0c
CPU revision    : 1
";

        fs.expect_get_cpuinfo_contents()
            .times(1)
            .return_const(cpuinfo.to_string());

        fs.expect_get_numa_node_possible_contents()
            .times(1)
            .return_const(Some("0\n".to_string()));

        fs.expect_get_numa_node_cpulist_contents()
            .withf(move |n| *n == 0)
            .times(1)
            .return_const(Some("0,1\n".to_string()));

        fs.expect_get_cpu_online_contents()
            .withf(move |p| *p == 0)
            .times(1)
            .return_const(None);

        fs.expect_get_cpu_online_contents()
            .withf(move |p| *p == 1)
            .times(1)
            .return_const(Some("1\n".to_string()));

        fs.expect_get_proc_self_status_contents()
            .times(1)
            .return_const("Cpus_allowed_list: 0-1".to_string());

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        let processors = platform.get_all_processors();

        assert_eq!(processors.len(), 2);

        let p0 = &processors[0];
        assert_eq!(p0.as_target().id, 0);
        assert_eq!(p0.as_target().memory_region_id, 0);
        assert_eq!(
            p0.as_target().efficiency_class,
            EfficiencyClass::Performance
        );
        // This ARM-style cpuinfo has no `model name` field, so the model is synthesized from the
        // vendor and core-design fields the kernel reports instead.
        assert_eq!(
            p0.as_target().model.as_deref(),
            Some("cpuinfo(cpu implementer=0x41, cpu part=0xd0c)")
        );

        let p1 = &processors[1];
        assert_eq!(p1.as_target().id, 1);
        assert_eq!(p1.as_target().memory_region_id, 0);
        assert_eq!(
            p1.as_target().efficiency_class,
            EfficiencyClass::Performance
        );
    }

    #[test]
    fn cpuinfo_with_differing_part_reports_differing_model() {
        // Consumers use the model to tell hardware apart. Two ARM machines that differ only in
        // their core design must therefore not collapse into one identity, which is exactly what
        // happens if the synthesis ignores the core design or gives up when `model name` is
        // absent.
        let neoverse_n1 = "processor       : 0
BogoMIPS        : 50.00
CPU implementer : 0x41
CPU part        : 0xd0c
";

        let cortex_a72 = "processor       : 0
BogoMIPS        : 50.00
CPU implementer : 0x41
CPU part        : 0xd08
";

        let neoverse_n1_models = models_from_cpuinfo(neoverse_n1, [0]);
        let cortex_a72_models = models_from_cpuinfo(cortex_a72, [0]);

        assert!(neoverse_n1_models[0].is_some());
        assert_ne!(neoverse_n1_models, cortex_a72_models);
    }

    #[test]
    fn cpuinfo_with_heterogeneous_cores_reports_model_per_processor() {
        // A big.LITTLE system reports a different core design per processor, which must survive as
        // a different model per processor.
        let cpuinfo = "processor       : 0
BogoMIPS        : 50.00
CPU implementer : 0x41
CPU part        : 0xd0c

processor       : 1
BogoMIPS        : 50.00
CPU implementer : 0x41
CPU part        : 0xd03
";

        let models = models_from_cpuinfo(cpuinfo, [0, 1]);

        assert!(models[0].is_some());
        assert_ne!(models[0], models[1]);
    }

    #[test]
    fn cpuinfo_with_only_implementer_reports_model() {
        // Any stable discrimination beats none, so a lone identity field is still reported.
        let cpuinfo = "processor       : 0
BogoMIPS        : 50.00
CPU implementer : 0x41
";

        let models = models_from_cpuinfo(cpuinfo, [0]);

        assert_eq!(models[0].as_deref(), Some("cpuinfo(cpu implementer=0x41)"));
    }

    #[test]
    fn cpuinfo_with_only_part_reports_model() {
        let cpuinfo = "processor       : 0
BogoMIPS        : 50.00
CPU part        : 0xd0c
";

        let models = models_from_cpuinfo(cpuinfo, [0]);

        assert_eq!(models[0].as_deref(), Some("cpuinfo(cpu part=0xd0c)"));
    }

    #[test]
    fn cpuinfo_with_model_name_prefers_it_over_synthesis() {
        // A kernel-provided model identifies the processor far better than raw identity numbers
        // do, so it wins whenever both are available.
        let cpuinfo = "processor       : 0
bogomips        : 50.00
model name      : Example Processor 9000
CPU implementer : 0x41
CPU part        : 0xd0c
";

        let models = models_from_cpuinfo(cpuinfo, [0]);

        assert_eq!(models[0].as_deref(), Some("Example Processor 9000"));
    }

    #[test]
    fn cpuinfo_with_no_identifying_fields_reports_no_model() {
        // Nothing identifies this processor, so we report no model rather than inventing one.
        let cpuinfo = "processor       : 0
bogomips        : 50.00
whatever        : 123
";

        let models = models_from_cpuinfo(cpuinfo, [0]);

        assert_eq!(models[0], None);
    }

    #[test]
    fn cpuinfo_with_blank_fields_reports_no_model() {
        // Fields that are present but blank tell us nothing, so they must be treated as absent
        // rather than surfacing an empty or half-empty model string.
        let cpuinfo = "processor       : 0
bogomips        : 50.00
model name      :
CPU implementer :
CPU part        :
";

        let models = models_from_cpuinfo(cpuinfo, [0]);

        assert_eq!(models[0], None);
    }

    #[test]
    fn cpuinfo_with_blank_model_name_falls_back_to_synthesis() {
        // A blank `model name` identifies nothing, so the identity fields still have to be used.
        let cpuinfo = "processor       : 0
BogoMIPS        : 50.00
model name      :
CPU implementer : 0x41
CPU part        : 0xd0c
";

        let models = models_from_cpuinfo(cpuinfo, [0]);

        assert_eq!(
            models[0].as_deref(),
            Some("cpuinfo(cpu implementer=0x41, cpu part=0xd0c)")
        );
    }

    #[test]
    fn cpuinfo_with_trailing_machine_block_ignores_the_block() {
        // Some architectures append a block describing the machine rather than a processor. It
        // carries no processor index, so it must be skipped instead of counted as a processor.
        // The absence of the index is the whole reason we may skip it - see
        // `cpuinfo_with_unreadable_processor_index_fails_loudly` for the record that carries the
        // index but no readable value, which is not ours to skip.
        let cpuinfo = "processor       : 0
model name      : Example Processor rev 4 (v7l)
BogoMIPS        : 38.40
CPU implementer : 0x41
CPU part        : 0xd08

Hardware        : Example Board
Revision        : c03111
Serial          : 100000001b0f1a0d
";

        let models = models_from_cpuinfo(cpuinfo, [0]);

        assert_eq!(models.len(), 1);
        assert_eq!(models[0].as_deref(), Some("Example Processor rev 4 (v7l)"));
    }

    #[test]
    fn cpuinfo_with_unreadable_processor_index_fails_loudly() {
        // Dropping this record would report a two-processor machine as a one-processor machine,
        // and an undercount is invisible to consumers - a smaller machine is a perfectly
        // plausible reading. Only a record that names no processor at all may be skipped.
        let cpuinfo = "processor       : 0
bogomips        : 50.00

processor       : the second one
bogomips        : 50.00
";

        let mut fs = MockFilesystem::new();

        fs.expect_get_cpuinfo_contents()
            .times(1)
            .return_const(cpuinfo.to_string());

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        assert_panics(|| platform.get_cpuinfo());
    }

    #[test]
    fn cpuinfo_with_differently_rendered_fields_reports_one_model() {
        // Consumers partition their data by model, so the model must follow the hardware and
        // nothing else. These payloads describe one processor in every spelling the kernel could
        // plausibly reach for, and all of them have to land on the same model.
        let as_the_kernel_renders_it = "processor       : 0
BogoMIPS        : 50.00
CPU implementer : 0x41
CPU part        : 0xd0c
";

        let padded_wider = "processor       : 0
BogoMIPS        : 50.00
CPU implementer : 0x041
CPU part        : 0x0d0c
";

        let uppercase = "processor       : 0
BogoMIPS        : 50.00
CPU implementer : 0X41
CPU part        : 0xD0C
";

        // Written with escapes because the extra spacing is on the ends of the values, where a
        // multi-line literal would leave it at the mercy of trailing-whitespace tooling.
        let padded_with_whitespace =
            "processor : 0\nbogomips :  50.00 \nCPU implementer :   0x41  \nCPU part :  0xd0c \n";

        for cpuinfo in [
            as_the_kernel_renders_it,
            padded_wider,
            uppercase,
            padded_with_whitespace,
        ] {
            let models = models_from_cpuinfo(cpuinfo, [0]);

            assert_eq!(
                models[0].as_deref(),
                Some("cpuinfo(cpu implementer=0x41, cpu part=0xd0c)")
            );
        }
    }

    #[test]
    fn cpuinfo_with_unrecognized_field_rendering_reports_it_verbatim() {
        // A rendering we cannot read still tells one core design from another, so it is reported
        // rather than dropped.
        let cpuinfo = "processor       : 0
BogoMIPS        : 50.00
CPU implementer : ARM
CPU part        : Neoverse-N1
";

        let models = models_from_cpuinfo(cpuinfo, [0]);

        assert_eq!(
            models[0].as_deref(),
            Some("cpuinfo(cpu implementer=ARM, cpu part=Neoverse-N1)")
        );
    }

    #[test]
    fn cpuinfo_without_bogomips_reports_a_uniform_machine() {
        // The `bogomips` field is architecture-dependent and some kernels emit none at all. Such a
        // machine still has processors to enumerate - it merely discloses nothing about how they
        // compare, which is what a machine of identical processors looks like in any case.
        let cpuinfo = "processor       : 0
model name      : Example Processor 9000
isa             : rv64imafdch

processor       : 1
model name      : Example Processor 9000
isa             : rv64imafdch
";

        let processors = processors_from_cpuinfo(cpuinfo, [0, 1]);

        assert_eq!(processors.len(), 2);

        for processor in &processors {
            assert_eq!(
                processor.as_target().efficiency_class,
                EfficiencyClass::Performance
            );
            assert_eq!(
                processor.as_target().relative_speed,
                RelativeSpeed::UNDETERMINED
            );
        }
    }

    #[test]
    fn cpuinfo_with_unreadable_bogomips_still_reports_the_processor() {
        // A value we cannot read discloses no speed, exactly as an absent field discloses none,
        // and neither is reason to lose the processor - the caller would then be told about a
        // smaller machine than it has.
        let cpuinfo = "processor       : 0
bogomips        : ludicrous speed
model name      : Example Processor 9000
";

        let processors = processors_from_cpuinfo(cpuinfo, [0]);

        assert_eq!(processors.len(), 1);
        assert_eq!(
            processors[0].as_target().relative_speed,
            RelativeSpeed::UNDETERMINED
        );
    }

    #[test]
    fn cpuinfo_with_bogomips_on_only_some_processors_demotes_nobody() {
        // A kernel that discloses the metric for one processor and not another says nothing about
        // how the two compare, and an undisclosed speed is no evidence of a slower processor.
        // Demoting it would misdirect callers that place work by efficiency class.
        let cpuinfo = "processor       : 0
bogomips        : 50.00
model name      : Example Processor 9000

processor       : 1
model name      : Example Processor 9000
";

        let processors = processors_from_cpuinfo(cpuinfo, [0, 1]);

        assert_eq!(processors.len(), 2);

        for processor in &processors {
            assert_eq!(
                processor.as_target().efficiency_class,
                EfficiencyClass::Performance
            );
        }

        assert_eq!(
            processors[0].as_target().relative_speed,
            RelativeSpeed::from_os_metric(50)
        );
        assert_eq!(
            processors[1].as_target().relative_speed,
            RelativeSpeed::UNDETERMINED
        );
    }

    /// Loads processors from a raw `/proc/cpuinfo` payload, with all processors online, allowed
    /// and in a single memory region. Returns the model of each processor, in processor ID order.
    fn models_from_cpuinfo<const PROCESSOR_COUNT: usize>(
        cpuinfo: &str,
        processor_ids: [ProcessorId; PROCESSOR_COUNT],
    ) -> Vec<Option<String>> {
        processors_from_cpuinfo(cpuinfo, processor_ids)
            .into_iter()
            .map(|processor| {
                processor
                    .as_target()
                    .model
                    .as_deref()
                    .map(ToString::to_string)
            })
            .collect()
    }

    /// Loads processors from a raw `/proc/cpuinfo` payload, with all processors online, allowed
    /// and in a single memory region. Returns the processors in processor ID order.
    fn processors_from_cpuinfo<const PROCESSOR_COUNT: usize>(
        cpuinfo: &str,
        processor_ids: [ProcessorId; PROCESSOR_COUNT],
    ) -> NonEmpty<ProcessorFacade> {
        const MEMORY_REGION: MemoryRegionId = 0;

        let mut fs = MockFilesystem::new();

        fs.expect_get_cpuinfo_contents()
            .times(1)
            .return_const(cpuinfo.to_string());

        fs.expect_get_numa_node_possible_contents()
            .times(1)
            .return_const(Some(format!("{MEMORY_REGION}\n")));

        let cpulist = processor_ids.iter().join(",");

        fs.expect_get_numa_node_cpulist_contents()
            .withf(|node| *node == MEMORY_REGION)
            .times(1)
            .return_const(Some(format!("{cpulist}\n")));

        for processor_id in processor_ids {
            fs.expect_get_cpu_online_contents()
                .withf(move |p| *p == processor_id)
                .times(1)
                .return_const(Some("1\n".to_string()));
        }

        fs.expect_get_proc_self_status_contents()
            .times(1)
            .return_const(format!("Cpus_allowed_list: {cpulist}"));

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        platform.get_all_processors()
    }

    #[test]
    fn proc_self_status_with_empty_lines_interspersed() {
        // This test specifically verifies that empty lines in /proc/self/status
        // are correctly filtered out and do not cause parsing errors.
        let mut fs = MockFilesystem::new();

        let cpuinfo = "processor       : 0
bogomips        : 99.9
whatever        : 123
other           : ignored

processor       : 1
bogomips        : 99.9
whatever        : 123
other           : ignored

";

        fs.expect_get_cpuinfo_contents()
            .times(1)
            .return_const(cpuinfo.to_string());

        fs.expect_get_numa_node_possible_contents()
            .times(1)
            .return_const(Some("0\n".to_string()));

        fs.expect_get_numa_node_cpulist_contents()
            .withf(move |n| *n == 0)
            .times(1)
            .return_const(Some("0,1\n".to_string()));

        fs.expect_get_cpu_online_contents()
            .withf(move |p| *p == 0)
            .times(1)
            .return_const(Some("1\n".to_string()));

        fs.expect_get_cpu_online_contents()
            .withf(move |p| *p == 1)
            .times(1)
            .return_const(Some("1\n".to_string()));

        // Include empty lines interspersed in the status content.
        let status_with_empty_lines = "Name:   test_process

Umask:  0022

State:  R (running)

Cpus_allowed:   ffffffff

Cpus_allowed_list:      0-1

Mems_allowed:   1
";

        fs.expect_get_proc_self_status_contents()
            .times(1)
            .return_const(status_with_empty_lines.to_string());

        let platform = BuildTargetPlatform::new(
            BindingsFacade::from_mock(MockBindings::new()),
            FilesystemFacade::from_mock(fs),
        );

        // The key assertion: parsing should succeed despite empty lines.
        let processors = platform.get_all_processors();
        assert_eq!(processors.len(), 2);
        assert_eq!(processors[0].as_target().id, 0);
        assert_eq!(processors[1].as_target().id, 1);
    }
}
