use std::hint::black_box;
use std::mem::offset_of;
use std::num::NonZero;
use std::ptr::NonNull;
use std::sync::{Arc, OnceLock};

use folo_ffi::NativeBuffer;
use itertools::Itertools;
use nonempty::NonEmpty;
use smallvec::SmallVec;
use windows::Win32::Foundation::ERROR_INSUFFICIENT_BUFFER;
use windows::Win32::System::JobObjects::{
    JOB_OBJECT_CPU_RATE_CONTROL_ENABLE, JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP,
    JOB_OBJECT_CPU_RATE_CONTROL_MIN_MAX_RATE, JOBOBJECT_CPU_RATE_CONTROL_INFORMATION,
};
use windows::Win32::System::SystemInformation::{
    GROUP_AFFINITY, LOGICAL_PROCESSOR_RELATIONSHIP, RelationNumaNode, RelationNumaNodeEx,
    RelationProcessorCore, SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX,
};
use windows::core::HRESULT;

use crate::pal::windows::{Bindings, BindingsFacade, ProcessorGroupIndex, ProcessorIndexInGroup};
use crate::pal::{GroupMask, Platform, ProcessorFacade, ProcessorImpl};
use crate::{EfficiencyClass, MemoryRegionId, ProcessorId, RelativeSpeed};

/// Singleton instance of `BuildTargetPlatform`, used by public API types
/// to hook up to the correct PAL implementation.
pub static BUILD_TARGET_PLATFORM: BuildTargetPlatform =
    BuildTargetPlatform::new(BindingsFacade::target());

const PROCESSOR_GROUP_MAX_SIZE: usize = 64;

// Some of our logic will operate without heap allocations as long as the group count is not higher.
const ALLOC_FREE_GROUPS_MAX: usize = 8;

/// The platform that matches the crate's build target.
///
/// You would only use a different platform in unit tests that need to mock the platform.
/// Even then, whenever possible, unit tests should use the real platform for maximum realism.
#[derive(Debug)]
pub struct BuildTargetPlatform {
    bindings: BindingsFacade,

    max_processor_id: OnceLock<ProcessorId>,

    active_processor_count: OnceLock<NonZero<usize>>,

    // We cache these as we expect them to never change. This data is in non-local memory in
    // systems with multiple memory regions, which is not ideal but the bookkeeping to make it
    // local is also not really better. `#[thread_local]` might help but is currently unstable.
    // All these group arrays are indexed by group index.
    group_max_count: OnceLock<ProcessorGroupIndex>,
    group_max_sizes: OnceLock<Box<[ProcessorIndexInGroup]>>,
    group_active_sizes: OnceLock<Box<[ProcessorIndexInGroup]>>,
    group_start_offsets: OnceLock<Box<[ProcessorId]>>,

    // Combines some of the above information to make it easier to work with.
    group_metas: OnceLock<Box<[ProcessorGroupMeta]>>,
}

#[derive(Debug)]
struct ProcessorGroupMeta {
    max_processors: ProcessorIndexInGroup,
    active_processors: ProcessorIndexInGroup,

    start_offset: ProcessorId,

    // All the theoretical processor IDs, including processors that are offline.
    // Sorted by value, ascending.
    all_processor_ids: Vec<ProcessorId>,

    // All the processor IDs of active processors, without those that are not offline.
    // This is not the same as "available" processors because the operating system
    // resource constraint mechanisms may limit the set we can actually use further.
    // Sorted by value, ascending.
    active_processor_ids: Vec<ProcessorId>,
}

impl Platform for BuildTargetPlatform {
    fn get_all_processors(&self) -> NonEmpty<ProcessorFacade> {
        let group_metas = self.get_processor_group_metas();

        let efficiency_classes = self.get_processor_efficiency_classes();
        let memory_regions = self.get_processor_memory_regions();
        let relative_speeds = self.get_processor_relative_speeds();
        let models = self.get_processor_models();
        let allowed_processors = self.processors_allowed_by_job_constraints();

        // We are required to return all the processors ordered by the processor ID.
        // As we know that Windows assigns processor IDs sequentially, we can just
        // iterate in order through the groups and each processor in each group.
        NonEmpty::collect(group_metas.iter().enumerate()
            .flat_map(move |(group_index, meta)| {
                meta.active_processor_ids.iter().map(move |&processor_id| {
                    let index_in_group = processor_id
                        .checked_sub(meta.start_offset)
                        .and_then(|x| u8::try_from(x).ok())
                        .expect(
                        "processor ID calculation overflowed - platform must have given us bad inputs",
                    );

                    (group_index, index_in_group, processor_id)
                })
            })
            .filter_map(|(group_index, index_in_group, processor_id)| {
                if !allowed_processors.contains(&processor_id) {
                    return None;
                }

                let memory_region_index = *memory_regions
                    .get(processor_id as usize)
                    .expect("we expect to have the memory region for every processor ID unless the platform lied to us at some point");

                let efficiency_class = *efficiency_classes.get(processor_id as usize).expect("we expect to have the efficiency class for every processor ID unless the platform lied to us at some point");

                let relative_speed = *relative_speeds.get(processor_id as usize).expect("we expect to have the relative speed for every processor ID unless the platform lied to us at some point");

                let model = models.get(processor_id as usize).expect("we expect to have the model slot for every processor ID unless the platform lied to us at some point").clone();

                Some(ProcessorImpl::new(
                    group_index
                        .try_into()
                        .expect("group index can only overflow if our algorithm has a logic error"),
                    index_in_group,
                    processor_id,
                    memory_region_index,
                    efficiency_class,
                    relative_speed,
                    model,
                ))
            })
        ).expect(
            "we are returning all processors on the system - obviously there must be at least one",
        ).map(ProcessorFacade::Target)
    }

    fn pin_current_thread_to<P>(&self, processors: &NonEmpty<P>)
    where
        P: AsRef<ProcessorFacade>,
    {
        let group_count = self.get_processor_group_max_count();

        // We define one mask per processor group, even if that mask is all clear,
        // to ensure that we overwrite any previous state that may have existed.
        let mut affinity_masks = SmallVec::<[GROUP_AFFINITY; ALLOC_FREE_GROUPS_MAX]>::new();

        for group_index in 0..group_count {
            let mut mask = GroupMask::none();

            // We started with all bits clear and now set any that we do want to allow.
            for processor in processors
                .iter()
                .filter(|p| p.as_ref().as_target().group_index == group_index)
            {
                mask.add(processor.as_ref().as_target());
            }

            affinity_masks.push(GROUP_AFFINITY {
                Group: group_index,
                Mask: mask.value(),
                ..Default::default()
            });
        }

        self.bindings
            .set_current_thread_cpu_set_masks(&affinity_masks);
    }

    fn current_processor_id(&self) -> ProcessorId {
        let current_processor = self.bindings.get_current_processor_number_ex();

        let group_start_offsets = self.get_processor_group_start_offsets();

        let group_start_offset = *group_start_offsets
            .get(current_processor.Group as usize)
            .expect("the platform told us how many groups exist, so if it now tells us an out of bounds group, nothing we can do");

        group_start_offset
            .checked_add(ProcessorId::from(current_processor.Number))
            .expect("processor ID calculation overflowed - only possible if platform gives us bad IDs as inputs")
    }

    fn max_processor_id(&self) -> ProcessorId {
        *self.max_processor_id.get_or_init(|| {
            let group_max_sizes = self.get_processor_group_max_sizes();

            // Windows assigns processor IDs sequentially from 0, so we just calculate.
            // The max processor ID is the sum of all processors in all groups minus 1.
            group_max_sizes
                .iter()
                .map(|&s| ProcessorId::from(s))
                .sum::<ProcessorId>()
                .checked_sub(1)
                .expect("there must be at least 1 theoretical processor in each group")
        })
    }

    fn max_memory_region_id(&self) -> MemoryRegionId {
        self.bindings.get_numa_highest_node_number()
    }

    fn current_thread_processors(&self) -> NonEmpty<ProcessorId> {
        let mut current_thread_affinities = self.bindings.get_current_thread_cpu_set_masks();

        // A thread may have no masks defined, in which case it will inherit from the process.
        // Note that the process mask is ignored if a thread mask is set.
        if current_thread_affinities.is_empty() {
            current_thread_affinities = self.bindings.get_current_process_default_cpu_set_masks();
        }

        // A process may also have no mask defined! In this case, we check the legacy mechanisms
        // used before Windows was many-processor aware. While this crate never uses this mechanism
        // to **set** limits, we may still inherit such limits from e.g. "start /affinity".
        if current_thread_affinities.is_empty() {
            let legacy_affinities = self.bindings.get_current_thread_legacy_group_affinity();

            // The challenge here is that this always returns some data - the legacy affinity is
            // always *something*, unlike the CPU sets feature which is only present when customized
            // which means that even if the platform wants to apply no limits, there is a value and
            // we must detect the default value to ignore it here. This is important because the
            // legacy mechanism is only capable of affinitizing to a single processor group, so if
            // we obey its default value, we would inadvertently follow that limitation.
            //
            // Therefore, we apply the following heuristic: if the mask is set to a value that
            // allows all processors in this processor group, we consider it a default value and
            // pretend it does not exist, allowing the thread to run on all processors in all
            // processor groups.
            let group_max_sizes = self.get_processor_group_max_sizes();

            // TODO: Does the default mask contain all bits for max or active processors?
            let default_mask_processor_count = *group_max_sizes
                .get(legacy_affinities.Group as usize)
                .expect("platform referenced a processor group that was out of bounds");

            let is_default_mask =
                legacy_affinities.Mask.count_ones() == u32::from(default_mask_processor_count);

            if !is_default_mask {
                // Found a non-default legacy affinity mask, so we use it.
                current_thread_affinities = vec![legacy_affinities];
            }
        }

        // If the legacy mechanism also says nothing (== was at its default value)
        // then the current thread may use all processors in all memory regions.
        if current_thread_affinities.is_empty() {
            // Note that we also include offline processors here because this function
            // does not know or need to know which ones are active and which are not, as
            // this list is only used as input for further processing upstream.
            // Any inactive processors will be filtered out by the caller later.
            return NonEmpty::collect(0..=self.max_processor_id())
                .expect("range over non-empty set cannot result in empty result");
        }

        let group_metas = self.get_processor_group_metas();

        NonEmpty::collect(group_metas.iter().enumerate()
            .filter_map(move |(group_index, meta)| {
                let Some(affinity_mask) = current_thread_affinities.iter().find_map(|a| {
                    if usize::from(a.Group) == group_index {
                        Some(a.Mask)
                    } else {
                        None
                    }
                }) else {
                    // Depending on how we obtained the per-group affinity masks, it may in theory
                    // be possible that we do not have an affinity mask for some group. In that
                    // case we consider the group off-limits and skip it.
                    return None;
                };

                let mask = GroupMask::from_components(
                    affinity_mask,
                    ProcessorGroupIndex::try_from(group_index)
                        .expect("platform gave us processor group index that was out of bounds"),
                );

                Some((0..meta.max_processors).filter_map(move |index_in_group| {
                    if mask.contains_by_index_in_group(index_in_group) {
                        Some(
                            *meta
                                .all_processor_ids
                                .get(index_in_group as usize)
                                .expect("we validated the bounds above"),
                        )
                    } else {
                        None
                    }
                }))
            }).flatten()
        ).expect("we are returning the set of processors assigned to the current thread - obviously there must be at least one because the thread is executing")
    }

    fn max_processor_time(&self) -> f64 {
        // The job object processor time limits are relative to the total number of
        // active processors on the system and do not consider any per-process limitations.
        #[expect(clippy::cast_precision_loss, reason = "value is always in safe range")]
        let system_processor_count = self.active_processor_count() as f64;

        // This API, however, speaks in terms of processor time available to the current process,
        // so we do need to care about per-process limitations. Whatever value we return, it
        // cannot be greater than this because this is the upper limit available to the process.
        #[expect(
            clippy::cast_precision_loss,
            reason = "precision loss will never happen because all realistic values are in safe f64 range"
        )]
        let current_process_processor_count = self.get_all_processors().len() as f64;

        let Some(rate_control) = self.bindings.get_current_job_cpu_rate_control() else {
            // No rate control, so we can use all processors available to the current process.
            return current_process_processor_count;
        };

        // There are different rate control modes. We care about the following:
        // 1. Hard cap, which just gives us a single number.
        // 2. Soft cap (guaranteed + ceiling), in which case we use the ceiling.
        // Other modes (e.g. weighted) we ignore because they cannot be expressed in absolutes.
        if is_hard_capped(rate_control) {
            // SAFETY: Guarded by the flags we validated.
            let windows_cpu_rate = unsafe { rate_control.Anonymous.CpuRate };

            Self::windows_processor_rate_to_processor_time(windows_cpu_rate, system_processor_count)
                .min(current_process_processor_count)
        } else if is_soft_capped(rate_control) {
            // SAFETY: Guarded by the flags we validated.
            let windows_cpu_rate = unsafe { rate_control.Anonymous.Anonymous.MaxRate };

            Self::windows_processor_rate_to_processor_time(
                windows_cpu_rate.into(),
                system_processor_count,
            )
            .min(current_process_processor_count)
        } else {
            // Found no limits, fall back to our internally detected ceiling.
            current_process_processor_count
        }
    }

    fn active_processor_count(&self) -> usize {
        self.active_processor_count
            .get_or_init(|| {
                let group_active_sizes = self.get_processor_group_active_sizes();

                NonZero::new(
                    group_active_sizes
                        .iter()
                        .map(|&s| usize::from(s))
                        .sum::<usize>(),
                )
                .expect("a system with 0 active processors is impossible")
            })
            .get()
    }
}

impl BuildTargetPlatform {
    pub(crate) const fn new(bindings: BindingsFacade) -> Self {
        Self {
            bindings,
            group_max_count: OnceLock::new(),
            group_max_sizes: OnceLock::new(),
            group_active_sizes: OnceLock::new(),
            group_start_offsets: OnceLock::new(),
            max_processor_id: OnceLock::new(),
            group_metas: OnceLock::new(),
            active_processor_count: OnceLock::new(),
        }
    }

    #[must_use]
    fn get_processor_group_max_count(&self) -> ProcessorGroupIndex {
        *self
            .group_max_count
            .get_or_init(|| self.bindings.get_maximum_processor_group_count())
    }

    #[must_use]
    fn max_processor_count(&self) -> usize {
        (self.max_processor_id() as usize).checked_add(1)
            .expect("this could only overflow if the platform has usize::MAX processors, which is unrealistic")
    }

    /// Returns the max number of processors in each processor group, ordered ascending
    /// by group index. This is used to calculate the global index of a processor and/or
    /// the start offset of each processor group.
    #[must_use]
    fn get_processor_group_max_sizes(&self) -> &[u8] {
        self.group_max_sizes.get_or_init(|| {
            let group_count = self.get_processor_group_max_count();

            let mut group_sizes = Vec::with_capacity(group_count as usize);

            for group_index in 0..group_count {
                let processor_count = self.bindings.get_maximum_processor_count(group_index);

                let processor_count = u8::try_from(processor_count)
                    .expect("somehow encountered processor group with more than 256 processors, which is impossible as per Windows API - the max is 64");

                group_sizes.push(processor_count);
            }

            group_sizes.into_boxed_slice()
        })
    }

    /// Returns the max number of active in each processor group, ordered ascending
    /// by group index.
    #[must_use]
    fn get_processor_group_active_sizes(&self) -> &[u8] {
        self.group_active_sizes.get_or_init(|| {
            let group_count = self.get_processor_group_max_count();

            let mut group_sizes = Vec::with_capacity(group_count as usize);

            for group_index in 0..group_count {
                let processor_count = self.bindings.get_active_processor_count(group_index);

                let processor_count = u8::try_from(processor_count)
                    .expect("somehow encountered processor group with more than 256 processors, which is impossible as per Windows API - the max is 64");

                group_sizes.push(processor_count);
            }

            group_sizes.into_boxed_slice()
        })
    }

    #[must_use]
    fn get_processor_group_start_offsets(&self) -> &[ProcessorId] {
        self.group_start_offsets.get_or_init(|| {
            let group_sizes = self.get_processor_group_max_sizes();

            let mut group_offsets = Vec::with_capacity(group_sizes.len());

            let mut group_start_offset: ProcessorId = 0;
            for &size in group_sizes {
                group_offsets.push(group_start_offset);
                group_start_offset = group_start_offset.checked_add(ProcessorId::from(size))
                    .expect("processor group start offset overflowed - this is only possible if the platform have us bad inputs");
            }

            group_offsets.into_boxed_slice()
        })
    }

    #[must_use]
    fn get_processor_group_metas(&self) -> &[ProcessorGroupMeta] {
        self.group_metas.get_or_init(|| {
            let max_sizes = self.get_processor_group_max_sizes();
            let active_sizes = self.get_processor_group_active_sizes();
            let start_offsets = self.get_processor_group_start_offsets();

            assert_eq!(
                max_sizes.len(),
                start_offsets.len(),
                "platform must provide group sizes and offsets in equal amounts",
            );

            assert_eq!(
                max_sizes.len(),
                active_sizes.len(),
                "platform must provide group sizes and active sizes in equal amounts",
            );

            let mut group_metas = Vec::with_capacity(max_sizes.len());

            for ((group_index, &max_size), &start_offset) in max_sizes.iter().enumerate().zip(start_offsets.iter()) {
                let active_size = *active_sizes
                    .get(group_index)
                    .expect("we validated bounds above");

                let active_id_end = start_offset
                    .checked_add(ProcessorId::from(active_size))
                    .expect("processor ID calculation overflowed - platform must have given us invalid inputs");

                let all_id_end = start_offset
                    .checked_add(ProcessorId::from(max_size))
                    .expect("processor ID calculation overflowed - platform must have given us invalid inputs");

                group_metas.push(ProcessorGroupMeta {
                    max_processors: max_size,
                    active_processors: active_size,
                    start_offset,
                    active_processor_ids: (start_offset..active_id_end).collect_vec(),
                    all_processor_ids: (start_offset..all_id_end).collect_vec(),
                });
            }

            group_metas.into_boxed_slice()
        })
    }

    #[must_use]
    fn get_logical_processor_information_raw(
        &self,
        relationship: LOGICAL_PROCESSOR_RELATIONSHIP,
    ) -> NativeBuffer<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX> {
        loop {
            let mut required_length: u32 = 0;

            // SAFETY: Pointers must outlive the call (true - local variable lives beyond call).
            let probe_result = unsafe {
                self.bindings.get_logical_processor_information_ex(
                    relationship,
                    None,
                    &raw mut required_length,
                )
            };
            let e = probe_result.expect_err("GetLogicalProcessorInformationEx with null buffer must always fail and return required buffer size");
            assert_eq!(
                e.code(),
                HRESULT::from_win32(ERROR_INSUFFICIENT_BUFFER.0),
                "GetLogicalProcessorInformationEx size probe failed with unexpected error code {e}",
            );

            let required_length_usize = NonZero::<usize>::new(required_length as usize).expect(
                "GetLogicalProcessorInformationEx size probe said 0 bytes are needed - impossible",
            );
            let mut buffer =
                NativeBuffer::<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX>::new(required_length_usize);
            let mut final_length = required_length;

            // SAFETY: Pointers must outlive the call (true - local variables live beyond call).
            let real_result = unsafe {
                self.bindings.get_logical_processor_information_ex(
                    relationship,
                    Some(buffer.as_ptr().as_ptr()),
                    &raw mut final_length,
                )
            };

            // In theory, it could still have failed with "insufficient buffer" because the set of
            // processors available to us can change at any time. Super unlikely but let us be safe.
            match check_logical_processor_info_result(&real_result) {
                LogicalProcessorInfoResult::Retry => continue,
                LogicalProcessorInfoResult::Proceed => {}
            }

            // Signal the buffer that we wrote into it.
            // SAFETY: We must be sure that the specified number of bytes have actually been written.
            // We are sure, obviously, because the operating system just told us it did that.
            unsafe {
                buffer.set_len_bytes(final_length as usize);
            }

            return buffer;
        }
    }

    /// Gets the efficiency classes of all processors on the system, ordered by processor ID.
    /// This also returns data for offline processors but the value for those is unspecified.
    fn get_processor_efficiency_classes(&self) -> Box<[EfficiencyClass]> {
        let mut native_efficiency_classes: Vec<u8> = vec![0; self.max_processor_count()];

        let core_relationships_raw =
            self.get_logical_processor_information_raw(RelationProcessorCore);

        // We create a map of processor index to efficiency class. Then we simply take all
        // processors with the max efficiency class (whatever the numeric value) - those are the
        // performance processors.

        // The structures returned by the OS are dynamically sized so we only have various
        // disgusting options for parsing/processing them. Pointer wrangling is the most readable.
        let raw_range = core_relationships_raw.as_data_ptr_range();
        let mut next: NonNull<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX> = raw_range.start.cast();
        let end = raw_range.end.cast();

        while next < end {
            // SAFETY: We just process the data in the form the OS promises to give it to us.
            let info = unsafe { next.as_ref() };

            // SAFETY: We just process the data in the form the OS promises to give it to us.
            next = unsafe { next.byte_add(info.Size as usize) };

            assert_eq!(info.Relationship, RelationProcessorCore);

            // SAFETY: Guarded via info.Relationship, asserted above.
            let details = unsafe { &info.Anonymous.Processor };

            // API docs: If the PROCESSOR_RELATIONSHIP structure represents a processor core,
            // the GroupCount member is always 1.
            assert_eq!(details.GroupCount, 1);

            // There may be 1 or more bits set in the mask because one core might have multiple
            // logical processors via SMT (hyper-threading). We just iterate over all the bits
            // to check them individually without worrying about SMT logic.
            for processor_id in self.affinity_mask_to_processor_ids(&details.GroupMask[0]) {
                *native_efficiency_classes.get_mut(processor_id as usize)
                    .expect("the platform gave us a processor ID that was out of the range of valid processor IDs - it lied about the max ID!") = details.EfficiencyClass;
            }
        }

        let max_native_efficiency_class = native_efficiency_classes.iter().max().copied().expect(
            "there must be at least one processor - this code is running on one, after all",
        );

        native_efficiency_classes
            .into_iter()
            .map(|native| {
                if native == max_native_efficiency_class {
                    EfficiencyClass::Performance
                } else {
                    EfficiencyClass::Efficiency
                }
            })
            .collect_vec()
            .into_boxed_slice()
    }

    /// Gets the relative speed of all processors on the system, ordered by processor ID.
    /// This also returns data for offline processors but the value for those is unspecified.
    #[must_use]
    fn get_processor_relative_speeds(&self) -> Box<[RelativeSpeed]> {
        // The bindings surface each processor's nominal maximum clock frequency (MHz) by reading
        // the values Windows records in the registry for every logical processor across all
        // processor groups. This is a passive read that never changes any thread's affinity. The
        // MHz value is scaled into the abstract relative-speed domain by `from_os_metric`;
        // processors the platform reports no value for (0 MHz) map to a fallback value.
        self.bindings
            .get_processor_max_mhz(self.max_processor_count())
            .into_iter()
            .map(RelativeSpeed::from_os_metric)
            .collect_vec()
            .into_boxed_slice()
    }

    /// Gets the model strings of all processors on the system, ordered by processor ID.
    /// This also returns data for offline processors. Processors the platform reports no model
    /// for map to `None`.
    #[must_use]
    fn get_processor_models(&self) -> Box<[Option<Arc<str>>]> {
        // The bindings surface each processor's model string by reading the `ProcessorNameString`
        // value Windows records in the registry for every logical processor across all processor
        // groups. This is a passive read that never changes any thread's affinity.
        self.bindings
            .get_processor_name_strings(self.max_processor_count())
            .into_iter()
            .map(|name| name.map(Arc::from))
            .collect_vec()
            .into_boxed_slice()
    }

    /// Gets the efficiency classes of all processors on the system, ordered by processor ID.
    /// This also returns data for offline processors.
    #[must_use]
    #[cfg_attr(test, mutants::skip)] // Mutating this can lead to non-deterministic looping as mutations can violate memory safety. We could define a safer iterator abstraction but it hardly seems worth it.
    fn get_processor_memory_regions(&self) -> Box<[MemoryRegionId]> {
        // TODO: Verify that this returns correct data for offline processors.
        let memory_region_relationships_raw =
            self.get_logical_processor_information_raw(RelationNumaNodeEx);

        let mut result = vec![0; self.max_processor_count()];

        // The structures returned by the OS are dynamically sized so we only have various
        // disgusting options for parsing/processing them. Pointer wrangling is the most readable.
        let raw_range = memory_region_relationships_raw.as_data_ptr_range();
        let mut next: NonNull<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX> = raw_range.start.cast();
        let end = raw_range.end.cast();

        while next < end {
            let current = next;

            // SAFETY: We just process the data in the form the OS promises to give it to us.
            let info = unsafe { current.as_ref() };

            // SAFETY: We just process the data in the form the OS promises to give it to us.
            next = unsafe { next.byte_add(info.Size as usize) };

            // Even though we request NumaNodeEx, it returns entries with NumaNode. It just does.
            // It always does, asking for Ex simply allows it to return multiple processor groups
            // for the same NUMA node (instead of just the primary group as with plain NumaNode).
            assert_eq!(info.Relationship, RelationNumaNode);

            // SAFETY: Guarded via info.Relationship, asserted above.
            let details = unsafe { &info.Anonymous.NumaNode };

            let numa_node_number = details.NodeNumber;

            // In the struct definition, this is a 1-element array because Rust has no notion
            // of dynamic-size arrays. We use pointer arithmetic to access the real array elements.
            //
            // NOTE: that we need to start from scratch with the original pointer here!
            // Pointer -> shared ref -> pointer conversions are not guaranteed to return the
            // original pointer, so if we get rid of a pointer once, we cannot get it back!
            //
            // SAFETY: RelationNumaNodeEx guarantees that this union member is present.
            let mut group_mask_array = unsafe {
                current
                    .byte_add(offset_of!(
                        SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX,
                        Anonymous.NumaNode.Anonymous.GroupMask
                    ))
                    .cast::<GROUP_AFFINITY>()
            };

            for _ in 0..details.GroupCount {
                // SAFETY: The OS promises us that this array contains `GroupCount` elements.
                let affinity = unsafe { *group_mask_array.as_ref() };

                for processor_id in self.affinity_mask_to_processor_ids(&affinity) {
                    *result.get_mut(processor_id as usize)
                        .expect("the platform gave us a processor ID that was out of the range of valid processor IDs - it lied about the max ID!") = numa_node_number;
                }

                // SAFETY: The OS promises us that this array contains `GroupCount` elements.
                // It is fine to move past the end if we never access it (because the loop ends).
                group_mask_array = unsafe { group_mask_array.add(1) };
            }
        }

        result.into_boxed_slice()
    }

    #[must_use]
    fn affinity_mask_to_processor_ids(
        &self,
        affinity: &GROUP_AFFINITY,
    ) -> heapless::Vec<ProcessorId, PROCESSOR_GROUP_MAX_SIZE> {
        let mut result = heapless::Vec::new();

        let group_index: ProcessorGroupIndex = affinity.Group;

        let meta = self.get_processor_group_metas().get(group_index as usize).expect(
            "platform indicated a processor group that was out of range of known processor groups",
        );

        // Minimum effort approach for WOW64 support - we only see the first 32 in a group.
        let processors_in_group = meta
            .active_processors
            .min(u8::try_from(usize::BITS).expect("constant that always fits into u8"));

        let mask = GroupMask::from_components(affinity.Mask, affinity.Group);

        for index_in_group in 0..processors_in_group {
            if !mask.contains_by_index_in_group(index_in_group) {
                continue;
            }

            result.push(*meta.all_processor_ids.get(index_in_group as usize).expect(
                "internal conflict between processor group metadata - expected ID not found",
            )).expect("result could only be full if we somehow processed more than PROCESSOR_GROUP_MAX_SIZE processors, which is nonsense");
        }

        result
    }

    /// The job object (if it exists) defines hard limits for what processors we are allowed to use.
    /// If there is no limit defined, we allow all processors and return all processor IDs.
    #[must_use]
    fn processors_allowed_by_job_constraints(&self) -> NonEmpty<ProcessorId> {
        let job_affinity_masks = self.bindings.get_current_job_cpu_set_masks();

        if job_affinity_masks.is_empty() {
            return NonEmpty::collect(0..=self.max_processor_id())
                .expect("range over non-empty set cannot result in empty result");
        }

        let group_metas = self.get_processor_group_metas();

        NonEmpty::collect(group_metas
            .iter()
            .enumerate()
            .filter_map(|(group_index, meta)| {
                job_affinity_masks.iter().find_map(|a| {
                    if usize::from(a.Group) == group_index {
                        Some((a.Mask, group_index, meta))
                    } else {
                        // This group was not a member of the defined affinity mask, so we
                        // do not care about this group and do not consider it allowed.
                        None
                    }
                })
            })
            .flat_map(|(mask, group_index, meta)| {
                let mask = GroupMask::from_components(
                    mask,
                    ProcessorGroupIndex::try_from(group_index)
                        .expect("platform gave us processor group index that was out of bounds"),
                );

                (0..meta.max_processors).filter_map(move |index_in_group| {
                    if mask.contains_by_index_in_group(index_in_group) {
                        Some(meta.start_offset
                            .checked_add(ProcessorId::from(index_in_group))
                            .expect("processor ID calculation overflowed - platform must have given us bad inputs"))
                    } else {
                        None
                    }
                })
            }))
            .expect("we are returning the set of processors assigned to the current thread - obviously there must be at least one because the thread is executing")
    }

    /// Windows measures processor time in 1/100 of a percent (10000 = 100%) of the total available
    /// processor time available on the system, ignoring any per-process maximums and not counting
    /// offline processors.
    ///
    /// We measure time in seconds of processor time per second of real time.
    ///
    /// This converts from Windows to our internal representation.
    fn windows_processor_rate_to_processor_time(
        windows_rate: u32,
        system_processor_count: f64,
    ) -> f64 {
        // Get the ratio of processor time on all processors on the system.
        // 1 means the process can use all processor time on the system (if not otherwise limited).
        let cpu_ratio = f64::from(windows_rate) / 10_000.0;

        cpu_ratio * system_processor_count
    }

    /// Returns the processors assigned to the current thread without facade caching.
    ///
    /// This entry point exists for direct PAL benchmarks.
    #[must_use]
    #[cfg_attr(test, mutants::skip)] // Just for benchmarking, not real code.
    pub fn current_thread_processors_for_bench(&self) -> NonEmpty<ProcessorId> {
        self.current_thread_processors()
    }

    /// Converts a Windows group affinity directly into logical processor IDs.
    ///
    /// This entry point exists for direct PAL benchmarks.
    #[must_use]
    #[cfg_attr(test, mutants::skip)] // Just for benchmarking, not real code.
    pub fn affinity_mask_to_processor_ids_for_bench(
        &self,
        mask: &GROUP_AFFINITY,
    ) -> heapless::Vec<ProcessorId, PROCESSOR_GROUP_MAX_SIZE> {
        self.affinity_mask_to_processor_ids(mask)
    }

    /// Enumerates all processors without facade caching.
    ///
    /// This entry point consumes the result so benchmarks measure the complete PAL operation.
    #[cfg_attr(test, mutants::skip)] // Just for benchmarking, not real code.
    pub fn get_all_processors_for_bench(&self) {
        black_box(self.get_all_processors());
    }
}

#[cfg_attr(test, mutants::skip)] // No-op mutates the constant, not helpful.
fn is_hard_capped(rate_control: JOBOBJECT_CPU_RATE_CONTROL_INFORMATION) -> bool {
    const HARD_CAP_FLAGS: u32 =
        JOB_OBJECT_CPU_RATE_CONTROL_ENABLE.0 | JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP.0;

    rate_control.ControlFlags.0 & HARD_CAP_FLAGS == HARD_CAP_FLAGS
}

#[cfg_attr(test, mutants::skip)] // No-op mutates the constant, not helpful.
fn is_soft_capped(rate_control: JOBOBJECT_CPU_RATE_CONTROL_INFORMATION) -> bool {
    const SOFT_CAP_FLAGS: u32 =
        JOB_OBJECT_CPU_RATE_CONTROL_ENABLE.0 | JOB_OBJECT_CPU_RATE_CONTROL_MIN_MAX_RATE.0;

    rate_control.ControlFlags.0 & SOFT_CAP_FLAGS == SOFT_CAP_FLAGS
}

/// Result of checking the outcome of `GetLogicalProcessorInformationEx`.
enum LogicalProcessorInfoResult {
    /// The caller should retry the call because the buffer size changed.
    Retry,
    /// The call succeeded and the caller should proceed with processing the data.
    Proceed,
}

/// Checks the result of `GetLogicalProcessorInformationEx` and returns the action to take.
///
/// Panics if the call failed with an unexpected error.
// Excluded from coverage because:
// 1. The retry path requires the processor set to change between size probe and actual call,
//    which is extremely rare and impractical to test.
// 2. The panic path requires an OS-level failure that is unrealistic to trigger.
#[cfg_attr(coverage_nightly, coverage(off))]
fn check_logical_processor_info_result(
    result: &windows::core::Result<()>,
) -> LogicalProcessorInfoResult {
    if let Err(e) = result {
        if e.code() == HRESULT::from_win32(ERROR_INSUFFICIENT_BUFFER.0) {
            // Buffer size changed between probe and actual call. Retry.
            return LogicalProcessorInfoResult::Retry;
        }

        panic!("GetLogicalProcessorInformationEx failed with unexpected error code: {e}");
    }

    LogicalProcessorInfoResult::Proceed
}

#[allow(
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::indexing_slicing,
    reason = "we need not worry in tests"
)]
#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::hint::black_box;
    use std::mem::offset_of;
    use std::sync::Arc;

    use itertools::Itertools;
    use mockall::Sequence;
    use static_assertions::assert_eq_size;
    use testing::f64_diff_abs;
    use windows::Win32::System::JobObjects::{
        JOB_OBJECT_CPU_RATE_CONTROL, JOBOBJECT_CPU_RATE_CONTROL_INFORMATION,
        JOBOBJECT_CPU_RATE_CONTROL_INFORMATION_0, JOBOBJECT_CPU_RATE_CONTROL_INFORMATION_0_0,
    };
    use windows::Win32::System::Kernel::PROCESSOR_NUMBER;

    use super::*;
    use crate::MemoryRegionId;
    use crate::pal::windows::{MockBindings, ProcessorIndexInGroup};

    const PROCESSOR_TIME_CLOSE_ENOUGH: f64 = 0.01;

    #[test]
    fn constructors_execute_at_runtime() {
        let bindings = black_box(BindingsFacade::target());
        let platform = black_box(BuildTargetPlatform::new(bindings));

        assert!(matches!(platform.bindings, BindingsFacade::Target(_)));
    }

    #[test]
    fn get_all_processors_smoke_test() {
        // We imagine a simple system with 2 physical cores, 4 logical processors, all in a
        // single processor group and a single memory region. Welcome to 2010!
        let mut bindings = MockBindings::new();
        simulate_processor_layout(
            &mut bindings,
            [4],
            [4],
            [vec![0, 0, 0, 0]],
            [vec![0, 0, 0, 0]],
            None, // All processors are allowed by job constraints.
        );

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));

        let processors = platform.get_all_processors();

        // We expect to see 4 logical processors. This API does not care about the physical cores.
        assert_eq!(processors.len(), 4);

        // All processors must be in the same group and memory region.
        assert_eq!(
            1,
            processors
                .iter()
                .map(|p| p.as_target().group_index)
                .dedup()
                .count()
        );
        assert_eq!(
            1,
            processors
                .iter()
                .map(|p| p.as_target().memory_region_id)
                .dedup()
                .count()
        );

        let p0 = &processors[0];
        assert_eq!(p0.as_target().group_index, 0);
        assert_eq!(p0.as_target().index_in_group, 0);
        assert_eq!(p0.as_target().memory_region_id, 0);

        let p1 = &processors[1];
        assert_eq!(p1.as_target().group_index, 0);
        assert_eq!(p1.as_target().index_in_group, 1);
        assert_eq!(p1.as_target().memory_region_id, 0);

        let p2 = &processors[2];
        assert_eq!(p2.as_target().group_index, 0);
        assert_eq!(p2.as_target().index_in_group, 2);
        assert_eq!(p2.as_target().memory_region_id, 0);

        let p3 = &processors[3];
        assert_eq!(p3.as_target().group_index, 0);
        assert_eq!(p3.as_target().index_in_group, 3);
        assert_eq!(p3.as_target().memory_region_id, 0);
    }

    #[test]
    fn get_all_processors_reports_relative_speed() {
        // A simple single-group system with 4 logical processors. The simulated layout reports a
        // distinct nominal clock speed per processor - processor `i` reports `(i + 1) * 1000` MHz -
        // which is scaled by `RelativeSpeed::from_os_metric` into the processor's relative speed.
        // The raw MHz figure is deliberately scaled (by pi), not surfaced unchanged, so the
        // assertion below compares against `from_os_metric` rather than the raw MHz value.
        let mut bindings = MockBindings::new();
        simulate_processor_layout(
            &mut bindings,
            [4],
            [4],
            [vec![0, 0, 0, 0]],
            [vec![0, 0, 0, 0]],
            None,
        );

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));

        let processors = platform.get_all_processors();

        assert_eq!(processors.len(), 4);

        for (index, processor) in processors.iter().enumerate() {
            let expected_mhz = (u32::try_from(index).unwrap() + 1) * 1000;
            assert_eq!(
                processor.as_target().relative_speed,
                RelativeSpeed::from_os_metric(expected_mhz)
            );
        }
    }

    #[test]
    fn get_all_processors_reports_model() {
        // A simple single-group system with 4 logical processors. The simulated layout reports the
        // same model string for every processor, which must surface unchanged as the processor's
        // model.
        let mut bindings = MockBindings::new();
        simulate_processor_layout(
            &mut bindings,
            [4],
            [4],
            [vec![0, 0, 0, 0]],
            [vec![0, 0, 0, 0]],
            None,
        );

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));

        let processors = platform.get_all_processors();

        assert_eq!(processors.len(), 4);

        for processor in &processors {
            assert_eq!(
                processor.as_target().model.as_deref(),
                Some("Test Processor Model")
            );
        }
    }

    #[test]
    fn get_all_processors_reports_relative_speed_across_groups() {
        // Two processor groups with two logical processors each. The registry-backed binding reports
        // one nominal clock speed per processor across all groups at once, so this proves that
        // processors in every group - not just the first - receive their reported relative speed.
        // Processor `i` (global ID) reports `(i + 1) * 1000` MHz, and the two groups occupy processor
        // IDs 0..2 and 2..4.
        let mut bindings = MockBindings::new();
        simulate_processor_layout(
            &mut bindings,
            [2, 2],
            [2, 2],
            [vec![0, 0], vec![0, 0]],
            [vec![0, 0], vec![1, 1]],
            None,
        );

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));

        let processors = platform.get_all_processors();

        assert_eq!(processors.len(), 4);

        for (index, processor) in processors.iter().enumerate() {
            let expected_mhz = (u32::try_from(index).unwrap() + 1) * 1000;
            assert_eq!(
                processor.as_target().relative_speed,
                RelativeSpeed::from_os_metric(expected_mhz)
            );
        }
    }

    #[test]
    fn two_numa_nodes_efficiency_performance() {
        let mut bindings = MockBindings::new();
        // Two groups, each with 2 active processors:
        // Group 0 -> [Performance, Efficiency], Group 1 -> [Efficiency, Performance].
        simulate_processor_layout(
            &mut bindings,
            [2, 2],
            [2, 2],
            [vec![1, 0], vec![0, 1]],
            [vec![0, 0], vec![1, 1]],
            None, // All processors are allowed by job constraints.
        );

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));
        let processors = platform.get_all_processors();
        assert_eq!(processors.len(), 4);

        // Group 0
        let p0 = &processors[0];
        assert_eq!(p0.as_target().group_index, 0);
        assert_eq!(p0.as_target().index_in_group, 0);
        assert_eq!(p0.as_target().memory_region_id, 0);
        assert_eq!(
            p0.as_target().efficiency_class,
            EfficiencyClass::Performance
        );

        let p1 = &processors[1];
        assert_eq!(p1.as_target().group_index, 0);
        assert_eq!(p1.as_target().index_in_group, 1);
        assert_eq!(p1.as_target().memory_region_id, 0);
        assert_eq!(p1.as_target().efficiency_class, EfficiencyClass::Efficiency);

        // Group 1
        let p2 = &processors[2];
        assert_eq!(p2.as_target().group_index, 1);
        assert_eq!(p2.as_target().index_in_group, 0);
        assert_eq!(p2.as_target().memory_region_id, 1);
        assert_eq!(p2.as_target().efficiency_class, EfficiencyClass::Efficiency);

        let p3 = &processors[3];
        assert_eq!(p3.as_target().group_index, 1);
        assert_eq!(p3.as_target().index_in_group, 1);
        assert_eq!(p3.as_target().memory_region_id, 1);
        assert_eq!(
            p3.as_target().efficiency_class,
            EfficiencyClass::Performance
        );
    }

    #[test]
    fn one_big_numa_two_small_nodes() {
        let mut bindings = MockBindings::new();
        // Three groups: group 0 -> 4 Performance, group 1 -> 2 Efficiency, group 2 -> 2 Efficiency
        simulate_processor_layout(
            &mut bindings,
            [4, 2, 2],
            [4, 2, 2],
            [vec![1, 1, 1, 1], vec![0, 0], vec![0, 0]],
            [vec![0, 0, 0, 0], vec![1, 1], vec![2, 2]],
            None, // All processors are allowed by job constraints.
        );

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));
        let processors = platform.get_all_processors();
        assert_eq!(processors.len(), 8);

        // First 4 in group 0 => Performance
        for i in 0..4 {
            let p = &processors[i];
            assert_eq!(p.as_target().group_index, 0);
            assert_eq!(p.as_target().index_in_group, i as ProcessorIndexInGroup);
            assert_eq!(p.as_target().memory_region_id, 0);
            assert_eq!(p.as_target().efficiency_class, EfficiencyClass::Performance);
        }
        // Next 2 in group 1 => Efficiency
        for i in 4..6 {
            let p = &processors[i];
            assert_eq!(p.as_target().group_index, 1);
            assert_eq!(
                p.as_target().index_in_group,
                (i - 4) as ProcessorIndexInGroup
            );
            assert_eq!(p.as_target().memory_region_id, 1);
            assert_eq!(p.as_target().efficiency_class, EfficiencyClass::Efficiency);
        }
        // Last 2 in group 2 => Efficiency
        for i in 6..8 {
            let p = &processors[i];
            assert_eq!(p.as_target().group_index, 2);
            assert_eq!(
                p.as_target().index_in_group,
                (i - 6) as ProcessorIndexInGroup
            );
            assert_eq!(p.as_target().memory_region_id, 2);
            assert_eq!(p.as_target().efficiency_class, EfficiencyClass::Efficiency);
        }
    }

    #[test]
    fn one_active_one_inactive_numa_node() {
        let mut bindings = MockBindings::new();
        // Group 0 -> inactive, Group 1 -> [Performance, Efficiency, Performance]
        simulate_processor_layout(
            &mut bindings,
            [0, 3],
            [2, 3],
            [vec![], vec![1, 0, 1]],
            [vec![], vec![1, 1, 1]],
            None, // All processors are allowed by job constraints.
        );

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));
        let processors = platform.get_all_processors();
        assert_eq!(processors.len(), 3);

        // Group 1 => [Perf, Eff, Perf]
        let p0 = &processors[0];
        assert_eq!(p0.as_target().group_index, 1);
        assert_eq!(p0.as_target().index_in_group, 0);
        assert_eq!(p0.as_target().memory_region_id, 1);
        assert_eq!(
            p0.as_target().efficiency_class,
            EfficiencyClass::Performance
        );

        let p1 = &processors[1];
        assert_eq!(p1.as_target().group_index, 1);
        assert_eq!(p1.as_target().index_in_group, 1);
        assert_eq!(p1.as_target().memory_region_id, 1);
        assert_eq!(p1.as_target().efficiency_class, EfficiencyClass::Efficiency);

        let p2 = &processors[2];
        assert_eq!(p2.as_target().group_index, 1);
        assert_eq!(p2.as_target().index_in_group, 2);
        assert_eq!(p2.as_target().memory_region_id, 1);
        assert_eq!(
            p2.as_target().efficiency_class,
            EfficiencyClass::Performance
        );
    }

    #[test]
    fn two_numa_nodes_some_inactive_processors() {
        let mut bindings = MockBindings::new();
        // Group 0 -> Efficiency, Group 1 -> Performance
        simulate_processor_layout(
            &mut bindings,
            [2, 2],
            [4, 4],
            [vec![0, 0], vec![1, 1]],
            [vec![0, 0], vec![1, 1]],
            None, // All processors are allowed by job constraints.
        );

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));
        let processors = platform.get_all_processors();
        assert_eq!(processors.len(), 4);

        // Group 0 => [Eff, Eff]
        let p0 = &processors[0];
        assert_eq!(p0.as_target().group_index, 0);
        assert_eq!(p0.as_target().index_in_group, 0);
        assert_eq!(p0.as_target().memory_region_id, 0);
        assert_eq!(p0.as_target().efficiency_class, EfficiencyClass::Efficiency);

        let p1 = &processors[1];
        assert_eq!(p1.as_target().group_index, 0);
        assert_eq!(p1.as_target().index_in_group, 1);
        assert_eq!(p1.as_target().memory_region_id, 0);
        assert_eq!(p1.as_target().efficiency_class, EfficiencyClass::Efficiency);

        // Group 1 => [Perf, Perf]
        let p2 = &processors[2];
        assert_eq!(p2.as_target().group_index, 1);
        assert_eq!(p2.as_target().index_in_group, 0);
        assert_eq!(p2.as_target().memory_region_id, 1);
        assert_eq!(
            p2.as_target().efficiency_class,
            EfficiencyClass::Performance
        );

        let p3 = &processors[3];
        assert_eq!(p3.as_target().group_index, 1);
        assert_eq!(p3.as_target().index_in_group, 1);
        assert_eq!(p3.as_target().memory_region_id, 1);
        assert_eq!(
            p3.as_target().efficiency_class,
            EfficiencyClass::Performance
        );
    }

    #[test]
    fn one_multi_group_numa_node_one_small() {
        let mut bindings = MockBindings::new();
        // Group 0 -> [Perf, Perf], Group 1 -> [Eff, Eff], Group 2 -> [Perf]
        simulate_processor_layout(
            &mut bindings,
            [2, 2, 1],
            [2, 2, 1],
            [vec![1, 1], vec![0, 0], vec![1]],
            [vec![0, 0], vec![0, 0], vec![1]],
            None, // All processors are allowed by job constraints.
        );

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));
        let processors = platform.get_all_processors();
        assert_eq!(processors.len(), 5);

        // Group 0 => [Performance, Performance]
        for (i, p) in processors.iter().enumerate() {
            let p = p.as_target();

            if i < 2 {
                assert_eq!(p.group_index, 0);
                assert_eq!(p.memory_region_id, 0);
            } else if i < 4 {
                assert_eq!(p.group_index, 1);
                assert_eq!(p.memory_region_id, 0);
            } else {
                assert_eq!(p.group_index, 2);
                assert_eq!(p.memory_region_id, 1);
            }
            assert_eq!(p.index_in_group, i as ProcessorIndexInGroup % 2); // Groups of max 2.

            if i < 2 {
                assert_eq!(p.efficiency_class, EfficiencyClass::Performance);
            } else if i < 4 {
                assert_eq!(p.efficiency_class, EfficiencyClass::Efficiency);
            } else {
                assert_eq!(p.efficiency_class, EfficiencyClass::Performance);
            }
        }
    }

    #[test]
    fn job_affinity_limits_applied() {
        let mut bindings = MockBindings::new();
        // Three groups, 3x2 processors. Job constraints limit us to 2+1+0.
        simulate_processor_layout(
            &mut bindings,
            [2, 2, 2],
            [2, 2, 2],
            [vec![1, 1], vec![1, 1], vec![1, 1]],
            [vec![0, 0], vec![0, 0], vec![0, 0]],
            Some([vec![true, true], vec![true, false], vec![false, false]]),
        );

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));
        let processors = platform.get_all_processors();
        assert_eq!(processors.len(), 3);

        // Group 0
        let p0 = &processors[0];
        assert_eq!(p0.as_target().group_index, 0);
        assert_eq!(p0.as_target().index_in_group, 0);
        assert_eq!(p0.as_target().memory_region_id, 0);

        let p1 = &processors[1];
        assert_eq!(p1.as_target().group_index, 0);
        assert_eq!(p1.as_target().index_in_group, 1);
        assert_eq!(p1.as_target().memory_region_id, 0);

        // Group 1
        let p2 = &processors[2];
        assert_eq!(p2.as_target().group_index, 1);
        assert_eq!(p2.as_target().index_in_group, 0);
        assert_eq!(p2.as_target().memory_region_id, 0);
    }

    #[test]
    fn insufficient_buffer_retry() {
        use mockall::Sequence;

        let mut bindings = MockBindings::new();
        let mut seq = Sequence::new();

        // First iteration, probe (None) => size=64 => insufficient buffer
        bindings
            .expect_get_logical_processor_information_ex()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|rel, buf, _| rel == &RelationProcessorCore && buf.is_none())
            .returning(|_, _, returned_length| {
                // SAFETY: No safety requirements.
                unsafe {
                    *returned_length = 64;
                }
                Err(windows::core::Error::from_hresult(
                    ERROR_INSUFFICIENT_BUFFER.to_hresult(),
                ))
            });

        // First iteration, real call (Some(64)) => still insufficient => size=96
        bindings
            .expect_get_logical_processor_information_ex()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|rel, buf, _| rel == &RelationProcessorCore && buf.is_some())
            .returning(|_, _, returned_length| {
                // SAFETY: No safety requirements.
                unsafe {
                    *returned_length = 96;
                }
                Err(windows::core::Error::from_hresult(
                    ERROR_INSUFFICIENT_BUFFER.to_hresult(),
                ))
            });

        // Second iteration, probe (None) => size=128 => insufficient
        bindings
            .expect_get_logical_processor_information_ex()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|rel, buf, _| rel == &RelationProcessorCore && buf.is_none())
            .returning(|_, _, returned_length| {
                // SAFETY: No safety requirements.
                unsafe {
                    *returned_length = 128;
                }

                Err(windows::core::Error::from_hresult(
                    ERROR_INSUFFICIENT_BUFFER.to_hresult(),
                ))
            });

        // Second iteration, real call (Some(128)) => success
        bindings
            .expect_get_logical_processor_information_ex()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|rel, buf, _| rel == &RelationProcessorCore && buf.is_some())
            .returning(|_, _, returned_length| {
                // SAFETY: No safety requirements.
                unsafe {
                    *returned_length = 128;
                }

                Ok(())
            });

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));
        let buffer = platform.get_logical_processor_information_raw(RelationProcessorCore);
        assert!(!buffer.is_empty(), "Expected final buffer to be filled");
    }

    /// Configures mock bindings to simulate a particular type of processor layout.
    ///
    /// The simulation is valid for one call to `get_all_processors()` only.
    fn simulate_processor_layout<const GROUP_COUNT: usize>(
        bindings: &mut MockBindings,
        // If entry is 0 the group is not considered active. Such groups have to be at the end.
        // Presumably. Lack of machine to actually test this on limits our ability to be sure.
        group_active_counts: [ProcessorIndexInGroup; GROUP_COUNT],
        group_max_counts: [ProcessorIndexInGroup; GROUP_COUNT],
        // Only for the active processors in each group.
        efficiency_ratings_per_group: [Vec<u8>; GROUP_COUNT],
        // Only for the active processors in each group.
        memory_regions_per_group: [Vec<MemoryRegionId>; GROUP_COUNT],
        // Only for the active processors in each group. If None, all are allowed.
        job_affinitized_processors_per_group: Option<[Vec<bool>; GROUP_COUNT]>,
    ) {
        assert_eq!(group_active_counts.len(), group_max_counts.len());
        assert_eq!(group_active_counts.len(), memory_regions_per_group.len());
        assert_eq!(
            group_active_counts.len(),
            efficiency_ratings_per_group.len()
        );

        for (group_index, group_active_processors) in group_active_counts.iter().enumerate() {
            assert!(*group_active_processors <= group_max_counts[group_index]);

            assert_eq!(
                efficiency_ratings_per_group[group_index].len(),
                *group_active_processors as usize
            );
        }

        // Move into Arc because we need to keep referencing it in the mock.
        let group_active_counts = Arc::from(group_active_counts);
        let group_max_counts = Arc::from(group_max_counts);
        let efficiency_ratings_per_group = Arc::from(efficiency_ratings_per_group);
        let memory_regions_per_group = Arc::from(memory_regions_per_group);

        simulate_get_counts(bindings, &group_active_counts, &group_max_counts);
        simulate_get_numa_relations(bindings, &memory_regions_per_group);
        simulate_get_core_info(
            bindings,
            &group_active_counts,
            &efficiency_ratings_per_group,
        );
        simulate_get_relative_speeds(bindings);
        simulate_get_models(bindings);

        // Transform the booleans to IDs.
        let job_affinitized_processors_per_group =
            job_affinitized_processors_per_group.map(|groups| {
                groups
                    .into_iter()
                    .map(|processor_bools| {
                        processor_bools
                            .into_iter()
                            .enumerate()
                            .filter_map(|(index_in_group, is_allowed)| {
                                if is_allowed {
                                    Some(index_in_group as ProcessorIndexInGroup)
                                } else {
                                    None
                                }
                            })
                            .collect_vec()
                    })
                    .collect_vec()
            });

        simulate_job_constraints(bindings, job_affinitized_processors_per_group);
    }

    fn simulate_get_counts(
        bindings: &mut MockBindings,
        group_active_counts: &Arc<[ProcessorIndexInGroup]>,
        group_max_counts: &Arc<[ProcessorIndexInGroup]>,
    ) {
        let max_group_count = group_active_counts.len() as u16;

        // The "get maximum" we expect the platform to always report the same numbers for, so we
        // do not care how many times they are called. The "get active" we only want to call once
        // per processor group to avoid multiple calls into the platform getting different results.

        // Note that "get active group count" is not actually used - the code probes all groups
        // and just checks number of processors in group to identify if the group is active.
        bindings
            .expect_get_maximum_processor_group_count()
            .return_const(max_group_count);

        bindings
            .expect_get_active_processor_count()
            .times(group_max_counts.len())
            .returning({
                let group_active_counts = Arc::clone(group_active_counts);

                move |group_number| u32::from(group_active_counts[group_number as usize])
            });

        bindings.expect_get_maximum_processor_count().returning({
            let group_max_counts = Arc::clone(group_max_counts);

            move |group_number| u32::from(group_max_counts[group_number as usize])
        });
    }

    /// Registers a default `get_processor_max_mhz` expectation that reports a distinct nominal clock
    /// speed per processor ID: processor `i` reports `(i + 1) * 1000` MHz. The binding surfaces one
    /// value per processor across all processor groups, indexed directly by processor ID, so this
    /// gives every simulated processor a unique, real (non-fallback) relative speed and lets tests
    /// observe the value flowing through `get_all_processors()` for processors in every group.
    fn simulate_get_relative_speeds(bindings: &mut MockBindings) {
        bindings
            .expect_get_processor_max_mhz()
            .returning(move |max_processor_count| {
                (0..max_processor_count)
                    .map(|processor_id| {
                        let processor_id =
                            u32::try_from(processor_id).expect("processor ID fits in u32");
                        (processor_id + 1) * 1000
                    })
                    .collect_vec()
            });
    }

    /// Registers a default `get_processor_name_strings` expectation that reports the same model
    /// string for every processor ID, mirroring a homogeneous machine. The binding surfaces one
    /// value per processor across all processor groups, indexed directly by processor ID, so this
    /// lets tests observe the model flowing through `get_all_processors()` for processors in every
    /// group.
    fn simulate_get_models(bindings: &mut MockBindings) {
        bindings
            .expect_get_processor_name_strings()
            .returning(move |max_processor_count| {
                std::iter::repeat_with(|| Some("Test Processor Model".to_string()))
                    .take(max_processor_count)
                    .collect_vec()
            });
    }

    fn simulate_get_numa_relations(
        bindings: &mut MockBindings,
        // Active processors only.
        active_processor_memory_regions_per_group: &Arc<[Vec<MemoryRegionId>]>,
    ) {
        // The mask logic is weird on 32-bit, so we have not bothered to implement/test it.
        // We do not today care about correctly operating in 32-bit builds.
        assert_eq_size!(u64, usize);

        // NB! WARNING! size_of<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX>() is not correct! This is
        // because there is a dynamically-sized array in there that in the Rust bindings is just
        // hardcoded to size 1. The real size in memory may be greater than size_of()!
        //
        // We define here some additional buffer to add on top of the "known" memory.
        const MAX_ADDITIONAL_PROCESSOR_GROUPS: usize = 32;

        #[repr(C)]
        struct ExpandedLogicalProcessorInformation {
            root: SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX,
            // We do not necessarily use it in-place, this is a fake member
            // just to make room in the sizeof calculation.
            extra_buffer: [GROUP_AFFINITY; MAX_ADDITIONAL_PROCESSOR_GROUPS],
        }

        // There will be a call to get the groups that are members of each NUMA node.
        // The NUMA response is structured by memory region, not by processor group.
        // Therefore we need to also structure our data this way.
        let memory_regions_to_group_indexes = active_processor_memory_regions_per_group
            .iter()
            .enumerate()
            .flat_map(|(group_index, active_processor_memory_regions)| {
                active_processor_memory_regions
                    .iter()
                    .map(move |memory_region_index| {
                        (*memory_region_index, group_index as ProcessorGroupIndex)
                    })
                    .unique()
            })
            .into_group_map();

        let memory_region_count = memory_regions_to_group_indexes.len();

        let memory_region_responses = memory_regions_to_group_indexes
            .into_iter()
            .map(|(memory_region_index, group_indexes)| {
                // We made some additional space in the struct to allow for more groups
                // but there is still an upper limit to what we can fit, so be careful.
                assert!(group_indexes.len() <= MAX_ADDITIONAL_PROCESSOR_GROUPS);

                let mut response = ExpandedLogicalProcessorInformation {
                    root: SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX {
                        // This is always RelationNumaNode, even if RelationNumaNodeEx is requested.
                        // RelationNumaNodeEx simply allows the group count to be more than 1.
                        Relationship: RelationNumaNode,
                        Size: size_of::<ExpandedLogicalProcessorInformation>() as u32,
                        ..Default::default()
                    },
                    extra_buffer: [GROUP_AFFINITY::default(); MAX_ADDITIONAL_PROCESSOR_GROUPS],
                };

                // SAFETY: We define RelationNumaNode above so this dereference is valid.
                let response_numa = unsafe { &mut response.root.Anonymous.NumaNode };

                response_numa.NodeNumber = memory_region_index;
                response_numa.GroupCount = group_indexes.len() as u16;

                // The type definition just has an array of 1 here (not correct), so we write
                // through pointers to fill each array element. We have to do this dance of starting
                // out borrow from the root of the document we are returning to prove to the
                // compiler and Miri that we have the rights to write beyond the 1-element array.
                let document = &raw mut response;

                // SAFETY: We define GroupCount above so this dereference is valid.
                let mut group_mask = unsafe {
                    document
                        .byte_add(offset_of!(
                            ExpandedLogicalProcessorInformation,
                            root.Anonymous.NumaNode.Anonymous.GroupMask
                        ))
                        .cast::<GROUP_AFFINITY>()
                };

                for group_index in group_indexes {
                    let mut mask: u64 = 0;

                    for (index_in_group, memory_region_id) in
                        active_processor_memory_regions_per_group[group_index as usize]
                            .iter()
                            .enumerate()
                    {
                        if *memory_region_id != memory_region_index {
                            continue;
                        }

                        mask |= 1 << index_in_group;
                    }

                    let group_data = GROUP_AFFINITY {
                        Group: group_index,
                        Mask: mask as usize,
                        ..Default::default()
                    };

                    // SAFETY: We define GroupCount above and ensured that we have enough space
                    // via MAX_ADDITIONAL_PROCESSOR_GROUPS so this is valid.
                    unsafe {
                        group_mask.write(group_data);
                    }

                    // SAFETY: We define GroupCount above and ensured that we have enough space
                    // via MAX_ADDITIONAL_PROCESSOR_GROUPS so this is valid.
                    group_mask = unsafe { group_mask.add(1) };
                }

                response
            })
            .collect_vec();

        let memory_region_response_buffer = NativeBuffer::from_items(memory_region_responses);
        let memory_region_response_len = memory_region_response_buffer.len_bytes();

        let mut seq = Sequence::new();

        // This will actually be two calls, one for the size probe and one for the real data.
        bindings
            .expect_get_logical_processor_information_ex()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|relationship_type, buffer, _| {
                *relationship_type == RelationNumaNodeEx && buffer.is_none()
            })
            .returning(move |_, _, returned_length| {
                // SAFETY: Caller must guarantee that the pointer is valid for use.
                unsafe {
                    *returned_length = memory_region_response_len as u32;
                }

                Err(windows::core::Error::from_hresult(HRESULT::from_win32(
                    ERROR_INSUFFICIENT_BUFFER.0,
                )))
            });

        bindings
            .expect_get_logical_processor_information_ex()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|relationship_type, buffer, _| {
                *relationship_type == RelationNumaNodeEx && buffer.is_some()
            })
            .returning_st(move |_, buffer, returned_length| {
                // SAFETY: Caller must guarantee that the pointer is valid for use.
                unsafe {
                    *returned_length = memory_region_response_len as u32;
                }

                // SAFETY: Caller must guarantee that the pointer is valid for use.
                unsafe {
                    buffer
                        .unwrap()
                        .cast::<ExpandedLogicalProcessorInformation>()
                        .copy_from_nonoverlapping(
                            memory_region_response_buffer.as_ptr().as_ptr(),
                            memory_region_count,
                        );
                }

                Ok(())
            });
    }

    fn simulate_get_core_info(
        bindings: &mut MockBindings,
        group_active_counts: &Arc<[ProcessorIndexInGroup]>,
        // Active processors only.
        active_processor_efficiency_ratings_per_group: &Arc<[Vec<u8>]>,
    ) {
        // Next we define the "get core info" simulation, used to access metadata of each processor.
        // Specifically, this provides the efficiency class. The other info is currently unused.

        // In case of RelationProcessorCore, we do not use any dynamically sized arrays, which
        // keeps the size logic a bit simpler than with the NUMA nodes response. We have one entry
        // in the response for each processor. For the sake of simplicity (the code does not care)
        // we pretend that each core is 1 logical processor (no SMT).
        let response_entries = group_active_counts
            .iter()
            .enumerate()
            .flat_map(|(group_index, group_active_count)| {
                (0..*group_active_count).map({
                    let efficiency_ratings_per_group =
                        Arc::clone(active_processor_efficiency_ratings_per_group);

                    move |index_in_group| {
                        let mut entry = SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX {
                            Relationship: RelationProcessorCore,
                            Size: size_of::<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX>() as u32,
                            ..Default::default()
                        };

                        // SAFETY: We define RelationProcessorCore above so this dereference is valid.
                        let entry_processor = unsafe { &mut entry.Anonymous.Processor };

                        entry_processor.EfficiencyClass =
                            efficiency_ratings_per_group[group_index][index_in_group as usize];

                        entry_processor.GroupCount = 1;
                        entry_processor.GroupMask[0].Group = group_index as u16;
                        entry_processor.GroupMask[0].Mask = 1 << index_in_group;

                        entry
                    }
                })
            })
            .collect_vec();

        let core_info_response_entry_count = response_entries.len();
        let core_info_response_buffer = NativeBuffer::from_items(response_entries);
        let core_info_response_len = core_info_response_buffer.len_bytes();

        let mut seq = Sequence::new();

        // This will actually be two calls, one for the size probe and one for the real data.
        bindings
            .expect_get_logical_processor_information_ex()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|relationship_type, buffer, _| {
                *relationship_type == RelationProcessorCore && buffer.is_none()
            })
            .returning(move |_, _, returned_length| {
                // SAFETY: Caller must guarantee that the pointer is valid for use.
                unsafe {
                    *returned_length = core_info_response_len as u32;
                }

                Err(windows::core::Error::from_hresult(HRESULT::from_win32(
                    ERROR_INSUFFICIENT_BUFFER.0,
                )))
            });

        bindings
            .expect_get_logical_processor_information_ex()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|relationship_type, buffer, _| {
                *relationship_type == RelationProcessorCore && buffer.is_some()
            })
            .returning_st(move |_, buffer, returned_length| {
                // SAFETY: Caller must guarantee that the pointer is valid for use.
                unsafe {
                    *returned_length = core_info_response_len as u32;
                }

                // SAFETY: Caller must guarantee that the pointer is valid for use.
                unsafe {
                    buffer.unwrap().copy_from_nonoverlapping(
                        core_info_response_buffer.as_ptr().as_ptr(),
                        core_info_response_entry_count,
                    );
                }

                Ok(())
            });
    }

    fn simulate_job_constraints(
        bindings: &mut MockBindings,
        affinities_per_group: Option<Vec<Vec<ProcessorIndexInGroup>>>,
    ) {
        match affinities_per_group {
            Some(groups) => {
                let group_affinities = groups
                    .into_iter()
                    .enumerate()
                    .filter_map(|(group_index, group_processors)| {
                        if group_processors.is_empty() {
                            None
                        } else {
                            let mut affinity = GROUP_AFFINITY {
                                Group: group_index as ProcessorGroupIndex,
                                Mask: 0,
                                ..Default::default()
                            };

                            for processor_index in group_processors {
                                affinity.Mask |= 1 << processor_index;
                            }

                            Some(affinity)
                        }
                    })
                    .collect_vec();

                bindings
                    .expect_get_current_job_cpu_set_masks()
                    .return_once(|| group_affinities);
            }
            None => {
                bindings
                    .expect_get_current_job_cpu_set_masks()
                    .return_once(Vec::new);
            }
        }
    }

    #[test]
    fn pin_current_thread_to_single_processor() {
        let mut bindings = MockBindings::new();
        simulate_processor_layout(
            &mut bindings,
            [1],
            [1],
            [vec![0]],
            [vec![0]],
            None, // All processors are allowed by job constraints.
        );
        bindings
            .expect_set_current_thread_cpu_set_masks()
            .withf(|affinities| {
                affinities.len() == 1 && affinities[0].Group == 0 && affinities[0].Mask == 1
            })
            .return_const(());

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));
        let processors = platform.get_all_processors();
        platform.pin_current_thread_to(&processors);
    }

    #[test]
    fn pin_current_thread_to_multiple_processors() {
        let mut bindings = MockBindings::new();
        simulate_processor_layout(
            &mut bindings,
            [2],
            [2],
            [vec![0, 0]],
            [vec![0, 0]],
            None, // All processors are allowed by job constraints.
        );
        bindings
            .expect_set_current_thread_cpu_set_masks()
            .withf(|affinities| {
                affinities.len() == 1 && affinities[0].Group == 0 && affinities[0].Mask == 3
            })
            .return_const(());

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));
        let processors = platform.get_all_processors();
        platform.pin_current_thread_to(&processors);
    }

    #[test]
    fn pin_current_thread_to_multiple_groups() {
        let mut bindings = MockBindings::new();
        simulate_processor_layout(
            &mut bindings,
            [1, 1],
            [1, 1],
            [vec![0], vec![0]],
            [vec![0], vec![1]],
            None, // All processors are allowed by job constraints.
        );
        bindings
            .expect_set_current_thread_cpu_set_masks()
            .withf(|affinities| {
                affinities.len() == 2
                    && affinities[0].Group == 0
                    && affinities[0].Mask == 1
                    && affinities[1].Group == 1
                    && affinities[1].Mask == 1
            })
            .return_const(());

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));
        let processors = platform.get_all_processors();
        platform.pin_current_thread_to(&processors);
    }

    #[test]
    fn pin_current_thread_to_efficiency_processors() {
        let mut bindings = MockBindings::new();
        // Group 0 -> [Performance, Efficiency], Group 1 -> [Efficiency, Performance]
        simulate_processor_layout(
            &mut bindings,
            [2, 2],
            [2, 2],
            [vec![1, 0], vec![0, 1]],
            [vec![0, 0], vec![1, 1]],
            None, // All processors are allowed by job constraints.
        );
        bindings
            .expect_set_current_thread_cpu_set_masks()
            .withf(|affinities| {
                affinities.len() == 2
                    && affinities[0].Group == 0
                    && affinities[0].Mask == 2
                    && affinities[1].Group == 1
                    && affinities[1].Mask == 1
            })
            .return_const(());

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));
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

    #[test]
    fn single_group_multiple_memory_regions() {
        let mut bindings = MockBindings::new();
        // Group 0 -> [MemoryRegion 0, MemoryRegion 1, MemoryRegion 0, MemoryRegion 1]
        simulate_processor_layout(
            &mut bindings,
            [4],
            [4],
            [vec![0, 0, 0, 0]],
            [vec![0, 1, 0, 1]],
            None, // All processors are allowed by job constraints.
        );

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));
        let processors = platform.get_all_processors();
        assert_eq!(processors.len(), 4);

        // Check memory regions
        assert_eq!(processors[0].as_target().memory_region_id, 0);
        assert_eq!(processors[1].as_target().memory_region_id, 1);
        assert_eq!(processors[2].as_target().memory_region_id, 0);
        assert_eq!(processors[3].as_target().memory_region_id, 1);
    }

    #[test]
    fn current_thread_processors_if_no_affinity_set() {
        let mut bindings = MockBindings::new();

        // We are only obtaining processor IDs here, so we never actually perform a "get processors"
        // call, therefore we cannot use our "simulate_processor_layout" helper because that exists
        // specifically to facilitate the "get processors" API call.
        bindings
            .expect_get_maximum_processor_group_count()
            .times(1)
            .return_const(2 as ProcessorGroupIndex);
        bindings
            .expect_get_maximum_processor_count()
            .times(1)
            .withf(|group| *group == 0)
            .return_const(2 as ProcessorId);
        bindings
            .expect_get_maximum_processor_count()
            .times(1)
            .withf(|group| *group == 1)
            .return_const(1 as ProcessorId);

        bindings
            .expect_get_current_thread_cpu_set_masks()
            .return_once(Vec::new);
        bindings
            .expect_get_current_process_default_cpu_set_masks()
            .return_once(Vec::new);
        bindings
            .expect_get_current_thread_legacy_group_affinity()
            .return_once(|| GROUP_AFFINITY {
                Group: 0,
                Mask: 3, // Processors 0 and 1 - the default mask for this group.
                ..Default::default()
            });

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));

        let processor_ids = platform.current_thread_processors();
        assert_eq!(processor_ids.len(), 3);

        assert!(processor_ids.contains(&0));
        assert!(processor_ids.contains(&1));
        assert!(processor_ids.contains(&2));
    }

    #[test]
    fn current_thread_processors_if_no_affinity_set_default_group1() {
        let mut bindings = MockBindings::new();

        // We are only obtaining processor IDs here, so we never actually perform a "get processors"
        // call, therefore we cannot use our "simulate_processor_layout" helper because that exists
        // specifically to facilitate the "get processors" API call.
        bindings
            .expect_get_maximum_processor_group_count()
            .times(1)
            .return_const(2 as ProcessorGroupIndex);
        bindings
            .expect_get_maximum_processor_count()
            .times(1)
            .withf(|group| *group == 0)
            .return_const(2 as ProcessorId);
        bindings
            .expect_get_maximum_processor_count()
            .times(1)
            .withf(|group| *group == 1)
            .return_const(1 as ProcessorId);

        bindings
            .expect_get_current_thread_cpu_set_masks()
            .return_once(Vec::new);
        bindings
            .expect_get_current_process_default_cpu_set_masks()
            .return_once(Vec::new);
        bindings
            .expect_get_current_thread_legacy_group_affinity()
            .return_once(|| GROUP_AFFINITY {
                // We default to group 1. This is still a default value and should be ignored as
                // there is no requirement that the default affinitization be in group 0.
                Group: 1,
                Mask: 1, // Processor 0 - the default mask for this group.
                ..Default::default()
            });

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));

        let processor_ids = platform.current_thread_processors();
        assert_eq!(processor_ids.len(), 3);

        assert!(processor_ids.contains(&0));
        assert!(processor_ids.contains(&1));
        assert!(processor_ids.contains(&2));
    }

    #[test]
    fn current_thread_processors_if_legacy_affinity_set() {
        let mut bindings = MockBindings::new();

        // We are only obtaining processor IDs here, so we never actually perform a "get processors"
        // call, therefore we cannot use our "simulate_processor_layout" helper because that exists
        // specifically to facilitate the "get processors" API call.
        bindings
            .expect_get_maximum_processor_group_count()
            .times(1)
            .return_const(2 as ProcessorGroupIndex);
        bindings
            .expect_get_maximum_processor_count()
            .times(1)
            .withf(|group| *group == 0)
            .return_const(2 as ProcessorId);
        bindings
            .expect_get_maximum_processor_count()
            .times(1)
            .withf(|group| *group == 1)
            .return_const(1 as ProcessorId);
        bindings
            .expect_get_active_processor_count()
            .times(1)
            .withf(|group| *group == 0)
            .return_const(2 as ProcessorId);
        bindings
            .expect_get_active_processor_count()
            .times(1)
            .withf(|group| *group == 1)
            .return_const(1 as ProcessorId);

        bindings
            .expect_get_current_thread_cpu_set_masks()
            .return_once(Vec::new);
        bindings
            .expect_get_current_process_default_cpu_set_masks()
            .return_once(Vec::new);
        bindings
            .expect_get_current_thread_legacy_group_affinity()
            .return_once(|| GROUP_AFFINITY {
                Group: 0,
                Mask: 1, // Processor 0 allowed, processor 1 forbidden - not the default value!
                ..Default::default()
            });

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));

        let processor_ids = platform.current_thread_processors();
        assert_eq!(processor_ids.len(), 1);

        assert!(processor_ids.contains(&0));
    }

    #[test]
    fn current_thread_processors_if_process_affinity_set() {
        let mut bindings = MockBindings::new();

        // We are only obtaining processor IDs here, so we never actually perform a "get processors"
        // call, therefore we cannot use our "simulate_processor_layout" helper because that exists
        // specifically to facilitate the "get processors" API call.
        bindings
            .expect_get_maximum_processor_group_count()
            .times(1)
            .return_const(2 as ProcessorGroupIndex);
        bindings
            .expect_get_maximum_processor_count()
            .times(1)
            .withf(|group| *group == 0)
            .return_const(2 as ProcessorId);
        bindings
            .expect_get_maximum_processor_count()
            .times(1)
            .withf(|group| *group == 1)
            .return_const(1 as ProcessorId);
        bindings
            .expect_get_active_processor_count()
            .times(1)
            .withf(|group| *group == 0)
            .return_const(2 as ProcessorId);
        bindings
            .expect_get_active_processor_count()
            .times(1)
            .withf(|group| *group == 1)
            .return_const(1 as ProcessorId);

        // Process affinity is only queried if thread affinity contains nothing.
        bindings
            .expect_get_current_thread_cpu_set_masks()
            .return_once(Vec::new);
        bindings
            .expect_get_current_process_default_cpu_set_masks()
            .return_once(|| {
                vec![
                    GROUP_AFFINITY {
                        Group: 0,
                        Mask: 1,
                        ..Default::default()
                    },
                    GROUP_AFFINITY {
                        Group: 1,
                        Mask: 1,
                        ..Default::default()
                    },
                ]
            });

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));

        let processor_ids = platform.current_thread_processors();
        assert_eq!(processor_ids.len(), 2);

        assert!(processor_ids.contains(&0));
        assert!(processor_ids.contains(&2));
    }

    #[test]
    fn current_thread_processors_if_thread_affinity_set() {
        let mut bindings = MockBindings::new();

        // We are only obtaining processor IDs here, so we never actually perform a "get processors"
        // call, therefore we cannot use our "simulate_processor_layout" helper because that exists
        // specifically to facilitate the "get processors" API call.
        bindings
            .expect_get_maximum_processor_group_count()
            .times(1)
            .return_const(2 as ProcessorGroupIndex);
        bindings
            .expect_get_maximum_processor_count()
            .times(1)
            .withf(|group| *group == 0)
            .return_const(2 as ProcessorId);
        bindings
            .expect_get_maximum_processor_count()
            .times(1)
            .withf(|group| *group == 1)
            .return_const(1 as ProcessorId);
        bindings
            .expect_get_active_processor_count()
            .times(1)
            .withf(|group| *group == 0)
            .return_const(2 as ProcessorId);
        bindings
            .expect_get_active_processor_count()
            .times(1)
            .withf(|group| *group == 1)
            .return_const(1 as ProcessorId);

        // Note that we do not define any expectation that process affinity is queried - if a thread
        // affinity is set, process affinity is ignored and should not even be queried.
        bindings
            .expect_get_current_thread_cpu_set_masks()
            .return_once(|| {
                vec![
                    GROUP_AFFINITY {
                        Group: 0,
                        Mask: 1,
                        ..Default::default()
                    },
                    GROUP_AFFINITY {
                        Group: 1,
                        Mask: 1,
                        ..Default::default()
                    },
                ]
            });

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));

        let processor_ids = platform.current_thread_processors();
        assert_eq!(processor_ids.len(), 2);

        assert!(processor_ids.contains(&0));
        assert!(processor_ids.contains(&2));
    }

    #[test]
    fn basic_facts_are_represented() {
        let mut bindings = MockBindings::new();
        // 3 groups, 3 processors each group, 3 different memory regions.
        simulate_processor_layout(
            &mut bindings,
            [3, 3, 3],
            [3, 3, 3],
            [vec![0, 0, 0], vec![0, 0, 0], vec![0, 0, 0]],
            [vec![0, 0, 0], vec![1, 1, 1], vec![2, 2, 2]],
            None, // All processors are allowed by job constraints.
        );

        bindings
            .expect_get_numa_highest_node_number()
            .times(1)
            .return_once(|| 2);

        bindings
            .expect_get_current_processor_number_ex()
            .times(1)
            .return_once(|| PROCESSOR_NUMBER {
                Group: 1,
                Number: 2,
                ..Default::default()
            });

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));

        let current_processor_id = platform.current_processor_id();
        assert_eq!(current_processor_id, 5);

        let max_processor_id = platform.max_processor_id();
        assert_eq!(max_processor_id, 8);

        let max_memory_region_id = platform.max_memory_region_id();
        assert_eq!(max_memory_region_id, 2);

        // We do this just to ensure we meet all the expectations declared by the simulation.
        drop(platform.get_all_processors());
    }

    #[test]
    fn max_processor_time_without_job() {
        // If there is no job, max_processor_time is the same as the number of available processors.
        let mut bindings = MockBindings::new();

        // 4 processors in a single group, 2 more offline processors.
        simulate_processor_layout(&mut bindings, [4], [6], [vec![0; 4]], [vec![0; 4]], None);

        bindings
            .expect_get_current_job_cpu_rate_control()
            .times(1)
            .return_const(None);

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));

        // This will internally call "get all processors" to identify the total count,
        // so our simulation will correctly exercise the mocked simulation calls.
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
    fn max_processor_time_without_limits_on_job() {
        // If there is a job but it has no limits, max_processor_time is
        // the same as the number of available processors.
        let mut bindings = MockBindings::new();

        // 4 processors in a single group, 2 more offline processors.
        simulate_processor_layout(&mut bindings, [4], [6], [vec![0; 4]], [vec![0; 4]], None);

        bindings
            .expect_get_current_job_cpu_rate_control()
            .times(1)
            .return_const(Some(JOBOBJECT_CPU_RATE_CONTROL_INFORMATION::default()));

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));

        // This will internally call "get all processors" to identify the total count,
        // so our simulation will correctly exercise the mocked simulation calls.
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
    fn max_processor_time_below_available_hard_cap() {
        // If the job limit is less than the number of available processors,
        // we should use the job limit as the processor time limit.
        let mut bindings = MockBindings::new();

        // 4 processors in a single group, 2 more offline processors.
        simulate_processor_layout(&mut bindings, [4], [6], [vec![0; 4]], [vec![0; 4]], None);

        bindings
            .expect_get_current_job_cpu_rate_control()
            .times(1)
            .return_const(Some(JOBOBJECT_CPU_RATE_CONTROL_INFORMATION {
                ControlFlags: JOB_OBJECT_CPU_RATE_CONTROL(
                    JOB_OBJECT_CPU_RATE_CONTROL_ENABLE.0 | JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP.0,
                ),
                Anonymous: JOBOBJECT_CPU_RATE_CONTROL_INFORMATION_0 {
                    CpuRate: 5000, // 50% of the available processors (2 out of 4).
                },
            }));

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));

        // This will internally call "get all processors" to identify the total count,
        // so our simulation will correctly exercise the mocked simulation calls.
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
    fn max_processor_time_below_available_soft_cap() {
        // If the job limit is less than the number of available processors,
        // we should use the job limit as the processor time limit.
        let mut bindings = MockBindings::new();

        // 4 processors in a single group, 2 more offline processors.
        simulate_processor_layout(&mut bindings, [4], [6], [vec![0; 4]], [vec![0; 4]], None);

        bindings
            .expect_get_current_job_cpu_rate_control()
            .times(1)
            .return_const(Some(JOBOBJECT_CPU_RATE_CONTROL_INFORMATION {
                ControlFlags: JOB_OBJECT_CPU_RATE_CONTROL(
                    JOB_OBJECT_CPU_RATE_CONTROL_ENABLE.0
                        | JOB_OBJECT_CPU_RATE_CONTROL_MIN_MAX_RATE.0,
                ),
                Anonymous: JOBOBJECT_CPU_RATE_CONTROL_INFORMATION_0 {
                    Anonymous: JOBOBJECT_CPU_RATE_CONTROL_INFORMATION_0_0 {
                        MinRate: 3000, // 30% - we ignore this.
                        MaxRate: 7500, // 75% - this is the cap.
                    },
                },
            }));

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));

        // This will internally call "get all processors" to identify the total count,
        // so our simulation will correctly exercise the mocked simulation calls.
        let max_processor_time = platform.max_processor_time();

        #[expect(
            clippy::float_cmp,
            reason = "we use absolute error, which is the right way to compare"
        )]
        {
            assert_eq!(
                f64_diff_abs(max_processor_time, 3.0, PROCESSOR_TIME_CLOSE_ENOUGH),
                0.0
            );
        }
    }

    #[test]
    fn max_processor_time_above_available() {
        // If the job limit is greater than the number of available processors due to affinity,
        // we should use the number of available processors as the processor time limit.
        let mut bindings = MockBindings::new();

        // 4 processors in a single group, 2 more offline processors.
        simulate_processor_layout(
            &mut bindings,
            [4],
            [6],
            [vec![0; 4]],
            [vec![0; 4]],
            // However! We are limited to only 2 processors by affinity!
            Some([vec![true, true, false, false]]),
        );

        bindings
            .expect_get_current_job_cpu_rate_control()
            .times(1)
            .return_const(Some(JOBOBJECT_CPU_RATE_CONTROL_INFORMATION {
                ControlFlags: JOB_OBJECT_CPU_RATE_CONTROL(
                    JOB_OBJECT_CPU_RATE_CONTROL_ENABLE.0
                        | JOB_OBJECT_CPU_RATE_CONTROL_MIN_MAX_RATE.0,
                ),
                Anonymous: JOBOBJECT_CPU_RATE_CONTROL_INFORMATION_0 {
                    Anonymous: JOBOBJECT_CPU_RATE_CONTROL_INFORMATION_0_0 {
                        MinRate: 3000, // 30% - we ignore this.
                        MaxRate: 7500, // 75% - this is the cap.
                    },
                },
            }));

        let platform = BuildTargetPlatform::new(BindingsFacade::from_mock(bindings));

        // This will internally call "get all processors" to identify the total count,
        // so our simulation will correctly exercise the mocked simulation calls.
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
}
