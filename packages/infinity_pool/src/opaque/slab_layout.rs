use std::alloc::Layout;
use std::num::NonZero;

use new_zealand::nz;
use num_integer::Integer;

use crate::SlotMeta;

// TODO: We could decrease the size of this by calculating slot_array_layout and slot_layout
// at runtime. Might be worth it if we can do it as const? Can we, though? Mmmm not so sure.

/// Precalculates factors of the slab layout, based on the object layout and slab capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SlabLayout {
    /// Number of slots in the slab.
    capacity: NonZero<usize>,

    /// Layout of a single object that fits into a slot. All objects in a slab use the same layout.
    object_layout: Layout,

    /// Memory layout of a single slot, consisting of metadata and the object in the slot (if
    /// the slot is occupied), including any padding. The size of this layout is the stride
    /// between consecutive slots in the slab's memory block.
    slot_layout: Layout,

    /// Byte offset from a slot to the object contained within the slot (if occupied).
    slot_to_object_offset: usize,

    /// Total memory layout for the entire array of slots that is the storage capacity of the slab.
    slot_array_layout: Layout,
}

impl SlabLayout {
    /// Calculates the layout of a slab for a given object layout.
    ///
    /// # Panics
    ///
    /// Panics if the object layout has zero size.
    ///
    /// Panics if the resulting slab would exceed the size of virtual memory.
    #[must_use]
    pub(crate) fn new(object_layout: Layout) -> Self {
        assert!(
            object_layout.size() > 0,
            "SlabLayoutInfo cannot be calculated for zero-sized object layout"
        );

        // Calculate the combined layout for SlotMeta + object.
        let meta_layout = Layout::new::<SlotMeta>();

        let (slot_layout, slot_to_object_offset) = meta_layout
            .extend(object_layout)
            .expect("layout extension cannot fail for valid layouts with reasonable sizes");

        // Padding the slot first makes its stored size equal the array stride reported by
        // `Layout::repeat()`.
        let slot_layout = slot_layout.pad_to_align();

        let capacity = determine_capacity(
            NonZero::new(slot_layout.size())
                .expect("slot layout size is non-zero because object layout size is non-zero"),
        );

        let (slot_array_layout, _) = slot_layout
            .repeat(capacity.get())
            .expect("the resulting slab size would be greater than virtual memory - can only be the result of invalid math");

        Self {
            capacity,
            object_layout,
            slot_layout,
            slot_to_object_offset,
            slot_array_layout,
        }
    }

    #[must_use]
    #[cfg_attr(test, mutants::skip)] // cargo-mutants does not understand how to deal with NonZero return value, so all the mutations are unviable.
    pub(crate) fn capacity(&self) -> NonZero<usize> {
        self.capacity
    }

    #[must_use]
    pub(crate) fn object_layout(&self) -> Layout {
        self.object_layout
    }

    #[must_use]
    pub(crate) fn slot_layout(&self) -> Layout {
        self.slot_layout
    }

    #[must_use]
    pub(crate) fn slot_to_object_offset(&self) -> usize {
        self.slot_to_object_offset
    }

    #[must_use]
    pub(crate) fn slot_array_layout(&self) -> Layout {
        self.slot_array_layout
    }

    /// Configures a small fixture without changing slot layout or the production capacity policy.
    #[cfg(test)]
    #[cfg_attr(coverage_nightly, coverage(off))]
    pub(crate) fn with_capacity(mut self, capacity: NonZero<usize>) -> Self {
        self.capacity = capacity;
        self.slot_array_layout = self.slot_layout.repeat(capacity.get()).unwrap().0;
        self
    }
}

/// At the moment, we use a built-in algorithm to determine a reasonable capacity for each slab.
///
/// The capacity is always the same for a given object layout, and tries to make reasonable use
/// of memory for that layout. We assume that slabs will have a reasonable number of objects in
/// them, so fill factor is not a problem (after all, if you only have a few objects, why would
/// you even be using a slab/pool).
#[must_use]
#[cfg_attr(test, mutants::skip)] // cargo-mutants does not understand how to deal with NonZero return value, so all the mutations are unviable.
fn determine_capacity(slot_size: NonZero<usize>) -> NonZero<usize> {
    // No matter what we are storing, we want at this this many objects per slab.
    // If we have overly tiny slabs, the slab management overhead could become significant.
    const MIN_CAPACITY: NonZero<usize> = nz!(32);

    // If no other condition preempts us, we want to have this capacity.
    const IDEAL_CAPACITY: NonZero<usize> = nz!(128);

    // A slab is meant for bulk allocation, so let us ensure there is at least some bulk.
    // As long as MIN_CAPACITY is met, we increase capacity until we reach this size,
    // rounding up to avoid partial pages. This may still leave us under IDEAL_CAPACITY.
    //
    // If using huge pages, we should bump this up to the huge page size. We do not
    // currently support huge pages but it is an obvious future enhancement.
    const MIN_BYTES: NonZero<usize> = nz!(16_384);

    // As long as our minimums are satisfied, we get as close as we can to IDEAL_CAPACITY
    // without exceeding this size. We just seek to avoid excessive memory use per slab, in
    // case the objects in the slab are large.
    const MAX_BYTES: NonZero<usize> = nz!(1_048_576);

    let min_capacity_by_bytes = Integer::div_floor(&MIN_BYTES.get(), &slot_size.get());
    let min_capacity_real = min_capacity_by_bytes.max(MIN_CAPACITY.get());

    let max_capacity_by_bytes = Integer::div_floor(&MAX_BYTES.get(), &slot_size.get());
    let ideal_capacity_bytes = IDEAL_CAPACITY.get().saturating_mul(slot_size.get());

    // If ideal capacity size is below max capacity size, we take ideal, otherwise max.
    let desired_capacity = if ideal_capacity_bytes <= MAX_BYTES.get() {
        IDEAL_CAPACITY.get()
    } else {
        max_capacity_by_bytes
    };

    // If desired capacity is above minimum, we use it. Otherwise, we use minimum.
    let final_capacity = desired_capacity.max(min_capacity_real);

    NonZero::new(final_capacity).expect("minimum capacity is non-zero")
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::mem::size_of;

    use super::*;

    #[test]
    #[should_panic]
    fn panics_on_zero_sized_type() {
        _ = SlabLayout::new(Layout::new::<()>());
    }

    #[test]
    fn for_u8() {
        let layout = SlabLayout::new(Layout::new::<u8>());

        assert!(layout.slot_layout().size() > size_of::<SlotMeta>());
        assert!(layout.slot_to_object_offset() > 0);
        assert!(
            layout.slot_array_layout().size()
                >= layout
                    .slot_layout()
                    .size()
                    .saturating_mul(layout.capacity().get())
        );
    }

    #[test]
    fn for_u64() {
        let layout = SlabLayout::new(Layout::new::<u64>());

        // Verify u64 alignment is respected
        assert_eq!(layout.slot_to_object_offset() % 8, 0);
        assert!(layout.slot_layout().size() > size_of::<SlotMeta>());
    }

    #[test]
    fn for_array() {
        let layout = SlabLayout::new(Layout::new::<[u32; 8]>());

        // Verify array alignment is respected (u32 = 4 bytes)
        assert_eq!(layout.slot_to_object_offset() % 4, 0);
        assert!(layout.slot_layout().size() > size_of::<SlotMeta>());
    }

    #[test]
    fn for_type_with_large_alignment() {
        #[repr(align(128))]
        #[expect(
            dead_code,
            reason = "Test struct to verify layout calculations with large alignment"
        )]
        struct LargeAligned(u64);

        let layout = SlabLayout::new(Layout::new::<LargeAligned>());

        // Critical: ensure 128-byte alignment is respected
        assert_eq!(layout.slot_to_object_offset() % 128, 0);
        assert!(layout.slot_layout().align() >= 128);
    }

    #[test]
    fn determine_capacity_meets_minimum_capacity() {
        // This tests a specific algorithm and specific constants.
        // Expect to update this test whenever either changes.

        // With tiny slot size, MIN_BYTES (16,384) requirement forces capacity to 16,384.
        let capacity = determine_capacity(nz!(1));
        assert_eq!(capacity.get(), 16_384);
    }

    #[test]
    fn determine_capacity_reaches_ideal_capacity() {
        // This tests a specific algorithm and specific constants.
        // Expect to update this test whenever either changes.

        // With a slot size just right, between min/max values, we reach the ideal capacity (128).
        // A slot size of 256 is enough to push us past MIN_CAPACITY (16K) to 32K,
        // proving that we are not just hitting the bottom stop.
        let capacity = determine_capacity(nz!(256));
        assert_eq!(capacity.get(), 128);
    }

    #[test]
    fn determine_capacity_rounds_down_below_min_bytes() {
        // This tests a specific algorithm and specific constants.
        // Expect to update this test whenever either changes.

        // With slot_size=100, we can fit a little more than 163 slots into the minimum capacity.
        // Rounding 163.84 down gets us to a capacity of 163 slots.
        let capacity = determine_capacity(nz!(100));
        assert_eq!(capacity.get(), 163);
    }

    #[test]
    fn determine_capacity_does_not_exceed_max_bytes_if_not_required() {
        // This tests a specific algorithm and specific constants.
        // Expect to update this test whenever either changes.

        // With slot_size=8193, IDEAL_CAPACITY slightly overshoots MAX_BYTES (by 128 bytes).
        // This restricts us to a little short of IDEAL_CAPACITY, which is fine because that
        // is just a target, not a requirement - only the minimums are hard requirements.
        let capacity = determine_capacity(nz!(8193));
        assert_eq!(capacity.get(), 127);
    }

    #[test]
    fn determine_capacity_exceeds_max_if_required_to_meet_min() {
        // This tests a specific algorithm and specific constants.
        // Expect to update this test whenever either changes.

        // With huge slot_size=2,000,000, MIN_CAPACITY forces us to 32 slots,
        // even though it exceeds MAX_BYTES. That is fine - minimums are mandatory.
        let capacity = determine_capacity(nz!(2_000_000));
        assert_eq!(capacity.get(), 32);
    }
}
