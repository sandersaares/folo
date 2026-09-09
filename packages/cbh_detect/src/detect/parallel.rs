//! Chunk-partition math for the spawner-distributed analysis passes.
//!
//! Some analysis passes walk a slice whose elements are independent — detecting
//! each series, or fetching + parsing each stored object, for instance — and
//! distribute it across worker tasks dispatched through an [`anyspawn::Spawner`].
//! The partition is always the same shape: split the slice into one balanced,
//! contiguous chunk per worker, process each chunk on its own blocking task, and
//! recombine in input order. The arithmetic that decides how many workers to use and
//! how large each chunk is lives here, separate from the dispatch itself, so the
//! detection pass (in this crate) and the shell's object loader share one
//! implementation rather than drifting apart.
//!
//! The worker count is the available parallelism capped at the slice length, so a
//! single available CPU (Miri reports one by default) yields a single worker: one
//! chunk covering the whole slice, dispatched as one task. There is no separate serial
//! code path to keep in step — the one-worker case is just the degenerate partition,
//! and the synchronous spawner that tests and Miri inject runs that one task inline on
//! the calling thread, so the dispatch stays under Miri's checks.

use std::num::NonZero;
use std::thread;

/// How many worker tasks to split `len` items across.
///
/// The available parallelism capped at `len`, so no worker is handed an empty chunk.
/// This is `0` only when `len` is `0` (nothing to do) and otherwise at least `1` — a
/// single available CPU, or a single-element slice, yields one worker, i.e. one chunk
/// covering everything.
//
// Mutation-skipped: the return value only selects how the work is partitioned across
// workers, never the result. Every worker count yields the same order-preserving
// output, so no behavioral test can distinguish one partitioning from another.
#[cfg_attr(test, mutants::skip)]
pub fn worker_count(len: usize) -> usize {
    thread::available_parallelism()
        .map_or(1, NonZero::get)
        .min(len)
}

/// The lengths of exactly `workers` contiguous chunks that partition `len` items as
/// evenly as possible.
///
/// The first `len % workers` chunks hold one extra item, the rest hold `len / workers`.
///
/// For `1 <= workers <= len` every chunk is non-empty and the sizes differ by at most
/// one, with `workers == 1` yielding a single chunk of all `len` items. The degenerate
/// `workers == 0` (only ever paired with `len == 0`, the empty slice) yields no chunks.
/// The unit test covers both.
pub fn balanced_chunk_sizes(len: usize, workers: usize) -> impl Iterator<Item = usize> {
    // `workers` may be `0` (the empty-slice case), so the checked divide and remainder
    // fall back to `0`; for `workers >= 1` the division is exact and `base + 1 <= len`
    // cannot overflow. The checked operators keep the workspace's arithmetic lint
    // satisfied without masking a real bug.
    let base = len.checked_div(workers).unwrap_or(0);
    let remainder = len.checked_rem(workers).unwrap_or(0);
    (0..workers).map(move |index| {
        if index < remainder {
            base.saturating_add(1)
        } else {
            base
        }
    })
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn balanced_chunk_sizes_splits_into_exactly_workers_balanced_chunks() {
        // An even split gives every worker the same size.
        let even: Vec<usize> = balanced_chunk_sizes(12, 4).collect();
        assert_eq!(even, vec![3, 3, 3, 3]);

        // A remainder is spread one-per-chunk across the leading chunks, so the
        // larger chunks come first and sizes differ by at most one.
        let uneven: Vec<usize> = balanced_chunk_sizes(13, 4).collect();
        assert_eq!(uneven, vec![4, 3, 3, 3]);

        // The "just above the worker count" case a naive `div_ceil` chunking gets
        // wrong (len = workers + 1 yields ~workers/2 chunks): it must still produce
        // exactly `workers` non-empty chunks — one of size two, the rest size one —
        // never collapse to fewer and leave cores idle.
        let just_above: Vec<usize> = balanced_chunk_sizes(33, 32).collect();
        assert_eq!(just_above.len(), 32);
        assert_eq!(just_above.iter().filter(|&&size| size == 2).count(), 1);
        assert_eq!(just_above.iter().filter(|&&size| size == 1).count(), 31);

        // The empty slice partitions into no chunks: `worker_count` returns `0` for
        // `len == 0`, and zero workers must yield an empty iterator (not panic on the
        // divide-by-zero).
        assert_eq!(balanced_chunk_sizes(0, 0).count(), 0);
    }

    #[test]
    fn balanced_chunk_sizes_cover_the_input_across_partition_shapes() {
        // Across many shapes: exactly `workers` chunks, every chunk non-empty, sizes
        // differ by at most one, and they sum back to `len` (a full, non-overlapping
        // partition).
        // Small inputs already cover empty remainders, every remainder position and
        // more workers than items per chunk. Native retains the wider scaling sweep.
        let max_len = if cfg!(miri) { 8 } else { 64 };
        for len in 1..=max_len {
            for workers in 1..=len.min(16) {
                let sizes: Vec<usize> = balanced_chunk_sizes(len, workers).collect();
                assert_eq!(sizes.len(), workers, "len={len} workers={workers}");
                assert_eq!(
                    sizes.iter().sum::<usize>(),
                    len,
                    "len={len} workers={workers}"
                );
                let max = *sizes.iter().max().expect("workers >= 1");
                let min = *sizes.iter().min().expect("workers >= 1");
                assert!(min >= 1, "len={len} workers={workers}: {sizes:?}");
                assert!(
                    max.saturating_sub(min) <= 1,
                    "len={len} workers={workers}: {sizes:?}"
                );
            }
        }
    }
}
