# alloc_tracker implementation

User-visible behavior is defined in the package [design](design.md).

## Architecture

Measurement is a one-way pipeline. Each stage owns a distinct piece of state and hands the
next stage a value rather than a view into its own:

```text
allocator events → per-thread counters → span measurement → operation metrics
                                                                    ↓
                                              outputs ← report ← session
```

The allocator wrapper turns each successful allocation and each deallocation into an update
of the calling thread's counters, which live for the lifetime of the process and belong to
no session. Two windows are deliberately not counted: allocations made while a thread's own
counters are being created, and deallocations on a thread that has never allocated, because
creating counters from a free would re-enter the allocator.

A span reads those counters at its boundaries and turns the difference into one measurement
record. Thread and process spans differ only in which counters they read and in whether they
can observe a peak; both produce the same record.

An operation folds each record into streaming statistics behind a mutex. Nothing per-span is
retained, so the cost of an operation does not grow with the number of spans recorded for
it. A session owns the map from operation name to those statistics, and handing out the same
name twice hands out the same statistics.

A report is a detached snapshot of that state: it copies the accumulators themselves rather
than the figures derived from them, which is what lets two reports merge into a statistically
correct third one. Both the table and the JSON files are rendered from that one snapshot, so
the figures they show for an operation always agree. Which operations they list can differ.
A report holding no measurable work renders as a single "no statistics" line rather than a
table of rows, while writing that same report to JSON still emits a file for every operation
that has statistics — which a zero-iteration operation does, having recorded a span. When the
report does render a table, it carries a row for every registered operation, marking one that
recorded no spans as unavailable, whereas JSON omits that operation entirely.

## Counters

The allocator wrapper maintains one counter block per thread: cumulative bytes, cumulative
allocation count, currently outstanding bytes, and a watermark of outstanding bytes. Blocks
live for the lifetime of the process and are shared through a process-wide registry so that
process-scope measurement can sum across threads, while each thread keeps a reference to its
own block in thread-local storage.

Only the owning thread ever writes to a block. That single-writer discipline is what makes
the hot path cheap: the counters are atomics purely so that other threads may read them, and
writes are a relaxed load followed by a relaxed store rather than a read-modify-write. Cross-
thread readers accordingly see values that may be slightly stale, which is acceptable because
process-scope figures are approximate by nature.

Outstanding bytes is signed. A block is attributed to whichever thread performs the
deallocation, so a thread that frees memory allocated elsewhere drives its own outstanding
count below zero. This is a deliberate consequence of measuring allocator events per thread
rather than tracking ownership of live allocations, which would require per-allocation
bookkeeping.

## Reentrancy

Tracking runs inside the global allocator, so it must not allocate. Registering a thread's
counter block does allocate — it constructs a shared block and inserts it into the registry —
so a guard flag marks the window during which a thread is initializing its own block, and
allocations occurring inside that window go untracked.

The thread-local reference is published as the last step before the guard is cleared, and
nothing between those two steps allocates. The presence of the reference therefore already
proves that the thread is outside the initialization window, so the hot path performs a
single thread-local lookup and needs no separate guard check.

Deallocation never initializes a counter block. A free from a thread that has none is simply
not counted. Initializing on the deallocation path would re-enter the allocator, and would
also consume the one-shot flag used by the panic-on-next-allocation debugging feature.

Recording happens after the inner allocator call and only when it succeeded, so a failed
allocation moves no counters.

## Watermark protocol

The watermark is per-thread state, but the metric it feeds is per-span, so spans hand the
watermark back and forth rather than owning it.

A thread span records the current outstanding level and the current watermark on entry, then
lowers the watermark to the entry level. From that point the watermark tracks only what this
span itself accumulates. On drop the span takes the difference between the watermark and its
entry level as its own peak, and restores the enclosing watermark to the higher of its saved
value and the level this span reached — memory held by an inner span was equally outstanding
from the enclosing span's perspective.

Restoration happens before every early exit in the drop path, including the panic-unwinding
case and the missing-iteration-count panic. A span abandoned without recording must still
return the watermark, or it would silently suppress the peak of the span enclosing it.

The hand-back is what makes reverse-order drops a requirement rather than a convention:
restoring an outer span's saved watermark while an inner span is still live would raise the
inner span's baseline and inflate its reported peak. The requirement is stated in the
design rather than enforced, because enforcing it would mean giving every span an identity
and a stack to check against, which is more machinery than an ordering convention that
scoped bindings satisfy automatically.

## Peak aggregation

The peak reuses the span accumulator that every other metric of an operation uses. That
accumulator estimates a per-iteration figure by least-squares fitting each span's whole-span
total against the number of iterations the span covered, with the fitted line forced through
the origin: zero iterations must cost nothing, so the fit has a slope but no intercept. The
slope is the reported per-iteration figure.

Writing nᵢ for the iteration count of span i and peakᵢ for that span's baseline-relative
peak — the watermark it reached less the outstanding level it entered on — a peak is not a
total: it is a level that does not grow with nᵢ, so the accumulator's model does not
describe it. Multiplying it by nᵢ on the way in makes it behave like one. The regression
then divides that scaling back out, and the estimate reduces to the span peaks averaged
with weight nᵢ²:

```text
slope = Σ(nᵢ · peakᵢ·nᵢ) / Σ(nᵢ²) = Σ(nᵢ²·peakᵢ) / Σ(nᵢ²)
```

That is the whole reason for the scaling: it buys the warmup robustness of the shared
estimator, and its confidence interval, for a quantity the estimator was not written for. A
one-iteration warmup batch alongside thousand-iteration steady-state batches carries a
millionth of their weight.

Whether a peak is available at all is not a property of the accumulator, so the accumulator
is held inside the state that says a peak is available. Any span lacking a watermark replaces
that state with the unavailable one, which no later span or merge can undo. Folding an
unmeasurable span in as a zero would understate the operation instead of withholding it.

## Reporting

The human-readable table is rendered from a fixed column set with widths computed from the
formatted cell contents, so adding a column does not require touching the layout logic. The
JSON output omits the peak fields entirely when the peak is unavailable, which keeps them
additive for existing consumers.
