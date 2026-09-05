//! What a closing span hands to its operation.

/// One span's contribution to an operation.
///
/// This is the sole channel from a span to its operation's metrics. Both span scopes
/// produce it as they close — thread spans with a measured peak, process spans without one
/// — and `OperationMetrics::add_span` is its only consumer. An absent peak is therefore not
/// merely a missing figure: it makes the whole operation's peak unavailable, permanently
/// and across merges (`docs/implementation.md`, "Peak aggregation").
///
/// The byte and allocation figures are whole-span totals rather than per-iteration rates:
/// dividing is the operation's job, because it weights spans against each other
/// (`docs/implementation.md`, "Peak aggregation").
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SpanMeasurement {
    /// How many iterations of the operation the span covered.
    pub(crate) iterations: u64,

    /// Bytes allocated over the span's lifetime.
    pub(crate) bytes: u64,

    /// Number of allocations over the span's lifetime.
    pub(crate) count: u64,

    /// The high-water mark of the span's own outstanding bytes, measured from the level
    /// outstanding when it began, or `None` when the span is of a kind that cannot
    /// observe it.
    ///
    /// Baseline-relative rather than absolute: memory the span released but did not
    /// allocate creates headroom that offsets its own later allocations, so this is not
    /// the same as the memory live on the thread. `docs/design.md`, "Limits of the peak
    /// figure", covers what that costs.
    ///
    /// Unlike the other figures this is a level, not a total: it does not grow with the
    /// number of iterations the span covered.
    pub(crate) peak_outstanding_bytes: Option<u64>,
}
