//! The one output-facing projection of what an analysis actually judged.
//!
//! [`SeriesCensus`] is the detector's raw account. Every surface a reader or an
//! automation meets — the text report, the Markdown report, the JSON document and
//! the pull-request comment composed from it — needs the same three things out of
//! that account: a verdict that does not overstate what was ruled out, the complete
//! per-reason breakdown, and how much of the suite the verdict covers. Deriving
//! those independently per surface is how they drift apart, so they are derived
//! once here and every surface reads them from [`Coverage`].

use cbh_detect::{SeriesCensus, UnjudgedReason};

/// The primary verdict of a successful analysis.
///
/// This is deliberately separate from execution success and platform coverage:
/// a command that fails does not produce an analysis verdict, and findings can
/// coexist with a partially failed external collection matrix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnalysisOutcome {
    /// At least one finding survived the detection policy.
    Findings,
    /// Every in-scope series was judged and no finding survived.
    Clean,
    /// In-scope series existed, but none carried enough evidence to be judged.
    InsufficientBaseline,
    /// No series entered analysis, or every accounted series was absent at the
    /// analyzed context commit.
    NothingInScope,
    /// Some in-scope series were judged and some were not, with no findings.
    Partial,
}

impl AnalysisOutcome {
    /// Every successful analysis outcome, in wire-order for documentation and tests.
    pub const ALL: [Self; 5] = [
        Self::Findings,
        Self::Clean,
        Self::InsufficientBaseline,
        Self::NothingInScope,
        Self::Partial,
    ];

    /// The stable `snake_case` wire name used by automation.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Findings => "findings",
            Self::Clean => "clean",
            Self::InsufficientBaseline => "insufficient_baseline",
            Self::NothingInScope => "nothing_in_scope",
            Self::Partial => "partial",
        }
    }

    /// Derives the verdict from findings and the shared coverage projection.
    #[must_use]
    pub fn from_analysis(notable: bool, coverage: &Coverage) -> Self {
        if notable {
            return Self::Findings;
        }

        match coverage.state() {
            CoverageState::NoSeries | CoverageState::NothingInScope => Self::NothingInScope,
            CoverageState::NothingJudged => Self::InsufficientBaseline,
            CoverageState::Partial => Self::Partial,
            CoverageState::Full => Self::Clean,
        }
    }
}

/// How much of the in-scope suite an analysis reached a verdict on.
///
/// The three "nothing was judged" situations are distinct operational states with
/// distinct remedies — no results at all, results that were all ghosts, and results
/// that were all declined by the gates — so they are distinct variants rather than
/// one lumped "blind" state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoverageState {
    /// The analysis accounted for no series: the selected partition was empty, every
    /// matching run was excluded, or nothing survived loading. The empty-outcome hint
    /// names which of those happened; this variant does not.
    NoSeries,
    /// Series were accounted for, but every one of them was a ghost, so nothing was
    /// in scope at the analyzed context commit. Remedy: check that the benchmarks still
    /// run at the analyzed context commit.
    NothingInScope,
    /// In-scope series existed and none of them could be judged. Remedy: the
    /// per-reason breakdown says which evidence floor they fell short of.
    NothingJudged,
    /// Some, but not all, in-scope series were judged. A silent report is an
    /// all-clear over the judged part only.
    Partial,
    /// Every in-scope series was judged, so a silent report is a full all-clear.
    Full,
}

impl CoverageState {
    /// Every coverage state, in order of how much of the suite the verdict reaches.
    ///
    /// The appendix lists every state, so the inventory lives next to the type rather
    /// than being restated beside the table.
    pub const ALL: [Self; 5] = [
        Self::NoSeries,
        Self::NothingInScope,
        Self::NothingJudged,
        Self::Partial,
        Self::Full,
    ];

    /// The stable `snake_case` wire name of the state, as the JSON report carries it
    /// and downstream automation matches on it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoSeries => "no_series",
            Self::NothingInScope => "nothing_in_scope",
            Self::NothingJudged => "nothing_judged",
            Self::Partial => "partial",
            Self::Full => "full",
        }
    }

    /// How far a silent run's verdict reaches, in coverage terms only.
    ///
    /// A silent verdict states that no reportable move survived the gates; it never proves
    /// that nothing moved. This describes *how much of the suite* that statement covers for
    /// each state, in the same `accounted for` / `in scope` / `judged` vocabulary the rest
    /// of [`Coverage`] uses. It backs the book's state table; the report states its own
    /// count-bearing qualification through [`qualifications`](Self::qualifications), which
    /// uses the same predicate but is not this string.
    #[must_use]
    pub fn reach(self) -> &'static str {
        match self {
            Self::NoSeries => {
                "Nothing: no series was accounted for. The empty-outcome hint explains why."
            }
            Self::NothingInScope => {
                "Nothing at the analyzed context commit: every accounted series was measured elsewhere."
            }
            Self::NothingJudged => {
                "Nothing: in-scope series existed but none could be judged; the breakdown \
                 says which evidence floor they fell short of."
            }
            Self::Partial => {
                "The judged series only: no reportable move among them, and no claim about \
                 the in-scope series that went unjudged."
            }
            Self::Full => {
                "The whole in-scope suite: every in-scope series was judged, so this is the \
                 only silent state with no coverage qualification. The verdict stays no \
                 notable changes detected."
            }
        }
    }
}

/// What an analysis judged, projected from a [`SeriesCensus`] into the account every
/// rendering reports.
///
/// Counts *metric series* throughout, matching the census: one benchmark measured for
/// several metrics contributes one entry per metric.
///
/// Ghosts sit outside [`in_scope`](Self::in_scope), which is the denominator of both
/// the [`state`](Self::state) and every ratio a rendering states. A pull request
/// benchmarks only the packages it touches while analysis reads the whole store, so
/// every untouched package's series is a ghost; a denominator counting those would leave
/// a healthy run reading as a handful of series judged out of thousands, and train
/// readers to ignore the field. The exclusion reaches only the ratio that decides
/// whether an all-clear is warranted: [`total`](Self::total) and
/// [`reasons`](Self::reasons) keep the whole account, so a consumer that needs the ghosts
/// has them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Coverage {
    state: CoverageState,
    judged: usize,
    in_scope: usize,
    total: usize,
    unjudged: Vec<(UnjudgedReason, usize)>,
}

impl Coverage {
    /// Projects a census onto the coverage every surface reports.
    #[must_use]
    pub fn from_census(census: &SeriesCensus) -> Self {
        let unjudged: Vec<(UnjudgedReason, usize)> = census.reasons().collect();
        let ghosts = unjudged
            .iter()
            .find_map(|&(reason, count)| (reason == UnjudgedReason::Ghost).then_some(count))
            .unwrap_or(0);
        let total = census.total();
        let in_scope = total.saturating_sub(ghosts);
        let judged = census.judged();

        let state = if total == 0 {
            CoverageState::NoSeries
        } else if in_scope == 0 {
            CoverageState::NothingInScope
        } else if judged == 0 {
            CoverageState::NothingJudged
        } else if judged < in_scope {
            CoverageState::Partial
        } else {
            CoverageState::Full
        };

        Self {
            state,
            judged,
            in_scope,
            total,
            unjudged,
        }
    }

    /// How much of the in-scope suite was judged.
    #[must_use]
    pub fn state(&self) -> CoverageState {
        self.state
    }

    /// How many series the detectors reached a verdict on.
    #[must_use]
    pub fn judged(&self) -> usize {
        self.judged
    }

    /// How many series could have been judged: every accounted series except the
    /// ghosts, which no analysis can judge.
    #[must_use]
    pub fn in_scope(&self) -> usize {
        self.in_scope
    }

    /// Every series the analysis accounted for, ghosts included.
    #[must_use]
    pub fn total(&self) -> usize {
        self.total
    }

    /// How many series went unjudged, for any reason.
    #[must_use]
    pub fn unjudged(&self) -> usize {
        self.total.saturating_sub(self.judged)
    }

    /// The unjudged series broken down by reason, in the census's reporting order.
    /// Complete: ghosts are listed here even though they sit outside
    /// [`in_scope`](Self::in_scope).
    pub fn reasons(&self) -> impl Iterator<Item = (UnjudgedReason, usize)> + '_ {
        self.unjudged.iter().copied()
    }

    /// The primary verdict for a report that produced no findings.
    ///
    /// An all-clear is a claim about the series that were judged, so a run that judged
    /// nothing does not open with one, and a run that judged only part of its suite
    /// opens with an all-clear that says so.
    #[must_use]
    pub fn verdict(&self) -> &'static str {
        match self.state {
            CoverageState::Full => "No notable changes detected.",
            // An analysis that accounted for no series judged nothing, so it is in no
            // position to report an absence of change; the empty-outcome hint that
            // accompanies this case explains the emptiness itself.
            CoverageState::NoSeries => "Nothing was analyzed, so no change could be detected.",
            CoverageState::NothingInScope => {
                "Nothing was in scope at the analyzed context commit, so nothing was judged."
            }
            CoverageState::NothingJudged => {
                "Nothing was judged, so no change could be detected either way."
            }
            CoverageState::Partial => {
                "No notable changes detected among the series that were judged."
            }
        }
    }

    /// The sentences qualifying the verdict, in reading order: what the silence covers,
    /// then what it does not.
    ///
    /// Between them they account for the whole suite: the judged series are counted
    /// against [`in_scope`](Self::in_scope), and the breakdown that follows names every
    /// unjudged series — ghosts included — so the judged count and the listed reasons
    /// together reach [`total`](Self::total).
    #[must_use]
    pub fn qualifications(&self) -> Vec<String> {
        let mut sentences: Vec<String> = self.coverage_sentence().into_iter().collect();
        if self.unjudged() > 0 {
            let reasons: Vec<String> = self
                .reasons()
                .map(|(reason, count)| format!("{count} series {}", reason.describe()))
                .collect();
            sentences.push(format!("Not judged: {}.", reasons.join("; ")));
        }
        sentences
    }

    /// The sentence stating how far the verdict reaches, absent where there is no reach
    /// to state.
    fn coverage_sentence(&self) -> Option<String> {
        match self.state {
            // An analysis that accounted for nothing has no ratio to state, and its
            // verdict already says that nothing was analyzed — as does the empty-outcome
            // hint that accompanies it. A third statement of the one fact is noise.
            CoverageState::NoSeries => None,
            CoverageState::NothingInScope => Some(format!(
                "None of the {} series accounted for is measured at the analyzed context \
                 commit, so nothing was tested.",
                self.total
            )),
            CoverageState::NothingJudged => Some(format!(
                "Judged 0 of {} in-scope series, so nothing was tested: this silence is \
                 not evidence that nothing moved.",
                self.in_scope
            )),
            CoverageState::Partial | CoverageState::Full => Some(format!(
                "Judged {} of {} in-scope series; no reportable move survived the gates.",
                self.judged, self.in_scope
            )),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::panic::{RefUnwindSafe, UnwindSafe};

    use cbh_detect::Testability;
    use static_assertions::assert_impl_all;

    use super::*;

    assert_impl_all!(AnalysisOutcome: Send, Sync, Unpin, UnwindSafe, RefUnwindSafe);

    /// A census of `judged` judged series plus the given unjudged breakdown.
    fn census_of(judged: usize, unjudged: &[(UnjudgedReason, usize)]) -> SeriesCensus {
        let mut census = SeriesCensus::default();
        for _ in 0..judged {
            census.record(Testability::Judged);
        }
        for &(reason, count) in unjudged {
            census.record_unjudged(reason, count);
        }
        census
    }

    /// Every distinct shape a census can present, with the state it projects to and
    /// the in-scope ratio behind it. Table-driven so a new [`UnjudgedReason`] or a new
    /// [`CoverageState`] is added here once and checked against every surface.
    fn coverage_cases() -> Vec<(&'static str, SeriesCensus, CoverageState, usize, usize)> {
        vec![
            (
                "absent census",
                SeriesCensus::default(),
                CoverageState::NoSeries,
                0,
                0,
            ),
            (
                "every series a ghost",
                census_of(0, &[(UnjudgedReason::Ghost, 4)]),
                CoverageState::NothingInScope,
                0,
                0,
            ),
            (
                "in scope but nothing judged",
                census_of(0, &[(UnjudgedReason::TooFewPoints, 3)]),
                CoverageState::NothingJudged,
                0,
                3,
            ),
            (
                "partial: ghost",
                census_of(2, &[(UnjudgedReason::Ghost, 1)]),
                // Ghosts sit outside the denominator, so a run whose only shortfall is
                // ghosts judged everything it could: full coverage, not partial.
                CoverageState::Full,
                2,
                2,
            ),
            (
                "partial: too few points",
                census_of(2, &[(UnjudgedReason::TooFewPoints, 1)]),
                CoverageState::Partial,
                2,
                3,
            ),
            (
                "partial: too few points since blessing",
                census_of(2, &[(UnjudgedReason::TooFewPointsSinceBlessing, 1)]),
                CoverageState::Partial,
                2,
                3,
            ),
            (
                "partial: not measured on branch",
                census_of(2, &[(UnjudgedReason::NotMeasuredOnBranch, 1)]),
                CoverageState::Partial,
                2,
                3,
            ),
            (
                "partial: too few base commits",
                census_of(2, &[(UnjudgedReason::TooFewBaseCommits, 1)]),
                CoverageState::Partial,
                2,
                3,
            ),
            (
                "mixed reasons",
                census_of(
                    4,
                    &[
                        (UnjudgedReason::Ghost, 2),
                        (UnjudgedReason::TooFewPoints, 3),
                        (UnjudgedReason::NotMeasuredOnBranch, 1),
                    ],
                ),
                CoverageState::Partial,
                4,
                8,
            ),
            (
                "full coverage",
                census_of(5, &[]),
                CoverageState::Full,
                5,
                5,
            ),
        ]
    }

    #[test]
    fn every_census_shape_projects_to_its_state() {
        for (name, census, expected, judged, in_scope) in coverage_cases() {
            let coverage = Coverage::from_census(&census);
            assert_eq!(coverage.state(), expected, "{name}");
            assert_eq!(coverage.judged(), judged, "{name}");
            assert_eq!(coverage.in_scope(), in_scope, "{name}");
            assert_eq!(coverage.total(), census.total(), "{name}");
            assert_eq!(coverage.unjudged(), census.unjudged(), "{name}");
        }
    }

    #[test]
    fn analysis_outcome_combines_findings_with_coverage() {
        let cases = [
            (SeriesCensus::default(), AnalysisOutcome::NothingInScope),
            (
                census_of(0, &[(UnjudgedReason::Ghost, 2)]),
                AnalysisOutcome::NothingInScope,
            ),
            (
                census_of(0, &[(UnjudgedReason::TooFewPoints, 2)]),
                AnalysisOutcome::InsufficientBaseline,
            ),
            (
                census_of(1, &[(UnjudgedReason::TooFewPoints, 1)]),
                AnalysisOutcome::Partial,
            ),
            (census_of(2, &[]), AnalysisOutcome::Clean),
        ];

        for (census, expected) in cases {
            let coverage = Coverage::from_census(&census);
            assert_eq!(AnalysisOutcome::from_analysis(false, &coverage), expected);
            assert_eq!(
                AnalysisOutcome::from_analysis(true, &coverage),
                AnalysisOutcome::Findings
            );
        }
    }

    #[test]
    fn analysis_outcome_wire_names_are_distinct_and_stable() {
        let names: Vec<&str> = AnalysisOutcome::ALL
            .iter()
            .map(|outcome| outcome.as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "findings",
                "clean",
                "insufficient_baseline",
                "nothing_in_scope",
                "partial"
            ]
        );
    }

    #[test]
    fn only_a_complete_verdict_reads_as_an_all_clear() {
        // The point of the projection: silence claims an all-clear only where something
        // was actually judged, so a blind run cannot read as a clean one.
        for (name, census, expected, ..) in coverage_cases() {
            let coverage = Coverage::from_census(&census);
            let claims_all_clear = coverage.verdict() == "No notable changes detected.";
            let earned = matches!(expected, CoverageState::Full);
            assert_eq!(claims_all_clear, earned, "{name}: {}", coverage.verdict());
        }
    }

    #[test]
    fn every_ratio_a_verdict_carries_agrees_with_the_state() {
        // The headline and the ratio beneath it must not tell a reader opposite things,
        // which they can only do while they answer to different denominators.
        for (name, census, expected, judged, in_scope) in coverage_cases() {
            let coverage = Coverage::from_census(&census);
            let Some(ratio) = coverage
                .qualifications()
                .into_iter()
                .find_map(|sentence| parse_ratio(&sentence))
            else {
                assert!(
                    matches!(
                        expected,
                        CoverageState::NoSeries | CoverageState::NothingInScope
                    ),
                    "{name}: a state over an in-scope suite states its ratio"
                );
                continue;
            };
            assert_eq!(ratio, (judged, in_scope), "{name}");
            assert_eq!(
                ratio.0 == ratio.1,
                matches!(expected, CoverageState::Full),
                "{name}: a complete ratio and a complete state are the same fact"
            );
        }
    }

    /// The `judged` and `of` counts of a `Judged N of M in-scope series` sentence, or
    /// `None` for a sentence that states no ratio.
    fn parse_ratio(sentence: &str) -> Option<(usize, usize)> {
        let rest = sentence.strip_prefix("Judged ")?;
        let (judged, rest) = rest.split_once(" of ")?;
        let (in_scope, _) = rest.split_once(" in-scope series")?;
        Some((judged.parse().ok()?, in_scope.parse().ok()?))
    }

    #[test]
    fn a_ghost_is_counted_once_per_metric_series() {
        // One benchmark measured for two metrics is two series, and the census counts
        // series, so a two-metric ghost leaves two accounted for and none in scope.
        let coverage = Coverage::from_census(&census_of(0, &[(UnjudgedReason::Ghost, 2)]));
        assert_eq!(coverage.total(), 2);
        assert_eq!(coverage.in_scope(), 0);
        assert_eq!(coverage.state(), CoverageState::NothingInScope);
    }

    #[test]
    fn qualifications_account_for_every_series() {
        let coverage = Coverage::from_census(&census_of(
            4,
            &[
                (UnjudgedReason::Ghost, 2),
                (UnjudgedReason::TooFewPoints, 3),
            ],
        ));
        let sentences = coverage.qualifications();
        assert_eq!(
            sentences,
            vec![
                "Judged 4 of 7 in-scope series; no reportable move survived the gates.".to_owned(),
                "Not judged: 2 series not measured at the analyzed context commit; 3 series \
                 with too few points in the analyzed window."
                    .to_owned(),
            ]
        );
    }

    #[test]
    fn a_fully_judged_analysis_lists_no_shortfall() {
        let coverage = Coverage::from_census(&census_of(3, &[]));
        assert_eq!(
            coverage.qualifications(),
            vec![
                "Judged 3 of 3 in-scope series; no reportable move survived the gates.".to_owned()
            ]
        );
    }

    #[test]
    fn a_run_whose_only_shortfall_is_ghosts_reads_as_fully_covered() {
        // The contradiction this guards against: an unqualified all-clear over a ratio
        // that reads as partial coverage, leaving the two halves of one silent report
        // telling a reader opposite things.
        let coverage = Coverage::from_census(&census_of(3, &[(UnjudgedReason::Ghost, 2)]));
        assert_eq!(coverage.verdict(), "No notable changes detected.");
        assert_eq!(
            coverage.qualifications(),
            vec![
                "Judged 3 of 3 in-scope series; no reportable move survived the gates.".to_owned(),
                "Not judged: 2 series not measured at the analyzed context commit.".to_owned(),
            ]
        );
    }

    #[test]
    fn a_run_that_judged_nothing_in_scope_counts_against_the_in_scope_suite() {
        let coverage = Coverage::from_census(&census_of(
            0,
            &[
                (UnjudgedReason::Ghost, 4),
                (UnjudgedReason::TooFewPoints, 2),
            ],
        ));
        assert_eq!(
            coverage.qualifications(),
            vec![
                "Judged 0 of 2 in-scope series, so nothing was tested: this silence is \
                 not evidence that nothing moved."
                    .to_owned(),
                "Not judged: 4 series not measured at the analyzed context commit; 2 series \
                 with too few points in the analyzed window."
                    .to_owned(),
            ]
        );
    }

    #[test]
    fn an_analysis_that_accounted_for_nothing_states_no_qualification() {
        // The verdict already says nothing was analyzed and the empty-outcome hint
        // explains why, so there is neither a ratio nor a breakdown left to add.
        let coverage = Coverage::from_census(&SeriesCensus::default());
        assert_eq!(coverage.qualifications(), Vec::<String>::new());
    }

    #[test]
    fn state_wire_names_are_distinct_and_stable() {
        let names: Vec<&str> = CoverageState::ALL
            .iter()
            .map(|state| state.as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "no_series",
                "nothing_in_scope",
                "nothing_judged",
                "partial",
                "full"
            ]
        );
    }

    #[test]
    fn every_state_states_a_distinct_reach_that_does_not_overclaim() {
        let reaches: Vec<&str> = CoverageState::ALL
            .iter()
            .map(|state| state.reach())
            .collect();
        for reach in &reaches {
            assert!(!reach.is_empty(), "a state must describe its reach");
        }
        let mut unique = reaches.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), reaches.len(), "each reach must be distinct");

        // A silent partial run must not read as proof that the judged series did not
        // move; it states only that none crossed the reporting threshold.
        assert!(
            CoverageState::Partial
                .reach()
                .contains("no reportable move"),
            "{}",
            CoverageState::Partial.reach()
        );
        // Full is the one state whose silence carries no coverage qualification.
        assert!(
            CoverageState::Full
                .reach()
                .contains("no coverage qualification"),
            "{}",
            CoverageState::Full.reach()
        );
    }
}
