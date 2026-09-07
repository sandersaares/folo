# Reporting

Everything so far produced findings and a census. This chapter is how they reach you, and how
to read what you get.

## Terms used here

{{#include generated/terms-reporting.md}}

## Ranking

Findings are ordered by **descending relative move**, then by analysis method, then by a stable
identity tie-break so two runs over the same data produce the same order.

There is deliberately **no severity classification**. Whether a 4% regression in a hot path
matters more than a 40% regression in a rarely-used one is a judgment about your project, and
the tool does not have the information to make it. It sorts by the one quantity it can
measure, and leaves the ranking of importance to you.

Note in particular that history findings carry **no** confidence score to sort by. Every
reported history finding already cleared its test, so any such number would be uniformly high
and would rank almost nothing — see
[Detection](detection.md#history-chance-levels-not-a-confidence-score). Branch findings report
an observed range plus report-wide historical context instead of a probability score.

## Reading a finding

{{#include generated/reporting-finding-annotated.md}}

The chart is drawn against **topology**, not against the observations. One column per commit,
so a gap is a gap and the trailing stretch after the last observation is visible as one. That
matters: a chart that packed the observations together would hide exactly the sparseness that
[detection cannot see](reconstruction.md#gaps-are-holes-not-zeroes).

Where a series has more commits than the chart has columns, commits are grouped before
plotting. Grouping first is deliberate — the alternative attenuates an isolated observation
surrounded by gaps into nothing.

## The formats

The tool emits its findings in five forms, each requested independently:

- **Text** — the default terminal report.
- **Markdown** (`--markdown <path>`) — the same content, for a pull request or an issue.
- **JSON** (`--json <path>`) — the complete machine-readable result.
- **Condensed summary** (`--markdown-summary <path>`, `analyze` only) — a short, capped Markdown
  digest for a size-limited destination such as a pull request comment.
- **Outcome** (`--outcome <path>`, `analyze` only) — one stable wire name for selecting an
  automation message without parsing the full JSON report.

Text and Markdown carry every finding but omit the per-reason census when findings exist. **JSON
always carries the complete census**, which makes it the machine-readable signal: each finding
self-describing, plus every unjudged reason the human formats print only on a silent report. It
deliberately omits the per-commit chart series — that is presentation, and
[`examine`](../commands/examine.md) is the way to get the underlying points. The **condensed
summary** is lossy by design — capped, and flattened so the per-set grouping is dropped — so it is
the one output you must not automate against. The outcome file repeats only the JSON report's
top-level `outcome` field; it carries no report details. The full table is under
[Where output goes](#where-output-goes).

Here is the same analysis in each form. The JSON excerpt is illustrative of the shape
automation reads, not a complete field catalog.

{{#include generated/reporting-text.md}}

{{#include generated/reporting-json.md}}

## Findings never fail the build

The exit code reflects whether the analysis **ran**. A report full of regressions exits
successfully; a report that could not reach its storage backend does not.

*Why:* a benchmark tool that breaks builds on a measurement gets disabled, and a disabled tool
detects nothing. Findings are advisory by design.

*What this means for automation:* write the JSON report to a file with `--json`. The top-level
`outcome` field combines findings with coverage into `findings`, `clean`,
`insufficient_baseline`, `nothing_in_scope` or `partial`; the `notable` boolean remains `true`
exactly for `findings`. Use `--outcome <path>` when this one verdict is all the caller needs.
The full JSON remains necessary for counts, reasons and findings. See [Multiplicity and
coverage](coverage.md#reading-a-silent-report).

## The coverage line is not decoration

Every report states how many series it judged.

{{#include generated/reporting-census.md}}

This is given the same prominence as the findings on purpose. "No notable changes" means
*nothing changed among the series this run was able to judge*, and without the denominator you
cannot tell that statement apart from "the benchmarks did not run".

Note the asymmetry: the **reasons** a series went unjudged are spelled out only when a report
has no findings to show — otherwise the human-readable formats print the tally alone, on the
grounds that a report with findings has something more urgent to say. The JSON census always
carries the full breakdown, which is another reason to read it rather than the text.

## Comparison-base lag

Branch mode compares the analyzed context commit against the base ref's recent commits **within
the same discriminant set**. On a rotating CI pool, the newest base-ref commits may only carry
data under a different machine key — so your runner compares against base data from several
commits back.

{{#include generated/reporting-lag.svg}}

The empty stretch after the comparison base is the lag: commits that exist on the base ref, but
not for the context run's machine key.

The report says so, naming how far behind and which of two reasons applies:

{{#include generated/reporting-lag.md}}

It is advisory. It never changes which findings are reported, and never affects the exit code.
What it changes is how much weight you should give a marginal branch finding: a comparison
against a base state ten commits old is a comparison against a base state ten commits old.

## Branch range and historical context

A branch finding does not borrow history-mode change-point wording. It states the measured context
value, the lowest and highest values observed in the selected current-base regime, and the excess
beyond the nearest edge. "Slower than all 20 current-base observations" is a claim about those
recorded observations, not a probability estimate.

The discriminant-set section then gives the report-wide historical comparison, for example:

```text
None of 10 comparable base commits showed as much out-of-range movement as this branch
(12 series compared).
```

When too little shared history exists, the report says the comparison could not be formed and
still prints every factual range excursion. JSON exposes the same distinction: finding-level
`branch` range metadata and set-level `branch_comparison` counts.

## Where output goes

The text report goes to **stdout**. Diagnostics, the effective-selection summary and verbose
notes go to **stderr**.

Warnings are the exception, and it is deliberate: a comparison-base lag notice, or the note that
a dirty run was admitted on the base tip, is rendered **into the report body** rather than onto
stderr. Those qualify the findings printed beside them, so a reader who captured only the report
would otherwise lose the caveat that changes how to read it.

The other formats are written to files rather than to a stream — `--markdown`, `--json`,
`--markdown-summary` and `--outcome` each take a path, and `--no-text` suppresses the stdout
report when you want only those.

{{#include generated/reporting-formats.md}}

## What comes next

Nothing — this is the end of the pipeline. If a report left you with a question rather than an
answer, [Insights](insights.md) is the triage chapter.
