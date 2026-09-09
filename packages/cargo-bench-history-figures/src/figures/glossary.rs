//! The generated glossary table.
//!
//! The glossary page carries its own introduction; the table of terms is generated from
//! [`crate::glossary::TERMS`] so the page, each chapter's term table, and the test that
//! holds them together all read from one list.

use std::fmt::Write as _;

use crate::assets::Asset;
use crate::glossary::TERMS;

/// The glossary table, plus each chapter's own list of the terms it introduces.
#[must_use]
pub fn assets() -> Vec<Asset> {
    let mut assets = vec![Asset::new("glossary-table.md", table())];
    assets.extend(chapter_terms());
    assets
}

/// One "Terms used here" table per chapter that introduces a term.
///
/// Generated rather than written into each chapter because the same term must not be
/// explained two ways in two places — which is exactly what had happened while these tables
/// were hand-written, one chapter having quietly dropped the clause that made a definition
/// correct.
fn chapter_terms() -> Vec<Asset> {
    let mut chapters: Vec<&'static str> = TERMS.iter().map(|term| term.chapter).collect();
    chapters.sort_unstable();
    chapters.dedup();

    chapters
        .into_iter()
        .map(|chapter| {
            let mut terms: Vec<_> = TERMS
                .iter()
                .filter(|term| term.chapter == chapter)
                .collect();
            terms.sort_by_key(|term| term.phrase);

            let mut markdown = String::from("| Term | What it means |\n|---|---|\n");
            for term in terms {
                writeln!(markdown, "| **{}** | {} |", term.phrase, term.definition)
                    .expect("writing to a String never fails");
            }

            let stem = chapter.strip_suffix(".md").unwrap_or(chapter);
            Asset::new(format!("terms-{stem}.md"), markdown)
        })
        .collect()
}

/// Renders the terms as a Markdown table, sorted for lookup.
fn table() -> String {
    let mut terms: Vec<_> = TERMS.iter().collect();
    terms.sort_by_key(|term| term.phrase);

    let mut markdown = String::from("| Term | What it means | Also called | Introduced in |\n");
    markdown.push_str("|---|---|---|---|\n");

    for term in terms {
        writeln!(
            markdown,
            "| {} | {} | {} | [{}]({}) |",
            term.phrase,
            term.definition,
            term.formal_name,
            chapter_title(term.chapter),
            term.chapter
        )
        .expect("writing to a String never fails");
    }

    markdown
}

/// The human-readable title of an appendix chapter file.
///
/// The mapping is spelled out rather than derived from the filename so a chapter can be
/// renamed on disk without silently changing how the glossary refers to it.
fn chapter_title(chapter: &str) -> &'static str {
    match chapter {
        "shape.md" => "Shape of the data",
        "collection.md" => "Collection",
        "selection.md" => "Selection",
        "reconstruction.md" => "Reconstruction",
        "detection.md" => "Detection",
        "gates.md" => "Noise gates",
        "coverage.md" => "Multiplicity and coverage",
        "reporting.md" => "Reporting",
        "insights.md" => "Insights",
        "limits.md" => "Limits",
        _ => "Data pipeline",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;

    #[test]
    fn every_term_appears_in_the_table() {
        let markdown = table();
        let phrases: BTreeSet<&str> = markdown
            .lines()
            .skip(2)
            .filter_map(|line| line.split('|').nth(1))
            .map(str::trim)
            .collect();

        for term in TERMS {
            assert!(
                phrases.contains(term.phrase),
                "'{}' is missing from the glossary table",
                term.phrase
            );
        }
    }

    #[test]
    fn terms_are_sorted_for_lookup() {
        let markdown = table();
        let phrases: Vec<&str> = markdown
            .lines()
            .skip(2)
            .filter_map(|line| line.split('|').nth(1))
            .map(str::trim)
            .collect();

        let mut sorted = phrases.clone();
        sorted.sort_unstable();

        assert_eq!(phrases, sorted);
    }

    #[test]
    fn every_chapter_reference_resolves_to_a_title() {
        for term in TERMS {
            assert_ne!(
                chapter_title(term.chapter),
                "Data pipeline",
                "'{}' points at an unrecognized chapter '{}'",
                term.phrase,
                term.chapter
            );
        }
    }

    /// A chapter's own terms table and the glossary must give the same definition, or the two
    /// places a reader can look would disagree.
    #[test]
    fn a_chapter_table_carries_the_glossary_definition() {
        let assets = chapter_terms();

        for term in TERMS {
            let stem = term.chapter.strip_suffix(".md").unwrap_or(term.chapter);
            let expected = format!("terms-{stem}.md");
            let asset = assets
                .iter()
                .find(|asset| asset.path == expected)
                .unwrap_or_else(|| panic!("no terms table for {}", term.chapter));

            assert!(
                asset.content.contains(term.definition),
                "'{}' is defined differently in {}",
                term.phrase,
                expected
            );
        }
    }

    #[test]
    fn a_chapter_table_holds_only_its_own_terms() {
        let chapters: BTreeMap<&str, &str> = TERMS
            .iter()
            .map(|term| (term.phrase, term.chapter))
            .collect();

        for asset in chapter_terms() {
            let stem = asset
                .path
                .strip_prefix("terms-")
                .and_then(|rest| rest.strip_suffix(".md"))
                .expect("chapter tables are named after their chapter");
            let chapter = format!("{stem}.md");

            for line in asset.content.lines().skip(2) {
                let phrase = line
                    .split('|')
                    .nth(1)
                    .map(str::trim)
                    .and_then(|phrase| phrase.strip_prefix("**"))
                    .and_then(|phrase| phrase.strip_suffix("**"))
                    .expect("a chapter table row names a bold glossary term");
                assert_eq!(
                    chapters.get(phrase).copied(),
                    Some(chapter.as_str()),
                    "{} lists a term belonging to another chapter: {}",
                    asset.path,
                    phrase
                );
            }
        }
    }
}
