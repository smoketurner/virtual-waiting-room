//! `docs/RUNBOOK.md`'s automated readiness checklist is generated from the
//! same catalogue the dashboard's readiness panel renders (issue #70), so the
//! two cannot drift: a check added, renamed or re-pointed at another
//! requirement without regenerating the runbook fails here.
//!
//! Regenerate after changing the catalogue:
//! `cargo test -p admin -- --ignored regenerate_runbook_checklist`.

#![expect(
    clippy::expect_used,
    reason = "test code panics on a missing or malformed runbook"
)]

use std::path::PathBuf;

use admin::readiness::{RUNBOOK_BEGIN, RUNBOOK_END, runbook_checklist};

fn runbook_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/RUNBOOK.md")
}

/// The byte range of the generated block, markers included and through the
/// newline after the end marker.
fn generated_span(runbook: &str) -> (usize, usize) {
    let start = runbook
        .find(RUNBOOK_BEGIN)
        .expect("docs/RUNBOOK.md must carry the readiness-checks:begin marker");
    let end_marker = runbook[start..].find(RUNBOOK_END).expect(
        "docs/RUNBOOK.md must carry the readiness-checks:end marker after the begin marker",
    ) + start;
    let mut end = end_marker + RUNBOOK_END.len();
    if runbook[end..].starts_with('\n') {
        end += 1;
    }
    (start, end)
}

/// Rewrites the generated block in `docs/RUNBOOK.md` from the catalogue.
#[test]
#[ignore = "writes docs/RUNBOOK.md; run explicitly after changing the readiness catalogue"]
fn regenerate_runbook_checklist() {
    let runbook = std::fs::read_to_string(runbook_path()).expect("read docs/RUNBOOK.md");
    let (start, end) = generated_span(&runbook);
    let updated = format!(
        "{}{}{}",
        &runbook[..start],
        runbook_checklist(),
        &runbook[end..]
    );
    std::fs::write(runbook_path(), updated).expect("write docs/RUNBOOK.md");
}

#[test]
fn the_runbook_checklist_matches_the_readiness_catalogue() {
    let runbook = std::fs::read_to_string(runbook_path()).expect("read docs/RUNBOOK.md");
    let (start, end) = generated_span(&runbook);
    assert_eq!(
        &runbook[start..end],
        runbook_checklist(),
        "docs/RUNBOOK.md's readiness checklist has drifted from crates/admin/src/readiness.rs; \
         run `cargo test -p admin -- --ignored regenerate_runbook_checklist`"
    );
}

#[test]
fn the_generated_block_appears_once() {
    let runbook = std::fs::read_to_string(runbook_path()).expect("read docs/RUNBOOK.md");
    assert_eq!(runbook.matches(RUNBOOK_BEGIN).count(), 1);
    assert_eq!(runbook.matches(RUNBOOK_END).count(), 1);
}
