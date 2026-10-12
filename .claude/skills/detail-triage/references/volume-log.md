# Detail Volume Log

Measured history of Detail's findings, so a later run can tell a trend from a
blip. Add a row to the batch table and append one section per triage pass.
Counted figures come from `scripts/detail-stats.py`; do not hand-edit them. The
judgement columns come from the batch's record in `.local/`.

## Batch table

| column | source | meaning |
|---|---|---|
| issues, fix PRs, docs PRs | script | what Detail opened on the detection date; docs PRs file no issue |
| Detail PR | script | findings blamed on one of Detail's own PRs |
| PR≤3d | script | findings blamed on any PR merged at most 3 days before detection, ours included |
| not as written | record | PRs that needed changes, were superseded, or were closed after review |
| dispositions | record | what happened to the batch's PRs |
| residue | record | findings that match residue or an open decision in an earlier record |
| rules | record | Detail rules requested for the batch's classes, and whether synced |

| batch | issues | fix PRs | docs PRs | Detail PR | PR≤3d | not as written | dispositions | residue | rules |
|---|---|---|---|---|---|---|---|---|---|
| 2026-09-11 | 16 | 11 | 1 | 0 | 16 | not recorded | not recorded | not recorded | 0 |
| 2026-09-15 | 4 | 4 | 1 | 0 | 4 | not recorded | not recorded | not recorded | 0 |
| 2026-09-16 | 2 | 2 | 1 | 0 | 2 | not recorded | not recorded | not recorded | 0 |
| 2026-09-17 | 13 | 13 | 3 | 1 | 4 | not recorded | not recorded | not recorded | 0 |
| 2026-09-18 | 2 | 2 | 2 | 2 | 2 | not recorded | not recorded | not recorded | 0 |
| 2026-09-21 | 0 | 0 | 1 | 0 | 0 | not recorded | not recorded | not recorded | 0 |

No dead-code PR has arrived yet, so that column is omitted until one does. No
batch has a triage record yet, so the judgement columns start with the next
one.

## 2026-10-12 — baseline

37 issues, 2026-09-11 through 2026-09-18, all closed. No Detail rules exist for
this repository yet.

| month | n | no attr | median age | p90 age | <30d | >90d |
|-------|---|---------|-----------|---------|------|------|
| 2026-09 | 37 | 0 | 1 | 6 | 37 | 0 |

Findings are young: a median age of one day and every one against code under
30 days old. The repository is young too, and feature work lands in large
PRs, so this is evidence of neither backlog excavation nor a regression trend.
Every finding in the first batch was blamed on a PR merged within the
preceding three days, which is the figure to watch.

The clusters the script sees, in rank order (keyword clustering, directional
only): admin display/audit, phase transition guard, silent corruption or
malformed input, credential/gate bypass, admission arithmetic/no-show,
position duplication/overlap, edge gate/KVS config. The classes confirmed by
reading rather than by keyword are listed in step 2 of `SKILL.md`: unscoped
`PreQueue` scans, unconditioned read-modify-write of operator state, the
debounce anchor, live-join claims racing the open, and the no-show EWMA fed a
non-measurement.

Merged-PR blame rate as of this date (`--fix-defect-rate 2026-10-12`): over a
14-day window, 3 of 39 Detail PRs and 19 of 113 human PRs were later blamed by
a finding. Not like-for-like, and it understates the true rate, but it is the
evidence that review is worth doing on every fix regardless of author.
