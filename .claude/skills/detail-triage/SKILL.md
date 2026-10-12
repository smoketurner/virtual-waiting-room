---
name: detail-triage
description: Triage a batch of Detail bug issues and fix PRs — measure the trend, classify each finding as an instance or a class, review each fix for defects in the fix itself, and decide what merges versus what gets fixed as a class with a guardrail. Use when asked to "triage the Detail batch", "Detail opened a batch", "why is Detail volume rising", "classify Detail findings", or when a new batch of `[Detail Bug]` issues appears.
---

# Triage a Detail Batch

Detail scans the whole repository on each pass and files one issue per finding,
usually with a paired fix PR. Merging each PR on its own closes the instance and
leaves the pattern in place for the next pass to rediscover somewhere else. This
skill exists to break that loop: measure what is actually happening, separate
instances from classes, and make sure every class fix carries something that
prevents the class from coming back.

Work through the steps in order. Step 2 gates everything after it — nothing
merges before the classification table exists.

## Step 0: Know what you may not edit

`.claude/skills/detail-rules/` and `.claude/skills/detail-create-rules/` are
managed on the Detail side and synced down via `detail rules pull` and
`detail skill rules`. Neither exists in this repository yet — no rule has been
requested for it. When they appear, **never write a rule and never edit those
files by hand.** Detail scans with its own copy of each rule, so a local edit or
a hand-written rule file is invisible to the scanner, and the next sync
overwrites it anyway.

The only way to get a rule is to ask Detail to generate one through the Detail
CLI, then sync the result into this repository — see step 5.

Everything under `.claude/skills/detail-triage/` is repo-owned and yours to
maintain.

## Step 1: Measure the batch and the trend

```bash
python3 .claude/skills/detail-triage/scripts/detail-stats.py
python3 .claude/skills/detail-triage/scripts/detail-stats.py --json   # for scripting
```

The script reads every Detail-authored issue from the GitHub REST API, extracts
the "Introduced in [#N] … on DATE" attribution from each body, and reports
volume, bug age, and keyword-clustered classes per detection month. It needs no
`gh` CLI: the token comes from `GH_TOKEN` or `GITHUB_TOKEN` (set in sandboxed
sessions), falling back to `gh auth token`, and the repository comes from
`GH_REPO` or the `origin` remote.

Read three things together:

- **median / p90 age** — how far back into history this pass reached.
- **`<30d`** — findings against code merged in the last month.
- **`Detail PR` and `PR<=3d`** (per-batch table) — findings attributed to one of
  Detail's own fix PRs, and to any PR merged at most three days before
  detection, whoever wrote it. The same table counts Detail's fix PRs and its
  no-issue PRs: `detail/fix-docs/` documentation corrections and
  `detail/dead-code/` removals, neither of which files an issue.

A climbing median with a flat `<30d` means Detail is working through backlog:
volume is high for a reason that no process change will fix, and the right
response is throughput. A climbing `<30d` means new code is generating findings
as fast as old code is cleaned up, and the right response is a guardrail. Say
which of the two you are looking at before proposing any remedy.

This repository is young and every finding so far is under 30 days old with a
median age of a day, so `<30d` is not yet a signal on its own: it tracks the
rate of feature work, and feature work lands here in large PRs that each
produce a batch within days. Read `PR<=3d` instead.

`PR<=3d` is the one that decides how this batch gets merged. When it is
high, merging fix PRs as-is is feeding the next scan, and the loop only breaks
by making class decisions *before* merging. Part of the signal is mechanical —
as Detail's merged PR count grows, its commits are increasingly the last to
touch any given line — so confirm a spike by checking whether the finding is a
defect in the logic the fix *added* rather than merely in a file it touched.
Watch the human-authored case as closely as Detail's: a class fix we merged the
day before can account for every finding in a batch while the Detail-only
column reads zero. The first batch here (2026-09-11) had all sixteen findings
blamed on PRs merged within three days.

The class table is **keyword clustering over titles — directional, not
rigorous**. Titles overlap classes and some match none. Use it to spot
recurrence worth investigating; never quote its counts as fact. When a new
recurring pattern shows up in the unmatched titles, add it to `CLASSES` in the
script.

Compare against `references/volume-log.md`, which holds the measured history;
its batch table is the per-batch series to extend.

## Step 2: Classify every open finding

For each open issue in the batch, establish three things and put them in one
table:

| column | what it means |
|---|---|
| class | which recurring pattern it belongs to, or "isolated" |
| live? | is the defect still present in current `main`? |
| siblings | other call sites sharing the pattern, found by reading the code |

**Verify "live?" against the tree, never against the issue body.** Detail
authors a batch from a snapshot, and that snapshot can predate a merge from the
same day. A finding can be real while its quoted code and line numbers are
stale, and a reviewer trusting the body reaches the wrong conclusion in either
direction. Mechanisms have been removed outright here (entry tickets,
open-time demotion, controller-driven expiry, the origin authorizer — the
`docs/adr/00xx-remove-*.md` records), so a finding can describe code that no
longer exists.

**Match against stated residue before hunting.** Read the residue and
open-decision sections of the last few records in `.local/`. A finding that
matches something a review already accepted is a residue recurrence, not a
surprise. Count it in the batch table. The lever for a residue recurrence is
fixing residue in the PR that left it, not a new guardrail.

Finding the siblings is the actual work of this step, and it is what turns a
list of instances into a class. Prefer `codegraph_explore` or `ast-grep` over
ripgrep here: the question is structural ("every read-modify-write of the
edge config without a version condition"), not textual. This repository's
history has clear examples:

- unscoped `PreQueue` scans leaking another event's rows (#152, #160, #161)
  were one class of a query missing its `event_id` scope;
- concurrent admin mutations silently overwriting each other in the
  KeyValueStore and on the `Counters` item (#183, #192, #194) were one class
  of read-modify-write without a condition;
- the debounce anchor regressing under fail-open, set-rules and open-now
  (#100, #183, #196) was one class of a stamp written by every mutation path
  but guarded in only one;
- duplicate or overlapping positions (#88, #90, #193) were one class of a
  live-join claim racing the open;
- the no-show EWMA corrupting under an empty queue, a tiny value, or an
  operator hold (#102, #106, #197) was one class of a smoother fed a sample
  that was not a measurement.

**The sibling hunt disqualifies classes as often as it confirms them, and that
is a result worth having.** Exclude test-only sites — `#[cfg(test)]` modules,
`crates/wr-common/tests/`, `infra/modules/edge/tests/`, `infra/modules/*/tests/`
— and judge each sibling's consequence rather than counting matches. A site
that fails in the safe direction, or only affects a log line or a dashboard
label, is not a sibling worth fixing. A class of three where two members are
harmless is not a class. Say so, and merge the instance.

Weigh findings by what they can do to a visitor. `crates/harness` is the local
load generator behind `make load` (#104, #195, #198, #214 were all harness
findings): a defect there skews a measurement and never touches admission, so
it never justifies a guardrail in a production crate and sits last in the
batch. The admin dashboard's display and audit findings (#86, #87, #103) are
operator-facing, one rung up. Anything on the join, position, admission, or
gate path is the batch's real content.

Issues Detail filed **without** a paired fix PR belong in this table too. They
are usually the ones it judged too structural to auto-fix, which makes them the
strongest class candidates in the batch, not the weakest.

### Docs PRs

Detail's `detail/fix-docs/` PRs file no issue and correct documentation to
match the code. Review the direction of the correction, not just its accuracy:
when a document and the code disagree, the document can be the one that is
right, and a docs PR that rewrites `docs/DESIGN.md` or an ADR to match a bug
retires the only record that it is a bug. Check `docs/adr/` for the decision
before accepting a change to what a document says the system does.

Two layers describe this system — `docs/` for people and
`.kiro/specs/virtual-waiting-room/` for agents — joined by requirement IDs. A
docs PR that changes one layer and not the other leaves them disagreeing;
rationale lives only in ADRs, so a PR that adds rationale to a steering file
or a spec is putting it in the wrong place. The checkboxes in
`.kiro/specs/.../tasks.md` track what is built: a docs PR may not tick one.
Code comments cite GitHub issues only, never ADRs or requirement IDs, and
describe behaviour rather than history.

### Dead Code PRs

Detail opens Dead Code PRs on `detail/dead-code/…`; none has arrived here yet.
CI runs `cargo clippy --all-targets --all-features -- -D warnings`, so the
compiler proves most Rust removals: a function, impl, or constant that still
had a caller would not build. Review the removals the compiler cannot see:

- **Anything the CloudFront Function reads.** `gate.js.tftpl` mirrors the
  session credential and the rule wire encoding from `wr-common`, and reads
  its configuration from KeyValueStore keys the admin Lambda writes. A Rust
  field, KVS key, or encoding variant with no Rust reader may still have a
  JavaScript one. Run `node --test infra/modules/edge/tests/*.test.js` and
  regenerate the conformance vectors before accepting.
- **A Terraform variable, output, env var, or IAM statement.** `main.rs` reads
  the env vars, but nothing compiles the templates. `make validate` and the
  `*.tftest.hcl` files under `infra/modules/*/tests/` are the check.
- **Unreachable branches kept on purpose.** The `_`-free exhaustive matches
  and the `StoredControl` two-value codec exist so that an impossible state
  is a compile error, not a dead arm. Check `CLAUDE.md` before accepting their
  deletion.

Deployments are ephemeral and per event, so a stored-shape change needs no
migration path: the concern is the edge mirror, not a previous release.

## Step 3: Decide instance versus class

**A Detail fix PR is a reviewed draft, never a merge candidate.** Green CI, a
detailed PR body, and a full test suite make a PR look finished; step 6 exists
because a fix can be actively wrong under exactly that appearance.

The policy is not a claim that Detail writes bad fixes. The point is that
post-merge blame understates the defect rate, because the scanner does not
re-find everything it introduces. Review is what closes that gap, and it is
worth doing regardless of who wrote the fix. `detail-stats.py --fix-defect-rate
YYYY-MM-DD` measures how often merged PRs are later blamed by a finding, Detail
against human.

Three or more open instances of one pattern makes this decision mandatory, not
optional. For each class pick one and record which:

- **Instance** — genuinely isolated. Merge Detail's PR once it has been through
  step 6 and is green. No surrounding refactor.
- **Class** — one change that fixes every site found in step 2. Close Detail's
  individual PRs as superseded rather than merging them, and say so in each.

**Never merge the instance PR for a finding that belongs to a class.** Merging
it closes the instance and erases the evidence that the class exists, so the
next pass rediscovers the same pattern at a different call site and the cycle
repeats.

**Settle contradictory findings together.** A batch can hold findings that pull
in opposite directions, or a fix that reverses a decision already on record —
most often here, a fix that moves a failure from fail-closed to fail-open or
back, or that trades a gap in the position space for a duplicate. Resolve the
governing record once — the ADR in `docs/adr/`, `docs/DESIGN.md`, memory, or
the PR that made the decision — then dispose of the whole set against it. Gaps
are acceptable and duplicates are not; admission fails closed except through
the operator's `fail_open_until` window; the burst never touches compute.

A class fix that lands without a guardrail will regress, so step 4 is part of
the same PR, not a follow-up.

## Step 4: Attach a guardrail to every class fix

Pick the strongest mechanism that fits, and be honest about what it does not
cover.

1. **A type whose invalid state cannot be constructed.** The strongest option:
   the wrong thing stops compiling. `StoredControl` is the model — the storage
   codec has two values where the resolved type has three, so `FailOpen` can
   never be written as a string. Best fit whenever the class is "compared or
   stored a value that was never scoped, resolved, or bounded". A newtype only
   guards if its field is private; one that exposes `pub String` still lets
   any caller build it without the constructor.
2. **A workspace lint or a `clippy.toml` `disallowed-methods` entry.** There is
   no `clippy.toml` yet; adding one is fine. `-D warnings` turns an entry into
   a CI failure, and `allow_attributes = "deny"` means nobody can silence it at
   the call site — but it matches *a named function or type*, nothing more.
3. **A property or behavioral test** asserting the invariant across every
   site: `proptest` for the permutation and counter, a conformance vector in
   `crates/wr-common/tests/vectors.rs` for anything the CloudFront Function
   must agree on, a `*.tftest.hcl` for a Terraform invariant (the
   `api_key_required` test is the pattern), or a test driven through the
   crate's `FakeStore`. Never a test that scans source text.

**A guardrail can under-cover its class.** A lint on a constructor cannot see a
value that was built correctly and then compared at the wrong precision or
written without a condition. When you pick a mechanism, write down the part of
the class it does not cover, and either add a second mechanism or state the
residue in the PR.

Verify the guardrail the way the project verifies tests: break the code, confirm
CI catches it, then fix it. A guardrail never observed failing is not known to
work.

**A class fix produces the next batch's findings.** Before merging a class fix,
hunt siblings of the new type or guard: every consumer of the old value it
replaces, and every caller it did not touch.

Look past Rust. When a fix adds an AWS call, check the function's IAM policy
in `infra/modules/core`. When a fix changes the session credential, a rule's
wire encoding, or a KeyValueStore key, the CloudFront Function in
`infra/modules/edge/functions/gate.js.tftpl` has to change with it and the
vectors have to be regenerated. When a fix adds a path `waiting.js` calls, the
WAF challenge list in `edge/waf.tf` has to carry it. When a fix adds a resource
or a schedule, cost it between events (N1: under $5/month idle). When a fix
changes what a stored record means, check every reader: `read`,
`generate_token`, `nojs`, `admin`, and `docs/DYNAMODB.md`.

## Step 5: Request a Detail rule for each confirmed class

This is the step that reduces future volume. A class the scanner knows about is
reported as a rule violation on the way in, rather than rediscovered instance by
instance for months.

**Request the rule; never write one.** `detail rules create` submits a request
and *Detail* generates the rule text. Hand-editing a rule file under
`.claude/skills/detail-rules/references/` is the thing that is forbidden — those
are generated artifacts and the next sync overwrites local edits.

Use the `detail-create-rules` skill once `detail skill rules` has installed
it. The evidence is already in the issues: `detail-stats.py --json` emits an
`open_bug_ids` map from issue number to the `bug_<uuid>` in its body.

```bash
detail rules create --description "<the invariant, stated as a rule>" \
                    --bug-ids <bug_id1,bug_id2,...>
```

Then poll with `detail rules requests show <rcr_...>`, review each result with
`detail rules show <rule_id>`, pull with `detail rules pull <rule_id>`, and
commit the synced files as `chore(detail): …`.

**Read the generated rule against the merged tree before pulling it, and read
its correct-pattern section, not only its detection section.** Detail generates
from the bug-report snapshot, which predates the batch's own fixes, so a rule
can hold up as "already correct" the exact code the batch just replaced. That
is worse than no rule: it tells the next reviewer the defect is the model.

The remedy is a fresh `detail rules create` whose description names the stale
rule and spells out what it got wrong. Hand-editing the pulled file is
forbidden (step 0) and the next sync would overwrite it, and the CLI has
`create`, `pull`, `show`, `list`, `requests` and `propose` — no refine verb.

A good request describes an invariant, not an incident. "Every query against
`PreQueue` or `Positions` carries the event's scope, because an unscoped scan
reads another event's rows into this event's demotion set" gives Detail
something to generate a rule from; "issue #152 was fixed wrong" does not.

When a rule for the class **already exists and did not catch it**, say so in the
description and ask for refinement rather than adding a second overlapping rule.
More overlapping coverage is rarely the lever.

A Detail rule is wired into no CI job, git hook, or Makefile target — it only
runs when someone asks, so a rule that exists still does not gate a merge. If
the goal is to stop a class at the door, changing that wiring is a separate
decision to raise with the user rather than assume.

## Step 6: Review each fix for defects in the fix itself

This is the step that earns the triage. Do not treat it as a merge checklist.

### Scope the reading first

Most of a Detail PR is tests. Split production from test additions so the batch
is tractable:

```bash
python3 .claude/skills/detail-triage/scripts/detail-stats.py --pr-diff-sizes 215-219
```

Read the production hunks. Read tests only to check that the assertion pins the
behavior the issue describes. Before reporting a test as missing, search the
PR's test files for it: a diff filtered to production paths hides them.

The split is by file path, so a PR reporting zero test lines has inline
`#[cfg(test)] mod tests` in the production file rather than no tests — every
Lambda crate keeps its `FakeStore` and tests inline, so treat the production
figure as an upper bound.

### The failure-mode checklist

Interrogate what the fix introduces, not just whether it addresses the report:

- **New arithmetic in the controller or the permutation** — is every operation
  checked or saturating? The release profile has no overflow checks, and a
  wrapped subtraction in `serving_counter` arithmetic releases a burst. What
  happens at exactly zero, at equality, and at the DynamoDB Number minimum
  (#106 stalled the controller when the EWMA decayed below it)?
- **A new error path** — which way does it fail, and is that the way this path
  is allowed to fail? Admission (`generate_token`, `nojs`, the gate) fails
  closed except through the operator's `fail_open_until` window;
  `/status` degrades; the join path returns 200 over an SQS error by design, so
  "it returned 200" proves nothing there. A fix that resolves an unreadable
  value to the permissive side (#184 showed admission as open on a malformed
  `admission_control`) has changed the failure direction, not fixed a bug.
- **A new DynamoDB write** — is every key built by `wr_common::expr`, never at
  the call site? Does it carry a condition expression, or does it read, modify
  and write back (#192, #194)? Does it put a sequence anywhere but the event's
  own `Counters` item? Does it store a resolved value — `FailOpen` is never a
  stored string?
- **A new phase guard or conditional update** — does every layer above it
  distinguish "condition failed" from "already done" (#213 reported a
  wrong-phase rejection as a successful no-op)? Does the admin's open-now and
  the scheduled open share the guard, or only one of them?
- **A new write to a field another path also writes** — the debounce anchor
  and the audit stamp are written by every operator mutation; does the new
  path carry the same guard against regressing them (#183, #196)?
- **A new guard** — does it cover every call site, or only the reported one?
  `generate_token::admit` is shared with `nojs`; the KeyValueStore is written
  by fail-open, set-rules and open-now alike. Search the codebase for the
  pattern, do not trust the diff's coverage.
- **A new branch or match arm** — the inverse of the guard question: does
  anything *upstream* stop it being reached? A fix that adds an arm to an
  existing match inherits every early return above it. Read the enclosing
  function from its top, not from the diff hunk. The match must stay
  exhaustive with no `_` arm.
- **A new query or scan** — is it scoped to the event (#152, #161)? Is it on
  the admission path, where an extra read is an extra way to fail (#153,
  #151)?
- **A change that crosses into the edge** — the session credential, a rule's
  wire encoding, percent-decoding of a path (#191), or a KVS key: is the
  CloudFront Function changed to match, and were the conformance vectors
  regenerated and run? A Rust-only fix to a shared encoding is half a fix.
- **A new configuration knob or Terraform resource** — does it bill between
  events? Is it a toggle for something that costs nothing, which should be
  derived from whether the artifact exists instead?
- **A changed stored shape** — every reader listed under step 4, and the
  `docs/DYNAMODB.md` description.

### Check the fix against the class it fixes

When the bug is a parsing, encoding, or scoping defect, the fix is written in
the same idiom that produced it and tends to inherit the same blind spot. Test
the fix against the governing contract's own cases — the conformance vectors
for anything the gate reads, the AWS-documented behaviour for DynamoDB
conditions and CloudFront Function limits — and against neighbouring inputs in
the same class.

A test whose input is derived from the code's own output cannot find a
disagreement with the real counterpart. The credential and rule encodings have
two implementations, Rust and JavaScript, and only
`gate.conformance.test.js` running the shipped `gate.js.tftpl` under `node:vm`
proves they agree. A Rust unit test on the Rust side alone does not.

### Reproduce, do not argue

For a suspected defect in a pure function, extract the function body into a
file in the session's scratchpad directory and run it. It takes a minute and
converts "this looks wrong" into a confirmed blocker with output to paste into
the review:

```bash
rustc -O -o <scratch>/t <scratch>/t.rs && <scratch>/t   # the PR's function body, verbatim
node <scratch>/t.js                                      # a gate.js.tftpl branch, verbatim
```

A review comment saying "I think this mishandles a wrapped counter" invites
debate. One showing the wrong output does not.

### Then the hygiene checks

- Confirm any AWS behavior the PR body asserts against the AWS documentation
  and quote it; never accept the PR body's paraphrase. Check the quote's scope
  as well as its strength.
- When trimming narrative comments, keep every GitHub issue citation and
  remove any ADR or requirement-ID citation a fix adds — comments here cite
  issues only. Cut the history, not the requirement.
- Confirm new persistence goes through the crate's `Store` trait and tests use
  its `FakeStore`, not ad-hoc stubs or a mocked SDK client.
- Confirm no `#[allow]`, no `unwrap`, no `features = [...]` at the workspace
  root, no `^` or `~` in a version pin, and nothing that pulls `ring` or
  `openssl` onto the admission path.
- Confirm no single-caller helpers were added.
- Confirm `tasks.md` checkboxes were not ticked by a fix that delivers part of
  a task.

### Merge mechanics

`main` is governed by a ruleset: a merge queue (squash, up to five entries,
five-minute minimum wait), required signed commits, linear history, and a
pull request for every change. Auto-merge at the repository level is off; the
queue is the only way in.

Use the GitHub REST API rather than the `gh` CLI, so the steps work from a
sandboxed session: the token is in `GH_TOKEN` or `GITHUB_TOKEN`, and the calls
below assume

```bash
H=(-H "Authorization: Bearer $GH_TOKEN" -H "Accept: application/vnd.github+json" \
   -H "X-GitHub-Api-Version: 2022-11-28")
R=https://api.github.com/repos/smoketurner/virtual-waiting-room
```

**The queue does not wait for CI.** The ruleset has no required-status-checks
rule and no workflow has a `merge_group` trigger, so an enqueued PR merges
after the minimum wait whether or not its checks passed. Reading CI yourself is
the only gate. Read it fresh at the moment you decide — runs get superseded,
and a stale failing run can hide a passing one:

```bash
sha=$(curl -s "${H[@]}" $R/pulls/<n> | python3 -c 'import json,sys; print(json.load(sys.stdin)["head"]["sha"])')
curl -s "${H[@]}" $R/commits/$sha/check-runs \
  | python3 -c 'import json,sys; [print(r["name"], r["status"], r["conclusion"]) for r in json.load(sys.stdin)["check_runs"]]'
```

Re-run that until nothing is `in_progress`; when a wait is unavoidable, loop
it in the background rather than sleeping blind. The workflows are
path-filtered: Rust CI (fmt, clippy, test, cargo-deny) runs on a Rust change,
Terraform CI (fmt, validate) only when `infra/` changes, and Client CI
(`node --test` over the edge tests) only when the client files change. A PR
with one check run is not missing two; a PR touching `infra/` with one check
run is.

Before enqueueing, check that every commit is signed. A follow-up commit Detail
pushes after its CI fails (a rustfmt fix, say) can arrive unsigned, and the
queue rejects the whole branch:

```bash
curl -s "${H[@]}" $R/pulls/<n>/commits \
  | python3 -c 'import json,sys; [print(c["sha"][:8], c["commit"]["verification"]["verified"], c["commit"]["verification"]["reason"]) for c in json.load(sys.stdin)]'
```

Rebuild such a branch with signed commits rather than rewriting Detail's.

Enqueue through the asynchronous merge endpoint, pinned to the head SHA you
reviewed, and poll the request it returns. The queue chooses the merge method;
sending one is rejected with a 422:

```bash
curl -s "${H[@]}" -X PUT $R/pulls/<n>/merge-async -d "{\"merge_action\":\"merge_queue\",\"sha\":\"$sha\"}"
curl -s "${H[@]}" $R/pulls/<n>/merge-async/<uuid>      # status: pending, enqueued, merged, failed
```

A push that lands after enqueueing cancels the merge rather than merging
something unreviewed, and a queued PR's branch rejects pushes ("protected
branch hook declined"). Removing a PR from the queue has no REST endpoint — it
is the GraphQL `dequeuePullRequest` mutation, which sandboxed sessions cannot
reach, or the "Remove from queue" button on github.com. So finish the review
and push every change *before* enqueueing; if a queued PR must change, ask the
user to dequeue it. When Detail's fix needs a change, push the change to its
branch rather than opening a parallel PR, so the issue's paired PR is the one
that closes it.

After the batch lands, run the full set on `main` — the queue builds entries
together, but branches touching disjoint files can still break each other:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace
node --test infra/modules/edge/tests/*.test.js
make validate
```

## Step 7: Record the metric

Write the batch record to `.local/detail-triage-<YYYY-MM-DD>.md`: the
classification table, the review findings, and the decisions. `.local/` is
gitignored working memory — **read the most recent record before starting**.

Give the record a **Residue** section listing everything the review accepted and
did not fix, each with the finding it would become. The next pass matches new
findings against it (step 2).

A gap the review notices is residue only after the user chooses to leave it.
"Not a regression" or "already true before this PR" is an observation, not a
decision: a gap waved through as pre-existing comes back as the next batch's
finding. Put such a gap on the decision list, and follow what it admits to
where that artifact is consumed before sizing it.

Then extend `references/volume-log.md`: add a row to the batch table, with the
counts from `detail-stats.py` and the judgement columns defined above the table,
and append a section with the monthly row, the classes, and what was decided.
The next run reads both to tell a trend from a blip.

Every few batches, read all the records in `.local/` together and fold any
lesson that has recurred into this skill. Otherwise the records are read one at
a time, and a lesson that lives only in a record is not applied.

Success is **the `PR<=3d` share and the per-class counts falling** over
successive batches. It is not an empty issue list — while feature work is
landing in large PRs, a batch after each one is expected and says nothing
about code quality going in.
