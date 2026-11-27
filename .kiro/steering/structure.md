# Project structure

## Repository layout

```
docs/                         Human-facing narrative (source of the SDD)
  ARCHITECTURE.md             What the deployed system does, read from the code — start here
  DESIGN.md                   How the system works, with sourced constraints (§14)
  DYNAMODB.md                 Table design, access patterns, ceilings
  REQUIREMENTS.md             Numbered requirements + acceptance criteria
  DEPLOY.md                   Deployment
  RUNBOOK.md                  Operating a live event
  LOCAL_STACK.md              make stack and make sim: running the system locally against its claims
  adr/                        Architecture decision records — why it works that way
.kiro/
  specs/virtual-waiting-room/ SDD source of truth (agent-facing)
    requirements.md           EARS-form requirements; IDs mirror docs/REQUIREMENTS.md
    design.md                 Agent-facing design summary; points into docs/DESIGN.md
    tasks.md                  Checkbox tasks tracing to requirement IDs — the build-state record
  steering/                   Persistent rules loaded into every session (this dir)
```

```
infra/                        All Terraform — kept separate from the Rust workspace
  environments/
    dev/                      The only deployable root (terraform apply runs here)
  modules/
    core/                     DynamoDB, SQS, Lambdas, IAM, regional REST API + validator,
                              the edge gate's CloudFront KeyValueStore (issue #71)
    edge/                     CloudFront cache behaviours, the S3-hosted waiting page, and the
                              admission gate CloudFront Function (functions/gate.js.tftpl)
    demo-origin/              Fixture standing in for an operator origin in the dev root
crates/                       Rust workspace — one crate per Lambda + shared lib
  wr-common/                  permutation, ids + items, expr, crypto, rules, turns, http — re-exported flat.
                              tests/vectors.rs generates the cross-language conformance vectors
                              infra/modules/edge/tests/*.conformance.test.js consume
  assign_position/            SQS consumer: pre-queue registration, position range claim + Positions writes
  open_event/                 The conditional open, after the draw closes and the join queue catches up
  read/                       /_wr/v1/status, /_wr/v1/queue_num
  generate_token/             Admission check + signed session cookie minting (issue #71)
  nojs/                       The queue without JavaScript: form join + server-rendered wait page (ADR-0037)
  controller/                 Outflow control: calls places in line and holds their spots
  admin/                      Axum operator UI and /_wr/admin/* actions, including the edge gate's
                              KeyValueStore writer (edge.rs)
  harness/                    make stack and make sim: the system run locally, checked against its claims
scripts/
examples/                     Deployable example + generated variable reference — not built
openapi/                      OpenAPI spec (N8) — not built
```

Every Lambda crate follows the same split, and new ones should: `lib.rs` is pure logic generic
over a `Store` trait, `dynamo.rs` is the SDK-backed implementation, `http.rs` (API Gateway) or
`handler.rs` (SQS) is the handler, generic over the `Store` and taking its time from the event,
and `main.rs` is runtime wiring. Time passed between functions is a `jiff::Timestamp`.

## Two document layers, kept in sync

- `docs/` is the **narrative** — prose, diagrams, sourced constraints, ADRs. Written for a
  human reader and for auditing decisions.
- `.kiro/specs/virtual-waiting-room/` is the **SDD** — EARS requirements, design, tasks.
  Written for spec-driven agent work.
- Requirement IDs (`F*`, `C*`, `N*`, `O*`) are the join key between the two layers. When a
  requirement changes, update both, keeping the ID stable.
- A requirement that no longer holds is **retired, never deleted**: move it to the
  `Retired requirements` table in both layers, naming what it required, why it went, and the ADR
  carrying the reasoning. IDs are never reused and never renumbered — a gap in the numbering is a
  record that something was dropped without one.
- ADRs are the shared rationale. Both layers link to `docs/adr/`; neither restates the "why".
- `tasks.md` is the record of what is **built**. An item only partly delivered stays unchecked
  and carries a `Partial:` note saying what is missing. Neither design layer should describe an
  unbuilt mechanism as present.

## The four DynamoDB tables

| Table | PK | Holds |
|---|---|---|
| `Counters` | `event_id` | The event item `EVT#{id}`, its striped shard items `EVT#{id}#PQ#{n}` and `EVT#{id}#DF#{n}`, 26 shelves of arrivals counters `EVT#{id}#AR#{shelf}#{n}`, and the admission log `EVT#{id}#LOG` |
| `PreQueue` | `r` (request_id) | Shard `s` + local index `l` (+ time `t`); global index `i=offset[s]+l` assembled at T−0; never scanned by the system |
| `Positions` | `request_id` | Written at join for a live joiner, at admission for a pre-queue member; reclaimed by DynamoDB TTL |
| `Tokens` | `request_id` | Two tagged kinds: operator OIDC sessions `SESS#`, pending PKCE logins `PKCE#`. The key is named `request_id` though it holds session ids and PKCE states; renaming a hash key replaces the table |

Keys are tagged only where a table holds more than one kind of item; `Positions` and `PreQueue`
take bare ids because a tag there disambiguates nothing and costs bytes in every row. Every key
is built by `wr_common::expr`, never at the call site, and `event_id` may not contain `#`.
