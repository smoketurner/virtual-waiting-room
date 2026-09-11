# Project structure

## Repository layout

```
docs/                         Human-facing narrative (source of the SDD)
  REQUIREMENTS.md             Numbered requirements + acceptance criteria
  DESIGN.md                   How the system works, with sourced constraints (§14)
  PLAN.md                     Phased implementation plan
  DEPLOY.md                   Deployment and operations
  adr/                        Architecture decision records — why it works that way
.kiro/
  specs/virtual-waiting-room/ SDD source of truth (agent-facing)
    requirements.md           EARS-form requirements; IDs mirror docs/REQUIREMENTS.md
    design.md                 Design mirroring docs/DESIGN.md; §4.3 expanded; admin-UI section
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
    authorizer/               Origin authorizer + optional CloudFront VPC origin
    demo-origin/              Fixture standing in for an operator origin in the dev root
crates/                       Rust workspace — one crate per Lambda + shared lib
  wr-common/                  permutation, ids + items, expr, crypto, rules — re-exported flat.
                              tests/vectors.rs generates the cross-language conformance vectors
                              infra/modules/edge/tests/*.conformance.test.js consume
  assign_position/            SQS consumer: position range claim + Positions writes
  seal_event/                 T−0 conditional seal
  read/                       /v1/status, /v1/queue_num
  generate_token/             Admission check + signed session cookie minting (issue #71)
  controller/                 Outflow control and position expiry
  admin/                      Axum operator UI and /admin/* actions, including the edge gate's
                              KeyValueStore writer (edge.rs)
  authorizer/                 The alternative origin gate
scripts/
  bootstrap_edge_gate.py       Writes the signing secret to SSM and the edge gate's
                              KeyValueStore in one run (issue #71)
examples/                     Deployable example + generated variable reference — not built
openapi/                      OpenAPI spec (N8) — not built
```

Every Lambda crate follows the same three-file split, and new ones should: `lib.rs` is pure
logic generic over a `Store` trait, `dynamo.rs` is the SDK-backed implementation, `main.rs` is
runtime wiring.

## Two document layers, kept in sync

- `docs/` is the **narrative** — prose, diagrams, sourced constraints, ADRs. Written for a
  human reader and for auditing decisions.
- `.kiro/specs/virtual-waiting-room/` is the **SDD** — EARS requirements, design, tasks.
  Written for spec-driven agent work.
- Requirement IDs (`F*`, `C*`, `N*`, `O*`) are the join key between the two layers. When a
  requirement changes, update both, keeping the ID stable.
- ADRs are the shared rationale. Both layers link to `docs/adr/`; neither restates the "why".
- `tasks.md` is the record of what is **built**. An item only partly delivered stays unchecked
  and carries a `Partial:` note saying what is missing. Neither design layer should describe an
  unbuilt mechanism as present.

## The four DynamoDB tables

| Table | PK | Holds |
|---|---|---|
| `Counters` | `event_id` | The event item `EVT#{id}`, plus its striped shard items `EVT#{id}#PQ#{n}` and `EVT#{id}#AR#{n}` |
| `PreQueue` | `r` (request_id) | Shard `s` + local index `l` (+ time `t`); global index `i=offset[s]+l` assembled at T−0; scanned only at audit |
| `Positions` | `request_id` | Written lazily at admission; carries expiry for the controller |
| `Tokens` | `request_id` | Three tagged kinds: admission-token reservations `TKN#`, operator OIDC sessions `SESS#`, pending PKCE logins `PKCE#` |

Keys are tagged only where a table holds more than one kind of item; `Positions` and `PreQueue`
take bare ids because a tag there disambiguates nothing and costs bytes in every row. Every key
is built by `wr_common::expr`, never at the call site, and `event_id` may not contain `#`.
