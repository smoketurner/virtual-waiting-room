# Project structure

## Repository layout

```
docs/                         Human-facing narrative (source of the SDD)
  REQUIREMENTS.md             Numbered requirements + acceptance criteria
  DESIGN.md                   How the system works, with sourced constraints (§14)
  PLAN.md                     Phased implementation plan
  adr/                        Architecture decision records — why it works that way
.kiro/
  specs/virtual-waiting-room/ SDD source of truth (agent-facing)
    requirements.md           EARS-form requirements; IDs mirror docs/REQUIREMENTS.md
    design.md                 Design mirroring docs/DESIGN.md; §4.3 expanded; admin-UI section
    tasks.md                  Checkbox tasks tracing to requirement IDs
  steering/                   Persistent rules loaded into every session (this dir)
```

Code layout (`infra/` created; `crates/` Phase 1+):

```
infra/                        All Terraform — kept separate from the Rust workspace
  environments/
    dev/                      The only deployable root (terraform apply runs here)
  modules/
    core/                     DynamoDB, SQS, Lambdas, IAM, regional REST API + validator
    edge/                     CloudFront (3 cache behaviours), WAF
    authorizer/               Origin authorizer + optional CloudFront VPC origin
crates/                       Rust workspace — one crate per Lambda + shared lib
  <crate>/src/                e.g. assign_position, authorizer, controller, admin
examples/                     Deployable example + generated variable reference
openapi/                      OpenAPI spec; public + admin surfaces generated from it
```

## Two document layers, kept in sync

- `docs/` is the **narrative** — prose, diagrams, sourced constraints, ADRs. Written for a
  human reader and for auditing decisions.
- `.kiro/specs/virtual-waiting-room/` is the **SDD** — EARS requirements, design, tasks.
  Written for spec-driven agent work.
- Requirement IDs (`F*`, `C*`, `N*`, `O*`) are the join key between the two layers. When a
  requirement changes, update both, keeping the ID stable.
- ADRs are the shared rationale. Both layers link to `docs/adr/`; neither restates the "why".

## The four DynamoDB tables

| Table | PK | Holds |
|---|---|---|
| `Counters` | `event_id` | One item per event: all sequences, phase, seed, rate, message |
| `PreQueue` | `r` (request_id) | Shard `s` + local index `l` (+ time `t`); global index `i=offset[s]+l` assembled at T−0; scanned only at audit |
| `Positions` | `request_id` | Written lazily at admission; carries expiry for the controller |
| `Tokens` | `request_id` | Admission-token metadata and session status |
