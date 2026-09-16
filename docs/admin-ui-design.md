# Admin interface — design note (Stage 1)

> **Superseded on two points.** Auth is no longer SigV4: ADR-0016 replaced it with an OIDC
> Authorization Code + PKCE login session stored in the `Tokens` table, enforced in the admin
> Lambda (API Gateway auth is `NONE`). Styling is no longer Cloudscape design tokens: ADR-0018
> replaced them with the self-contained Vouch stylesheet. The action set, route→handler→state
> mapping and crate shape below still describe what was built.

Scope lock for the operator control plane: a single Axum Rust Lambda rendering
server-side HTML, behind the SigV4 (`AWS_IAM`) admin routes already scaffolded
in `infra/modules/core/api.tf`. This note fixes the action set, the
route→handler→state mapping, the crate shape, and the auth model before any
code (Stages 2–5 implement it).

## Crate shape

New workspace member `crates/admin`:

- **bin `bootstrap`** — `lambda_http` runtime hosting an **`axum`** `Router`
  over the admin routes; `tower`/`tower-http` for middleware.
- **lib** — pure handler logic over a `Store` trait seam (fakes in tests,
  `DynamoStore` real impl), plus **`askama`** compile-time templates. Same
  test-without-AWS pattern as the other crates.
- Deps (per `tech.md` Option A, already sanctioned): `axum`, `tower`,
  `tower-http`, `askama`, `lambda_http`, `aws-config`, `aws-sdk-dynamodb`,
  `serde`, `serde_json`, `tokio`, `thiserror`/`anyhow`, `tracing`. Exact-pinned,
  `default-features = false`, features in the member crate only.

## Auth model

- Every admin route is `authorization = "AWS_IAM"` in API Gateway — the caller
  signs requests with **SigV4**; API Gateway rejects an unsigned/badly-signed
  request with **403 before the Lambda runs**. The handler does not re-implement
  auth; it trusts that reaching the Lambda means the request was IAM-authorized.
- The UI is a thin server-rendered client over the same logic (F5.5 API-first);
  it adds no capability the signed API lacks.

## Routes → handler → state

All admin actions are one **conditional `UpdateItem`** on the single `Counters`
item (`event_id` PK), mirroring how `open_event` writes. Attribute names come
from `wr-domain::Counters` / `Phase`.

| Route | Method | Handler | Writes on `Counters` |
|---|---|---|---|
| `/admin` | GET | Render dashboard: current phase, serving/queue counters, N, message. Read the `Counters` item, render with askama. | — (read only) |
| `/admin/phase` | POST | Transition phase (form field `phase` ∈ idle/pre_queue/active/post_event). One conditional `UpdateItem`. Refuses `active` for an event the open has never run for. | `phase` |
| `/admin/open_now` | POST | Open the event immediately instead of waiting for the scheduled start: invokes the `open_event` function the schedule invokes. | `shuffle_seed`, prefix offsets, `participant_count`, `phase = active` (written by `open_event`); audit fields here |
| `/admin/rate` | POST | Set the admission target rate the outflow controller reads (form `rate`). | `target_rate` (new attribute; controller is future work but the knob is set here) |
| `/admin/message` | POST | Set the operator broadcast message shown on phase pages / `/status` (form `message`). | `message` |
| `/admin/reset` | POST | Force **maintenance** phase (the override that suppresses standby alarms), the safe operator stop. | `phase = maintenance` |
| `/admin/rules` | POST | Replace the edge gate's whole ruleset (path/header/cookie/user-agent match), written to the CloudFront KeyValueStore. | KeyValueStore `rules`; audited on `Counters` |

### MVP action set (implemented in Stages 3–5)

`phase`, `rate`, `message`, `reset` (maintenance override), `pause`/`resume`,
`fail_open`/`recover`, `start_time`, `open_now`, `rules`, and the read-only
`/admin` render.
Nothing is wired as a route that answers "deferred": a control the operator
cannot use is left out rather than shown as a dead panel.

## Phase transition rules

Legal transitions follow the lifecycle (`design.md` §3):
`idle → pre_queue → active → post_event`, and `maintenance` reachable from any
phase (operator-forced). The handler validates the requested target against the
current phase and rejects an illegal jump with a 4xx rather than writing it.
The write is a conditional `UpdateItem` (guard on the current phase) so two
concurrent operators cannot race a transition.

**`active` is the one phase the operator cannot set.** It is not a phase change
at all: `open_event` writes the permutation seed, the ten prefix offsets, the
cohort size and `phase = active` in one conditional update guarded by
`attribute_not_exists(shuffle_seed)`. A phase set to `active` on its own is an
event that reports itself open while `/queue_num` answers "not yet open" to
every pre-queue registrant, because nothing has written the seed their position
is derived from. So the dropdown offers nothing from `pre_queue` — the schedule
opens the event, or the operator presses **Open now** — and `/admin/phase`
refuses `active` outright for an event with no `participant_count`, which also
covers the recovery path out of `maintenance` for an event that was stopped
before it ever opened.

## UI / styling (Stage 2)

Cloudscape **design tokens only** — no React, no component library, no bundler
in the request path. Token *values* extracted at build time from the blessed
`index-visual-refresh.json` into a vendored `:root { --color-…: … }` stylesheet.
Interactivity is plain HTML `<form>` POSTs to the same `/admin/*` actions; a
tiny vanilla-JS metrics poller is additive. **Core actions work with JS
disabled** (verified in Stage 5).

## Out of scope (explicitly, this build)

Outflow controller loop, `generate_token` admission,
standby alarms, protection-rule evaluation. The admin plane sets the knobs
(`target_rate`, `phase`, `message`) those systems will later read; it does not
implement them.
