# ADR-0037: A visitor without JavaScript queues through a small server-rendered path

**Status:** Accepted

## 1. Context

The visitor state machine runs in the browser (issue #67). `waiting.js` mints the identity,
joins, polls and redeems. With no code in the request path, the client *has* to be the state
machine. A browser that runs no script saw the waiting page and could never join, and for a
scheduled event it was silently left out of a draw its visitor believed they had entered. For the
public-sector and regulated operators this product targets, that is an accessibility and equity
failure, not a compatibility note.

`/v1/join` cannot help. It is a direct API Gateway → SQS integration and cannot render a response
(ADR-0005), and the burst path must stay compute-free.

## 2. Decision

**A separate, small `nojs` Lambda serves a form-driven queue for browsers that run no script.
It joins through the same queue and admits through the same code as the scripted path.**

- **Entry.** The waiting page's `<noscript>` block is a plain form posting to `POST /v1/enter`.
  If the script is blocked or fails to download while JavaScript is on, the script tag's
  `onerror` reveals the same form. `/v1/enter` mints the identity `waiting.js` would (a UUIDv7
  and a 32-byte possession secret, [ADR-0035](0035-request-id-proof-of-possession.md)), keeps it
  in an `HttpOnly` cookie scoped to `/v1/`, and enqueues the identical
  `{request_id, event_id, h}` message on the join queue. `assign_position` cannot tell the two
  kinds of join apart, and needs no change. A visitor who already holds an identity is not
  joined twice.
- **Waiting.** `GET /v1/wait` is a server-rendered page that refreshes itself with
  `<meta http-equiv="refresh">`: every 5 s while the join is in flight or the visitor is near
  the front, 20 s otherwise, 30 s while the event is not admitting. Each load calls
  `generate_token::admit`. When the visitor's turn comes, the same call sets the session cookie
  (IP-tagged, [ADR-0036](0036-optional-session-ip-binding.md)) and sends them on with a 303.
  `nojs` depends on `generate_token`'s library, so there is exactly one way a session is minted.
- **Destination.** A plain form cannot copy the waiting page's `next=` into its post, so
  `/v1/enter` reads it from the `Referer` (which CloudFront now forwards on these paths) and
  carries it forward in the wait page's URL. It is validated as a same-site path, rejecting `//`
  and `/\`; the same check was hardened in `waiting.js`.
- **Isolation.** This is the only join that runs compute, so the function has its own reserved
  concurrency (`nojs_reserved_concurrency`, default 5). A flood of form posts throttles here and
  cannot take capacity from `generate_token` or the burst path. The paths are uncached and are
  not gated, like `/v1/generate_token`.

## 3. Consequences

- **A visitor without JavaScript can join, wait and be admitted,** for both a live event and a
  scheduled pre-queue.
- **The page reloads rather than polls.** Each reload is one uncached Lambda call and a few
  DynamoDB reads, which is fine for the few visitors on this path but would not be for the whole
  room. Adaptive polling and `/status` collapsing (ADR-0013, ADR-0023) do not apply here.
- **Idle cost is unchanged (N1).** An unused Lambda bills nothing.
- **A failed enqueue is visible.** The visitor is told and gets the form back, and
  `nojs_join_failed` alarms on the first occurrence.
- **Joining costs an attacker a Lambda invocation instead of an SQS write.** The reserved
  concurrency bounds what that can cost the operator. It adds no new way to take more places
  than `/v1/join` already allows (#59).
