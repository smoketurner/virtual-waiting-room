# ADR-0004: Write positions with `PutItem`, not `BatchWriteItem`

**Status:** Accepted

## Context

Position writes on the live-join path occur one per visitor. `BatchWriteItem` is the
conventional choice for bulk writes.

## Decision

Use `PutItem` with `ConditionExpression: attribute_not_exists(request_id)`.

## Consequences

| Constraint | `BatchWriteItem` |
|---|---|
| Items per call | 25 |
| Conditional expressions | **not supported** |
| Write capacity | billed per item, no saving over `PutItem` |
| Partial failure | returns `UnprocessedItems`; caller retries with backoff |

From the API reference: "you cannot specify conditions on individual put and delete
requests."

Requirement F2.5 states that a repeated write for the same `request_id` must not consume a
second position — a conditional write. `BatchWriteItem` cannot express it and saves no
capacity; it reduces only HTTP request count. SQS standard queues are at-least-once, so
duplicate delivery is expected and the condition is the mechanism that absorbs it.
