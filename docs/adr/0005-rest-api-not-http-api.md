# ADR-0005: Use a REST API, not an HTTP API

**Status:** Accepted

## Context

API Gateway HTTP APIs cost $1.00 per million requests against REST's $3.50, and both support
a direct SQS integration.

## Decision

Use a regional REST API.

## Consequences

- API Gateway sees only CloudFront cache misses — approximately three per visitor regardless
  of wait duration. A 1,000,000-visitor event generates ~3M billable requests, so the saving
  from an HTTP API is **$7.50 per event**.
- HTTP APIs do not support request validators, API keys, or VTL response mapping. Losing
  synchronous request validation at the gateway is not worth $7.50.
- Regional rather than edge-optimized, because CloudFront already fronts the API and an
  edge-optimized endpoint would stack a second CDN.
