# ADR-0007: Deploy single-tenant into the client's account

**Status:** Accepted

## Context

A hosted multi-tenant service is the conventional model for this category and is what the
established competitor operates.

## Decision

Deploy into the client's own AWS account. Operate no shared infrastructure.

## Consequences

- Hosting other organizations' waiting rooms would make us a Cloud Service Provider requiring
  our own FedRAMP authorization: $250K–$2M initial, 6–24 months, approximately $500K/year
  continuous monitoring, plus per-change assessment. Not viable at our scale.
- The client inherits AWS's existing authorization under their own ATO; we are a systems
  integrator writing Terraform.
- Blast radius is one client. The reusable asset is the module, not a running service.
- Suits organizations that cannot route traffic through a third party — public sector,
  regulated industries.
- CloudFront SaaS Manager is tooling for the multi-tenant architecture this rejects. It fits
  one narrow case, a single client running many branded domains, as a later variant.
- We cannot offer a managed SLA, because we do not operate the infrastructure.
