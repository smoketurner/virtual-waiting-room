# ADR-0012: Ship the anti-DDoS rule group in Count mode

**Status:** Accepted

## Context

`AWSManagedRulesAntiDDoSRuleSet` detects and challenges requests suspected of participating
in layer-7 attacks. It establishes a traffic baseline before acting.

## Decision

Deploy it in Count mode by default. Promotion to Block is a per-client decision after
observing at least one real event.

## Consequences

- A waiting room's normal state is zero traffic and its legitimate peak is shaped exactly
  like a volumetric attack, so the baseline is pathological for this workload.
- AWS documents that baselines established during an attack take two to three times longer
  to settle.
- Running in Block for a first on-sale risks challenging legitimate visitors at the moment
  the system is most visible.
- The COUNT-then-BLOCK promotion discipline is part of the operator runbook.
