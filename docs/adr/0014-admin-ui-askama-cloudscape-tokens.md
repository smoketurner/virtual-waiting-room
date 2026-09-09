# ADR-0014: Render the admin UI with askama and Cloudscape design tokens

**Status:** Accepted — the askama, server-rendered, React-free architecture stands.
The **Cloudscape design-token** styling is **superseded by ADR-0018** (the Vouch
design language). Everything below about *how* the UI is rendered still holds; only
the visual token source changed.

## Context

The operator surface is API-first: a SigV4-authenticated admin REST API and a `/metrics`
JSON endpoint (F5.5). Operators also want a browser dashboard they can drive during an event
without wiring up client tooling. The dashboard should feel like a native Amazon Web Services
(AWS) service.

[Cloudscape](https://cloudscape.design/) is AWS's open-source design system and the obvious
way to get that look. But two facts constrain how it can be used here:

- **Cloudscape's component library is React-only.** There is no first-party server-rendered
  HTML component distribution — the components are shipped as React.
- **The design-tokens package presupposes the components.** `@cloudscape-design/design-tokens`
  ships Sass and JavaScript variables and is documented to "only be used together with the
  components package." It is not a runtime-consumable plain-CSS artifact.

Adopting the React components would add a JavaScript build, a bundle, and a single-page
application (SPA) runtime to a system whose entire premise is near-zero idle cost (N1) and a
deployment small enough to read in one sitting (N6). Every other function in the system is a
Rust Lambda.

Three options were considered:

- **A — askama + Cloudscape design tokens, no React.** Server-render semantic HTML from a Rust
  Lambda; style it with Cloudscape token *values* extracted at build time.
- **B — askama shell + Cloudscape React islands.** Server-render the shell, hydrate interactive
  parts with real Cloudscape components. Adds a JS build and bundle.
- **C — Axum Lambda serving a prebuilt Cloudscape React SPA + JSON API.** The Lambda is a thin
  static/API server; the UI is a React SPA.

## Decision

Option A. The operator dashboard is a **single Axum-based Rust Lambda** (`arm64`,
`provided.al2023`) that renders server-side HTML with **askama** compile-time templates,
styled with the Cloudscape **design language** — its tokens, not its components.

- **No runtime dependency on `@cloudscape-design/design-tokens`.** At build time, extract the
  token *values* from Cloudscape's blessed JSON artifact `index-visual-refresh.json` (which
  Cloudscape explicitly supports processing "to suit your development stack" via
  `style-dictionary`) into a plain Cascading Style Sheets (CSS) custom-properties stylesheet
  (`:root { --color-…: … }`) vendored into the Lambda. The runtime stays React-free and
  dependency-free.
- **Interactivity is HTML-first.** Plain `<form>` elements POST to the same `/admin/*` actions
  the REST API already exposes; a small vanilla-JavaScript poller refreshes metrics within the
  60-second freshness window. No React, no bundler in the request path. Core actions work with
  JavaScript disabled.
- **Same auth, same logic.** The Lambda uses the existing SigV4 admin authentication and reuses
  the existing admin handlers. The UI is a thin server-rendered client that adds no capability
  the API lacks, so F5.5 (API-first) still holds.

## Consequences

- One React-free Rust Lambda fits N1 (idle cost — no standing compute or cache tier) and N6
  (deployment size — no `node_modules`, no bundler, no SPA runtime to audit).
- **Tradeoff:** we hand-author the markup that Cloudscape-React would provide as components
  (tables, forms, containers). This is manual discipline against Cloudscape conventions rather
  than free component reuse. Accepted in exchange for a dependency-free, self-contained
  operator UI on the same stack as the rest of the system.
- The build gains one step: regenerate the token CSS from `index-visual-refresh.json` when the
  pinned Cloudscape version changes. Cloudscape token *values* may change between versions;
  their *keys* are stable, so regeneration is a value refresh, not a rewrite.
- The visual identity tracks Cloudscape by construction, but AWS console interaction patterns
  (top navigation, side navigation, status indicators) must be reproduced by hand and can
  drift from the current console over time.
- Options B and C remain available later if component fidelity becomes worth a JavaScript
  toolchain; this decision does not preclude them, but reversing it means introducing the
  build and runtime that A was chosen to avoid.

Satisfies F7.1–F7.6; additive to F5.5.
