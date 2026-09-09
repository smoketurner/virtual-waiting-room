# ADR-0018: Style the admin UI in the Vouch design language

**Status:** Accepted (supersedes the styling of ADR-0014)

## Context

ADR-0014 styled the admin dashboard with **Cloudscape design tokens** — the AWS
console visual language — hand-authored over server-rendered HTML. In practice it
read as flat white boxes on white: generic, low-hierarchy, and wrong for a
real-time operator console. It also had no design relationship to the product the
operator authenticates against.

The operator authenticates through **Vouch** (ADR-0016), whose own surfaces
(`vouch.sh`, `us.vouch.sh`) are a clean, functional **dark black-and-green**
design. Matching it gives the admin plane a coherent identity with the auth
provider and a denser, more glanceable console appropriate for incident use.

## Decision

Replace the Cloudscape design tokens with the **Vouch design language**. The
askama, server-rendered, React-free architecture of ADR-0014 is unchanged — only
the visual system changes.

- **Palette** (from vouch.sh): near-black surfaces `#0a0a0a / #141414 / #1e1e1e`,
  green accent `#3ecf8e` (hover `#5edba5`), text `#e5e5e5 / #888`, borders
  `#2a2a2a / #333`; danger `#ff5f57`, warn `#febc2e` for status.
- **Type**: Inter for UI, JetBrains Mono for metrics/identifiers — using the
  system `ui-sans-serif` / `ui-monospace` fallbacks Vouch itself declares, so **no
  font files are bundled** and the strict CSP (ADR: no external `font-src`) is
  untouched. Accepted tradeoff: on a machine without Inter installed the UI falls
  back to the system sans, a negligible visual difference against the palette.
- **Layout**: a top bar, a content header, KPI stat tiles (mono numerals), status
  pills, a responsive card grid, and a visually dominant red-bordered **Emergency**
  card for the andon cord (ADR-0017).
- The vendored Cloudscape `tokens.css` and its `extract_tokens.py` build step are
  **deleted** (conventions.md: replace, don't deprecate). The stylesheet is now
  self-contained.

## Consequences

- The console looks like Vouch — coherent with the auth provider, and a genuine
  dark ops aesthetic rather than a generic AWS-console pastiche.
- One fewer build step and one fewer vendored asset: no token extraction, no
  `tokens.css`. The admin Lambda embeds a single self-contained stylesheet.
- **Loses the Cloudscape visual identity** ADR-0014 chose. That identity was the
  point of 0014's styling; it is explicitly abandoned here because it did not serve
  the console. The architecture that 0014 also decided (askama, no React, no
  bundler, JS-optional) is retained.
- Fonts are system-stack, not the exact Inter/JetBrains Mono webfonts Vouch serves;
  bundling the woff2 files (and widening `font-src`/adding a `/static/fonts` route)
  is a later option if pixel-matching the type becomes worth the assets.
- Additive to ADR-0016/0017 (auth, andon cord); supersedes only the styling half of
  ADR-0014.
