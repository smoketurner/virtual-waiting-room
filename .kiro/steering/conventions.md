# Conventions

## Rust

- Edition-current, latest stable via `rustup`. `thiserror` for library errors, `anyhow` for
  binaries. `tracing` for logging (`error!`/`warn!`/`info!`/`debug!`), never `println!`.
- Newtypes over primitives (`EventId(String)`, `RegistrationIndex(u64)`); enums for state
  machines, not boolean flags. Prefer `let…else` for early returns; keep the happy path
  unindented. Explicit destructuring over `matches!` so a field change breaks the build.
- Lints: `clippy --all-targets --all-features -- -D warnings`. Deny `unwrap_used`, `panic`,
  `todo`, `dbg_macro`, `print_stdout`/`print_stderr`, `await_holding_lock`. Fix every warning
  from every tool; if one truly can't be fixed, add an inline ignore with a justification.
- Workspace deps: in the workspace root specify version + `default-features = false` only.
  Never put `features = [...]` in `[workspace.dependencies]` — each member crate opts in via
  `workspace = true, features = ["..."]`.

## Terraform

- `validate` and `plan` are the default verification. **Never** run `terraform apply` /
  `destroy` unless explicitly asked — they create and delete real AWS resources.
- Keep the core module ≤ 80 resources (N6). Every new resource counts; justify additions.
- Pin GitHub Actions to SHA with a version comment; `persist-credentials: false`; scan with
  `zizmor` and `actionlint` before committing.

## Code quality

- **No speculative features / no premature abstraction / no phantom features.** Don't add
  flags, config, utilities, or docs for things not actively needed. Don't document or validate
  features that aren't implemented.
- **Replace, don't deprecate.** When a new implementation replaces an old one, delete the old
  one — no shims, dual formats, or migration paths. Flag dead code.
- **Clarity over cleverness.** Self-documenting code; delete commented-out code. If a comment
  explains *what* the code does, refactor instead.
- Plain, factual language in prose, commits, and PRs. A bug fix is a bug fix — avoid
  "critical", "robust", "comprehensive", "elegant".

## Git & PRs

- Imperative mood, ≤72-char subject, one logical change per commit. Never amend/rebase commits
  already pushed to shared branches. **Never push to `main`** — feature branches and PRs only.
- Never commit secrets. The per-deployment signing key lives in Secrets Manager, never in the
  repo.
- PR descriptions describe what the diff does now — not discarded approaches or alternatives.
- Install and run `prek` (pre-commit) in the repo; run `prek run` before committing.
