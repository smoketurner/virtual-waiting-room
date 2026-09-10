# Conventions

## Rust

**Runtime:** latest stable via `rustup`. Edition 2024, `resolver = "3"`.

| purpose | tool |
|---|---|
| build & deps | `cargo` |
| lint | `cargo clippy --all-targets --all-features -- -D warnings` |
| format | `cargo fmt` |
| test | `cargo test` |
| supply chain | `cargo deny check` (advisories, licenses, bans) |
| safety check | `cargo careful test` (stdlib debug assertions + UB checks) |

### Style

- Prefer `for` loops with mutable accumulators over long iterator chains.
- Shadow variables through transformations (no `raw_x`/`parsed_x` prefixes).
- Prefer patterns that break on type changes: no `_` arms, exhaustive `match` over `matches!`,
  explicit fields over `..`.
- `let…else` for early returns; keep the happy path unindented.

### Type design

- Newtypes over primitives (`EventId(String)`, `RegistrationIndex(u64)`), not bare `u64`.
- Enums for state machines, not boolean flags.
- `thiserror` for libraries, `anyhow` for binaries.
- `tracing` for logging (`error!`/`warn!`/`info!`/`debug!`), never `println!`.

### Performance

Write efficient code by default: correct algorithm, appropriate data structures, no
unnecessary allocations. Profile before micro-optimizing, and measure after.

### Dependency pinning (non-negotiable)

- **Exact version pins only.** Every dependency uses `= "=x.y.z"` — no `^`, `~`, or ranges.
- **`default-features = false` on everything** in `[workspace.dependencies]`.
- **No `features = [...]` at the workspace root.** Each member crate opts into exactly the
  features it uses via `dep = { workspace = true, features = ["..."] }`. Enabling a feature at
  the root forces it on every crate and is forbidden.
- A new dependency needs a stated reason (see `tech.md`); the crate list is deliberately small.

### Lints — panic-free by construction

`[workspace.lints.clippy]` is inherited by every crate (`[lints] workspace = true`). Run
`clippy --all-targets --all-features -- -D warnings`. The denylist (matching the reference
`Cargo.toml`):

```toml
[workspace.lints.clippy]
pedantic = { level = "warn", priority = -1 }
# Panic prevention
unwrap_used = "deny"
expect_used = "warn"
panic = "deny"
panic_in_result_fn = "deny"
unimplemented = "deny"
# No cheating
allow_attributes = "deny"
# Code hygiene
dbg_macro = "deny"
todo = "deny"
print_stdout = "deny"
print_stderr = "deny"
# Safety
await_holding_lock = "deny"
large_futures = "deny"
exit = "deny"
mem_forget = "deny"
# Pedantic relaxations (too noisy)
module_name_repetitions = "allow"
similar_names = "allow"
```

Fix every warning from every tool. If one truly can't be fixed, `allow_attributes = "deny"`
means you cannot silently `#[allow]` it — justify it or remove the cause.

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
- Never commit secrets. The per-deployment signing keys live in SSM Parameter Store
  SecureStrings, never in the repo. Note the exception recorded in ADR-0020: the CloudFront
  signing key pair is generated at apply time and therefore lives in Terraform state, which a
  deployment whose threat model excludes state must override by supplying the pair out of band.
- PR descriptions describe what the diff does now — not discarded approaches or alternatives.
- Install and run `prek` (pre-commit) in the repo; run `prek run` before committing.
