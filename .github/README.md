# Repository automation

This directory follows the miju repository's collaboration workflow, adapted for
the single `miju-services` crate. Adding these files does not configure hosted
repository settings or prove that GitHub-hosted jobs have run.

## Pull requests and maintenance

- `CODEOWNERS` assigns review ownership to `@RivTian`.
- Dependabot checks Cargo and GitHub Actions weekly.
- The issue forms request reproduction details or a concrete use case. The PR
  template asks for the behavior change and relevant validation.
- `ci.yaml` follows miju with separate cargo-check, cargo-test, cargo-fmt,
  cargo-clippy, cargo-doc and cargo-udeps jobs. Direct Cargo steps retain the
  Linux/macOS/Windows default, minimal and all-feature matrix and Rust 1.89.
  Ordinary checks use `actions-rust-lang/setup-rust-toolchain`; docs and udeps
  use nightly as in miju. Windows remains
  configured for subsequent hosted validation. The separate original Windows
  x64 run passed functional checks; resource-trend v2 remains on hold.
- `pr-checks.yaml` prepares same-repository PRs, then validates the live title and
  changelog. Titles contain the PR's own `#N`. Dependabot titles and changelog
  entries are exempt; maintainers can use `skip-changelog` to waive the entry check.
  This label does not disable same-repository automatic preparation.
- `update-changelog.yaml` appends or updates one entry per PR in `CHANGELOG.md`.
  Repeated runs are idempotent. Release PR titles `bump: to vX.Y.Z #N` require an
  existing `## [X.Y.Z]` heading and place the entry there.
- Fork PRs receive read-only checks and must update their title/changelog manually.
  No fork code runs with changelog credentials. Workflows use `pull_request`, not
  `pull_request_target`.

Same-repository preparation uses `GITHUB_TOKEN` for PR metadata. If the title
already contains the PR's own `#N` and `CHANGELOG.md` already has the corresponding
entry, preparation passes without another credential.

Automatic source commits require the optional repository secret `CHANGELOG_TOKEN`:
a fine-grained token restricted to this repository, with **Contents: read and
write** and **Pull requests: read and write**. Unlike a `GITHUB_TOKEN` source push,
this token triggers checks for the new PR head. Without it, preparation reports
which changelog update must be committed manually and does not push a new head
whose checks would be suppressed. The bot author remains `github-actions[bot]`.

CI and PR checks run for any PR base, so stacked feature PRs receive the same
checks as a PR targeting `main`. Each PR includes its own changelog entry. Review
and merge dependencies in order; after a squash merge, retarget and update the
next branch before merging it into `main`.

## Dependency checks

`audit.yaml` runs cargo-audit, cargo-deny and cargo-vet for every PR, on main-branch
dependency/configuration changes, daily, on manual dispatch, and as a reusable
release prerequisite. PR runs deliberately have no path filter, so required audit
checks also run for changelog-only commits.
`deny.toml` defines allowed licenses and registries; `.cargo/audit.toml` enables
unmaintained/yanked reporting without inheriting miju's unrelated exceptions.

The vet store imports public audits from Bytecode Alliance, Google, Mozilla and
Zcash. Version-specific exemptions are explicitly **unaudited bootstrap baselines**,
not local security certifications. Dependency updates must resolve new vet findings
and document any exemption; never turn a failing check into an invented audit.

## Releases

Pushing a `vX.Y.Z` tag calls CI and dependency audit first. `cd.yaml` checks that
this is the sole `miju-services` package, the tag equals its Cargo version, and its
dependencies come from crates.io. It then publishes that one package using the
repository secret `CARGO_REGISTRY_TOKEN`, scoped to publish `miju-services`.
Registry errors fail; a rerun skips an already-indexed, non-yanked exact version.
A yanked version is rejected.

Configure the publish token before a release and the optional changelog token
before enabling automatic source updates. The workflow jobs request their needed
write permissions; the repository default remains read-only. Branch protection
requires `pr-title` and `pr-content`. Neither secret values nor repository settings
are stored here. The
miju-specific external content denylist and multi-crate release order are not
requirements of this separate repository.
