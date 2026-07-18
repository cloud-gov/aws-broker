# Pre-commit hooks (aws-broker)

> Added on the Oracle-19c branch (epic [#519](https://github.com/cloud-gov/aws-broker/issues/519))
> so the cloud.gov scanners validate our changes. This is repo-wide dev tooling,
> not Oracle-specific.

The repo has a `.pre-commit-config.yaml` consuming the internal
[`cloud-gov/pre-commit-templates`](https://github.com/cloud-gov/pre-commit-templates)
(pinned `rev: v0.5.3`) plus a few local Go hooks.

## Running

**On a cloud.gov dev host with [caulking](https://github.com/cloud-gov/caulking)
installed (most of us):**

- **Do NOT run `pre-commit install`.** Caulking sets `core.hooksPath` globally and
  the pre-commit framework will refuse (`cowardly refusing to install hooks with
  core.hooksPath set`). Caulking's `prek` invokes `.pre-commit-config.yaml`
  automatically on every commit.
- Run manually any time: `pre-commit run --all-files` (or scope to staged files:
  `pre-commit run --files $(git diff --cached --name-only)`).

**Without caulking (CI / a fresh box):** `pre-commit install` then commit normally.

## What runs

| Hook | Notes |
|------|-------|
| hygiene (merge-conflict, large-files, trailing-whitespace, end-of-file, check-yaml, check-json) | `catalog-template.yml` excluded from check-yaml (spruce `(( ... ))` operators) |
| `shellcheck`, `shfmt-check` | the many `ci/*.sh` + `local/scripts/*.sh` scripts |
| `check-gsa-email` | commit-author compliance |
| `go-fmt-changed`, `go-vet` | whole module; near-zero false positives |
| `golangci-lint-oracle` (`scripts/lint-oracle-go.sh`) | **scoped to the Oracle-19c files this branch authored** — see below |

## Why golangci-lint is scoped (not whole-repo)

aws-broker had **no prior lint gate** (CI was `go test ./...` only) and carries
~53 pre-existing golangci-lint findings (errcheck/ineffassign/unused/staticcheck)
in code this branch did not write. Turning on a whole-repo gate would block every
commit on inherited debt, which trains people to `--no-verify`. So
`scripts/lint-oracle-go.sh` runs the full linter suite but fails only on findings
in the files this branch authored (`engine.go`, `engine_baselines.go`,
`baselines.go`, and their tests). Those must stay clean.

**Follow-up (tracked):** burn down the inherited findings, then widen the gate to
`./...`. Until then, whole-repo debt is visible via `golangci-lint run ./...` but
not gated.

## Tools

Installed via Homebrew + `go install` on dev hosts:
`golangci-lint`, `hadolint`, `shellcheck`, `shfmt`, `yamllint` (brew);
`gosec`, `govulncheck` (`go install`). gitleaks is **not** a hook here — caulking
runs it globally.
