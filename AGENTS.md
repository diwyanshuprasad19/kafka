# Agent / AI coding guide

Use this file so code written here (Cursor or Claude Code) stays consistent and **human**, not AI-slop.

## Principles
- Match existing project structure and naming; do not invent parallel frameworks.
- Prefer small, reviewable diffs; one concern per PR.
- Never commit secrets (`.env`, keys, tokens). Use examples only.
- Add or update tests when changing behavior.
- Document operator-facing changes in README or PR body.

## Anti-slop (enforced by `make anti-slop` / `make auto`)
- No narrative comments that restate what the next line does.
- No `# === Helpers ===` banner comments or apologetic TODOs.
- No bare `except:` / empty `except Exception: pass`.
- No placeholder `pass  # implement later` left in merged code.
- Prefer clear names over generic `data`, `result`, `temp`, `helper2`.
- Run `ruff format` / `ruff check --fix` before finishing.
- Tools: **aislop**, **sloplint**, **agent-slop-lint**, **ruff**, optional **Strix**.

## Before finishing a task
1. `make anti-slop REPO=<kafka|redis|coding>`
2. `make security REPO=…`
3. Summarize risk (auth, data, deploy) in the PR.
4. If deploy-related: note traffic shift + rollback.

## Stack awareness (kafka)
- Kafka consumers: commit offsets **after** DB success; rely on idempotency.
- Prod uses SA impersonation / WIF — never bake JSON keys into the repo.
- Observability: JSON logs + Prometheus metrics; correlation ids on requests.

## Data locally
- Shared Postgres/Redis: `make data-up` in platform-ops (5433 / 6380).
- Migrations: `make alembic REPO=kafka` or `REPO=redis`.
