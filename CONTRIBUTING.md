# Contributing

## Workflow
1. `make new-branch NAME=feature/short-name` (from `platform-ops`, with `PROJECT_DIR` set)
2. Implement + add/update tests
3. `make review` — lint, security, graphify
4. Commit with a clear why-focused message
5. `make pr TITLE="…"` — opens PR with markdown template
6. After merge: `make tag VERSION=vX.Y.Z` on `main`

## Code standards
See `AGENTS.md`. Prefer small PRs, explicit error handling, no secrets in git.

## Review bar
- CI green
- Security scans addressed or justified
- PR description has test plan
