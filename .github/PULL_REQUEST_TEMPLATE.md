## Summary
<!-- Why this change exists. 1–3 bullets. -->
-

## Type of change
- [ ] Feature
- [ ] Bug fix
- [ ] Security
- [ ] Docs / tooling
- [ ] Deploy / infra

## Test plan
- [ ] `make review` (platform-ops) clean enough to merge
- [ ] Local `make build` / `make run`
- [ ] Security: gitleaks + bandit + pip-audit checked
- [ ] Manual verification notes below

## Security checklist
- [ ] No secrets in diff (`.env`, keys, tokens)
- [ ] AuthZ / input validation considered
- [ ] Dependencies audited if lockfile changed

## Deploy notes (if any)
- Environment:
- Traffic shift plan:
- Rollback:

## Graph / review notes
<!-- Filled automatically when using `make pr` after `make review` -->
