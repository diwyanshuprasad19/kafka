# Human-code / anti-slop rules for platform-ops PRs

When asked to **raise a PR** on `kafka`, `redis`, or `coding`, always run through platform-ops:

```bash
cd platform-ops
make auto REPO=<name> TITLE="…"
```

That pipeline **must** pass:
1. **anti-slop** — aislop + sloplint + ruff (+ optional Strix)
2. **security** — gitleaks, bandit, pip-audit, semgrep, trivy
3. **graphify** — impact graph
4. Then push + PR + review comment

Do not open a raw `gh pr create` that skips these gates unless the user says `ALLOW_SLOP=1`.
