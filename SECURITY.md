# Security Policy

## Reporting
Email or open a **private** security advisory on GitHub. Do not file public issues for exploitable vulns.

## Supported
Active `main` branch of this repository.

## Local expectations
Before opening a PR, run from `platform-ops`:

```bash
make security PROJECT_DIR=../<this-repo>
```

That runs gitleaks, bandit, pip-audit, semgrep, and trivy.
