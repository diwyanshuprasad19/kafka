#!/usr/bin/env bash
# Thin wrappers — same as platform-ops make gcp-* (kept in kafka for discoverability).
# Prefer: make -C ../platform-ops gcp-pause|gcp-resume|gcp-status
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../platform-ops" && pwd)"
CMD="${1:?usage: $0 pause|resume|status}"
case "$CMD" in
  pause)  exec "$ROOT/scripts/gcp_pause.sh" ;;
  resume) exec "$ROOT/scripts/gcp_resume.sh" ;;
  status) exec "$ROOT/scripts/gcp_status.sh" ;;
  *) echo "usage: $0 pause|resume|status" >&2; exit 1 ;;
esac
