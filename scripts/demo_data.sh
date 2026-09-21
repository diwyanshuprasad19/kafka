#!/usr/bin/env bash
# Seed local demo data for Grafana (delegates to platform-ops).
# Usage: ./scripts/demo_data.sh
#        EVENTS=10000 ./scripts/demo_data.sh
set -euo pipefail
OPS="$(cd "$(dirname "$0")/../../platform-ops" && pwd)"
exec "$OPS/scripts/local_demo_seed.sh"
