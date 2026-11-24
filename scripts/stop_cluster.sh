#!/usr/bin/env bash
# scripts/stop_cluster.sh
# Gracefully stops a StreamLog cluster started with `scripts/start_cluster.sh --bg`.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PID_FILE="${ROOT_DIR}/.streamlog_pids"

if [[ ! -f "$PID_FILE" ]]; then
    echo "[streamlog] No .streamlog_pids file found — cluster may not be running in background."
    exit 0
fi

echo "[streamlog] Stopping cluster nodes ..."
while IFS= read -r pid; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
    fi
done < "$PID_FILE"

rm -f "$PID_FILE"
echo "[streamlog] Cluster stopped."
