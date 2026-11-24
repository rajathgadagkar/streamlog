#!/usr/bin/env bash
# scripts/start_cluster.sh
# ─────────────────────────────────────────────────────────────────────────────
# Starts a local StreamLog cluster:
#   • 1 Coordinator on port 9000
#   • 3 Brokers on ports 9001, 9002, 9003
#
# Usage:
#   bash scripts/start_cluster.sh            # foreground (Ctrl-C to stop all)
#   bash scripts/start_cluster.sh --bg       # background (use scripts/stop_cluster.sh)
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

PYTHON="${PYTHON:-python3}"
DATA_DIR="${DATA_DIR:-data}"
COORD_HOST="${COORD_HOST:-127.0.0.1}"
COORD_PORT="${COORD_PORT:-9000}"
PID_FILE="${ROOT_DIR}/.streamlog_pids"
BG_MODE=false

if [[ "${1:-}" == "--bg" ]]; then
    BG_MODE=true
fi

PIDS=()

cleanup() {
    if [[ "$BG_MODE" == "true" ]]; then
        return
    fi
    echo ""
    echo "[streamlog] Shutting down cluster ..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    rm -f "$PID_FILE"
    echo "[streamlog] All nodes stopped."
}

trap cleanup EXIT INT TERM

echo "════════════════════════════════════════════"
echo "  StreamLog Distributed Pub-Sub Cluster"
echo "════════════════════════════════════════════"

# Clean old runtime data (comment out to preserve partition logs across restarts)
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"
: > "$PID_FILE"

echo "[streamlog] Starting Coordinator on ${COORD_HOST}:${COORD_PORT} ..."
"$PYTHON" -m streamlog.coordinator \
    --host "$COORD_HOST" \
    --port "$COORD_PORT" &
COORD_PID=$!
PIDS+=("$COORD_PID")
echo "$COORD_PID" >> "$PID_FILE"
sleep 1

for i in 1 2 3; do
    PORT=$((COORD_PORT + i))
    echo "[streamlog] Starting broker${i} on ${COORD_HOST}:${PORT} ..."
    "$PYTHON" -m streamlog.broker \
        --id "broker${i}" \
        --host "$COORD_HOST" \
        --port "$PORT" \
        --coord-host "$COORD_HOST" \
        --coord-port "$COORD_PORT" \
        --data-dir "$DATA_DIR" &
    BROKER_PID=$!
    PIDS+=("$BROKER_PID")
    echo "$BROKER_PID" >> "$PID_FILE"
    sleep 0.3
done

echo ""
echo "[streamlog] Cluster is UP:"
echo "  Coordinator : ${COORD_HOST}:${COORD_PORT}"
echo "  Broker 1    : ${COORD_HOST}:$((COORD_PORT + 1))"
echo "  Broker 2    : ${COORD_HOST}:$((COORD_PORT + 2))"
echo "  Broker 3    : ${COORD_HOST}:$((COORD_PORT + 3))"
echo ""

if [[ "$BG_MODE" == "true" ]]; then
    echo "  Running in background (PIDs saved to .streamlog_pids)."
    echo "  Stop with: bash scripts/stop_cluster.sh"
    echo "════════════════════════════════════════════"
else
    echo "  Press Ctrl-C to stop all nodes."
    echo "════════════════════════════════════════════"
    wait
fi
