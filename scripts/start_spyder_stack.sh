#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

load_env_file() {
    local env_file="$1"
    if [[ -f "$env_file" ]]; then
        set -a
        # shellcheck disable=SC1090
        source "$env_file"
        set +a
    fi
}

load_env_file "$ROOT_DIR/.env"
load_env_file "$ROOT_DIR/scripts/spyder-stack.env"

: "${ALL_PROXY:=socks5h://127.0.0.1:9050}"
: "${all_proxy:=$ALL_PROXY}"
: "${SPYDER_WORK_CONCURRENCY:=4}"
: "${SPYDER_SCAN_CONCURRENCY:=4}"
: "${SPYDER_LEADS_LIMIT:=250}"
: "${SPYDER_FRONTEND_RESTART_SECONDS:=10}"
: "${SPYDER_WORK_INTERVAL_SECONDS:=5}"
: "${SPYDER_SCAN_INTERVAL_SECONDS:=3600}"
: "${SPYDER_LEADS_INTERVAL_SECONDS:=900}"
: "${SPYDER_REFRESH_INTERVAL_SECONDS:=300}"
: "${SPYDER_LOG_DIR:=$ROOT_DIR/logs}"

export ALL_PROXY all_proxy

mkdir -p "$SPYDER_LOG_DIR"

pids=()

timestamp() {
    date -u +"%Y-%m-%dT%H:%M:%SZ"
}

run_loop() {
    local name="$1"
    local interval="$2"
    shift 2
    local log_file="$SPYDER_LOG_DIR/${name}.log"
    local child_pid=""
    local child_has_session=0

    stop_loop() {
        trap - INT TERM EXIT
        if [[ -n "$child_pid" ]]; then
            if ((child_has_session)); then
                kill -TERM -- "-$child_pid" 2>/dev/null || true
            else
                kill "$child_pid" 2>/dev/null || true
            fi
            wait "$child_pid" 2>/dev/null || true
        fi
        exit 0
    }

    trap stop_loop INT TERM

    while true; do
        {
            echo "[$(timestamp)] starting ${name}: $*"
            set +e
            if command -v setsid >/dev/null 2>&1; then
                setsid "$@" &
                child_has_session=1
            else
                "$@" &
                child_has_session=0
            fi
            child_pid=$!
            wait "$child_pid"
            status=$?
            child_pid=""
            child_has_session=0
            set -e
            echo "[$(timestamp)] ${name} exited with status ${status}; restarting in ${interval}s"
        } >>"$log_file" 2>&1
        sleep "$interval"
    done
}

start_loop() {
    local name="$1"
    local interval="$2"
    shift 2
    run_loop "$name" "$interval" "$@" &
    pids+=("$!")
    echo "Started ${name} supervisor as PID ${pids[-1]}"
}

shutdown() {
    trap - INT TERM
    echo "Stopping Spyder stack..."
    if ((${#pids[@]} > 0)); then
        kill "${pids[@]}" 2>/dev/null || true
        wait "${pids[@]}" 2>/dev/null || true
    fi
    exit 0
}

trap shutdown INT TERM

start_loop frontend "$SPYDER_FRONTEND_RESTART_SECONDS" \
    cargo run --release --bin frontend

start_loop crawler "$SPYDER_WORK_INTERVAL_SECONDS" \
    cargo run --release --bin spyder -- work --onion-only --concurrency "$SPYDER_WORK_CONCURRENCY"

start_loop service-scan "$SPYDER_SCAN_INTERVAL_SECONDS" \
    cargo run --release --bin spyder -- ssh-scan --concurrency "$SPYDER_SCAN_CONCURRENCY"

start_loop leads "$SPYDER_LEADS_INTERVAL_SECONDS" \
    cargo run --release --bin spyder -- leads recompute --limit "$SPYDER_LEADS_LIMIT"

start_loop refresh "$SPYDER_REFRESH_INTERVAL_SECONDS" \
    cargo run --release --bin spyder -- refresh-relationships

echo "Spyder stack running. Logs are in $SPYDER_LOG_DIR"
echo "  - Frontend on http://127.0.0.1:8000"
echo "  - Crawler: every ${SPYDER_WORK_INTERVAL_SECONDS}s"
echo "  - Service scan: every ${SPYDER_SCAN_INTERVAL_SECONDS}s"
echo "  - Leads recompute: every ${SPYDER_LEADS_INTERVAL_SECONDS}s"
echo "  - Relationship refresh: every ${SPYDER_REFRESH_INTERVAL_SECONDS}s"
wait
