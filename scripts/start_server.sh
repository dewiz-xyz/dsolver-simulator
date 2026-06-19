#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: start_server.sh [--repo <path>] [--log-file <path>] [--chain-id <id>] [--env KEY=VALUE] [--enable-vm-pools]

Start the local DSolver simulator service stack from a repo checkout.

Options:
  --repo             Path to repo root (default: current directory)
  --log-file         Log file path (default: <repo>/logs/tycho-sim-server.log)
  --chain-id         Runtime chain id from simulator-manifest.toml. Overrides CHAIN_ID from env/.env.
  --env              Export KEY=VALUE before starting (repeatable)
  --enable-vm-pools  Shortcut for --env ENABLE_VM_POOLS=true
  -h, --help         Show this help
USAGE
}

repo="."
log_file=""
chain_id_arg=""
env_overrides=()
local_broadcaster_start_timeout=300

pid_is_running() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

cleanup_stale_pid_file() {
  local pid_file="$1"
  local label="$2"

  if [[ ! -f "$pid_file" ]]; then
    return 1
  fi

  local pid
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if pid_is_running "$pid"; then
    echo "$label already running (pid $pid)."
    return 0
  fi

  rm -f "$pid_file"
  return 1
}

read_pid_file() {
  local pid_file="$1"
  cat "$pid_file" 2>/dev/null || true
}

read_metadata_value() {
  local metadata_file="$1"
  local key="$2"

  awk -v key="$key" '
    index($0, key "=") == 1 {
      print substr($0, length(key) + 2)
      exit
    }
  ' "$metadata_file" 2>/dev/null || true
}

broadcaster_metadata_matches() {
  local metadata_file="$1"
  local broadcaster_url="$2"
  local bind_host="$3"
  local bind_port="$4"
  local status_url="$5"

  [[ -f "$metadata_file" ]] \
    && [[ "$(read_metadata_value "$metadata_file" "broadcaster_url")" == "$broadcaster_url" ]] \
    && [[ "$(read_metadata_value "$metadata_file" "bind_host")" == "$bind_host" ]] \
    && [[ "$(read_metadata_value "$metadata_file" "bind_port")" == "$bind_port" ]] \
    && [[ "$(read_metadata_value "$metadata_file" "status_url")" == "$status_url" ]]
}

write_broadcaster_metadata() {
  local metadata_file="$1"
  local broadcaster_url="$2"
  local bind_host="$3"
  local bind_port="$4"
  local status_url="$5"

  {
    printf 'broadcaster_url=%s\n' "$broadcaster_url"
    printf 'bind_host=%s\n' "$bind_host"
    printf 'bind_port=%s\n' "$bind_port"
    printf 'status_url=%s\n' "$status_url"
  } > "$metadata_file"
}

broadcaster_process_is_helper_owned() {
  local pid="$1"
  local command

  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  [[ "$command" == *"dsolver-tycho-broadcaster-service"* ]]
}

stop_broadcaster_pid() {
  local pid="$1"
  local pid_file="$2"
  local metadata_file="$3"

  if [[ -z "$pid" ]] || ! pid_is_running "$pid"; then
    rm -f "$pid_file" "$metadata_file"
    return 0
  fi

  if ! broadcaster_process_is_helper_owned "$pid"; then
    echo "Refusing to replace live process $pid from $pid_file; it does not look like dsolver-tycho-broadcaster-service." >&2
    return 1
  fi

  echo "Stopping broadcaster service with stale launch metadata (pid $pid)."
  kill "$pid" 2>/dev/null || true

  for _ in {1..10}; do
    if ! pid_is_running "$pid"; then
      rm -f "$pid_file" "$metadata_file"
      return 0
    fi
    sleep 1
  done

  echo "Timed out stopping broadcaster service pid $pid." >&2
  return 1
}

resolve_local_broadcaster() {
  python3 - "$TYCHO_BROADCASTER_URL" <<'PY'
import sys
from urllib.parse import urlparse

raw_url = sys.argv[1]
url = urlparse(raw_url)
scheme = url.scheme.lower()

if scheme not in {"http", "https"}:
    print("TYCHO_BROADCASTER_URL must use http:// or https://", file=sys.stderr)
    raise SystemExit(2)

host = url.hostname or ""
local_hosts = {"localhost", "127.0.0.1", "::1"}

if scheme != "http" or host not in local_hosts:
    print("false\t\t\t")
    raise SystemExit(0)

try:
    port = url.port
except ValueError as error:
    print(f"invalid TYCHO_BROADCASTER_URL port: {error}", file=sys.stderr)
    raise SystemExit(2)

if port is None:
    print("local TYCHO_BROADCASTER_URL must include an explicit port", file=sys.stderr)
    raise SystemExit(2)

if url.path not in {"", "/"}:
    actual_path = url.path or "/"
    print(f"local TYCHO_BROADCASTER_URL must use the HTTP base path, got {actual_path}", file=sys.stderr)
    raise SystemExit(2)

bind_host = "127.0.0.1" if host == "localhost" else host
status_host = f"[{bind_host}]" if ":" in bind_host else bind_host
status_url = f"http://{status_host}:{port}/status"

print(f"true\t{bind_host}\t{port}\t{status_url}")
PY
}

start_broadcaster_if_local() {
  local managed bind_host bind_port status_url
  IFS=$'\t' read -r managed bind_host bind_port status_url < <(resolve_local_broadcaster)

  if [[ "$managed" != "true" ]]; then
    echo "Broadcaster URL is not loopback http://; assuming broadcaster is externally managed."
    return 0
  fi

  local broadcaster_pid_file="$repo/.tycho-broadcaster-service.pid"
  local broadcaster_metadata_file="$repo/.tycho-broadcaster-service.meta"
  local broadcaster_log_file="$repo/logs/tycho-broadcaster-service.log"

  local broadcaster_pid
  broadcaster_pid="$(read_pid_file "$broadcaster_pid_file")"
  if pid_is_running "$broadcaster_pid"; then
    local status_check status_state status_code actual_chain
    status_check="$(broadcaster_status_check "$status_url" "$CHAIN_ID")"
    read -r status_state status_code actual_chain <<<"$status_check"
    if [[ "$status_state" == "ok" ]]; then
      if ! broadcaster_metadata_matches "$broadcaster_metadata_file" "$TYCHO_BROADCASTER_URL" "$bind_host" "$bind_port" "$status_url"; then
        write_broadcaster_metadata "$broadcaster_metadata_file" "$TYCHO_BROADCASTER_URL" "$bind_host" "$bind_port" "$status_url"
      fi
      echo "Broadcaster service already running (pid $broadcaster_pid)."
      return 0
    fi
    if [[ "$status_state" == "chain-mismatch" ]]; then
      echo "Broadcaster already responding at $status_url for chain_id=$actual_chain, expected CHAIN_ID=$CHAIN_ID." >&2
      return 1
    fi

    if broadcaster_metadata_matches "$broadcaster_metadata_file" "$TYCHO_BROADCASTER_URL" "$bind_host" "$bind_port" "$status_url"; then
      echo "Broadcaster service already running (pid $broadcaster_pid)."
      wait_for_broadcaster_http "$status_url" "$broadcaster_pid_file" "$broadcaster_log_file"
      return 0
    fi

    stop_broadcaster_pid "$broadcaster_pid" "$broadcaster_pid_file" "$broadcaster_metadata_file"
  elif [[ -f "$broadcaster_pid_file" || -f "$broadcaster_metadata_file" ]]; then
    rm -f "$broadcaster_pid_file" "$broadcaster_metadata_file"
  fi

  if cleanup_stale_pid_file "$broadcaster_pid_file" "Broadcaster service"; then
    wait_for_broadcaster_http "$status_url" "$broadcaster_pid_file" "$broadcaster_log_file"
    return 0
  fi

  local status_check status_state status_code actual_chain
  status_check="$(broadcaster_status_check "$status_url" "$CHAIN_ID")"
  read -r status_state status_code actual_chain <<<"$status_check"
  if [[ "$status_state" == "ok" ]]; then
    echo "Broadcaster already responding at $status_url (HTTP $status_code)."
    return 0
  fi
  if [[ "$status_state" == "chain-mismatch" ]]; then
    echo "Broadcaster already responding at $status_url for chain_id=$actual_chain, expected CHAIN_ID=$CHAIN_ID." >&2
    return 1
  fi

  mkdir -p "$repo/logs"

  (
    cd "$repo"
    HOST="$bind_host" PORT="$bind_port" nohup cargo run -p apps --bin dsolver-tycho-broadcaster-service --release > "$broadcaster_log_file" 2>&1 &
    echo $! > "$broadcaster_pid_file"
  )
  write_broadcaster_metadata "$broadcaster_metadata_file" "$TYCHO_BROADCASTER_URL" "$bind_host" "$bind_port" "$status_url"

  echo "Started dsolver-tycho-broadcaster-service."
  echo "Broadcaster PID: $(cat "$broadcaster_pid_file")"
  echo "Broadcaster URL: $TYCHO_BROADCASTER_URL"
  echo "Broadcaster log: $broadcaster_log_file"

  wait_for_broadcaster_http "$status_url" "$broadcaster_pid_file" "$broadcaster_log_file"
}

wait_for_broadcaster_http() {
  local status_url="$1"
  local pid_file="$2"
  local log_file="$3"
  local start_time
  start_time="$(date +%s)"

  while true; do
    local status_check status_state status_code actual_chain
    status_check="$(broadcaster_status_check "$status_url" "$CHAIN_ID")"
    read -r status_state status_code actual_chain <<<"$status_check"
    if [[ "$status_state" == "ok" ]]; then
      echo "Broadcaster HTTP is responding at $status_url (HTTP $status_code)."
      return 0
    fi
    if [[ "$status_state" == "chain-mismatch" ]]; then
      echo "Broadcaster HTTP at $status_url reports chain_id=$actual_chain, expected CHAIN_ID=$CHAIN_ID. See $log_file." >&2
      return 1
    fi

    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if ! pid_is_running "$pid"; then
      echo "Broadcaster service exited before HTTP was reachable. See $log_file." >&2
      rm -f "$pid_file"
      return 1
    fi

    local now
    now="$(date +%s)"
    if ((now - start_time >= local_broadcaster_start_timeout)); then
      echo "Timed out waiting for broadcaster HTTP at $status_url. See $log_file." >&2
      return 1
    fi

    sleep 2
  done
}

broadcaster_status_check() {
  local status_url="$1"
  local expected_chain_id="$2"
  local response http_code body

  response="$(curl -sS --max-time 2 -w $'\n%{http_code}' "$status_url" 2>/dev/null || true)"
  http_code="${response##*$'\n'}"
  body="${response%$'\n'*}"

  if [[ -z "$http_code" || "$http_code" == "000" ]]; then
    return 0
  fi

  BROADCASTER_STATUS_BODY="$body" python3 - "$http_code" "$expected_chain_id" <<'PY'
import json
import os
import sys

http_code = sys.argv[1]
expected_chain_id = int(sys.argv[2])

try:
    payload = json.loads(os.environ["BROADCASTER_STATUS_BODY"])
except (json.JSONDecodeError, ValueError):
    raise SystemExit(0)

if not isinstance(payload, dict):
    raise SystemExit(0)

if (
    isinstance(payload.get("status"), str)
    and isinstance(payload.get("chain_id"), int)
    and isinstance(payload.get("upstream"), dict)
    and isinstance(payload.get("snapshot"), dict)
    and isinstance(payload.get("snapshot_sessions"), dict)
    and isinstance(payload.get("backends"), dict)
):
    actual_chain_id = payload["chain_id"]
    if actual_chain_id == expected_chain_id:
        print(f"ok {http_code}")
    else:
        print(f"chain-mismatch {http_code} {actual_chain_id}")
PY
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      repo="$2"
      shift 2
      ;;
    --log-file)
      log_file="$2"
      shift 2
      ;;
    --chain-id)
      chain_id_arg="$2"
      shift 2
      ;;
    --env)
      env_overrides+=("$2")
      shift 2
      ;;
    --enable-vm-pools)
      env_overrides+=("ENABLE_VM_POOLS=true")
      shift 1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

repo="$(cd "$repo" && pwd)"

if [[ ! -f "$repo/Cargo.toml" ]]; then
  echo "Error: Cargo.toml not found in $repo" >&2
  exit 1
fi

pid_file="$repo/.tycho-sim-server.pid"
simulator_already_running=false
if cleanup_stale_pid_file "$pid_file" "Simulator service"; then
  simulator_already_running=true
fi

if [[ -f "$repo/.env" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$repo/.env"
  set +a
else
  echo "Warning: .env not found; ensure TYCHO_API_KEY is set." >&2
fi

if [[ -z "${TYCHO_API_KEY:-}" ]]; then
  echo "Warning: TYCHO_API_KEY not set; server may fail to start." >&2
fi

if [[ -z "${RUST_LOG:-}" ]]; then
  export RUST_LOG=info
fi

if ((${#env_overrides[@]})); then
  for pair in "${env_overrides[@]}"; do
    export "$pair"
  done
fi

if [[ -n "$chain_id_arg" ]]; then
  export CHAIN_ID="$chain_id_arg"
fi

if [[ "$simulator_already_running" == "true" ]]; then
  if [[ -z "${TYCHO_BROADCASTER_URL:-}" ]]; then
    echo "TYCHO_BROADCASTER_URL not set; skipping broadcaster startup for running simulator."
  elif [[ -z "${CHAIN_ID:-}" ]]; then
    echo "Error: missing chain id. Pass --chain-id or set CHAIN_ID in env/.env." >&2
    exit 2
  else
    start_broadcaster_if_local
  fi
  exit 0
fi

if [[ -z "${TYCHO_BROADCASTER_URL:-}" ]]; then
  echo "Error: TYCHO_BROADCASTER_URL is required for simulator startup." >&2
  exit 2
fi

if [[ -z "${CHAIN_ID:-}" ]]; then
  echo "Error: missing chain id. Pass --chain-id or set CHAIN_ID in env/.env." >&2
  exit 2
fi

if [[ -z "$log_file" ]]; then
  mkdir -p "$repo/logs"
  log_file="$repo/logs/tycho-sim-server.log"
fi

if [[ -n "${TYCHO_BROADCASTER_URL:-}" ]]; then
  start_broadcaster_if_local
fi

(
  cd "$repo"
  nohup cargo run -p apps --bin dsolver-simulator-service --release > "$log_file" 2>&1 &
  echo $! > "$pid_file"
)

echo "Started dsolver-simulator-service."
echo "Chain ID: $CHAIN_ID"
echo "PID: $(cat "$pid_file")"
echo "Log: $log_file"
echo "Tip: tail -f $log_file"
