#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: stop_server.sh [--repo <path>] [--force]

Stop services started by start_server.sh.

Options:
  --repo       Path to repo root (default: current directory)
  --force      Send SIGKILL if the process does not exit
  -h, --help   Show this help
USAGE
}

repo="."
force="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      repo="$2"
      shift 2
      ;;
    --force)
      force="true"
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

stop_process() {
  local label="$1"
  local pid_file="$2"

  if [[ ! -f "$pid_file" ]]; then
    echo "No $label pid file found at $pid_file."
    return 0
  fi

  local pid
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [[ -z "$pid" ]]; then
    echo "$label pid file is empty; removing." >&2
    rm -f "$pid_file"
    return 0
  fi

  if ! kill -0 "$pid" 2>/dev/null; then
    echo "$label process $pid is not running; removing pid file." >&2
    rm -f "$pid_file"
    return 0
  fi

  echo "Stopping $label (pid $pid)..."
  kill "$pid"

  for _ in {1..20}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$pid_file"
      echo "Stopped $label."
      return 0
    fi
    sleep 0.25
  done

  if [[ "$force" == "true" ]]; then
    echo "$label still running; sending SIGKILL." >&2
    kill -9 "$pid" || true
    rm -f "$pid_file"
    return 0
  fi

  echo "$label still running; re-run with --force to SIGKILL." >&2
  return 1
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

stop_redis() {
  local metadata_file="$repo/.tycho-redis-service.meta"

  if [[ ! -f "$metadata_file" ]]; then
    echo "No local Redis metadata found at $metadata_file."
    return 0
  fi

  local compose_file
  compose_file="$(read_metadata_value "$metadata_file" "compose_file")"
  if [[ -z "$compose_file" || ! -f "$compose_file" ]]; then
    echo "Local Redis compose file is missing; removing stale metadata." >&2
    rm -f "$metadata_file"
    return 0
  fi
  local compose_project
  compose_project="$(read_metadata_value "$metadata_file" "compose_project")"
  if [[ -z "$compose_project" ]]; then
    echo "Local Redis compose project is missing; removing stale metadata." >&2
    rm -f "$metadata_file"
    return 0
  fi
  if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
    echo "Docker Compose v2 is required to stop local Redis from $compose_file." >&2
    return 1
  fi

  echo "Stopping local Redis..."
  (
    cd "$repo"
    docker compose -p "$compose_project" -f "$compose_file" stop redis
  )
  rm -f "$metadata_file"
  echo "Stopped local Redis."
}

status=0
if ! stop_process "simulator service" "$repo/.tycho-sim-server.pid"; then
  status=1
  if [[ "$force" != "true" ]]; then
    echo "Preserving broadcaster service because simulator service did not stop." >&2
    exit "$status"
  fi
fi
redis_can_stop=true
if stop_process "broadcaster service" "$repo/.tycho-broadcaster-service.pid"; then
  rm -f "$repo/.tycho-broadcaster-service.meta"
else
  status=1
  redis_can_stop=false
fi
if [[ "$redis_can_stop" == "true" ]]; then
  if ! stop_redis; then
    status=1
  fi
else
  echo "Preserving local Redis because broadcaster service did not stop." >&2
fi

exit "$status"
