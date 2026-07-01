#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: verify_state_history.sh [--repo <path>] [--keep-services]

Start the local Postgres + MinIO stack and run the state history storage harness.

Options:
  --repo           Path to repo root (default: current directory)
  --keep-services Keep Docker services running after the harness exits
  -h, --help       Show this help
USAGE
}

repo="."
keep_services="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      repo="$2"
      shift 2
      ;;
    --keep-services)
      keep_services="true"
      shift
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
compose_file="$repo/docker-compose.state-history.yml"

if [[ ! -f "$compose_file" ]]; then
  echo "State history compose file not found at $compose_file." >&2
  exit 1
fi
if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required for the local state history stack." >&2
  exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
  echo "Docker Compose v2 is required for the local state history stack." >&2
  exit 1
fi

compose_project_name() {
  python3 - "$repo" <<'PY'
import hashlib
import os
import re
import sys

repo = os.path.realpath(sys.argv[1])
name = re.sub(r"[^a-z0-9_-]+", "-", os.path.basename(repo).lower()).strip("-_")
if not name:
    name = "dsolver-simulator"
digest = hashlib.sha1(repo.encode("utf-8")).hexdigest()[:12]
print(f"{name}-state-history-{digest}")
PY
}

tcp_ready() {
  local host="$1"
  local port="$2"

  python3 - "$host" "$port" <<'PY'
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])

try:
    with socket.create_connection((host, port), timeout=1):
        raise SystemExit(0)
except OSError:
    raise SystemExit(1)
PY
}

wait_for_tcp() {
  local label="$1"
  local host="$2"
  local port="$3"
  local timeout_secs="$4"
  local started_at
  started_at="$(date +%s)"

  while true; do
    if tcp_ready "$host" "$port"; then
      echo "$label is reachable at $host:$port."
      return 0
    fi

    local now
    now="$(date +%s)"
    if ((now - started_at >= timeout_secs)); then
      echo "Timed out waiting for $label at $host:$port." >&2
      return 1
    fi
    sleep 1
  done
}

compose_project="$(compose_project_name)"
postgres_bind="${STATE_HISTORY_POSTGRES_BIND:-127.0.0.1}"
postgres_port="${STATE_HISTORY_POSTGRES_PORT:-55432}"
minio_bind="${STATE_HISTORY_MINIO_BIND:-127.0.0.1}"
minio_port="${STATE_HISTORY_MINIO_PORT:-59000}"

cleanup() {
  if [[ "$keep_services" == "true" ]]; then
    return
  fi
  (
    cd "$repo"
    docker compose -p "$compose_project" -f "$compose_file" down >/dev/null
  )
}
trap cleanup EXIT

echo "Starting local state history storage stack..."
(
  cd "$repo"
  docker compose -p "$compose_project" -f "$compose_file" up -d postgres minio
)

wait_for_tcp "Postgres" "$postgres_bind" "$postgres_port" 60
wait_for_tcp "MinIO" "$minio_bind" "$minio_port" 60

export STATE_HISTORY_DATABASE_URL="${STATE_HISTORY_DATABASE_URL:-postgres://postgres:postgres@${postgres_bind}:${postgres_port}/state_history}"
export STATE_HISTORY_S3_BUCKET="${STATE_HISTORY_S3_BUCKET:-state-history}"
export STATE_HISTORY_S3_PREFIX="${STATE_HISTORY_S3_PREFIX:-state-history/local-analysis}"
export STATE_HISTORY_S3_REGION="${STATE_HISTORY_S3_REGION:-us-east-1}"
export STATE_HISTORY_S3_ENDPOINT_URL="${STATE_HISTORY_S3_ENDPOINT_URL:-http://${minio_bind}:${minio_port}}"
export STATE_HISTORY_S3_FORCE_PATH_STYLE="${STATE_HISTORY_S3_FORCE_PATH_STYLE:-true}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-state-history}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-state-history-secret}"
export AWS_REGION="${AWS_REGION:-$STATE_HISTORY_S3_REGION}"

(
  cd "$repo"
  cargo run -p apps --bin state-history-analysis
)
