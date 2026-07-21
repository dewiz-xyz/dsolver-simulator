#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: verify_state_history_postgres.sh [--repo <path>]

Run the ignored state-history Postgres sqlx tests against a temporary Postgres
container.
USAGE
}

repo="."

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      repo="$2"
      shift 2
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

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to run the state-history Postgres verifier." >&2
  exit 2
fi

container=""
cleanup() {
  if [[ -n "$container" ]]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

container="$(
  docker run -d --rm \
    -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=postgres \
    -p 127.0.0.1::5432 \
    postgres:16-alpine -c fsync=off -c full_page_writes=off
)"

pg_port=""
for _ in {1..60}; do
  pg_port="$(docker port "$container" 5432/tcp 2>/dev/null | sed -E 's/.*:([0-9]+)$/\1/' || true)"
  if [[ -n "$pg_port" ]] && docker exec "$container" pg_isready -U postgres >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

if [[ -z "$pg_port" ]]; then
  echo "temporary Postgres container did not expose a host port." >&2
  exit 1
fi

(
  cd "$repo"
  DATABASE_URL="postgres://postgres:postgres@127.0.0.1:${pg_port}/postgres" \
    cargo test -p state-history -- --ignored --nocapture
)
