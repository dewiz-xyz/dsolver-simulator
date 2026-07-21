#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: verify_redis_promotion_handoff.sh [--repo <path>]

Run the ignored runtime test that verifies Redis promotion handoff markers through
the real Lua path against a temporary Redis container.
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
  echo "docker is required to run the Redis promotion handoff verifier." >&2
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
    -p 127.0.0.1::6379 \
    redis:7-alpine \
    redis-server --save "" --appendonly no
)"

redis_port=""
for _ in {1..60}; do
  redis_port="$(docker port "$container" 6379/tcp 2>/dev/null | sed -E 's/.*:([0-9]+)$/\1/' || true)"
  if [[ -n "$redis_port" ]] && docker exec "$container" redis-cli ping >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

if [[ -z "$redis_port" ]]; then
  echo "temporary Redis container did not expose a host port." >&2
  exit 1
fi

(
  cd "$repo"
  BROADCASTER_REDIS_URL="redis://127.0.0.1:${redis_port}/0" \
    cargo test -p runtime real_redis_promotion_marker_uses_lua_tail_values \
      -- --ignored --nocapture
)
