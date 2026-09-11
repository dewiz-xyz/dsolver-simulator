#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 [--repo PATH]"
}

repo=""
while (($# > 0)); do
  case "$1" in
    --repo)
      repo="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$repo" ]]; then
  repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi
repo="$(cd "$repo" && pwd)"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required for the two-broadcaster handoff verification." >&2
  exit 1
fi

container_name="dsolver-broadcaster-handoff-${$}"
cleanup() {
  docker rm -f "$container_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --rm -d \
  --name "$container_name" \
  -p 127.0.0.1::6379 \
  redis:7-alpine >/dev/null

for _ in $(seq 1 60); do
  if docker exec "$container_name" redis-cli ping 2>/dev/null | grep -q PONG; then
    break
  fi
  sleep 0.25
done
if ! docker exec "$container_name" redis-cli ping 2>/dev/null | grep -q PONG; then
  echo "Disposable Redis did not become ready." >&2
  exit 1
fi

redis_address="$(docker port "$container_name" 6379/tcp | head -n 1)"
redis_port="${redis_address##*:}"
(
  cd "$repo"
  BROADCASTER_REDIS_HANDOFF_TEST_URL="redis://127.0.0.1:${redis_port}/0" \
    cargo test --locked -p runtime \
      broadcaster::redis_publisher::tests::two_broadcasters_handoff_without_loss_or_duplication_on_real_redis \
      -- --ignored --exact --nocapture
)
