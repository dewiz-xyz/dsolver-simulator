#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DSOLVER_VERIFY_BROADCASTER_REDIS_SOURCE_ONLY=1
# shellcheck disable=SC1091
source "$repo/scripts/verify_broadcaster_redis.sh"

status_pointer='{"stream_key":"dsolver:broadcaster:local:events","stream_id":"chain-1-redis-stream-7","snapshot_id":"chain-1-redis-snapshot-7","snapshot_start_entry_id":"7-1","snapshot_end_entry_id":"7-2","live_cursor_entry_id":"7-2"}'
redis_pointer='{"schema_version":"1","stream_key":"dsolver:broadcaster:local:events","stream_id":"chain-1-redis-stream-7","snapshot_id":"chain-1-redis-snapshot-7","snapshot_start_entry_id":"7-1","snapshot_end_entry_id":"7-2","live_cursor_entry_id":"7-2"}'

pointer_range="$(compare_snapshot_pointers "$status_pointer" "$redis_pointer")"
[[ "$pointer_range" == $'7-1\t7-2' ]]

if compare_snapshot_pointers "$status_pointer" "${redis_pointer/chain-1-redis-snapshot-7/stale-snapshot}" 2>/dev/null; then
  echo "expected pointer comparison to reject stale Redis snapshot pointer" >&2
  exit 1
fi

assert_entry_retained "7-1" "7-1"
assert_entry_retained "7-2" "7-2"
if assert_entry_retained "" "7-1" 2>/dev/null; then
  echo "expected empty XRANGE result to fail exact retention check" >&2
  exit 1
fi
if assert_entry_retained "7-3" "7-1" 2>/dev/null; then
  echo "expected mismatched XRANGE result to fail exact retention check" >&2
  exit 1
fi

BROADCASTER_REDIS_URL=redis://127.0.0.1:6379/1
[[ "$(redis_db_number)" == "1" ]]
BROADCASTER_REDIS_URL=redis://127.0.0.1:6379
[[ "$(redis_db_number)" == "0" ]]

metadata_file="$(mktemp)"
trap 'rm -f "$metadata_file"' EXIT
{
  printf 'redis_url=%s\n' 'redis://127.0.0.1:6379/0'
  printf 'compose_file=%s\n' "$repo/docker-compose.redis.yml"
  printf 'compose_project=%s\n' 'dsolver-simulator-redis-test'
} > "$metadata_file"
BROADCASTER_REDIS_URL=redis://127.0.0.1:6379/0
redis_metadata_matches_current "$metadata_file" || {
  echo "expected Redis metadata to match current BROADCASTER_REDIS_URL" >&2
  exit 1
}
BROADCASTER_REDIS_URL=redis://127.0.0.1:6380/0
if redis_metadata_matches_current "$metadata_file"; then
  echo "expected stale Redis metadata to be ignored" >&2
  exit 1
fi

echo "verify_broadcaster_redis helper tests passed"
