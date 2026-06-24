#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DSOLVER_VERIFY_BROADCASTER_REDIS_SOURCE_ONLY=1
# shellcheck disable=SC1091
source "$repo/scripts/verify_broadcaster_redis.sh"

TYCHO_BROADCASTER_URL=http://127.0.0.1:3001
[[ "$(derive_status_url)" == "http://127.0.0.1:3001/status" ]]
TYCHO_BROADCASTER_URL=https://broadcaster.example/prod/base
[[ "$(derive_status_url)" == "https://broadcaster.example/prod/base/status" ]]
unset TYCHO_BROADCASTER_URL

status_body='{"status":"ready","chain_id":8453,"redis_publisher":{"healthy":true,"mode":"active","stream_key":"dsolver:broadcaster:local:8453:events","stream_id":"chain-8453-stream-2","snapshot_id":"chain-8453-snapshot-2","replay_boundary":{"streamKey":"dsolver:broadcaster:local:8453:events","streamId":"chain-8453-stream-2","snapshotId":"chain-8453-snapshot-2","generation":2,"exclusiveMessageSeq":14}}}'
boundary_json="$(extract_replay_boundary "$status_body")"
[[ "$(boundary_entry_id "$boundary_json")" == "2-14" ]]

if extract_replay_boundary "${status_body/\"mode\":\"active\"/\"mode\":\"passive\"}" >/dev/null 2>&1; then
  echo "expected passive redis_publisher mode to fail replay boundary parsing" >&2
  exit 1
fi

if extract_replay_boundary "${status_body/exclusiveMessageSeq/missingMessageSeq}" >/dev/null 2>&1; then
  echo "expected missing exclusiveMessageSeq to fail replay boundary parsing" >&2
  exit 1
fi

assert_entry_retained "2-14" "2-14"
if assert_entry_retained "" "2-14" 2>/dev/null; then
  echo "expected empty XRANGE result to fail exact retention check" >&2
  exit 1
fi
if assert_entry_retained "2-15" "2-14" 2>/dev/null; then
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
