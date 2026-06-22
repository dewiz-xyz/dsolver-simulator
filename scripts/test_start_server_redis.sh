#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DSOLVER_START_SERVER_SOURCE_ONLY=1
# shellcheck disable=SC1091
source "$repo/scripts/start_server.sh"

assert_local_redis() {
  local redis_url="$1"
  local expected_host="$2"
  local expected_port="$3"
  local managed host port

  BROADCASTER_REDIS_URL="$redis_url"
  IFS=$'\t' read -r managed host port < <(resolve_local_redis)
  unset BROADCASTER_REDIS_URL

  [[ "$managed" == "true" ]] || {
    echo "expected $redis_url to be managed locally, got managed=$managed" >&2
    return 1
  }
  [[ "$host" == "$expected_host" ]] || {
    echo "expected host $expected_host for $redis_url, got $host" >&2
    return 1
  }
  [[ "$port" == "$expected_port" ]] || {
    echo "expected port $expected_port for $redis_url, got $port" >&2
    return 1
  }
}

assert_external_redis() {
  local redis_url="$1"
  local managed

  BROADCASTER_REDIS_URL="$redis_url"
  IFS=$'\t' read -r managed _ < <(resolve_local_redis)
  unset BROADCASTER_REDIS_URL

  [[ "$managed" == "false" ]] || {
    echo "expected $redis_url to be externally managed, got managed=$managed" >&2
    return 1
  }
}

assert_local_broadcaster() {
  local broadcaster_url="$1"
  local expected_host="$2"
  local expected_port="$3"
  local expected_status_url="$4"
  local managed host port status_url

  TYCHO_BROADCASTER_URL="$broadcaster_url"
  IFS=$'\t' read -r managed host port status_url < <(resolve_local_broadcaster)
  unset TYCHO_BROADCASTER_URL

  [[ "$managed" == "true" ]] || {
    echo "expected $broadcaster_url to be managed locally, got managed=$managed" >&2
    return 1
  }
  [[ "$host" == "$expected_host" ]] || {
    echo "expected host $expected_host for $broadcaster_url, got $host" >&2
    return 1
  }
  [[ "$port" == "$expected_port" ]] || {
    echo "expected port $expected_port for $broadcaster_url, got $port" >&2
    return 1
  }
  [[ "$status_url" == "$expected_status_url" ]] || {
    echo "expected status URL $expected_status_url for $broadcaster_url, got $status_url" >&2
    return 1
  }
}

assert_external_broadcaster() {
  local broadcaster_url="$1"
  local managed

  TYCHO_BROADCASTER_URL="$broadcaster_url"
  IFS=$'\t' read -r managed _ < <(resolve_local_broadcaster)
  unset TYCHO_BROADCASTER_URL

  [[ "$managed" == "false" ]] || {
    echo "expected $broadcaster_url to be externally managed, got managed=$managed" >&2
    return 1
  }
}

assert_local_redis "redis://localhost:6380/0" "127.0.0.1" "6380"
assert_local_redis "redis://127.0.0.1:6379/0" "127.0.0.1" "6379"
assert_local_redis "redis://[::1]:6381/0" "::1" "6381"
assert_external_redis "rediss://localhost:6379/0"
assert_external_redis "redis://redis.internal:6379/0"

unset BROADCASTER_REDIS_URL
if resolve_local_redis >/dev/null 2>&1; then
  echo "expected missing BROADCASTER_REDIS_URL to fail" >&2
  exit 1
fi

assert_local_broadcaster "http://localhost:3001" "127.0.0.1" "3001" "http://127.0.0.1:3001/status"
assert_local_broadcaster "http://[::1]:3001" "::1" "3001" "http://[::1]:3001/status"
assert_external_broadcaster "https://broadcaster.example/prod/base"

TYCHO_BROADCASTER_URL="http://127.0.0.1:3001/snapshot-sessions"
if resolve_local_broadcaster >/dev/null 2>&1; then
  echo "expected local broadcaster URL with a path to fail" >&2
  exit 1
fi
unset TYCHO_BROADCASTER_URL

repo="/tmp/dsolver-simulator-a"
compose_project_a="$(redis_compose_project_name)"
repo="/tmp/another-checkout/dsolver-simulator"
compose_project_b="$(redis_compose_project_name)"
[[ "$compose_project_a" != "$compose_project_b" ]] || {
  echo "expected Redis Compose project name to differ by repo path" >&2
  exit 1
}

curl() {
  printf '%s\n%s' "$TEST_STATUS_BODY" "$TEST_STATUS_CODE"
}

TEST_STATUS_CODE=503
TEST_STATUS_BODY='{"status":"redis_publisher_unhealthy","chain_id":8453,"upstream":{},"snapshot":{},"snapshot_sessions":{},"backends":{},"redis_publisher":{"healthy":false}}'
status_check="$(broadcaster_status_check "http://127.0.0.1:3001/status" "8453")"
[[ "$status_check" == "redis-unhealthy 503" ]] || {
  echo "expected redis-unhealthy status check, got $status_check" >&2
  exit 1
}

echo "start_server Redis helper tests passed"
