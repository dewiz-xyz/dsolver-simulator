#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: verify_broadcaster_redis.sh [--repo <path>] [--status-url <url>] [--simulator-status-url <url>]

Verify that the local broadcaster Redis replay path is live.

The script reads BROADCASTER_REDIS_URL and BROADCASTER_REDIS_STREAM_KEY from
.env or the current environment. It checks:
  - broadcaster /status includes a healthy redis_publisher with replay_boundary
  - the Redis stream has at least one entry
  - the replay boundary cursor still has enough retained Redis history
  - simulator /status subscriptions caught up from the Redis replay boundary

Options:
  --repo                  Path to repo root (default: current directory)
  --status-url            Broadcaster status URL (default: derived from TYCHO_BROADCASTER_URL)
  --simulator-status-url  Simulator status URL (default: http://127.0.0.1:${PORT:-3000}/status)
  -h, --help              Show this help
USAGE
}

repo="."
status_url=""
simulator_status_url=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      repo="$2"
      shift 2
      ;;
    --status-url)
      status_url="$2"
      shift 2
      ;;
    --simulator-status-url)
      simulator_status_url="$2"
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

redis_db_number() {
  python3 - "${BROADCASTER_REDIS_URL:-}" <<'PY'
import sys
from urllib.parse import urlparse

raw_url = sys.argv[1]
url = urlparse(raw_url)
db_path = url.path.lstrip("/")
if not db_path:
    print("0")
    raise SystemExit(0)
if not db_path.isdigit():
    print(f"BROADCASTER_REDIS_URL database must be numeric, got {url.path}", file=sys.stderr)
    raise SystemExit(2)
print(db_path)
PY
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

redis_metadata_matches_current() {
  local metadata_file="$1"

  [[ -f "$metadata_file" ]] \
    && [[ "$(read_metadata_value "$metadata_file" "redis_url")" == "${BROADCASTER_REDIS_URL:-}" ]]
}

derive_status_url() {
  python3 - "${TYCHO_BROADCASTER_URL:-}" <<'PY'
import sys
from urllib.parse import urlparse, urlunparse

raw_url = sys.argv[1]
if not raw_url:
    print("TYCHO_BROADCASTER_URL is required when --status-url is omitted", file=sys.stderr)
    raise SystemExit(2)

url = urlparse(raw_url)
if url.scheme not in {"http", "https"}:
    print("TYCHO_BROADCASTER_URL must use http:// or https://", file=sys.stderr)
    raise SystemExit(2)
if not url.netloc:
    print("TYCHO_BROADCASTER_URL must include a host", file=sys.stderr)
    raise SystemExit(2)

base_path = url.path.rstrip("/")
status_path = f"{base_path}/status" if base_path else "/status"
print(urlunparse((url.scheme, url.netloc, status_path, "", "", "")))
PY
}

extract_replay_boundary() {
  local status_body="$1"

  STATUS_BODY="$status_body" python3 <<'PY'
import json
import os

payload = json.loads(os.environ["STATUS_BODY"])
publisher = payload.get("redis_publisher")
if not isinstance(publisher, dict):
    raise SystemExit("broadcaster /status did not include redis_publisher")
if publisher.get("healthy") is not True:
    raise SystemExit("broadcaster redis_publisher is not healthy")
boundary = publisher.get("replay_boundary")
if not isinstance(boundary, dict):
    raise SystemExit("broadcaster redis_publisher has no replay_boundary")

string_fields = ["streamKey", "streamId", "snapshotId"]
for field in string_fields:
    value = boundary.get(field)
    if not isinstance(value, str) or not value:
        raise SystemExit(f"broadcaster replay_boundary is missing {field}")

for field in ["generation", "exclusiveMessageSeq"]:
    value = boundary.get(field)
    if not isinstance(value, int) or value < 0:
        raise SystemExit(f"broadcaster replay_boundary has invalid {field}")

if boundary["generation"] == 0:
    raise SystemExit("broadcaster replay_boundary generation must be positive")

publisher_matches = {
    "stream_key": "streamKey",
    "stream_id": "streamId",
    "snapshot_id": "snapshotId",
}
for publisher_field, boundary_field in publisher_matches.items():
    value = publisher.get(publisher_field)
    if isinstance(value, str) and value and value != boundary[boundary_field]:
        raise SystemExit(
            f"broadcaster redis_publisher {publisher_field}={value} does not match replay_boundary {boundary_field}={boundary[boundary_field]}"
        )

print(json.dumps(boundary, sort_keys=True))
PY
}

boundary_entry_id() {
  local boundary_json="$1"

  BOUNDARY_JSON="$boundary_json" python3 <<'PY'
import json
import os

boundary = json.loads(os.environ["BOUNDARY_JSON"])
print(f"{boundary['generation']}-{boundary['exclusiveMessageSeq']}")
PY
}

boundary_message_seq() {
  local boundary_json="$1"

  BOUNDARY_JSON="$boundary_json" python3 <<'PY'
import json
import os

boundary = json.loads(os.environ["BOUNDARY_JSON"])
print(boundary["exclusiveMessageSeq"])
PY
}

boundary_stream_key() {
  local boundary_json="$1"

  BOUNDARY_JSON="$boundary_json" python3 <<'PY'
import json
import os

boundary = json.loads(os.environ["BOUNDARY_JSON"])
print(boundary["streamKey"])
PY
}

assert_entry_retained() {
  local range_output="$1"
  local expected_entry_id="$2"

  RANGE_OUTPUT="$range_output" EXPECTED_ENTRY_ID="$expected_entry_id" python3 <<'PY'
import os

range_output = os.environ["RANGE_OUTPUT"]
expected_entry_id = os.environ["EXPECTED_ENTRY_ID"]
if not range_output:
    raise SystemExit(f"Redis entry {expected_entry_id} is not retained")

first_line = range_output.splitlines()[0]
if first_line != expected_entry_id:
    raise SystemExit(
        f"Redis entry check returned {first_line}, expected {expected_entry_id}"
    )
PY
}

verify_simulator_replay_status() {
  local status_body="$1"

  SIMULATOR_STATUS_BODY="$status_body" python3 <<'PY'
import json
import os

payload = json.loads(os.environ["SIMULATOR_STATUS_BODY"])
backends = payload.get("backends")
if not isinstance(backends, dict):
    raise SystemExit("simulator /status did not include backends")

checked = []
for kind, backend in sorted(backends.items()):
    if not isinstance(backend, dict) or backend.get("enabled") is not True:
        continue
    subscription = backend.get("subscription")
    if not isinstance(subscription, dict):
        raise SystemExit(f"simulator /status backend {kind} has no subscription")
    boundary = subscription.get("redis_replay_boundary")
    if not isinstance(boundary, dict):
        raise SystemExit(f"simulator /status backend {kind} has no redis_replay_boundary")
    for field in ["streamKey", "streamId", "snapshotId", "generation", "exclusiveMessageSeq"]:
        if field not in boundary:
            raise SystemExit(f"simulator /status backend {kind} replay boundary is missing {field}")
    cursor = subscription.get("redis_catch_up_cursor")
    if not isinstance(cursor, str) or not cursor:
        raise SystemExit(f"simulator /status backend {kind} has no redis_catch_up_cursor")
    if subscription.get("redis_replay_caught_up") is not True:
        raise SystemExit(f"simulator /status backend {kind} has not caught up from Redis replay")
    gap = subscription.get("redis_gap_reason")
    if gap is not None:
        raise SystemExit(f"simulator /status backend {kind} has redis_gap_reason={gap}")
    checked.append(kind)

if not checked:
    raise SystemExit("simulator /status had no enabled backends to verify")

print(",".join(checked))
PY
}

if [[ "${DSOLVER_VERIFY_BROADCASTER_REDIS_SOURCE_ONLY:-}" == "1" ]]; then
  return 0 2>/dev/null || exit 0
fi

if [[ -f "$repo/.env" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$repo/.env"
  set +a
fi

if [[ -z "${BROADCASTER_REDIS_URL:-}" ]]; then
  echo "BROADCASTER_REDIS_URL is required." >&2
  exit 2
fi
if [[ -z "${BROADCASTER_REDIS_STREAM_KEY:-}" ]]; then
  echo "BROADCASTER_REDIS_STREAM_KEY is required." >&2
  exit 2
fi

if [[ -z "$status_url" ]]; then
  status_url="$(derive_status_url)"
fi
if [[ -z "$simulator_status_url" ]]; then
  simulator_status_url="http://127.0.0.1:${PORT:-3000}/status"
fi

redis_cli() {
  local metadata_file="$repo/.tycho-redis-service.meta"
  local db_number
  db_number="$(redis_db_number)"
  if [[ -f "$metadata_file" ]]; then
    local compose_file compose_project
    compose_file="$(read_metadata_value "$metadata_file" "compose_file")"
    compose_project="$(read_metadata_value "$metadata_file" "compose_project")"
    if redis_metadata_matches_current "$metadata_file" \
      && [[ -n "$compose_file" && -f "$compose_file" && -n "$compose_project" ]] \
      && command -v docker >/dev/null 2>&1 \
      && docker compose version >/dev/null 2>&1; then
      (
        cd "$repo"
        docker compose -p "$compose_project" -f "$compose_file" exec -T redis redis-cli -n "$db_number" "$@"
      )
      return
    fi
  fi

  if ! command -v redis-cli >/dev/null 2>&1; then
    echo "redis-cli is required when helper-managed Docker Redis is unavailable." >&2
    return 1
  fi
  redis-cli -u "$BROADCASTER_REDIS_URL" "$@"
}

status_body="$(curl -sS --max-time 5 "$status_url")"
boundary_json="$(extract_replay_boundary "$status_body")"
boundary_stream_key="$(boundary_stream_key "$boundary_json")"
if [[ "$boundary_stream_key" != "$BROADCASTER_REDIS_STREAM_KEY" ]]; then
  echo "Replay boundary streamKey=$boundary_stream_key does not match BROADCASTER_REDIS_STREAM_KEY=$BROADCASTER_REDIS_STREAM_KEY." >&2
  exit 1
fi

stream_len="$(redis_cli --raw XLEN "$BROADCASTER_REDIS_STREAM_KEY")"
if ! [[ "$stream_len" =~ ^[0-9]+$ ]] || ((stream_len == 0)); then
  echo "Redis stream $BROADCASTER_REDIS_STREAM_KEY has no entries." >&2
  exit 1
fi

boundary_entry_id="$(boundary_entry_id "$boundary_json")"
boundary_message_seq="$(boundary_message_seq "$boundary_json")"
if ((boundary_message_seq > 0)); then
  boundary_probe="$(redis_cli --raw XRANGE "$BROADCASTER_REDIS_STREAM_KEY" "$boundary_entry_id" "$boundary_entry_id" COUNT 1)"
  assert_entry_retained "$boundary_probe" "$boundary_entry_id"
fi

simulator_status_body="$(curl -sS --max-time 5 "$simulator_status_url")"
checked_backends="$(verify_simulator_replay_status "$simulator_status_body")"

echo "Broadcaster Redis replay path is healthy."
echo "Broadcaster status URL: $status_url"
echo "Simulator status URL: $simulator_status_url"
echo "Redis stream key: $BROADCASTER_REDIS_STREAM_KEY"
echo "Redis stream entries: $stream_len"
echo "Replay boundary entry: $boundary_entry_id"
echo "Simulator replay backends: $checked_backends"
