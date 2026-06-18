#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: verify_broadcaster_redis.sh [--repo <path>] [--status-url <url>]

Verify that the local broadcaster Redis publisher path is live.

The script reads BROADCASTER_REDIS_URL, BROADCASTER_REDIS_STREAM_KEY, and
BROADCASTER_REDIS_SNAPSHOT_KEY from .env or the current environment. It checks:
  - broadcaster /status includes a healthy redis_publisher
  - the Redis stream has at least one entry
  - the snapshot pointer exists and points at retained stream entries

Options:
  --repo        Path to repo root (default: current directory)
  --status-url  Broadcaster status URL (default: derived from TYCHO_BROADCASTER_WS_URL)
  -h, --help    Show this help
USAGE
}

repo="."
status_url=""

if [[ "${DSOLVER_VERIFY_BROADCASTER_REDIS_SOURCE_ONLY:-}" == "1" ]]; then
  skip_verify_main=1
else
  skip_verify_main=0
fi

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

compare_snapshot_pointers() {
  local status_pointer_json="$1"
  local redis_pointer_json="$2"

  STATUS_POINTER_JSON="$status_pointer_json" REDIS_POINTER_JSON="$redis_pointer_json" python3 <<'PY'
import json
import os

status_pointer = json.loads(os.environ["STATUS_POINTER_JSON"])
redis_pointer = json.loads(os.environ["REDIS_POINTER_JSON"])
fields = [
    "stream_key",
    "stream_id",
    "snapshot_id",
    "snapshot_start_entry_id",
    "snapshot_end_entry_id",
    "live_cursor_entry_id",
]
for field in fields:
    status_value = status_pointer.get(field)
    redis_value = redis_pointer.get(field)
    if not isinstance(status_value, str) or not status_value:
        raise SystemExit(f"broadcaster /status snapshot pointer is missing {field}")
    if status_value != redis_value:
        raise SystemExit(
            f"Redis snapshot pointer {field} mismatch: status={status_value}, redis={redis_value}"
        )

print(f"{redis_pointer['snapshot_start_entry_id']}\t{redis_pointer['snapshot_end_entry_id']}")
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

if [[ "$skip_verify_main" == "1" ]]; then
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
if [[ -z "${BROADCASTER_REDIS_SNAPSHOT_KEY:-}" ]]; then
  echo "BROADCASTER_REDIS_SNAPSHOT_KEY is required." >&2
  exit 2
fi

derive_status_url() {
  python3 - "${TYCHO_BROADCASTER_WS_URL:-}" <<'PY'
import sys
from urllib.parse import urlparse

raw_url = sys.argv[1]
if not raw_url:
    print("TYCHO_BROADCASTER_WS_URL is required when --status-url is omitted", file=sys.stderr)
    raise SystemExit(2)

url = urlparse(raw_url)
if url.scheme not in {"ws", "wss"}:
    print("TYCHO_BROADCASTER_WS_URL must use ws:// or wss://", file=sys.stderr)
    raise SystemExit(2)
if url.port is None:
    print("TYCHO_BROADCASTER_WS_URL must include a port when --status-url is omitted", file=sys.stderr)
    raise SystemExit(2)

status_scheme = "https" if url.scheme == "wss" else "http"
host = url.hostname or ""
if ":" in host and not host.startswith("["):
    host = f"[{host}]"
print(f"{status_scheme}://{host}:{url.port}/status")
PY
}

if [[ -z "$status_url" ]]; then
  status_url="$(derive_status_url)"
fi

redis_cli() {
  local metadata_file="$repo/.tycho-redis-service.meta"
  local db_number
  db_number="$(redis_db_number)"
  if [[ -f "$metadata_file" ]]; then
    local compose_file compose_project
    compose_file="$(read_metadata_value "$metadata_file" "compose_file")"
    compose_project="$(read_metadata_value "$metadata_file" "compose_project")"
    if redis_metadata_matches_current "$metadata_file" && [[ -n "$compose_file" && -f "$compose_file" && -n "$compose_project" ]]; then
      (
        cd "$repo"
        docker compose -p "$compose_project" -f "$compose_file" exec -T redis redis-cli -n "$db_number" "$@"
      )
      return
    fi
  fi

  if ! command -v redis-cli >/dev/null 2>&1; then
    echo "redis-cli is required when the local Docker Redis metadata is absent." >&2
    return 1
  fi
  redis-cli -u "$BROADCASTER_REDIS_URL" "$@"
}

status_body="$(curl -sS --max-time 5 "$status_url")"
status_pointer_json="$(STATUS_BODY="$status_body" python3 <<'PY'
import json
import os

payload = json.loads(os.environ["STATUS_BODY"])
publisher = payload.get("redis_publisher")
if not isinstance(publisher, dict):
    raise SystemExit("broadcaster /status did not include redis_publisher")
if publisher.get("healthy") is not True:
    raise SystemExit("broadcaster redis_publisher is not healthy")
pointer = publisher.get("latest_snapshot_pointer")
if not isinstance(pointer, dict):
    raise SystemExit("broadcaster redis_publisher has no latest_snapshot_pointer")
stream_id = publisher.get("stream_id")
if not isinstance(stream_id, str) or not stream_id:
    raise SystemExit("broadcaster redis_publisher stream_id is missing")
print(json.dumps(pointer, sort_keys=True))
PY
)"

stream_len="$(redis_cli --raw XLEN "$BROADCASTER_REDIS_STREAM_KEY")"
if ! [[ "$stream_len" =~ ^[0-9]+$ ]] || ((stream_len == 0)); then
  echo "Redis stream $BROADCASTER_REDIS_STREAM_KEY has no entries." >&2
  exit 1
fi

pointer_json="$(redis_cli --raw GET "$BROADCASTER_REDIS_SNAPSHOT_KEY")"
pointer_range="$(compare_snapshot_pointers "$status_pointer_json" "$pointer_json")"
IFS=$'\t' read -r snapshot_start_entry_id snapshot_end_entry_id <<<"$pointer_range"

snapshot_start_probe="$(redis_cli --raw XRANGE "$BROADCASTER_REDIS_STREAM_KEY" "$snapshot_start_entry_id" "$snapshot_start_entry_id" COUNT 1)"
assert_entry_retained "$snapshot_start_probe" "$snapshot_start_entry_id"
snapshot_end_probe="$(redis_cli --raw XRANGE "$BROADCASTER_REDIS_STREAM_KEY" "$snapshot_end_entry_id" "$snapshot_end_entry_id" COUNT 1)"
assert_entry_retained "$snapshot_end_probe" "$snapshot_end_entry_id"

echo "Broadcaster Redis publisher is healthy."
echo "Status URL: $status_url"
echo "Redis stream key: $BROADCASTER_REDIS_STREAM_KEY"
echo "Redis stream entries: $stream_len"
echo "Redis publisher stream_id: $(STATUS_POINTER_JSON="$status_pointer_json" python3 -c 'import json, os; print(json.loads(os.environ["STATUS_POINTER_JSON"])["stream_id"])')"
echo "Snapshot pointer range: $snapshot_start_entry_id..$snapshot_end_entry_id"
