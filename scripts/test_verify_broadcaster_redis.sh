#!/usr/bin/env bash
set -euo pipefail

test_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cleanup_paths=()
cleanup() {
  rm -rf "${cleanup_paths[@]}"
}
trap cleanup EXIT

DSOLVER_VERIFY_BROADCASTER_REDIS_SOURCE_ONLY=1
# shellcheck disable=SC1091
source "$test_repo/scripts/verify_broadcaster_redis.sh"

TYCHO_BROADCASTER_URL=http://127.0.0.1:3001
[[ "$(derive_status_url)" == "http://127.0.0.1:3001/status" ]]
TYCHO_BROADCASTER_URL=https://broadcaster.example/prod/base
[[ "$(derive_status_url)" == "https://broadcaster.example/prod/base/status" ]]
unset TYCHO_BROADCASTER_URL

status_body='{"status":"ready","chain_id":8453,"redis_publisher":{"healthy":true,"mode":"active","stream_key":"dsolver:broadcaster:local:8453:events","stream_id":"chain-8453-stream-2","snapshot_id":"chain-8453-snapshot-2","replay_boundary":{"streamKey":"dsolver:broadcaster:local:8453:events","streamId":"chain-8453-stream-2","snapshotId":"chain-8453-snapshot-2","generation":2,"exclusiveMessageSeq":14}}}'
boundary_json="$(extract_replay_boundary "$status_body")"
[[ "$(boundary_entry_id "$boundary_json")" == "2-14" ]]
[[ "$(post_boundary_entry_id "$boundary_json")" == "2-15" ]]

if extract_replay_boundary "${status_body/\"mode\":\"active\"/\"mode\":\"passive\"}" >/dev/null 2>&1; then
  echo "expected passive redis_publisher mode to fail replay boundary parsing" >&2
  exit 1
fi

if extract_replay_boundary "${status_body/exclusiveMessageSeq/missingMessageSeq}" >/dev/null 2>&1; then
  echo "expected missing exclusiveMessageSeq to fail replay boundary parsing" >&2
  exit 1
fi

BROADCASTER_REDIS_URL=redis://127.0.0.1:6379/1
[[ "$(redis_db_number)" == "1" ]]
BROADCASTER_REDIS_URL=redis://127.0.0.1:6379
[[ "$(redis_db_number)" == "0" ]]

metadata_file="$(mktemp)"
cleanup_paths+=("$metadata_file")
{
  printf 'redis_url=%s\n' 'redis://127.0.0.1:6379/0'
  printf 'compose_file=%s\n' "$test_repo/docker-compose.redis.yml"
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

simulator_status_body() {
  local caught_up="$1"
  local gap_reason="$2"
  local generation="${3:-2}"
  local stream_key="${4:-dsolver:broadcaster:local:8453:events}"
  local stream_id="${5:-chain-8453-stream-$generation}"
  local snapshot_id="${6:-chain-8453-snapshot-$generation}"
  local exclusive_message_seq="${7:-14}"

  cat <<JSON
{"status":"ready","backends":{"native":{"enabled":true,"subscription":{"redis_replay_boundary":{"streamKey":"$stream_key","streamId":"$stream_id","snapshotId":"$snapshot_id","generation":$generation,"exclusiveMessageSeq":$exclusive_message_seq},"redis_replay_checkpoint":"$generation-$exclusive_message_seq","redis_replay_caught_up":$caught_up,"redis_gap_reason":$gap_reason}}}}
JSON
}

run_verifier_fixture() {
  local simulator_body="$1"
  local first_required_probe="$2"
  local first_retained_probe="$3"
  local output_file="$4"
  local fixture_dir fixture_repo

  fixture_dir="$(mktemp -d)"
  cleanup_paths+=("$fixture_dir")
  fixture_repo="$fixture_dir/repo"
  mkdir -p "$fixture_repo"

  printf '%s\n' "$status_body" > "$fixture_dir/broadcaster-status.json"
  printf '%s\n' "$simulator_body" > "$fixture_dir/simulator-status.json"
  printf '%s' "$first_required_probe" > "$fixture_dir/first-required-xrange.txt"
  printf '%s' "$first_retained_probe" > "$fixture_dir/first-retained-xrange.txt"

  cat > "$fixture_dir/curl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

url="${!#}"
case "$url" in
  http://broadcaster/status)
    cat "$FIXTURE_DIR/broadcaster-status.json"
    ;;
  http://simulator/status)
    cat "$FIXTURE_DIR/simulator-status.json"
    ;;
  *)
    echo "unexpected curl URL: $url" >&2
    exit 2
    ;;
esac
SH
  chmod +x "$fixture_dir/curl"

  cat > "$fixture_dir/redis-cli" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

emit_probe() {
  local probe_file="$1"
  local probe

  probe="$(cat "$probe_file")"
  case "$probe" in
    __FAIL__:*)
      echo "${probe#__FAIL__:}" >&2
      exit 9
      ;;
    *)
      printf '%s' "$probe"
      ;;
  esac
}

command=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -u)
      shift 2
      ;;
    --raw)
      shift
      ;;
    *)
      command="$1"
      shift
      break
      ;;
  esac
done

case "$command" in
  XLEN)
    printf '7\n'
    ;;
  XRANGE)
    key="$1"
    start="$2"
    end="$3"
    case "$key:$start:$end" in
      "dsolver:broadcaster:local:8453:events:2-15:2-15")
        emit_probe "$FIXTURE_DIR/first-required-xrange.txt"
        ;;
      "dsolver:broadcaster:local:8453:events:-:+")
        emit_probe "$FIXTURE_DIR/first-retained-xrange.txt"
        ;;
      *)
        echo "unexpected XRANGE: $key $start $end" >&2
        exit 3
        ;;
    esac
    ;;
  *)
    echo "unexpected redis-cli command: $command" >&2
    exit 4
    ;;
esac
SH
  chmod +x "$fixture_dir/redis-cli"

  FIXTURE_DIR="$fixture_dir" \
    PATH="$fixture_dir:$PATH" \
    BROADCASTER_REDIS_URL=redis://127.0.0.1:6379/0 \
    BROADCASTER_REDIS_STREAM_KEY=dsolver:broadcaster:local:8453:events \
    "$test_repo/scripts/verify_broadcaster_redis.sh" \
      --repo "$fixture_repo" \
      --status-url http://broadcaster/status \
      --simulator-status-url http://simulator/status \
      > "$output_file" 2>&1
}

output_file="$(mktemp)"
cleanup_paths+=("$output_file")

if ! run_verifier_fixture "$(simulator_status_body true null)" "2-15" "2-15" "$output_file"; then
  echo "expected trimmed boundary entry to pass when simulator replay is caught up" >&2
  cat "$output_file" >&2
  exit 1
fi

if ! run_verifier_fixture "$(simulator_status_body true null 2 dsolver:broadcaster:local:8453:events chain-8453-stream-2 chain-8453-snapshot-2 9)" "2-15" "2-15" "$output_file"; then
  echo "expected older same-generation bootstrap boundary to pass when simulator replay is caught up" >&2
  cat "$output_file" >&2
  exit 1
fi

if run_verifier_fixture "$(simulator_status_body true null 1)" "2-15" "2-15" "$output_file"; then
  echo "expected caught-up simulator on old Redis generation to fail" >&2
  exit 1
fi
if ! grep -q "does not match current broadcaster replay boundary generation=2" "$output_file"; then
  echo "expected old-generation simulator failure context" >&2
  cat "$output_file" >&2
  exit 1
fi

if run_verifier_fixture "$(simulator_status_body true null 2 dsolver:broadcaster:local:8453:events stale-stream chain-8453-snapshot-2)" "2-15" "2-15" "$output_file"; then
  echo "expected caught-up simulator with stale Redis stream id to fail" >&2
  exit 1
fi
if ! grep -q "does not match current broadcaster replay boundary streamId=chain-8453-stream-2" "$output_file"; then
  echo "expected stale stream id simulator failure context" >&2
  cat "$output_file" >&2
  exit 1
fi

if run_verifier_fixture "$(simulator_status_body false null)" "2-15" "2-14" "$output_file"; then
  echo "expected simulator status that is not caught up to fail" >&2
  exit 1
fi
if ! grep -q "simulator /status backend native has not caught up from Redis replay" "$output_file"; then
  echo "expected simulator not-caught-up failure context" >&2
  cat "$output_file" >&2
  exit 1
fi

if run_verifier_fixture "$(simulator_status_body false null)" "" "2-16" "$output_file"; then
  echo "expected missing post-boundary retained history to fail" >&2
  exit 1
fi
if ! grep -q "missing first required post-boundary entry 2-15" "$output_file"; then
  echo "expected missing post-boundary retained history context" >&2
  cat "$output_file" >&2
  exit 1
fi

if run_verifier_fixture "$(simulator_status_body false null)" "__FAIL__:redis unavailable during required probe" "2-16" "$output_file"; then
  echo "expected Redis inspection failure during required-entry probe to fail" >&2
  exit 1
fi
if ! grep -q "Redis retained history inspection failed:" "$output_file"; then
  echo "expected Redis inspection failure prefix" >&2
  cat "$output_file" >&2
  exit 1
fi
if ! grep -q "failed to inspect first required post-boundary entry 2-15" "$output_file"; then
  echo "expected required-entry inspection failure context" >&2
  cat "$output_file" >&2
  exit 1
fi
if ! grep -q "redis unavailable during required probe" "$output_file"; then
  echo "expected redis-cli failure output to be preserved" >&2
  cat "$output_file" >&2
  exit 1
fi

if run_verifier_fixture "$(simulator_status_body false null)" "" "__FAIL__:redis unavailable during retained probe" "$output_file"; then
  echo "expected Redis inspection failure during first-retained probe to fail" >&2
  exit 1
fi
if ! grep -q "Redis retained history inspection failed:" "$output_file"; then
  echo "expected Redis inspection failure prefix" >&2
  cat "$output_file" >&2
  exit 1
fi
if ! grep -q "failed to inspect first retained Redis entry after missing first required post-boundary entry 2-15" "$output_file"; then
  echo "expected first-retained inspection failure context" >&2
  cat "$output_file" >&2
  exit 1
fi
if ! grep -q "redis unavailable during retained probe" "$output_file"; then
  echo "expected first-retained redis-cli failure output to be preserved" >&2
  cat "$output_file" >&2
  exit 1
fi

echo "verify_broadcaster_redis helper tests passed"
