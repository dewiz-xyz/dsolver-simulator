#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: wait_ready.sh [--url <status_url>] [--timeout <seconds>] [--interval <seconds>] [--expect-chain-id <id>] [--require-vm-ready] [--require-vm-pools-min <count>] [--require-rfq-ready] [--require-rfq-pools-min <count>]

Poll the /status endpoint until native readiness is ready or times out.

Options:
  --url                  Status URL (default: http://localhost:3000/status)
  --timeout              Timeout in seconds (default: 180)
  --interval             Poll interval in seconds (default: 2)
  --expect-chain-id      Require /status.chain_id to match this chain id
  --require-vm-ready     Require backends.vm.status=ready before succeeding
  --require-vm-pools-min Minimum backends.vm.pool_count required when --require-vm-ready is set (default: 1)
  --require-rfq-ready    Require backends.rfq.status=ready before succeeding
  --require-rfq-pools-min Minimum backends.rfq.pool_count required when --require-rfq-ready is set (default: 1)
  -h, --help             Show this help
USAGE
}

url="http://localhost:3000/status"
timeout=180
interval=2
expect_chain_id=""
require_vm_ready="false"
require_vm_pools_min=1
require_rfq_ready="false"
require_rfq_pools_min=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --url)
      url="$2"
      shift 2
      ;;
    --timeout)
      timeout="$2"
      shift 2
      ;;
    --interval)
      interval="$2"
      shift 2
      ;;
    --expect-chain-id)
      expect_chain_id="$2"
      shift 2
      ;;
    --require-vm-ready)
      require_vm_ready="true"
      shift 1
      ;;
    --require-vm-pools-min)
      require_vm_pools_min="$2"
      shift 2
      ;;
    --require-rfq-ready)
      require_rfq_ready="true"
      shift 1
      ;;
    --require-rfq-pools-min)
      require_rfq_pools_min="$2"
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

start_time="$(date +%s)"

while true; do
  response="$(curl -s -w "\n%{http_code}" "$url" || true)"
  body="${response%$'\n'*}"
  status_code="${response##*$'\n'}"

  if check_output="$(python3 -c '
import json
import sys

status_code = sys.argv[1]
require_vm = sys.argv[2] == "true"
vm_pools_min = int(sys.argv[3])
require_rfq = sys.argv[4] == "true"
rfq_pools_min = int(sys.argv[5])
expected_chain_raw = sys.argv[6].strip()
expected_chain = int(expected_chain_raw) if expected_chain_raw else None

try:
    payload = json.load(sys.stdin)
except Exception:
    raise SystemExit(2)

if expected_chain is not None:
    actual_chain = payload.get("chain_id")
    if actual_chain != expected_chain:
        print(f"expected chain_id={expected_chain}, got {actual_chain}")
        raise SystemExit(42)

if status_code != "200":
    raise SystemExit(2)

backends = payload.get("backends") or {}
native = backends.get("native") or {}

if native.get("status") != "ready":
    raise SystemExit(2)

def require_backend(kind, pools_min):
    backend = backends.get(kind) or {}
    if not backend.get("enabled"):
        raise SystemExit(2)
    if backend.get("status") != "ready":
        raise SystemExit(2)
    if int(backend.get("pool_count") or 0) < pools_min:
        raise SystemExit(2)

if require_vm:
    require_backend("vm", vm_pools_min)

if require_rfq:
    require_backend("rfq", rfq_pools_min)
' "$status_code" "$require_vm_ready" "$require_vm_pools_min" "$require_rfq_ready" "$require_rfq_pools_min" "$expect_chain_id" <<<"$body" 2>&1)"
  then
    echo "ready"
    exit 0
  else
    check_code=$?
    if [[ "$check_code" -eq 42 ]]; then
      echo "Chain mismatch while waiting for readiness: $check_output" >&2
      exit 1
    fi
  fi

  now="$(date +%s)"
  if (( now - start_time >= timeout )); then
    echo "Timed out waiting for readiness." >&2
    echo "Last response ($status_code): $body" >&2
    exit 1
  fi

  sleep "$interval"
done
