#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: start_server.sh [--repo <path>] [--log-file <path>] [--chain-id <id>] [--env KEY=VALUE] [--enable-vm-pools]

Start the dsolver-simulator-service from a repo checkout.

Options:
  --repo             Path to repo root (default: current directory)
  --log-file         Log file path (default: <repo>/logs/tycho-sim-server.log)
  --chain-id         Runtime chain id (1 or 8453). Overrides CHAIN_ID from env/.env.
  --env              Export KEY=VALUE before starting (repeatable)
  --enable-vm-pools  Shortcut for --env ENABLE_VM_POOLS=true
  -h, --help         Show this help
USAGE
}

validate_chain_id() {
  case "$1" in
    1|8453) ;;
    *)
      echo "Error: unsupported chain id '$1'. Supported values: 1 (Ethereum), 8453 (Base)." >&2
      return 1
      ;;
  esac
}

repo="."
log_file=""
chain_id_arg=""
env_overrides=()
rpc_url_overridden=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      repo="$2"
      shift 2
      ;;
    --log-file)
      log_file="$2"
      shift 2
      ;;
    --chain-id)
      chain_id_arg="$2"
      shift 2
      ;;
    --env)
      if [[ "$2" == RPC_URL=* ]]; then
        rpc_url_overridden=true
      fi
      env_overrides+=("$2")
      shift 2
      ;;
    --enable-vm-pools)
      env_overrides+=("ENABLE_VM_POOLS=true")
      shift 1
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

if [[ ! -f "$repo/Cargo.toml" ]]; then
  echo "Error: Cargo.toml not found in $repo" >&2
  exit 1
fi

pid_file="$repo/.tycho-sim-server.pid"
if [[ -f "$pid_file" ]]; then
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "Server already running (pid $pid)."
    exit 0
  fi
  rm -f "$pid_file"
fi

if [[ -f "$repo/.env" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$repo/.env"
  set +a
else
  echo "Warning: .env not found; ensure TYCHO_API_KEY is set." >&2
fi

if [[ -z "${TYCHO_API_KEY:-}" ]]; then
  echo "Warning: TYCHO_API_KEY not set; server may fail to start." >&2
fi

if [[ -z "${RUST_LOG:-}" ]]; then
  export RUST_LOG=info
fi

if ((${#env_overrides[@]})); then
  for pair in "${env_overrides[@]}"; do
    export "$pair"
  done
fi

if [[ -n "$chain_id_arg" ]]; then
  export CHAIN_ID="$chain_id_arg"
fi

if [[ -z "${CHAIN_ID:-}" ]]; then
  echo "Error: missing chain id. Pass --chain-id or set CHAIN_ID in env/.env." >&2
  exit 2
fi
validate_chain_id "$CHAIN_ID"

if [[ "$CHAIN_ID" == "8453" && "$rpc_url_overridden" == false && -n "${BASE_RPC_URL:-}" ]]; then
  export RPC_URL="$BASE_RPC_URL"
fi

if [[ -z "$log_file" ]]; then
  mkdir -p "$repo/logs"
  log_file="$repo/logs/tycho-sim-server.log"
fi

(
  cd "$repo"
  # `cargo run --release` intentionally relies on Cargo.toml's default-run.
  nohup cargo run --release > "$log_file" 2>&1 &
  echo $! > "$pid_file"
)

echo "Started dsolver-simulator-service."
echo "Chain ID: $CHAIN_ID"
echo "PID: $(cat "$pid_file")"
echo "Log: $log_file"
echo "Tip: tail -f $log_file"
