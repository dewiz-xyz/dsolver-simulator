#!/usr/bin/env zsh
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
source "$script_dir/cw_env.zsh"

usage() {
  cat <<'USAGE'
Usage: cw_metrics.zsh [--since 12h] [--until now]
                     [--metrics memory,cpu]
                     [--cluster quoter-base]
                     [--service tycho-simulator]
                     [--task-ids id1,id2]
                     [--period 60] [--stat Average|Maximum|Minimum]
                     [--output table|json|tsv]
                     [--mode summary|series|both]
                     [--pivot 2026-02-04T11:38:00Z] [--window 10m]

Simulator defaults use cluster quoter-base and service tycho-simulator.
The shared producer uses cluster broadcaster-production and service broadcaster.
USAGE
}

args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      args+=("$1")
      shift
      ;;
  esac
done

python3 "$script_dir/cw_metrics.py" "${args[@]}"
