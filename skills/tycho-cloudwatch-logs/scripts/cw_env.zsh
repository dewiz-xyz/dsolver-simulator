#!/usr/bin/env zsh
set -e

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
env_file="${TYCHO_ENV_FILE:-$repo_root/.env}"

if [[ -z "${AWS_ACCESS_KEY_ID:-}" && -f "$env_file" ]]; then
  set -a
  source "$env_file"
  set +a
fi

# Valid log groups include /ecs/quoter-base/tycho-simulator, /ecs/solve-base/tycho-simulator, and /ecs/broadcaster-production/broadcaster.
: "${TYCHO_LOG_GROUP:=/ecs/quoter-base/tycho-simulator}"

if [[ -z "${AWS_REGION:-}" && -z "${AWS_DEFAULT_REGION:-}" ]]; then
  export AWS_REGION="eu-central-1"
  export AWS_DEFAULT_REGION="eu-central-1"
fi

export AWS_PAGER=""
