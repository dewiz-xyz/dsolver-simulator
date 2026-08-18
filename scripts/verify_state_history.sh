#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: verify_state_history.sh [--repo <path>]

Start the local Postgres + MinIO stack and verify the state-history storage contract.

Options:
  --repo      Path to repo root (default: current directory)
  -h, --help  Show this help
USAGE
}

repo="."

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      repo="$2"
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
compose_file="$repo/docker-compose.state-history.yml"
compose_project="dsolver-state-history"

if [[ ! -f "$compose_file" ]]; then
  echo "State history compose file not found at $compose_file." >&2
  exit 1
fi
if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required for the state history verifier." >&2
  exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
  echo "Docker Compose v2 is required for the state history verifier." >&2
  exit 1
fi

cleanup() {
  local exit_status=$?
  trap - EXIT
  echo "Stopping local state history storage stack..."
  (
    cd "$repo"
    docker compose -p "$compose_project" -f "$compose_file" down -v --remove-orphans
  ) || true
  exit "$exit_status"
}
trap cleanup EXIT

export DATABASE_URL="postgres://postgres:postgres@127.0.0.1:55432/state_history"
export STATE_HISTORY_MIGRATION_DATABASE_URL="postgres://state_history_migration:state-history-migration-test-password@127.0.0.1:55432/state_history"
export STATE_HISTORY_WRITER_PASSWORD="state-history-writer-test-password"
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"
export AWS_REGION="eu-central-1"
export AWS_DEFAULT_REGION="$AWS_REGION"
export AWS_ENDPOINT_URL_S3="http://127.0.0.1:59000"
export AWS_EC2_METADATA_DISABLED="true"
export STATE_HISTORY_S3_BUCKET="state-history-analysis"
export STATE_HISTORY_S3_PREFIX="smoke"
export STATE_HISTORY_S3_REGION="$AWS_REGION"
export STATE_HISTORY_S3_ENDPOINT_URL="$AWS_ENDPOINT_URL_S3"
export STATE_HISTORY_S3_FORCE_PATH_STYLE="true"

# The AWS SDK initializes TLS even for the local HTTP endpoint. Some macOS shells expose no native
# roots, so use the system PEM bundle unless the caller already selected a trust source.
if [[ "$(uname -s)" == "Darwin" && -z "${SSL_CERT_FILE:-}" && -r /etc/ssl/cert.pem ]]; then
  export SSL_CERT_FILE="/etc/ssl/cert.pem"
fi

echo "Starting local state history storage stack..."
(
  cd "$repo"
  docker compose -p "$compose_project" -f "$compose_file" up -d --wait --wait-timeout 120 postgres minio
  docker compose -p "$compose_project" -f "$compose_file" run --rm minio-init
  docker compose -p "$compose_project" -f "$compose_file" exec -T postgres \
    psql -U postgres -d state_history -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE rds_iam NOLOGIN;
CREATE ROLE state_history_migration
    LOGIN PASSWORD 'state-history-migration-test-password'
    CREATEDB CREATEROLE NOSUPERUSER NOREPLICATION NOBYPASSRLS;
GRANT rds_iam TO state_history_migration WITH ADMIN OPTION;
ALTER DATABASE state_history OWNER TO state_history_migration;
SQL
)

echo "Running the state history migration task twice..."
(
  cd "$repo"
  cargo run -p apps --bin state-history-migrate
  cargo run -p apps --bin state-history-migrate
)

echo "Running ignored state-history integration tests..."
(
  cd "$repo"
  cargo test -p state-history -- --ignored
)

echo "Running state-history analysis harness..."
(
  cd "$repo"
  cargo run -p apps --bin state-history-analysis
)

echo "State history storage contract verification passed."
