#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
PROJECT_NAME="${KORM_TESTING_PROJECT_NAME:-korm-testing}"
ENV_FILE="${KORM_TESTING_ENV_FILE:-${REPO_ROOT}/.env.testing.local}"

if docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(docker-compose)
else
  echo "docker compose is required but was not found." >&2
  exit 1
fi

KORM_TESTING_PG_USER="${KORM_TESTING_PG_USER:-korm}"
KORM_TESTING_PG_PASSWORD="${KORM_TESTING_PG_PASSWORD:-korm}"
KORM_TESTING_PG_DB="${KORM_TESTING_PG_DB:-korm}"
KORM_TESTING_PG_PORT="${KORM_TESTING_PG_PORT:-15432}"

KORM_TESTING_MYSQL_ROOT_PASSWORD="${KORM_TESTING_MYSQL_ROOT_PASSWORD:-korm-root}"
KORM_TESTING_MYSQL_USER="${KORM_TESTING_MYSQL_USER:-korm}"
KORM_TESTING_MYSQL_PASSWORD="${KORM_TESTING_MYSQL_PASSWORD:-korm}"
KORM_TESTING_MYSQL_DB="${KORM_TESTING_MYSQL_DB:-default}"
KORM_TESTING_MYSQL_PORT="${KORM_TESTING_MYSQL_PORT:-13306}"

KORM_TESTING_S3_ACCESS_KEY_ID="${KORM_TESTING_S3_ACCESS_KEY_ID:-korm}"
KORM_TESTING_S3_SECRET_ACCESS_KEY="${KORM_TESTING_S3_SECRET_ACCESS_KEY:-kormpass}"
KORM_TESTING_S3_PORT="${KORM_TESTING_S3_PORT:-19000}"
KORM_TESTING_S3_CONSOLE_PORT="${KORM_TESTING_S3_CONSOLE_PORT:-19001}"

export KORM_TESTING_PG_USER
export KORM_TESTING_PG_PASSWORD
export KORM_TESTING_PG_DB
export KORM_TESTING_PG_PORT
export KORM_TESTING_MYSQL_ROOT_PASSWORD
export KORM_TESTING_MYSQL_USER
export KORM_TESTING_MYSQL_PASSWORD
export KORM_TESTING_MYSQL_DB
export KORM_TESTING_MYSQL_PORT
export KORM_TESTING_S3_ACCESS_KEY_ID
export KORM_TESTING_S3_SECRET_ACCESS_KEY
export KORM_TESTING_S3_PORT
export KORM_TESTING_S3_CONSOLE_PORT

generate_encryption_key() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 32
    return
  fi
  if command -v python3 >/dev/null 2>&1; then
    python3 - <<'PY'
import secrets
print(secrets.token_hex(32))
PY
    return
  fi
  if command -v node >/dev/null 2>&1; then
    node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
    return
  fi
  echo "Missing openssl, python3, or node to generate KORM_ENCRYPTION_KEY." >&2
  return 1
}

KORM_ENCRYPTION_KEY="${KORM_ENCRYPTION_KEY:-}"
if [ -z "$KORM_ENCRYPTION_KEY" ]; then
  KORM_ENCRYPTION_KEY="$(generate_encryption_key)"
fi

TESTING_PG_URL="postgres://${KORM_TESTING_PG_USER}:${KORM_TESTING_PG_PASSWORD}@127.0.0.1:${KORM_TESTING_PG_PORT}/${KORM_TESTING_PG_DB}"
TESTING_MYSQL_URL="mysql://${KORM_TESTING_MYSQL_USER}:${KORM_TESTING_MYSQL_PASSWORD}@127.0.0.1:${KORM_TESTING_MYSQL_PORT}/${KORM_TESTING_MYSQL_DB}"
TESTING_S3_ENDPOINT="http://127.0.0.1:${KORM_TESTING_S3_PORT}"
TESTING_S3_ACCESS_KEY_ID="${KORM_TESTING_S3_ACCESS_KEY_ID}"
TESTING_S3_SECRET_ACCESS_KEY="${KORM_TESTING_S3_SECRET_ACCESS_KEY}"

write_env_file() {
  cat >"$ENV_FILE" <<EOF
TESTING_PG_URL=${TESTING_PG_URL}
TESTING_MYSQL_URL=${TESTING_MYSQL_URL}
TESTING_S3_ENDPOINT=${TESTING_S3_ENDPOINT}
TESTING_S3_ACCESS_KEY_ID=${TESTING_S3_ACCESS_KEY_ID}
TESTING_S3_SECRET_ACCESS_KEY=${TESTING_S3_SECRET_ACCESS_KEY}
KORM_ENCRYPTION_KEY=${KORM_ENCRYPTION_KEY}
EOF
}

wait_for_pg() {
  local retries=30
  until "${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" -p "$PROJECT_NAME" exec -T pg \
    pg_isready -U "$KORM_TESTING_PG_USER" -d "$KORM_TESTING_PG_DB" >/dev/null 2>&1; do
    retries=$((retries - 1))
    if [ "$retries" -le 0 ]; then
      echo "Postgres did not become ready." >&2
      return 1
    fi
    sleep 1
  done
}

wait_for_mysql() {
  local retries=30
  until "${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" -p "$PROJECT_NAME" exec -T mysql \
    mysqladmin ping -h 127.0.0.1 -u"$KORM_TESTING_MYSQL_USER" -p"$KORM_TESTING_MYSQL_PASSWORD" \
    >/dev/null 2>&1; do
    retries=$((retries - 1))
    if [ "$retries" -le 0 ]; then
      echo "MySQL did not become ready." >&2
      return 1
    fi
    sleep 1
  done
}

wait_for_minio() {
  if ! command -v curl >/dev/null 2>&1; then
    return 0
  fi
  local retries=30
  local url="http://127.0.0.1:${KORM_TESTING_S3_PORT}/minio/health/ready"
  until curl -fsS "$url" >/dev/null 2>&1; do
    retries=$((retries - 1))
    if [ "$retries" -le 0 ]; then
      echo "MinIO did not become ready." >&2
      return 1
    fi
    sleep 1
  done
}

print_env() {
  cat <<EOF
export TESTING_PG_URL=${TESTING_PG_URL}
export TESTING_MYSQL_URL=${TESTING_MYSQL_URL}
export TESTING_S3_ENDPOINT=${TESTING_S3_ENDPOINT}
export TESTING_S3_ACCESS_KEY_ID=${TESTING_S3_ACCESS_KEY_ID}
export TESTING_S3_SECRET_ACCESS_KEY=${TESTING_S3_SECRET_ACCESS_KEY}
export KORM_ENCRYPTION_KEY=${KORM_ENCRYPTION_KEY}
EOF
}

usage() {
  cat <<EOF
Usage: $(basename "$0") [up|down|env]

Commands:
  up     Start Postgres, MySQL, and MinIO for local integration/hostile tests.
  down   Stop the containers and remove them.
  env    Print export statements for the TESTING_* variables.
EOF
}

command="${1:-up}"

case "$command" in
  up)
    "${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" -p "$PROJECT_NAME" up -d
    wait_for_pg
    wait_for_mysql
    wait_for_minio
    write_env_file
    echo "Wrote ${ENV_FILE}."
    echo "To use it:"
    echo "  set -a; source ${ENV_FILE}; set +a"
    echo "  ${SCRIPT_DIR}/stage-resources.sh env"
    ;;
  down)
    "${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" -p "$PROJECT_NAME" down
    ;;
  env)
    print_env
    ;;
  *)
    usage
    exit 1
    ;;
esac
