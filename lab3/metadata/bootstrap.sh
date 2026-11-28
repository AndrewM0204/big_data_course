#!/usr/bin/env bash
# Metadata-этап ЛР3: PostgreSQL ingestion и lineage в OpenMetadata.
# Секреты берутся из окружения или .env/LR3_ENV_FILE и не печатаются.
#
# Использование:
#   bash lab3/metadata/bootstrap.sh
#   bash lab3/metadata/bootstrap.sh --with-dashboard
#   bash lab3/metadata/bootstrap.sh --no-reader
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
cd -- "$ROOT_DIR"

BASE_COMPOSE="docker-compose.yaml"
LR3_COMPOSE="docker-compose.lr3.yml"
POSTGRES_SERVICE="${POSTGRES_SERVICE:-postgres}"
OM_SERVER_SERVICE_DEFAULT="lr3-openmetadata"
INGESTION_SERVICE_DEFAULT="lr3-ingestion"
WAIT_SECONDS="${WAIT_SECONDS:-120}"

WITH_DASHBOARD=0
WITH_READER=1

log() { printf '[om] %s\n' "$*"; }
die() { printf '[om] ОШИБКА: %s\n' "$*" >&2; exit 1; }

usage() {
    cat <<'EOF'
Metadata-этап ЛР3: OpenMetadata PostgreSQL ingestion и lineage.

Опции:
  --with-dashboard  создать dashboard service/dashboard и связи к BI-вью
  --no-reader       не создавать/обновлять роль om_reader
  -h, --help        показать эту справку

Переменные окружения:
  LR3_ENV_FILE              дополнительный env-файл compose (необязательно)
  OM_SERVER_URL             API OpenMetadata (по умолчанию http://lr3-openmetadata:8585/api)
  OM_ADMIN_EMAIL            администратор OpenMetadata (обязателен)
  OM_ADMIN_PASSWORD         пароль администратора (обязателен, не печатается)
  OM_POSTGRES_SERVICE       имя сервиса в OpenMetadata (по умолчанию steam-postgres)
  OM_POSTGRES_HOST          хост PostgreSQL (по умолчанию postgres)
  OM_POSTGRES_PORT          порт PostgreSQL (по умолчанию 5432)
  OM_POSTGRES_DB            база PostgreSQL (по умолчанию airflow)
  OM_POSTGRES_USER          роль ingestion (рекомендуется om_reader)
  OM_POSTGRES_PASSWORD      пароль роли ingestion (не печатается)
  OM_SERVER_SERVICE         имя сервиса OpenMetadata (по умолчанию lr3-openmetadata)
  OM_INGESTION_SERVICE      CLI-сервис ingestion (по умолчанию lr3-ingestion)
  OM_WITH_DASHBOARD=1       то же, что --with-dashboard
EOF
}

while (($#)); do
    case "$1" in
        --with-dashboard) WITH_DASHBOARD=1 ;;
        --no-reader) WITH_READER=0 ;;
        -h | --help)
            usage
            exit 0
            ;;
        *) die "Неизвестная опция: $1" ;;
    esac
    shift
done

read_env_key() {
    local key="$1" file line
    if [[ -n "${!key:-}" ]]; then
        printf '%s' "${!key}"
        return 0
    fi
    local files=()
    [[ -n "${LR3_ENV_FILE:-}" ]] && files+=("$LR3_ENV_FILE")
    files+=(".env")
    for file in "${files[@]}"; do
        [[ -r "$file" ]] || continue
        line="$(grep -m1 -E "^[[:space:]]*${key}=" "$file" 2>/dev/null || true)"
        [[ -n "$line" ]] || continue
        line="${line#*=}"
        line="${line%$'\r'}"
        line="${line#\"}"
        line="${line%\"}"
        line="${line#\'}"
        line="${line%\'}"
        printf '%s' "$line"
        return 0
    done
    return 1
}

COMPOSE_ARGS=(-f "$BASE_COMPOSE")
if [[ -f "$LR3_COMPOSE" ]]; then
    COMPOSE_ARGS+=(-f "$LR3_COMPOSE")
fi
if [[ -n "${LR3_ENV_FILE:-}" ]]; then
    COMPOSE_ARGS+=(--env-file "$LR3_ENV_FILE")
fi
compose() { docker compose "${COMPOSE_ARGS[@]}" "$@"; }

command -v docker >/dev/null 2>&1 || die "Docker CLI не найден в PATH."
docker info >/dev/null 2>&1 || die "Docker daemon недоступен."
docker compose version >/dev/null 2>&1 || die "Нужен Docker Compose v2."

PROFILES="$(compose config --profiles 2>/dev/null | tr -d '\r' || true)"
if grep -qx 'lr3' <<<"$PROFILES"; then
    COMPOSE_ARGS+=(--profile lr3)
fi
if grep -qx 'ingestion' <<<"$PROFILES"; then
    COMPOSE_ARGS+=(--profile ingestion)
fi

if ! SERVICES="$(compose config --services 2>/dev/null | tr -d '\r')"; then
    die "Не удалось разобрать compose. Проверьте LR3_* и LR3_ENV_FILE."
fi
grep -qx "$POSTGRES_SERVICE" <<<"$SERVICES" ||
    die "В compose нет сервиса $POSTGRES_SERVICE."

OM_SERVER_URL="$(read_env_key OM_SERVER_URL || true)"
OM_SERVER_URL="${OM_SERVER_URL:-http://lr3-openmetadata:8585/api}"
OM_ADMIN_EMAIL="$(read_env_key OM_ADMIN_EMAIL || true)"
OM_ADMIN_PASSWORD="$(read_env_key OM_ADMIN_PASSWORD || true)"
OM_POSTGRES_SERVICE="$(read_env_key OM_POSTGRES_SERVICE || true)"
OM_POSTGRES_SERVICE="${OM_POSTGRES_SERVICE:-steam-postgres}"
OM_POSTGRES_HOST="$(read_env_key OM_POSTGRES_HOST || true)"
OM_POSTGRES_HOST="${OM_POSTGRES_HOST:-postgres}"
OM_POSTGRES_PORT="$(read_env_key OM_POSTGRES_PORT || true)"
OM_POSTGRES_PORT="${OM_POSTGRES_PORT:-5432}"
OM_POSTGRES_DB="$(read_env_key OM_POSTGRES_DB || true)"
OM_POSTGRES_DB="${OM_POSTGRES_DB:-airflow}"
OM_POSTGRES_USER="$(read_env_key OM_POSTGRES_USER || true)"
OM_POSTGRES_PASSWORD="$(read_env_key OM_POSTGRES_PASSWORD || true)"
OM_SERVER_SERVICE="$(read_env_key OM_SERVER_SERVICE || true)"
OM_SERVER_SERVICE="${OM_SERVER_SERVICE:-$OM_SERVER_SERVICE_DEFAULT}"
OM_INGESTION_SERVICE="$(read_env_key OM_INGESTION_SERVICE || true)"
OM_INGESTION_SERVICE="${OM_INGESTION_SERVICE:-$INGESTION_SERVICE_DEFAULT}"

if [[ "$(read_env_key OM_WITH_DASHBOARD || true)" == "1" ]]; then
    WITH_DASHBOARD=1
fi

[[ -n "$OM_ADMIN_EMAIL" && -n "$OM_ADMIN_PASSWORD" ]] ||
    die "Не заданы OM_ADMIN_EMAIL/OM_ADMIN_PASSWORD."
[[ -n "$OM_POSTGRES_USER" && -n "$OM_POSTGRES_PASSWORD" ]] ||
    die "Не заданы OM_POSTGRES_USER/OM_POSTGRES_PASSWORD."

grep -qx "$OM_SERVER_SERVICE" <<<"$SERVICES" ||
    die "В compose нет сервиса $OM_SERVER_SERVICE."
grep -qx "$OM_INGESTION_SERVICE" <<<"$SERVICES" ||
    die "В compose нет сервиса $OM_INGESTION_SERVICE."

if [[ -z "$(compose ps -q "$OM_SERVER_SERVICE" 2>/dev/null || true)" ]]; then
    die "Сервис $OM_SERVER_SERVICE не запущен."
fi

log "Ожидаю PostgreSQL..."
POSTGRES_READY=0
for _ in $(seq 1 $((WAIT_SECONDS / 2))); do
    if compose exec -T "$POSTGRES_SERVICE" \
        pg_isready -U airflow -d airflow >/dev/null 2>&1; then
        POSTGRES_READY=1
        break
    fi
    sleep 2
done
((POSTGRES_READY)) || die "PostgreSQL не готов за ${WAIT_SECONDS} с."

if ((WITH_READER)); then
    log "Создаю/обновляю роль om_reader..."
    export OM_POSTGRES_PASSWORD
    compose exec -T -e OM_POSTGRES_PASSWORD "$POSTGRES_SERVICE" psql \
        -U airflow -d airflow -X -q -1 -v ON_ERROR_STOP=1 \
        <"lab3/metadata/sql/create_om_reader.sql" ||
        die "Не удалось создать роль om_reader."
fi

export OM_SERVER_URL
export OM_ADMIN_EMAIL
export OM_ADMIN_PASSWORD
export OM_POSTGRES_SERVICE
export OM_POSTGRES_HOST
export OM_POSTGRES_PORT
export OM_POSTGRES_DB
export OM_POSTGRES_USER
export OM_POSTGRES_PASSWORD

log "Запускаю PostgreSQL ingestion..."
compose run --rm --no-deps -T \
    --entrypoint /bin/bash \
    -v ./lab3/metadata/ingestion:/lab3/ingestion:ro \
    -e OM_SERVER_URL \
    -e OM_ADMIN_EMAIL \
    -e OM_ADMIN_PASSWORD \
    -e OM_POSTGRES_SERVICE \
    -e OM_POSTGRES_HOST \
    -e OM_POSTGRES_PORT \
    -e OM_POSTGRES_DB \
    -e OM_POSTGRES_USER \
    -e OM_POSTGRES_PASSWORD \
    "$OM_INGESTION_SERVICE" /lab3/ingestion/ingest_postgres.sh ||
    die "PostgreSQL ingestion завершился ошибкой."

LINEAGE_ARGS="/lab3/lineage/apply_lineage.py /lab3/lineage/lineage.json"
if ((WITH_DASHBOARD)); then
    LINEAGE_ARGS="$LINEAGE_ARGS --with-dashboard"
fi

log "Применяю lineage..."
compose run --rm --no-deps -T \
    --entrypoint /bin/bash \
    -v ./lab3/metadata/lineage:/lab3/lineage:ro \
    -e OM_SERVER_URL \
    -e OM_ADMIN_EMAIL \
    -e OM_ADMIN_PASSWORD \
    -e OM_POSTGRES_SERVICE \
    -e OM_POSTGRES_DB \
    "$OM_INGESTION_SERVICE" -c "python $LINEAGE_ARGS" ||
    die "Применение lineage завершилось ошибкой."

log "Metadata-этап завершён."
