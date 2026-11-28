#!/usr/bin/env bash
# Metadata-этап ЛР3: PostgreSQL ingestion и lineage в OpenMetadata.
# Работает с базовым docker-compose.yaml (сервисы openmetadata-*).
# Секреты берутся из окружения или .env и не печатаются.
#
# Использование:
#   bash lab3/metadata/bootstrap.sh
#   bash lab3/metadata/bootstrap.sh --no-dashboard
#   bash lab3/metadata/bootstrap.sh --no-reader
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
cd -- "$ROOT_DIR"

BASE_COMPOSE="docker-compose.yaml"
POSTGRES_SERVICE="${POSTGRES_SERVICE:-postgres}"
OM_SERVER_SERVICE_DEFAULT="openmetadata-server"
OM_INGESTION_SERVICE_DEFAULT="openmetadata-ingestion"
OM_LINEAGE_SERVICE_DEFAULT="openmetadata-lineage"
WAIT_SECONDS="${WAIT_SECONDS:-180}"

WITH_DASHBOARD=1
DASHBOARD_OVERRIDE=0
WITH_READER=1

log() { printf '[om] %s\n' "$*"; }
warn() { printf '[om] ВНИМАНИЕ: %s\n' "$*" >&2; }
die() { printf '[om] ОШИБКА: %s\n' "$*" >&2; exit 1; }

usage() {
    cat <<'EOF'
Metadata-этап ЛР3: OpenMetadata PostgreSQL ingestion и lineage.

Опции:
  --no-dashboard  не создавать dashboard service/dashboard и связи к BI-вью
  --no-reader     не создавать/обновлять роль om_reader
  -h, --help      показать эту справку

Переменные окружения (или строки в .env):
  OM_SERVER_URL             API OpenMetadata (по умолчанию http://openmetadata-server:8585/api)
  OM_ADMIN_EMAIL            администратор OpenMetadata
  OM_ADMIN_PASSWORD         пароль администратора (обязателен, не печатается)
  OPENMETADATA_ADMIN_EMAIL  запасное имя администратора
  OPENMETADATA_ADMIN_PASSWORD
  OM_POSTGRES_SERVICE       имя сервиса в OpenMetadata (steam-postgres)
  OM_POSTGRES_HOST          хост PostgreSQL (postgres)
  OM_POSTGRES_PORT          порт PostgreSQL (5432)
  OM_POSTGRES_DB            база PostgreSQL (airflow)
  OM_POSTGRES_USER          роль ingestion (om_reader; иное только с --no-reader)
  OM_POSTGRES_PASSWORD      пароль роли ingestion (обязателен)
  OM_SERVER_SERVICE         сервис OpenMetadata (openmetadata-server)
  OM_INGESTION_SERVICE      CLI-сервис ingestion (openmetadata-ingestion)
  OM_LINEAGE_SERVICE        сервис lineage (openmetadata-lineage)
  OM_WITH_DASHBOARD=1       то же, что включённый dashboard (по умолчанию)
  LR3_ENV_FILE              дополнительный файл только для чтения секретов
EOF
}

while (($#)); do
    case "$1" in
        --with-dashboard) WITH_DASHBOARD=1; DASHBOARD_OVERRIDE=1 ;;
        --no-dashboard) WITH_DASHBOARD=0; DASHBOARD_OVERRIDE=1 ;;
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

env_or_fallback() {
    local primary="$1" fallback="$2"
    local value
    value="$(read_env_key "$primary" || true)"
    if [[ -z "$value" ]]; then
        value="$(read_env_key "$fallback" || true)"
    fi
    printf '%s' "$value"
}

COMPOSE_ARGS=(-f "$BASE_COMPOSE")
compose() { docker compose "${COMPOSE_ARGS[@]}" "$@"; }

command -v docker >/dev/null 2>&1 || die "Docker CLI не найден в PATH."
docker info >/dev/null 2>&1 || die "Docker daemon недоступен."
docker compose version >/dev/null 2>&1 || die "Нужен Docker Compose v2."

PROFILES="$(compose config --profiles 2>/dev/null | tr -d '\r' || true)"
for profile in ingestion lineage; do
    if grep -qx "$profile" <<<"$PROFILES"; then
        COMPOSE_ARGS+=(--profile "$profile")
    fi
done

if ! SERVICES="$(compose config --services 2>/dev/null | tr -d '\r')"; then
    die "Не удалось разобрать $BASE_COMPOSE. Проверьте .env и YAML."
fi
for service in "$POSTGRES_SERVICE" "$OM_SERVER_SERVICE_DEFAULT" \
    "$OM_INGESTION_SERVICE_DEFAULT" "$OM_LINEAGE_SERVICE_DEFAULT"; do
    grep -qx "$service" <<<"$SERVICES" ||
        die "В compose нет сервиса $service."
done

OM_SERVER_URL="$(read_env_key OM_SERVER_URL || true)"
OM_SERVER_URL="${OM_SERVER_URL:-http://openmetadata-server:8585/api}"
OM_ADMIN_EMAIL="$(env_or_fallback OM_ADMIN_EMAIL OPENMETADATA_ADMIN_EMAIL)"
OM_ADMIN_PASSWORD="$(env_or_fallback OM_ADMIN_PASSWORD OPENMETADATA_ADMIN_PASSWORD)"
OM_ADMIN_EMAIL="${OM_ADMIN_EMAIL:-admin@open-metadata.org}"
OM_POSTGRES_SERVICE="$(read_env_key OM_POSTGRES_SERVICE || true)"
OM_POSTGRES_SERVICE="${OM_POSTGRES_SERVICE:-steam-postgres}"
OM_POSTGRES_HOST="$(read_env_key OM_POSTGRES_HOST || true)"
OM_POSTGRES_HOST="${OM_POSTGRES_HOST:-postgres}"
OM_POSTGRES_PORT="$(read_env_key OM_POSTGRES_PORT || true)"
OM_POSTGRES_PORT="${OM_POSTGRES_PORT:-5432}"
OM_POSTGRES_DB="$(read_env_key OM_POSTGRES_DB || true)"
OM_POSTGRES_DB="${OM_POSTGRES_DB:-airflow}"
OM_POSTGRES_USER="$(read_env_key OM_POSTGRES_USER || true)"
OM_POSTGRES_USER="${OM_POSTGRES_USER:-om_reader}"
OM_POSTGRES_PASSWORD="$(read_env_key OM_POSTGRES_PASSWORD || true)"
OM_SERVER_SERVICE="$(read_env_key OM_SERVER_SERVICE || true)"
OM_SERVER_SERVICE="${OM_SERVER_SERVICE:-$OM_SERVER_SERVICE_DEFAULT}"
OM_INGESTION_SERVICE="$(read_env_key OM_INGESTION_SERVICE || true)"
OM_INGESTION_SERVICE="${OM_INGESTION_SERVICE:-$OM_INGESTION_SERVICE_DEFAULT}"
OM_LINEAGE_SERVICE="$(read_env_key OM_LINEAGE_SERVICE || true)"
OM_LINEAGE_SERVICE="${OM_LINEAGE_SERVICE:-$OM_LINEAGE_SERVICE_DEFAULT}"

if ((DASHBOARD_OVERRIDE == 0)) &&
    [[ "$(read_env_key OM_WITH_DASHBOARD || true)" == "0" ]]; then
    WITH_DASHBOARD=0
fi

[[ -n "$OM_ADMIN_EMAIL" && -n "$OM_ADMIN_PASSWORD" ]] ||
    die "Не заданы OM_ADMIN_EMAIL/OM_ADMIN_PASSWORD."
[[ -n "$OM_POSTGRES_USER" && -n "$OM_POSTGRES_PASSWORD" ]] ||
    die "Не заданы OM_POSTGRES_USER/OM_POSTGRES_PASSWORD (значение пароля не выводится)."
if ((WITH_READER)) && [[ "$OM_POSTGRES_USER" != "om_reader" ]]; then
    die "При создании роли OM_POSTGRES_USER должен быть om_reader: SQL создаёт только эту роль. Используйте --no-reader для готовой другой роли."
fi
for secret_name in OM_ADMIN_PASSWORD OM_POSTGRES_PASSWORD; do
    lower="$(printf '%s' "${!secret_name}" | tr '[:upper:]' '[:lower:]')"
    case "$lower" in
        *change*me* | *placeholder*)
            die "$secret_name содержит заглушку; задайте безопасное значение в .env."
            ;;
    esac
done
[[ "$OM_ADMIN_PASSWORD" != "admin" ]] ||
    warn "Используется стандартный пароль OpenMetadata admin; для защиты смените его в compose и .env."

if [[ -z "$(compose ps -q "$OM_SERVER_SERVICE" 2>/dev/null || true)" ]]; then
    die "Сервис $OM_SERVER_SERVICE не запущен. Запустите docker compose up -d."
fi
if [[ -z "$(compose ps -q "$POSTGRES_SERVICE" 2>/dev/null || true)" ]]; then
    die "Сервис $POSTGRES_SERVICE не запущен."
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

log "Ожидаю OpenMetadata..."
OM_READY=0
for _ in $(seq 1 $((WAIT_SECONDS / 2))); do
    if compose exec -T "$OM_SERVER_SERVICE" \
        wget -q --spider http://localhost:8586/healthcheck >/dev/null 2>&1; then
        OM_READY=1
        break
    fi
    sleep 2
done
((OM_READY)) || die "OpenMetadata не готов за ${WAIT_SECONDS} с."

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
export OM_WITH_DASHBOARD="$WITH_DASHBOARD"

log "Запускаю PostgreSQL ingestion..."
compose run --rm --no-deps -T \
    -e OM_SERVER_URL \
    -e OM_ADMIN_EMAIL \
    -e OM_ADMIN_PASSWORD \
    -e OM_POSTGRES_SERVICE \
    -e OM_POSTGRES_HOST \
    -e OM_POSTGRES_PORT \
    -e OM_POSTGRES_DB \
    -e OM_POSTGRES_USER \
    -e OM_POSTGRES_PASSWORD \
    "$OM_INGESTION_SERVICE" ||
    die "PostgreSQL ingestion завершился ошибкой."

log "Применяю lineage..."
compose run --rm --no-deps -T \
    -e OM_SERVER_URL \
    -e OM_ADMIN_EMAIL \
    -e OM_ADMIN_PASSWORD \
    -e OM_POSTGRES_SERVICE \
    -e OM_POSTGRES_DB \
    -e OM_WITH_DASHBOARD \
    "$OM_LINEAGE_SERVICE" ||
    die "Применение lineage завершилось ошибкой."

log "Metadata-этап завершён."
