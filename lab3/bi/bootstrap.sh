#!/usr/bin/env bash
# BI-этап ЛР3: представления, роль чтения и Superset-объекты.
# Работает с базовым docker-compose.yaml (сервисы postgres и superset).
# Секреты берутся из окружения или .env и не печатаются.
#
# Использование:
#   bash lab3/bi/bootstrap.sh              # всё, нужен запущенный Superset
#   bash lab3/bi/bootstrap.sh --sql-only   # только SQL-представления и роль
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
cd -- "$ROOT_DIR"

BASE_COMPOSE="docker-compose.yaml"
BI_SQL_DIR="lab3/bi/sql"
SUPERSET_SCRIPT="lab3/bi/superset/bootstrap_superset.py"
POSTGRES_SERVICE="${POSTGRES_SERVICE:-postgres}"
SUPERSET_SERVICE_DEFAULT="superset"
WAIT_SECONDS="${WAIT_SECONDS:-120}"

SQL_ONLY=0

log() { printf '[bi] %s\n' "$*"; }
warn() { printf '[bi] ВНИМАНИЕ: %s\n' "$*" >&2; }
die() { printf '[bi] ОШИБКА: %s\n' "$*" >&2; exit 1; }

usage() {
    cat <<'EOF'
BI-этап ЛР3: представления и Superset.

Опции:
  --sql-only   применить только SQL-представления и роль чтения
  -h, --help   показать эту справку

Переменные окружения (или строки в .env):
  LR3_SUPERSET_ADMIN_USER         администратор Superset (обязателен)
  LR3_SUPERSET_ADMIN_PASSWORD     пароль администратора (обязателен)
  LR3_SUPERSET_READER_PASSWORD    пароль роли superset_reader (обязателен)
  SUPERSET_ADMIN_USERNAME         запасное имя администратора
  SUPERSET_ADMIN_PASSWORD         запасной пароль администратора
  SUPERSET_READER_PASSWORD        запасной пароль роли чтения
  SUPERSET_READER_USER            фиксировано superset_reader; иное значение отклоняется
  SUPERSET_SERVICE                сервис Superset (по умолчанию superset)
  SUPERSET_URL                    API Superset (по умолчанию http://localhost:8088)
  SUPERSET_DATABASE_NAME          имя подключения в Superset (steam-marts)
  SUPERSET_DASHBOARD_NAME         заголовок dashboard
  SUPERSET_DASHBOARD_SLUG         slug dashboard
  SUPERSET_UPDATE_EXISTING=1      обновлять параметры существующих charts
  WAIT_SECONDS                    ожидание PostgreSQL/Superset, секунды
  LR3_ENV_FILE                    дополнительный файл только для чтения секретов
EOF
}

while (($#)); do
    case "$1" in
        --sql-only) SQL_ONLY=1 ;;
        -h | --help)
            usage
            exit 0
            ;;
        *) die "Неизвестная опция: $1" ;;
    esac
    shift
done

# --- Чтение значений: окружение, затем LR3_ENV_FILE, затем .env -------------
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

# --- Compose -----------------------------------------------------------------
COMPOSE_ARGS=(-f "$BASE_COMPOSE")
compose() { docker compose "${COMPOSE_ARGS[@]}" "$@"; }

command -v docker >/dev/null 2>&1 || die "Docker CLI не найден в PATH."
docker info >/dev/null 2>&1 || die "Docker daemon недоступен."
docker compose version >/dev/null 2>&1 || die "Нужен Docker Compose v2."

if ! SERVICES="$(compose config --services 2>/dev/null | tr -d '\r')"; then
    die "Не удалось разобрать $BASE_COMPOSE. Проверьте .env и YAML."
fi
grep -qx "$POSTGRES_SERVICE" <<<"$SERVICES" ||
    die "В compose нет сервиса $POSTGRES_SERVICE."

[[ -r "$BI_SQL_DIR/00_create_bi_views.sql" ]] ||
    die "Не найден $BI_SQL_DIR/00_create_bi_views.sql."
[[ -r "$BI_SQL_DIR/10_create_superset_reader.sql" ]] ||
    die "Не найден $BI_SQL_DIR/10_create_superset_reader.sql."

# --- PostgreSQL --------------------------------------------------------------
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

log "Проверяю обязательные таблицы и поля marts..."
MISSING_MARTS="$(
    compose exec -T "$POSTGRES_SERVICE" psql \
        -U airflow -d airflow -AtX -v ON_ERROR_STOP=1 \
        -c "SELECT string_agg(t, ', ' ORDER BY t)
              FROM unnest(ARRAY[
                  'marts.mart_game_daily_metrics',
                  'marts.mart_activity_hourly',
                  'marts.mart_genre_daily_metrics',
                  'marts.mart_publisher_daily_metrics',
                  'marts.mart_dq_results'
              ]) AS t
             WHERE to_regclass(t) IS NULL;" | tr -d '\r'
)" || die "Не удалось проверить витрины."
[[ -z "$MISSING_MARTS" ]] ||
    die "Схема marts не готова; отсутствуют: $MISSING_MARTS. Сначала выполните DAG витрин."

MISSING_COLUMNS="$(
    compose exec -T "$POSTGRES_SERVICE" psql \
        -U airflow -d airflow -AtX -v ON_ERROR_STOP=1 \
        -c "SELECT string_agg(c, ', ' ORDER BY c)
              FROM unnest(ARRAY['day_of_week', 'day_name', 'is_weekend']) AS c
             WHERE NOT EXISTS (
                 SELECT 1 FROM information_schema.columns
                 WHERE table_schema = 'marts'
                   AND table_name = 'mart_activity_hourly'
                   AND column_name = c
             );" | tr -d '\r'
)" || die "Не удалось проверить поля mart_activity_hourly."
[[ -z "$MISSING_COLUMNS" ]] ||
    die "В marts.mart_activity_hourly нет полей: $MISSING_COLUMNS. Нужна интеграция витрин и DAG."

log "Применяю BI-представления..."
compose exec -T "$POSTGRES_SERVICE" psql \
    -U airflow -d airflow -X -q -1 -v ON_ERROR_STOP=1 \
    <"$BI_SQL_DIR/00_create_bi_views.sql" ||
    die "Не удалось создать BI-представления."

READER_PASSWORD="$(env_or_fallback SUPERSET_READER_PASSWORD LR3_SUPERSET_READER_PASSWORD)"
[[ -n "$READER_PASSWORD" ]] ||
    die "Не задан LR3_SUPERSET_READER_PASSWORD (или SUPERSET_READER_PASSWORD) в .env (значение не выводится)."
lower="$(printf '%s' "$READER_PASSWORD" | tr '[:upper:]' '[:lower:]')"
case "$lower" in
    *change*me* | *placeholder*)
        die "LR3_SUPERSET_READER_PASSWORD содержит заглушку; задайте безопасное значение."
        ;;
esac

log "Настраиваю роль superset_reader..."
export SUPERSET_READER_PASSWORD="$READER_PASSWORD"
compose exec -T -e SUPERSET_READER_PASSWORD "$POSTGRES_SERVICE" psql \
    -U airflow -d airflow -X -q -1 -v ON_ERROR_STOP=1 \
    <"$BI_SQL_DIR/10_create_superset_reader.sql" ||
    die "Не удалось настроить роль superset_reader."

if ((SQL_ONLY)); then
    log "SQL-этап завершён (--sql-only)."
    exit 0
fi

# --- Superset -----------------------------------------------------------------
SUPERSET_SERVICE="$(read_env_key SUPERSET_SERVICE || true)"
SUPERSET_SERVICE="${SUPERSET_SERVICE:-$SUPERSET_SERVICE_DEFAULT}"
grep -qx "$SUPERSET_SERVICE" <<<"$SERVICES" ||
    die "В compose нет сервиса $SUPERSET_SERVICE."

ADMIN_USER="$(env_or_fallback SUPERSET_ADMIN_USERNAME LR3_SUPERSET_ADMIN_USER)"
ADMIN_PASSWORD="$(env_or_fallback SUPERSET_ADMIN_PASSWORD LR3_SUPERSET_ADMIN_PASSWORD)"
[[ -n "$ADMIN_USER" && -n "$ADMIN_PASSWORD" ]] ||
    die "Не заданы LR3_SUPERSET_ADMIN_USER/LR3_SUPERSET_ADMIN_PASSWORD в .env."
lower="$(printf '%s' "$ADMIN_PASSWORD" | tr '[:upper:]' '[:lower:]')"
case "$lower" in
    *change*me* | *placeholder*)
        die "LR3_SUPERSET_ADMIN_PASSWORD содержит заглушку; задайте безопасное значение."
        ;;
esac

if [[ -z "$(compose ps -q "$SUPERSET_SERVICE" 2>/dev/null || true)" ]]; then
    die "Сервис $SUPERSET_SERVICE не запущен. Запустите docker compose up -d."
fi

log "Ожидаю HTTP Superset..."
SUPERSET_READY=0
for _ in $(seq 1 $((WAIT_SECONDS / 2))); do
    if compose exec -T "$SUPERSET_SERVICE" python -c \
        "import urllib.request; urllib.request.urlopen('http://localhost:8088/health', timeout=3).read()" \
        >/dev/null 2>&1; then
        SUPERSET_READY=1
        break
    fi
    sleep 2
done
((SUPERSET_READY)) || die "Superset не ответил за ${WAIT_SECONDS} с."

SUPERSET_URL="$(read_env_key SUPERSET_URL || true)"
SUPERSET_URL="${SUPERSET_URL:-http://localhost:8088}"
POSTGRES_HOST_VALUE="$(read_env_key POSTGRES_HOST || true)"
POSTGRES_HOST_VALUE="${POSTGRES_HOST_VALUE:-postgres}"
POSTGRES_PORT_VALUE="$(read_env_key POSTGRES_PORT || true)"
POSTGRES_PORT_VALUE="${POSTGRES_PORT_VALUE:-5432}"
POSTGRES_DB_VALUE="$(read_env_key POSTGRES_DB || true)"
POSTGRES_DB_VALUE="${POSTGRES_DB_VALUE:-airflow}"
DATABASE_NAME="$(read_env_key SUPERSET_DATABASE_NAME || true)"
DATABASE_NAME="${DATABASE_NAME:-steam-marts}"
DASHBOARD_NAME="$(read_env_key SUPERSET_DASHBOARD_NAME || true)"
DASHBOARD_NAME="${DASHBOARD_NAME:-Steam Analytics (ЛР3)}"
DASHBOARD_SLUG="$(read_env_key SUPERSET_DASHBOARD_SLUG || true)"
DASHBOARD_SLUG="${DASHBOARD_SLUG:-steam-analytics-lr3}"
READER_USER="$(read_env_key SUPERSET_READER_USER || true)"
if [[ -n "$READER_USER" && "$READER_USER" != "superset_reader" ]]; then
    die "SUPERSET_READER_USER поддерживает только superset_reader: SQL и grants жёстко используют эту роль."
fi
READER_USER="superset_reader"
UPDATE_EXISTING="$(read_env_key SUPERSET_UPDATE_EXISTING || true)"
UPDATE_EXISTING="${UPDATE_EXISTING:-0}"

export SUPERSET_URL
export SUPERSET_ADMIN_USERNAME="$ADMIN_USER"
export SUPERSET_ADMIN_PASSWORD="$ADMIN_PASSWORD"
export SUPERSET_READER_USER="$READER_USER"
export SUPERSET_READER_PASSWORD="$READER_PASSWORD"
export POSTGRES_HOST="$POSTGRES_HOST_VALUE"
export POSTGRES_PORT="$POSTGRES_PORT_VALUE"
export POSTGRES_DB="$POSTGRES_DB_VALUE"
export SUPERSET_DATABASE_NAME="$DATABASE_NAME"
export SUPERSET_DASHBOARD_NAME="$DASHBOARD_NAME"
export SUPERSET_DASHBOARD_SLUG="$DASHBOARD_SLUG"
export SUPERSET_UPDATE_EXISTING="$UPDATE_EXISTING"

CONTAINER_SCRIPT="/tmp/bootstrap_superset.py"
compose cp "$SUPERSET_SCRIPT" "${SUPERSET_SERVICE}:${CONTAINER_SCRIPT}" ||
    die "Не удалось скопировать скрипт в контейнер Superset."

cleanup() {
    compose exec -T "$SUPERSET_SERVICE" rm -f "$CONTAINER_SCRIPT" \
        >/dev/null 2>&1 || true
}
trap cleanup EXIT

log "Создаю datasets, charts и dashboard..."
set +e
compose exec -T \
    -e SUPERSET_URL \
    -e SUPERSET_ADMIN_USERNAME \
    -e SUPERSET_ADMIN_PASSWORD \
    -e SUPERSET_READER_USER \
    -e SUPERSET_READER_PASSWORD \
    -e POSTGRES_HOST \
    -e POSTGRES_PORT \
    -e POSTGRES_DB \
    -e SUPERSET_DATABASE_NAME \
    -e SUPERSET_DASHBOARD_NAME \
    -e SUPERSET_DASHBOARD_SLUG \
    -e SUPERSET_UPDATE_EXISTING \
    "$SUPERSET_SERVICE" python "$CONTAINER_SCRIPT"
RC=$?
set -e
((RC == 0)) || die "Скрипт Superset завершился с кодом $RC."

log "Готово: представления, роль чтения и объекты Superset."
