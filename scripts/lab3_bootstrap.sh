#!/usr/bin/env bash
# Bootstrap ЛР3: единый compose, проверка секретов и DDL после готовности DDS.
# Секреты берутся из .env (шаблон — .env_lab3_example) и не печатаются.
# Скрипт не запускает пайплайны ODS/DDS: он ждёт готовую схему dds.
#
# Порядок этапов:
#   1) проверка env и ресурсов Docker;
#   2) docker compose up -d --build (единый docker-compose.yaml);
#   3) ожидание PostgreSQL и проверка таблиц DDS;
#   4) единый DDL витрин sql/marts/init_marts.sql;
#   5) BI-представления lab3/bi/sql/00_create_bi_views.sql;
#   6) права роли superset_reader;
#   7) необязательные BI/lineage через lab3/bi и lab3/metadata.
#
# Использование: scripts/lab3_bootstrap.sh [--no-up] [--with-bi] [--with-lineage]
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd -- "$ROOT_DIR"

COMPOSE_FILE="docker-compose.yaml"
ENV_EXAMPLE=".env_lab3_example"
MART_SQL="sql/marts/init_marts.sql"
BI_VIEWS_SQL="lab3/bi/sql/00_create_bi_views.sql"
BI_READER_SQL="lab3/bi/sql/10_create_superset_reader.sql"
POSTGRES_SERVICE="postgres"
POSTGRES_USER="airflow"
POSTGRES_DB="airflow"
POSTGRES_WAIT_SECONDS="${POSTGRES_WAIT_SECONDS:-180}"

DO_UP=1
WITH_BI=0
WITH_LINEAGE=0

log() { printf '[lab3] %s\n' "$*"; }
warn() { printf '[lab3] ВНИМАНИЕ: %s\n' "$*" >&2; }
die() { printf '[lab3] ОШИБКА: %s\n' "$*" >&2; exit 1; }

usage() {
    cat <<'EOF'
Bootstrap ЛР3 на едином docker-compose.yaml.

Опции:
  --no-up         не запускать docker compose up; работать с уже запущенным стеком
  --with-bi       выполнить lab3/bi/bootstrap.sh (Superset: datasets, charts, dashboard)
  --with-lineage  выполнить lab3/metadata/bootstrap.sh (OpenMetadata ingestion и lineage)
  -h, --help      показать эту справку

Перед запуском перенесите переменные из .env_lab3_example в .env и замените
CHANGE_ME на безопасные значения. Отдельный env-файл не подключается.

Ресурсы: Docker должен иметь не менее 6 ГиБ RAM; для полного стека
(OpenMetadata + OpenSearch + Superset) рекомендуется 8 ГиБ.

Переменные окружения:
  POSTGRES_WAIT_SECONDS  таймаут ожидания PostgreSQL, секунды (по умолчанию 180)
EOF
}

# Читает значение ключа из .env, не печатая его. Поддерживает export и кавычки.
read_env_file_value() {
    local key="$1" line value="" found=0
    while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line%$'\r'}"
        if [[ "$line" =~ ^[[:space:]]*# ]]; then
            continue
        fi
        if [[ "$line" =~ ^[[:space:]]*(export[[:space:]]+)?${key}[[:space:]]*=(.*)$ ]]; then
            value="${BASH_REMATCH[2]}"
            value="${value#"${value%%[![:space:]]*}"}"
            value="${value%"${value##*[![:space:]]}"}"
            if [[ "$value" != \"* && "$value" != \'* ]]; then
                value="${value%%[[:space:]]#*}"
                value="${value%"${value##*[![:space:]]}"}"
            fi
            if ((${#value} >= 2)); then
                if [[ "$value" == \"* && "$value" == *\" ]]; then
                    value="${value:1:${#value}-2}"
                elif [[ "$value" == \'* && "$value" == *\' ]]; then
                    value="${value:1:${#value}-2}"
                fi
            fi
            found=1
        fi
    done < .env
    if ((found)); then
        printf '%s' "$value"
    fi
    return 0
}

# Приоритет как у compose: переменная оболочки перекрывает значение из .env.
effective_env_value() {
    local key="$1"
    if [[ -n "${!key+x}" ]]; then
        printf '%s' "${!key}"
    else
        read_env_file_value "$key"
    fi
}

validate_fernet() {
    local value="$1" encoded decoded_len=""
    if [[ ! "$value" =~ ^[A-Za-z0-9_-]{43}=$ ]]; then
        die "FERNET_KEY не похож на 32-байтный base64url ключ Fernet (нужно 44 символа)."
    fi
    if ! command -v base64 >/dev/null 2>&1; then
        warn "base64 не найден: у FERNET_KEY проверен только формат."
        return 0
    fi
    encoded="$(printf '%s' "$value" | tr '_-' '/+')"
    if printf '%s' "$encoded" | base64 -d >/dev/null 2>&1; then
        decoded_len="$(printf '%s' "$encoded" | base64 -d | wc -c | tr -d '[:space:]')" || decoded_len=""
    elif printf '%s' "$encoded" | base64 -D >/dev/null 2>&1; then
        decoded_len="$(printf '%s' "$encoded" | base64 -D | wc -c | tr -d '[:space:]')" || decoded_len=""
    else
        die "FERNET_KEY не декодируется доступным base64; сгенерируйте ключ заново."
    fi
    [[ "$decoded_len" == "32" ]] ||
        die "FERNET_KEY не декодируется в 32 байта; сгенерируйте ключ заново."
}

is_identifier() {
    [[ "$1" =~ ^[a-z_][a-z0-9_]{0,62}$ ]]
}

# Обязательные ключи: без них compose не поднимется или ЛР3 будет неполной.
REQUIRED_KEYS=(
    FERNET_KEY
    AIRFLOW__API_AUTH__JWT_SECRET
    LR3_SUPERSET_DB_USER
    LR3_SUPERSET_DB_NAME
    LR3_SUPERSET_DB_PASSWORD
    LR3_SUPERSET_SECRET_KEY
    LR3_SUPERSET_ADMIN_USER
    LR3_SUPERSET_ADMIN_PASSWORD
    LR3_SUPERSET_ADMIN_EMAIL
    LR3_SUPERSET_READER_PASSWORD
    LR3_OM_DB_USER
    LR3_OM_DB_NAME
    LR3_OM_DB_PASSWORD
    OM_ADMIN_EMAIL
    OM_ADMIN_PASSWORD
    OM_POSTGRES_USER
    OM_POSTGRES_PASSWORD
    OPENSEARCH_INITIAL_ADMIN_PASSWORD
)

# Проверяет обязательные переменные и минимальные требования к секретам.
validate_env() {
    local key value lower missing=""
    for key in "${REQUIRED_KEYS[@]}"; do
        value="$(effective_env_value "$key")"
        if [[ -z "$value" ]]; then
            missing="${missing} ${key}"
            continue
        fi
        lower="$(printf '%s' "$value" | tr '[:upper:]' '[:lower:]')"
        case "$lower" in
            *change*me* | *placeholder* | *change_me*)
                die "Переменная $key содержит заглушку; задайте безопасное значение в .env."
                ;;
        esac
    done
    [[ -z "$missing" ]] ||
        die "В .env (или окружении) отсутствуют:${missing}. Перенесите ключи из $ENV_EXAMPLE в .env."

    is_identifier "$(effective_env_value LR3_SUPERSET_DB_USER)" ||
        die "LR3_SUPERSET_DB_USER должен быть lower-case идентификатором PostgreSQL."
    is_identifier "$(effective_env_value LR3_SUPERSET_DB_NAME)" ||
        die "LR3_SUPERSET_DB_NAME должен быть lower-case идентификатором PostgreSQL."
    is_identifier "$(effective_env_value LR3_OM_DB_USER)" ||
        die "LR3_OM_DB_USER должен быть lower-case идентификатором PostgreSQL."
    is_identifier "$(effective_env_value LR3_OM_DB_NAME)" ||
        die "LR3_OM_DB_NAME должен быть lower-case идентификатором PostgreSQL."
    is_identifier "$(effective_env_value OM_POSTGRES_USER)" ||
        die "OM_POSTGRES_USER должен быть lower-case идентификатором PostgreSQL."

    local db_pw admin_pw admin_user secret reader_pw om_db_pw om_admin_pw os_pw jwt om_reader_pw
    db_pw="$(effective_env_value LR3_SUPERSET_DB_PASSWORD)"
    admin_pw="$(effective_env_value LR3_SUPERSET_ADMIN_PASSWORD)"
    admin_user="$(effective_env_value LR3_SUPERSET_ADMIN_USER)"
    secret="$(effective_env_value LR3_SUPERSET_SECRET_KEY)"
    reader_pw="$(effective_env_value LR3_SUPERSET_READER_PASSWORD)"
    om_db_pw="$(effective_env_value LR3_OM_DB_PASSWORD)"
    om_admin_pw="$(effective_env_value OM_ADMIN_PASSWORD)"
    om_reader_pw="$(effective_env_value OM_POSTGRES_PASSWORD)"
    os_pw="$(effective_env_value OPENSEARCH_INITIAL_ADMIN_PASSWORD)"
    jwt="$(effective_env_value AIRFLOW__API_AUTH__JWT_SECRET)"

    ((${#db_pw} >= 12)) || die "LR3_SUPERSET_DB_PASSWORD короче 12 символов."
    ((${#admin_pw} >= 12)) || die "LR3_SUPERSET_ADMIN_PASSWORD короче 12 символов."
    ((${#reader_pw} >= 12)) || die "LR3_SUPERSET_READER_PASSWORD короче 12 символов."
    ((${#om_db_pw} >= 12)) || die "LR3_OM_DB_PASSWORD короче 12 символов."
    ((${#om_admin_pw} >= 12)) || die "OM_ADMIN_PASSWORD короче 12 символов."
    ((${#om_reader_pw} >= 12)) || die "OM_POSTGRES_PASSWORD короче 12 символов."
    ((${#os_pw} >= 8)) || die "OPENSEARCH_INITIAL_ADMIN_PASSWORD короче 8 символов."
    [[ "$os_pw" =~ [A-Z] ]] ||
        die "OPENSEARCH_INITIAL_ADMIN_PASSWORD должен содержать заглавную букву."
    [[ "$os_pw" =~ [a-z] ]] ||
        die "OPENSEARCH_INITIAL_ADMIN_PASSWORD должен содержать строчную букву."
    [[ "$os_pw" =~ [0-9] ]] ||
        die "OPENSEARCH_INITIAL_ADMIN_PASSWORD должен содержать цифру."
    [[ "$os_pw" =~ [^A-Za-z0-9] ]] ||
        die "OPENSEARCH_INITIAL_ADMIN_PASSWORD должен содержать спецсимвол."
    ((${#secret} >= 32)) || die "LR3_SUPERSET_SECRET_KEY короче 32 символов."
    ((${#jwt} >= 16)) || die "AIRFLOW__API_AUTH__JWT_SECRET короче 16 символов."
    [[ "$admin_pw" != "$admin_user" ]] ||
        die "Пароль администратора Superset совпадает с логином."
    [[ "$(effective_env_value LR3_SUPERSET_ADMIN_EMAIL)" =~ ^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$ ]] ||
        die "LR3_SUPERSET_ADMIN_EMAIL не похож на корректный e-mail."
    [[ "$(effective_env_value OM_ADMIN_EMAIL)" =~ ^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$ ]] ||
        die "OM_ADMIN_EMAIL не похож на корректный e-mail."

    validate_fernet "$(effective_env_value FERNET_KEY)"
}

# Полный стек с OpenMetadata требует заметно больше памяти, чем базовый Airflow.
check_docker_memory() {
    local bytes min_gib=6 rec_gib=8
    bytes="$(docker info --format '{{.MemTotal}}' 2>/dev/null || true)"
    if [[ ! "$bytes" =~ ^[0-9]+$ ]]; then
        warn "Не удалось определить объём памяти Docker; проверьте вручную (нужно 6-8 ГиБ)."
        return 0
    fi
    if ((bytes < min_gib * 1024 * 1024 * 1024)); then
        die "Docker выделено $((bytes / 1024 / 1024 / 1024)) ГиБ; нужно минимум ${min_gib} ГиБ (рекомендуется ${rec_gib})."
    fi
    if ((bytes < rec_gib * 1024 * 1024 * 1024)); then
        warn "Docker выделено $((bytes / 1024 / 1024 / 1024)) ГиБ; рекомендуется ${rec_gib} ГиБ для OpenMetadata и OpenSearch."
    fi
}

while (($#)); do
    case "$1" in
        --no-up) DO_UP=0 ;;
        --with-bi) WITH_BI=1 ;;
        --with-lineage) WITH_LINEAGE=1 ;;
        -h | --help)
            usage
            exit 0
            ;;
        --)
            shift
            break
            ;;
        *)
            printf '[lab3] Неизвестная опция: %s\n' "$1" >&2
            usage >&2
            exit 2
            ;;
    esac
    shift
done
(($# == 0)) || {
    printf '[lab3] Позиционные аргументы не поддерживаются.\n' >&2
    exit 2
}

[[ -f "$COMPOSE_FILE" ]] || die "Не найден $COMPOSE_FILE. Запускайте скрипт из репозитория."
[[ -r "$ENV_EXAMPLE" ]] || die "Не найден шаблон $ENV_EXAMPLE."
[[ -r "$MART_SQL" ]] || die "Не найден $MART_SQL."
[[ -r "$BI_VIEWS_SQL" ]] || die "Не найден $BI_VIEWS_SQL."
[[ -r "$BI_READER_SQL" ]] || die "Не найден $BI_READER_SQL."
[[ -r ".env" ]] || die "Не найден .env. Создайте его из .env_example и добавьте переменные ЛР3."

if command -v git >/dev/null 2>&1 && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git check-ignore -q .env ||
        die ".env не исключён из git. Добавьте .env в .gitignore и не коммитьте секреты."
fi
[[ "$POSTGRES_WAIT_SECONDS" =~ ^[0-9]+$ ]] ||
    die "POSTGRES_WAIT_SECONDS должен быть целым числом секунд."

validate_env

command -v docker >/dev/null 2>&1 || die "Docker CLI не найден в PATH."
docker info >/dev/null 2>&1 || die "Docker daemon недоступен. Запустите Docker и повторите."
docker compose version >/dev/null 2>&1 || die "Нужен Docker Compose v2 (команда 'docker compose')."
check_docker_memory

# --- Compose: только единый файл, без overlay/profile lr3 ---------------------
COMPOSE=(docker compose -f "$COMPOSE_FILE")
compose() { "${COMPOSE[@]}" "$@"; }

if ! services="$(compose --profile ingestion config --services 2>/dev/null | tr -d '\r')"; then
    die "Не удалось разобрать конфигурацию compose (проверьте .env и YAML)."
fi
for service in postgres lab3-db-init superset openmetadata-server openmetadata-ingestion; do
    grep -qx "$service" <<<"$services" ||
        die "В compose нет сервиса $service."
done
if ! profiles="$(compose config --profiles 2>/dev/null | tr -d '\r')"; then
    die "Не удалось получить список профилей compose."
fi
for profile in ingestion lineage; do
    grep -qx "$profile" <<<"$profiles" ||
        warn "В compose нет профиля $profile; соответствующие сервисы недоступны."
done

# --- Запуск -------------------------------------------------------------------
if ((DO_UP)); then
    log "Запускаю единый compose (Superset, OpenMetadata и базовый стек)..."
    compose up -d --build
else
    log "--no-up: docker compose up пропущен."
fi

wait_for_postgres() {
    local waited=0
    while ((waited < POSTGRES_WAIT_SECONDS)); do
        if compose exec -T "$POSTGRES_SERVICE" \
            pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
        waited=$((waited + 2))
    done
    return 1
}

log "Ожидаю готовности PostgreSQL (до ${POSTGRES_WAIT_SECONDS} с)..."
wait_for_postgres ||
    die "PostgreSQL не готов за ${POSTGRES_WAIT_SECONDS} с. Проверьте: docker compose ps; логи postgres."

# --- DDS ----------------------------------------------------------------------
DDS_REQUIRED=(
    dds.dim_game
    dds.dim_user
    dds.dim_review
    dds.fact_review
    dds.fact_player_activity
    dds.dim_genre
    dds.dim_developer
    dds.dim_publisher
    dds.bridge_game_genre
    dds.bridge_game_developer
    dds.bridge_game_publisher
    dds.dq_results
)
DDS_LIST=""
for table in "${DDS_REQUIRED[@]}"; do
    DDS_LIST+="${DDS_LIST:+, }'${table}'"
done

log "Проверяю обязательные таблицы DDS..."
if ! dds_missing="$(
    compose exec -T "$POSTGRES_SERVICE" psql \
        -U "$POSTGRES_USER" -d "$POSTGRES_DB" -AtX -v ON_ERROR_STOP=1 \
        -c "SELECT string_agg(t, ', ' ORDER BY t)
              FROM unnest(ARRAY[${DDS_LIST}]::text[]) AS t
             WHERE to_regclass(t) IS NULL;" | tr -d '\r'
)"; then
    die "Не удалось проверить схему DDS в PostgreSQL."
fi
[[ -z "$dds_missing" ]] ||
    die "Схема DDS не готова. Отсутствуют таблицы: $dds_missing. Сначала выполните DDS-пайплайны и повторите bootstrap."

# --- Единый DDL витрин из sql/marts -------------------------------------------
log "Применяю единый DDL витрин $MART_SQL..."
if ! compose exec -T "$POSTGRES_SERVICE" psql \
    -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -X -q -1 -v ON_ERROR_STOP=1 <"$MART_SQL"; then
    die "Применение $MART_SQL завершилось ошибкой; транзакция отменена."
fi

MART_TABLES=(
    marts.mart_game_daily_metrics
    marts.mart_activity_hourly
    marts.mart_genre_daily_metrics
    marts.mart_publisher_daily_metrics
    marts.mart_dq_results
)
MART_LIST=""
for table in "${MART_TABLES[@]}"; do
    MART_LIST+="${MART_LIST:+, }'${table}'"
done
if ! marts_missing="$(
    compose exec -T "$POSTGRES_SERVICE" psql \
        -U "$POSTGRES_USER" -d "$POSTGRES_DB" -AtX -v ON_ERROR_STOP=1 \
        -c "SELECT string_agg(t, ', ' ORDER BY t)
              FROM unnest(ARRAY[${MART_LIST}]::text[]) AS t
             WHERE to_regclass(t) IS NULL;" | tr -d '\r'
)"; then
    die "Не удалось проверить созданные витрины."
fi
[[ -z "$marts_missing" ]] ||
    die "После применения не найдены объекты: $marts_missing."

# --- BI-представления и права reader ------------------------------------------
log "Применяю BI-представления $BI_VIEWS_SQL..."
if ! compose exec -T "$POSTGRES_SERVICE" psql \
    -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -X -q -1 -v ON_ERROR_STOP=1 <"$BI_VIEWS_SQL"; then
    die "Не удалось создать BI-представления."
fi

READER_PASSWORD="$(effective_env_value LR3_SUPERSET_READER_PASSWORD)"
[[ -n "$READER_PASSWORD" ]] || die "Не задан LR3_SUPERSET_READER_PASSWORD."
export SUPERSET_READER_PASSWORD="$READER_PASSWORD"

log "Настраиваю роль superset_reader (marts и BI-представления)..."
if ! compose exec -T -e SUPERSET_READER_PASSWORD "$POSTGRES_SERVICE" psql \
    -U "$POSTGRES_USER" -d "$POSTGRES_DB" -X -q -1 -v ON_ERROR_STOP=1 \
    <"$BI_READER_SQL"; then
    die "Не удалось настроить роль superset_reader."
fi

BI_VIEWS=(
    marts.bi_top15_games_by_avg_players
    marts.bi_activity_dow_hour_utc
    marts.bi_weekend_hourly
    marts.bi_reviews_daily
    marts.bi_top_genres_by_reviews
    marts.bi_genre_sentiment
    marts.bi_publisher_reviews_vs_online
    marts.bi_dq_latest
)
BI_LIST=""
for view in "${BI_VIEWS[@]}"; do
    BI_LIST+="${BI_LIST:+, }'${view}'"
done
if ! views_missing="$(
    compose exec -T "$POSTGRES_SERVICE" psql \
        -U "$POSTGRES_USER" -d "$POSTGRES_DB" -AtX -v ON_ERROR_STOP=1 \
        -c "SELECT string_agg(t, ', ' ORDER BY t)
              FROM unnest(ARRAY[${BI_LIST}]::text[]) AS t
             WHERE to_regclass(t) IS NULL;" | tr -d '\r'
)"; then
    die "Не удалось проверить BI-представления."
fi
[[ -z "$views_missing" ]] ||
    die "Не найдены BI-представления: $views_missing."

log "DDL витрин, BI-представления и права reader готовы."

# --- Необязательные BI/lineage ------------------------------------------------
# Псевдонимы для lab3/bi: скрипты читают SUPERSET_*, но значения берём из LR3_*.
export SUPERSET_SERVICE="${SUPERSET_SERVICE:-superset}"
export LR3_SUPERSET_SERVICE="${LR3_SUPERSET_SERVICE:-superset}"
export SUPERSET_ADMIN_USERNAME="${SUPERSET_ADMIN_USERNAME:-$(effective_env_value LR3_SUPERSET_ADMIN_USER)}"
export SUPERSET_ADMIN_PASSWORD="${SUPERSET_ADMIN_PASSWORD:-$(effective_env_value LR3_SUPERSET_ADMIN_PASSWORD)}"
export SUPERSET_READER_PASSWORD
export OM_SERVER_URL="${OM_SERVER_URL:-http://openmetadata-server:8585/api}"
export OM_SERVER_SERVICE="${OM_SERVER_SERVICE:-openmetadata-server}"
export OM_INGESTION_SERVICE="${OM_INGESTION_SERVICE:-openmetadata-ingestion}"
export OM_LINEAGE_SERVICE="${OM_LINEAGE_SERVICE:-openmetadata-lineage}"

if ((WITH_BI)); then
    [[ -r "lab3/bi/bootstrap.sh" ]] ||
        die "--with-bi: не найден lab3/bi/bootstrap.sh."
    log "Запускаю BI-этап: datasets, charts и dashboard Superset."
    bash "lab3/bi/bootstrap.sh" || die "BI-этап завершился ошибкой."
fi
if ((WITH_LINEAGE)); then
    [[ -r "lab3/metadata/bootstrap.sh" ]] ||
        die "--with-lineage: не найден lab3/metadata/bootstrap.sh."
    log "Запускаю metadata-этап: ingestion и lineage OpenMetadata."
    bash "lab3/metadata/bootstrap.sh" || die "Metadata-этап завершился ошибкой."
fi

log "Готово."
log "Airflow UI: http://localhost:8080"
log "Superset UI: http://127.0.0.1:8088"
log "OpenMetadata UI: http://127.0.0.1:8585"
if ((!WITH_BI && !WITH_LINEAGE)); then
    log "BI и lineage не запускались: используйте --with-bi и --with-lineage."
fi
