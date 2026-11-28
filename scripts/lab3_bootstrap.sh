#!/usr/bin/env bash
# Bootstrap ЛР3: базовый compose, profile lr3 и схема marts.
# Переменные ЛР3 берутся из .env (не из отдельного env-файла); шаблон —
# .env_lab3_example. Секреты не печатаются, данные не удаляются.
# Инфраструктура lr3 не подтверждена запуском: скрипт проверяет env,
# конфигурацию и готовность postgres, но не здоровье Superset/OpenMetadata.
#
# Необязательные этапы (только по явным флагам):
#   lab3/bi/bootstrap.sh        -- настройка BI;
#   lab3/metadata/bootstrap.sh  -- настройка lineage.
# Если хука нет, скрипт завершается с кодом 3 до любых изменений.
#
# Использование: scripts/lab3_bootstrap.sh [--no-up] [--base-only]
#                                            [--with-bi] [--with-lineage]
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd -- "$ROOT_DIR"

BASE_COMPOSE="docker-compose.yaml"
LR3_COMPOSE="docker-compose.lr3.yml"
LR3_ENV_EXAMPLE=".env_lab3_example"
SUPERSET_BUILD_DIR="docker/lab3/superset"
MART_SQL="sql/marts/init_marts.sql"
POSTGRES_SERVICE="postgres"
POSTGRES_USER="airflow"
POSTGRES_DB="airflow"
POSTGRES_WAIT_SECONDS="${POSTGRES_WAIT_SECONDS:-120}"

DO_UP=1
BASE_ONLY=0
WITH_BI=0
WITH_LINEAGE=0

log() { printf '[lab3] %s\n' "$*"; }
warn() { printf '[lab3] ВНИМАНИЕ: %s\n' "$*" >&2; }
die() { printf '[lab3] ОШИБКА: %s\n' "$*" >&2; exit 1; }
die_optional() { printf '[lab3] ОШИБКА: %s\n' "$*" >&2; exit 3; }

usage() {
    cat <<'EOF'
Bootstrap ЛР3: compose (+ profile lr3) и схема marts.

Опции:
  --no-up        не запускать docker compose up; работать с уже запущенным стеком
  --base-only    не подключать docker-compose.lr3.yml; переменные lr3 не требуются
  --with-bi      выполнить lab3/bi/bootstrap.sh после инициализации marts
  --with-lineage выполнить lab3/metadata/bootstrap.sh после инициализации marts
  -h, --help     показать эту справку

Переменные ЛР3 переносятся из .env_lab3_example в .env; отдельный env-файл
не подключается. Значения секретов не выводятся.

Переменные окружения:
  POSTGRES_WAIT_SECONDS  таймаут ожидания PostgreSQL, секунды (по умолчанию 120)
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

# Проверяет реальный формат ключа Fernet, а не только наличие строки в .env.
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

# Требует все ключи из .env_lab3_example в .env (или окружении) и проверяет секреты.
validate_lr3_env() {
    local key value lower missing="" keys
    local db_pw admin_pw admin_user secret om_pw
    keys="$(sed -n 's/^[[:space:]]*\([A-Za-z_][A-Za-z0-9_]*\)[[:space:]]*=.*/\1/p' "$LR3_ENV_EXAMPLE" | tr -d '\r')"
    [[ -n "$keys" ]] || die "В $LR3_ENV_EXAMPLE не найдены переменные ЛР3."
    while IFS= read -r key; do
        [[ -n "$key" ]] || continue
        value="$(effective_env_value "$key")"
        if [[ -z "$value" ]]; then
            missing="${missing} ${key}"
            continue
        fi
        lower="$(printf '%s' "$value" | tr '[:upper:]' '[:lower:]')"
        case "$lower" in
            *change*me* | *placeholder*)
                die "Переменная $key содержит заглушку; задайте безопасное значение в .env."
                ;;
        esac
    done <<<"$keys"
    [[ -z "$missing" ]] ||
        die "В .env (или окружении) отсутствуют:${missing}. Перенесите ключи из $LR3_ENV_EXAMPLE в .env; отдельный env-файл не подключается."

    db_pw="$(effective_env_value LR3_SUPERSET_DB_PASSWORD)"
    admin_pw="$(effective_env_value LR3_SUPERSET_ADMIN_PASSWORD)"
    admin_user="$(effective_env_value LR3_SUPERSET_ADMIN_USER)"
    secret="$(effective_env_value LR3_SUPERSET_SECRET_KEY)"
    om_pw="$(effective_env_value LR3_OM_DB_PASSWORD)"
    ((${#db_pw} >= 12)) || die "LR3_SUPERSET_DB_PASSWORD короче 12 символов."
    ((${#admin_pw} >= 12)) || die "LR3_SUPERSET_ADMIN_PASSWORD короче 12 символов."
    ((${#om_pw} >= 12)) || die "LR3_OM_DB_PASSWORD короче 12 символов."
    ((${#secret} >= 32)) || die "LR3_SUPERSET_SECRET_KEY короче 32 символов."
    [[ "$admin_pw" != "$admin_user" ]] ||
        die "Пароль администратора Superset совпадает с логином."
    [[ "$(effective_env_value LR3_SUPERSET_ADMIN_EMAIL)" =~ ^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$ ]] ||
        die "LR3_SUPERSET_ADMIN_EMAIL не похож на корректный e-mail."
}

while (($#)); do
    case "$1" in
        --no-up) DO_UP=0 ;;
        --base-only) BASE_ONLY=1 ;;
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

# Режим lr3 определяется до проверок env: при --base-only overlay не подключается.
LR3_ENABLED=0
if ((BASE_ONLY)); then
    log "--base-only: $LR3_COMPOSE игнорируется, переменные lr3 не требуются."
elif [[ -f "$LR3_COMPOSE" ]]; then
    LR3_ENABLED=1
else
    warn "$LR3_COMPOSE не найден: LR3-сервисы и переменные lr3 не проверяются."
fi

COMPOSE_HINT="docker compose -f $BASE_COMPOSE"
if ((LR3_ENABLED)); then
    COMPOSE_HINT="$COMPOSE_HINT -f $LR3_COMPOSE --profile lr3"
fi

# Проверяем запрошенные необязательные этапы до запуска чего-либо.
if ((WITH_BI || WITH_LINEAGE)); then
    ((LR3_ENABLED)) ||
        die_optional "BI/lineage требуют $LR3_COMPOSE и запуска без --base-only."
fi
if ((WITH_BI)); then
    [[ -r "lab3/bi/bootstrap.sh" ]] ||
        die_optional "--with-bi: файл lab3/bi/bootstrap.sh не найден. В lab3/bi пока только SQL; импорт не имитируется."
fi
if ((WITH_LINEAGE)); then
    [[ -r "lab3/metadata/bootstrap.sh" ]] ||
        die_optional "--with-lineage: файл lab3/metadata/bootstrap.sh не найден. Инфраструктура lineage не готова."
fi

# --- Предварительные проверки ------------------------------------------------
[[ -f "$BASE_COMPOSE" ]] || die "Не найден $BASE_COMPOSE. Запускайте скрипт из репозитория проекта."
[[ -r "$MART_SQL" ]] || die "Не найден или недоступен $MART_SQL."
[[ -r ".env" ]] ||
    die "Не найден .env. Создайте его из .env_example; переменные ЛР3 при необходимости перенесите из $LR3_ENV_EXAMPLE."
env_file_path="${ENV_FILE_PATH:-$(read_env_file_value ENV_FILE_PATH)}"
if [[ -n "$env_file_path" && "$env_file_path" != ".env" ]]; then
    die "ENV_FILE_PATH указывает не на .env. Переменные ЛР3 должны быть в .env; отдельный env-файл не подключается."
fi
if command -v git >/dev/null 2>&1 && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git check-ignore -q .env ||
        die ".env не исключён из git. Добавьте .env в .gitignore и не коммитьте секреты."
fi
[[ "$POSTGRES_WAIT_SECONDS" =~ ^[0-9]+$ ]] ||
    die "POSTGRES_WAIT_SECONDS должен быть целым числом секунд."

# FERNET_KEY: проверяем эффективное значение и реальный формат ключа.
fernet_value="$(effective_env_value FERNET_KEY)"
[[ -n "$fernet_value" ]] ||
    die "FERNET_KEY не задан в .env или окружении (значение не выводится)."
validate_fernet "$fernet_value"

if ((LR3_ENABLED)); then
    [[ -r "$LR3_ENV_EXAMPLE" ]] || die "Не найден шаблон $LR3_ENV_EXAMPLE."
    [[ -r "$SUPERSET_BUILD_DIR/Dockerfile" ]] ||
        die "Не найден $SUPERSET_BUILD_DIR/Dockerfile: образ Superset не собрать."
    [[ -r "$SUPERSET_BUILD_DIR/superset_config.py" ]] ||
        die "Не найден $SUPERSET_BUILD_DIR/superset_config.py."
    [[ -r "$SUPERSET_BUILD_DIR/init_superset.py" ]] ||
        die "Не найден $SUPERSET_BUILD_DIR/init_superset.py."
    validate_lr3_env
fi

command -v docker >/dev/null 2>&1 || die "Docker CLI не найден в PATH."
docker info >/dev/null 2>&1 || die "Docker daemon недоступен. Запустите Docker и повторите."
docker compose version >/dev/null 2>&1 || die "Нужен Docker Compose v2 (команда 'docker compose')."

# --- Compose: базовый файл и опциональный overlay/profile lr3 ----------------
COMPOSE_FILES=(-f "$BASE_COMPOSE")
if ((LR3_ENABLED)); then
    COMPOSE_FILES+=(-f "$LR3_COMPOSE")
    log "Подключаю $LR3_COMPOSE."
fi

compose() { docker compose "${COMPOSE_FILES[@]}" "$@"; }

# Явно убеждаемся, что compose-файлы парсятся и сервис postgres объявлен.
if ((LR3_ENABLED)); then
    if ! profiles="$(compose --profile lr3 config --profiles | tr -d '\r')"; then
        die "Не удалось разобрать конфигурацию compose (проверьте env и YAML)."
    fi
    grep -qx 'lr3' <<<"$profiles" ||
        die "В $LR3_COMPOSE нет профиля lr3. Исправьте overlay или запустите с --base-only."
    if ! services="$(compose --profile lr3 config --services | tr -d '\r')"; then
        die "Не удалось получить сервисы профиля lr3 (проверьте env и YAML)."
    fi
else
    if ! services="$(compose config --services | tr -d '\r')"; then
        die "Не удалось разобрать конфигурацию compose (docker compose config)."
    fi
fi
grep -qx "$POSTGRES_SERVICE" <<<"$services" ||
    die "В конфигурации compose нет сервиса $POSTGRES_SERVICE."

if ((LR3_ENABLED)); then
    for service in lr3-superset-db lr3-superset-init lr3-superset \
        lr3-openmetadata-db lr3-opensearch lr3-openmetadata-migrate lr3-openmetadata; do
        grep -qx "$service" <<<"$services" ||
            die "Профиль lr3 не включает сервис $service; проверьте $LR3_COMPOSE."
    done
fi

# --- Запуск -------------------------------------------------------------------
# lr3 стартует до DDL marts: Superset/OpenMetadata — цель этого этапа,
# а DDL выполняется ниже, после готовности postgres.
if ((DO_UP)); then
    if ((LR3_ENABLED)); then
        log "Запускаю compose с profile lr3 и сборкой образов..."
        compose --profile lr3 up -d --build
    else
        log "Запускаю базовый compose..."
        compose up -d
    fi
else
    log "--no-up: docker compose up пропущен."
fi
if ((LR3_ENABLED && DO_UP)); then
    warn "Инфраструктура lr3 не подтверждена запуском: готовность Superset/OpenMetadata скрипт не проверяет."
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
    die "PostgreSQL не готов за ${POSTGRES_WAIT_SECONDS} с. Проверьте: $COMPOSE_HINT ps; логи postgres."

# --- Проверка DDS перед созданием витрин -------------------------------------
# init_marts.sql создаёт FK на dds.dim_game, dds.dim_genre и dds.dim_publisher,
# поэтому без готовой схемы DDS применение упадёт.
log "Проверяю обязательные таблицы DDS..."
if ! dds_missing="$(
    compose exec -T "$POSTGRES_SERVICE" psql \
        -U "$POSTGRES_USER" -d "$POSTGRES_DB" -AtX -v ON_ERROR_STOP=1 \
        -c "SELECT string_agg(t, ', ' ORDER BY t)
              FROM unnest(ARRAY[
                  'dds.dim_game', 'dds.dim_user', 'dds.dim_review',
                  'dds.fact_review', 'dds.fact_player_activity',
                  'dds.dim_genre', 'dds.dim_publisher',
                  'dds.bridge_game_genre', 'dds.bridge_game_publisher'
              ]) AS t
             WHERE to_regclass(t) IS NULL;" | tr -d '\r'
)"; then
    die "Не удалось проверить схему DDS в PostgreSQL."
fi
[[ -z "$dds_missing" ]] ||
    die "Схема DDS не готова. Отсутствуют таблицы: $dds_missing. Сначала выполните DDS-пайплайны."

# --- Витрины: только stdin, без монтирования sql/marts ------------------------
# sql/ в контейнер не смонтирован, поэтому файл передаётся в psql через stdin.
# -1 делает применение атомарным; данные существующих таблиц не меняются.
log "Применяю $MART_SQL через stdin..."
if ! compose exec -T "$POSTGRES_SERVICE" psql \
    -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -X -q -1 -v ON_ERROR_STOP=1 <"$MART_SQL"; then
    die "Применение $MART_SQL завершилось ошибкой; транзакция отменена."
fi

if ! marts_missing="$(
    compose exec -T "$POSTGRES_SERVICE" psql \
        -U "$POSTGRES_USER" -d "$POSTGRES_DB" -AtX -v ON_ERROR_STOP=1 \
        -c "SELECT string_agg(t, ', ' ORDER BY t)
              FROM unnest(ARRAY[
                  'marts.mart_game_daily_metrics',
                  'marts.mart_activity_hourly',
                  'marts.mart_genre_daily_metrics',
                  'marts.mart_publisher_daily_metrics',
                  'marts.mart_dq_results'
              ]) AS t
             WHERE to_regclass(t) IS NULL;" | tr -d '\r'
)"; then
    die "Не удалось проверить созданные витрины."
fi
[[ -z "$marts_missing" ]] ||
    die "После применения не найдены объекты: $marts_missing."

log "Схема marts инициализирована; существующие данные не удалялись."

# --- Необязательные BI/lineage: только заранее подготовленные скрипты --------
run_hook() {
    local path="$1" name="$2"
    log "Запускаю необязательный этап $name: $path"
    bash "$path" || die "Этап $name ($path) завершился ошибкой."
}
if ((WITH_BI)); then
    run_hook "lab3/bi/bootstrap.sh" "BI"
fi
if ((WITH_LINEAGE)); then
    run_hook "lab3/metadata/bootstrap.sh" "lineage"
fi

if ((!WITH_BI && !WITH_LINEAGE)); then
    log "BI/lineage не запускались: используйте --with-bi/--with-lineage при наличии lab3/*/bootstrap.sh."
fi

if ((LR3_ENABLED)); then
    log "Состояние контейнеров: $COMPOSE_HINT ps"
    log "Ingestion вручную: $COMPOSE_HINT --profile ingestion up -d lr3-ingestion"
    warn "Инфраструктура lr3 не подтверждена запуском; Superset/OpenMetadata проверяйте вручную."
fi
