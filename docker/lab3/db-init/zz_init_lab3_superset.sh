#!/bin/bash
# Инициализация ЛР3 в PostgreSQL: отдельная БД Superset, роль чтения и DDL.
# Скрипт идемпотентен: его можно запускать при каждом старте стека.
# Если DDS ещё не создана, DDL витрин и BI-представлений пропускается;
# их применит scripts/lab3_bootstrap.sh после DDS-пайплайнов.
set -euo pipefail

POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-airflow}"
POSTGRES_DB="${POSTGRES_DB:-airflow}"
PGPASSWORD="${PGPASSWORD:-airflow}"
LR3_SUPERSET_DB_USER="${LR3_SUPERSET_DB_USER:-lab3_superset}"
LR3_SUPERSET_DB_NAME="${LR3_SUPERSET_DB_NAME:-lab3_superset}"
LR3_SUPERSET_DB_PASSWORD="${LR3_SUPERSET_DB_PASSWORD:?Не задан LR3_SUPERSET_DB_PASSWORD}"
LR3_SUPERSET_READER_PASSWORD="${LR3_SUPERSET_READER_PASSWORD:?Не задан LR3_SUPERSET_READER_PASSWORD}"

# Ограниченный retry TCP-подключения: postgres может быть healthy не сразу.
PSQL_ATTEMPTS="${LAB3_DB_INIT_ATTEMPTS:-30}"
PSQL_RETRY_SECONDS="${LAB3_DB_INIT_RETRY_SECONDS:-2}"

export PGPASSWORD
export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-5}"
# Секреты передаются psql через окружение и \getenv, а не аргументы командной строки.
export LAB3_SUPERSET_DB_USER="$LR3_SUPERSET_DB_USER"
export LAB3_SUPERSET_DB_PASSWORD="$LR3_SUPERSET_DB_PASSWORD"
export LAB3_SUPERSET_READER_PASSWORD="$LR3_SUPERSET_READER_PASSWORD"

die() {
    echo "ОШИБКА lab3-db-init: $*" >&2
    exit 1
}

# Имена роли и БД подставляются в SQL как идентификаторы: допускаем только
# безопасный шаблон, чтобы исключить инъекцию через .env.
is_identifier() {
    [[ "$1" =~ ^[a-z_][a-z0-9_]{0,62}$ ]]
}

is_bounded_int() {
    [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

is_bounded_int "$PSQL_ATTEMPTS" && ((PSQL_ATTEMPTS <= 120)) ||
    die "LAB3_DB_INIT_ATTEMPTS должен быть целым числом от 1 до 120."
is_bounded_int "$PSQL_RETRY_SECONDS" && ((PSQL_RETRY_SECONDS <= 10)) ||
    die "LAB3_DB_INIT_RETRY_SECONDS должен быть целым числом от 1 до 10."

is_identifier "$LR3_SUPERSET_DB_USER" ||
    die "LR3_SUPERSET_DB_USER должен быть lower-case идентификатором PostgreSQL."
is_identifier "$LR3_SUPERSET_DB_NAME" ||
    die "LR3_SUPERSET_DB_NAME должен быть lower-case идентификатором PostgreSQL."
[[ "$LR3_SUPERSET_DB_NAME" != "$POSTGRES_DB" ]] ||
    die "LR3_SUPERSET_DB_NAME не должен совпадать с БД Airflow ($POSTGRES_DB)."

PSQL=(psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1)

# psql с -h использует TCP; retry ограничен по числу попыток и паузе.
wait_for_psql() {
    local attempt=1
    while ((attempt <= PSQL_ATTEMPTS)); do
        if "${PSQL[@]}" -AtX -c 'SELECT 1' >/dev/null 2>&1; then
            return 0
        fi
        if ((attempt < PSQL_ATTEMPTS)); then
            sleep "$PSQL_RETRY_SECONDS"
        fi
        ((attempt++))
    done
    return 1
}

if ! wait_for_psql; then
    die "PostgreSQL не принял TCP-соединение за ${PSQL_ATTEMPTS} попыток по ${PSQL_RETRY_SECONDS} с."
fi

# --- Роли и их пароли ---------------------------------------------------------
"${PSQL[@]}" <<'SQL'
\getenv superset_db_user LAB3_SUPERSET_DB_USER
\getenv superset_db_password LAB3_SUPERSET_DB_PASSWORD
\getenv superset_reader_password LAB3_SUPERSET_READER_PASSWORD

SELECT format('CREATE ROLE %I LOGIN', :'superset_db_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'superset_db_user')
\gexec

ALTER ROLE :"superset_db_user" WITH LOGIN PASSWORD :'superset_db_password';

SELECT 'CREATE ROLE superset_reader LOGIN'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'superset_reader')
\gexec

ALTER ROLE superset_reader WITH LOGIN PASSWORD :'superset_reader_password';

-- Роль чтения не должна иметь лишних прав на БД Airflow.
REVOKE ALL ON DATABASE airflow FROM superset_reader;
GRANT CONNECT ON DATABASE airflow TO superset_reader;

CREATE SCHEMA IF NOT EXISTS marts;
GRANT USAGE ON SCHEMA marts TO superset_reader;
REVOKE CREATE ON SCHEMA marts FROM superset_reader;

-- Схема dds появляется после DDS-пайплайнов. USAGE без SELECT безвреден,
-- точные права на таблицы выдаёт lab3/bi/sql/10_create_superset_reader.sql.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'dds') THEN
        EXECUTE 'GRANT USAGE ON SCHEMA dds TO superset_reader';
    END IF;
END
$$;

ALTER ROLE superset_reader SET default_transaction_read_only = on;
ALTER ROLE superset_reader SET search_path = marts, pg_catalog;
SQL

# --- Отдельная БД метаданных Superset -----------------------------------------
if ! "${PSQL[@]}" -tAc \
    "SELECT 1 FROM pg_database WHERE datname = '${LR3_SUPERSET_DB_NAME}'" | grep -q 1; then
    createdb -h "$POSTGRES_HOST" -U "$POSTGRES_USER" \
        -O "$LR3_SUPERSET_DB_USER" "$LR3_SUPERSET_DB_NAME"
fi

"${PSQL[@]}" -c \
    "ALTER DATABASE \"${LR3_SUPERSET_DB_NAME}\" OWNER TO \"${LR3_SUPERSET_DB_USER}\";" \
    -c "GRANT CONNECT ON DATABASE \"${LR3_SUPERSET_DB_NAME}\" TO \"${LR3_SUPERSET_DB_USER}\";"

# --- DDL витрин и BI-представлений только после готовности DDS ----------------
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

missing_dds="$("${PSQL[@]}" -AtX -c \
    "SELECT string_agg(t, ', ' ORDER BY t)
       FROM unnest(ARRAY[${DDS_LIST}]::text[]) AS t
      WHERE to_regclass(t) IS NULL;")"

if [[ -n "$missing_dds" ]]; then
    echo "DDS ещё не создана (нет: $missing_dds)."
    echo "Витрины и BI-представления применит scripts/lab3_bootstrap.sh после DDS-пайплайнов."
    exit 0
fi

[[ -r /marts/init_marts.sql ]] ||
    die "Не найден /marts/init_marts.sql (проверьте монтирование sql/marts)."

echo "Применяю единый DDL витрин из sql/marts..."
"${PSQL[@]}" -f /marts/init_marts.sql

if [[ -r /bi/00_create_bi_views.sql ]]; then
    echo "Применяю BI-представления marts.bi_*..."
    "${PSQL[@]}" -f /bi/00_create_bi_views.sql

    if [[ -r /bi/10_create_superset_reader.sql ]]; then
        echo "Настраиваю права роли superset_reader на BI-представления..."
        SUPERSET_READER_PASSWORD="$LR3_SUPERSET_READER_PASSWORD" \
            "${PSQL[@]}" -f /bi/10_create_superset_reader.sql
    else
        echo "ВНИМАНИЕ: не найден /bi/10_create_superset_reader.sql; права reader не выданы."
    fi
else
    echo "ВНИМАНИЕ: не найден /bi/00_create_bi_views.sql; BI-представления не созданы."
fi

echo "Инициализация ЛР3 завершена."
