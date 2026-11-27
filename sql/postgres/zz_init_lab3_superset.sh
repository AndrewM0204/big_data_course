#!/bin/bash
set -euo pipefail

POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
POSTGRES_USER="${POSTGRES_USER:-airflow}"
POSTGRES_DB="${POSTGRES_DB:-airflow}"
PGPASSWORD="${PGPASSWORD:-airflow}"
SUPERSET_DB_PASSWORD="${SUPERSET_DB_PASSWORD:-superset}"
SUPERSET_READER_PASSWORD="${SUPERSET_READER_PASSWORD:-superset_reader}"

export PGPASSWORD

PSQL=(psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1)

"${PSQL[@]}" <<SQL
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'superset') THEN
        CREATE ROLE superset LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'superset_reader') THEN
        CREATE ROLE superset_reader LOGIN;
    END IF;
END
\$\$;

ALTER ROLE superset WITH PASSWORD '${SUPERSET_DB_PASSWORD}';
ALTER ROLE superset_reader WITH PASSWORD '${SUPERSET_READER_PASSWORD}';

CREATE SCHEMA IF NOT EXISTS marts;

GRANT CONNECT ON DATABASE airflow TO superset_reader;
GRANT USAGE ON SCHEMA dds, marts TO superset_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA dds, marts TO superset_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA dds, marts
    GRANT SELECT ON TABLES TO superset_reader;
SQL

if ! "${PSQL[@]}" -tAc "SELECT 1 FROM pg_database WHERE datname = 'superset'" | grep -q 1; then
    createdb -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -O superset superset
fi

"${PSQL[@]}" -c "GRANT CONNECT ON DATABASE superset TO superset;"

if [ -f /marts/init_marts.sql ]; then
    "${PSQL[@]}" -f /marts/init_marts.sql
fi

echo "Lab3 Superset database, read-only role and marts schema are ready."
