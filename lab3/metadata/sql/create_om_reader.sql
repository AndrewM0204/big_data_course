-- =========================================================
-- Роль OpenMetadata только для чтения ODS/DDS/витрин.
-- Пароль передаётся через переменную окружения OM_POSTGRES_PASSWORD
-- (psql \getenv), в файле не хранится.
--
-- Пример:
--   docker compose exec -T -e OM_POSTGRES_PASSWORD postgres \
--     psql -U airflow -d airflow -X -q -1 -v ON_ERROR_STOP=1 \
--     < lab3/metadata/sql/create_om_reader.sql
-- =========================================================

\set ON_ERROR_STOP on
\getenv reader_password OM_POSTGRES_PASSWORD

SELECT 'CREATE ROLE om_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'om_reader'
)
\gexec

ALTER ROLE om_reader WITH
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    PASSWORD :'reader_password';

REVOKE ALL ON DATABASE airflow FROM om_reader;
GRANT CONNECT ON DATABASE airflow TO om_reader;

GRANT USAGE ON SCHEMA steam, dds, marts TO om_reader;
REVOKE CREATE ON SCHEMA steam, dds, marts FROM om_reader;

GRANT SELECT ON ALL TABLES IN SCHEMA steam, dds, marts TO om_reader;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
    ON ALL TABLES IN SCHEMA steam, dds, marts FROM om_reader;

ALTER DEFAULT PRIVILEGES FOR ROLE airflow IN SCHEMA steam, dds, marts
    GRANT SELECT ON TABLES TO om_reader;

ALTER ROLE om_reader SET default_transaction_read_only = on;
ALTER ROLE om_reader SET search_path = steam, dds, marts, pg_catalog;

\echo 'Роль om_reader настроена: SELECT на steam/dds/marts.'
