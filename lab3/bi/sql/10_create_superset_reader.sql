-- =========================================================
-- Роль Superset только для чтения шести BI-представлений.
-- Пароль не хранится в файле: psql читает его из переменной
-- окружения SUPERSET_READER_PASSWORD через \getenv.
--
-- Пример:
--   docker compose exec -T -e SUPERSET_READER_PASSWORD postgres \
--     psql -U airflow -d airflow -X -q -1 -v ON_ERROR_STOP=1 \
--     < lab3/bi/sql/10_create_superset_reader.sql
--
-- Минимальные права: CONNECT к airflow, USAGE на marts и SELECT
-- только на marts.bi_*. Схемы steam и dds, а также сырые таблицы
-- marts не выдаются: представления выполняются от владельца airflow.
-- Старые grants роли на marts/dds отзываются. Членства в других
-- ролях этот скрипт не аудитирует и не отзывает.
-- =========================================================

\set ON_ERROR_STOP on
\getenv reader_password SUPERSET_READER_PASSWORD

SELECT 'CREATE ROLE superset_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'superset_reader'
)
\gexec

ALTER ROLE superset_reader WITH
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    PASSWORD :'reader_password';

-- На случай, если роль уже существовала с лишними правами.
REVOKE ALL ON DATABASE airflow FROM superset_reader;
GRANT CONNECT ON DATABASE airflow TO superset_reader;

REVOKE USAGE ON SCHEMA dds FROM superset_reader;
REVOKE CREATE ON SCHEMA marts, dds FROM superset_reader;
GRANT USAGE ON SCHEMA marts TO superset_reader;

-- Убираем прежние широкие grants и default privileges.
REVOKE ALL ON ALL TABLES IN SCHEMA marts, dds FROM superset_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE airflow IN SCHEMA marts, dds
    REVOKE SELECT ON TABLES FROM superset_reader;

GRANT SELECT ON
    marts.bi_top15_games_by_avg_players,
    marts.bi_activity_dow_hour_utc,
    marts.bi_reviews_daily,
    marts.bi_top_genres_by_reviews,
    marts.bi_genre_sentiment,
    marts.bi_publisher_reviews_vs_online
TO superset_reader;

-- Дополнительная страховка: сессии роли по умолчанию только для чтения.
ALTER ROLE superset_reader SET default_transaction_read_only = on;
ALTER ROLE superset_reader SET search_path = marts, pg_catalog;

\echo 'Роль superset_reader настроена: SELECT только на шести marts.bi_*; steam/dds закрыты.'
