-- =========================================================
-- Роль Superset только для чтения витрин marts и аналитического DDS.
-- Пароль не хранится в файле: psql читает его из переменной
-- окружения SUPERSET_READER_PASSWORD через \getenv.
--
-- Пример:
--   docker compose exec -T -e SUPERSET_READER_PASSWORD postgres \
--     psql -U airflow -d airflow -X -q -1 -v ON_ERROR_STOP=1 \
--     < lab3/bi/sql/10_create_superset_reader.sql
--
-- Состав прав:
--   * CONNECT к airflow, USAGE на marts и dds;
--   * SELECT на пять таблиц marts и восемь marts.bi_*;
--   * SELECT на безопасные аналитические таблицы dds:
--     dim_game, dim_genre, dim_developer, dim_publisher,
--     bridge_game_genre/developer/publisher, fact_player_activity,
--     dq_results;
--   * НЕТ доступа к steam, dds.dim_review (текст отзывов),
--     dds.dim_user (steamid) и dds.fact_review.
-- Сырые marts — агрегаты без PII; DDS с персональными данными закрыт.
-- Старые широкие grants и default privileges роли отзываются.
-- Членства в других ролях этот скрипт не аудитирует и не отзывает.
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

REVOKE USAGE ON SCHEMA steam FROM superset_reader;
REVOKE CREATE ON SCHEMA marts, dds, steam FROM superset_reader;
GRANT USAGE ON SCHEMA marts, dds TO superset_reader;

-- Убираем прежние широкие grants и default privileges.
REVOKE ALL ON ALL TABLES IN SCHEMA marts, dds, steam FROM superset_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE airflow IN SCHEMA marts, dds, steam
    REVOKE SELECT ON TABLES FROM superset_reader;

GRANT SELECT ON
    marts.mart_game_daily_metrics,
    marts.mart_activity_hourly,
    marts.mart_genre_daily_metrics,
    marts.mart_publisher_daily_metrics,
    marts.mart_dq_results,
    marts.bi_top15_games_by_avg_players,
    marts.bi_activity_dow_hour_utc,
    marts.bi_weekend_hourly,
    marts.bi_reviews_daily,
    marts.bi_top_genres_by_reviews,
    marts.bi_genre_sentiment,
    marts.bi_publisher_reviews_vs_online,
    marts.bi_dq_latest
TO superset_reader;

GRANT SELECT ON
    dds.dim_game,
    dds.dim_genre,
    dds.dim_developer,
    dds.dim_publisher,
    dds.bridge_game_genre,
    dds.bridge_game_developer,
    dds.bridge_game_publisher,
    dds.fact_player_activity,
    dds.dq_results
TO superset_reader;

-- Дополнительная страховка: сессии роли по умолчанию только для чтения.
ALTER ROLE superset_reader SET default_transaction_read_only = on;
ALTER ROLE superset_reader SET search_path = marts, dds, pg_catalog;

\echo 'Роль superset_reader настроена: marts и безопасный DDS; steam и PII-таблицы DDS закрыты.'
