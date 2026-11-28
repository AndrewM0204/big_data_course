-- =========================================================
-- Проверки BI-представлений ЛР3.
-- Запуск:
--   Get-Content -Raw -Encoding UTF8 lab3/bi/sql/20_verify_bi_views.sql |
--     docker compose exec -T postgres psql -U airflow -d airflow -X -v ON_ERROR_STOP=1
-- Проверки только читают данные. Пустые витрины не считаются ошибкой,
-- но отсутствие обязательных полей витрины — ошибка инфраструктуры.
-- =========================================================

\echo 'BI views: наличие объектов и обязательных полей'

DO $$
DECLARE
    missing_views TEXT;
    column_count INTEGER;
    invalid_count BIGINT;
    duplicate_count BIGINT;
BEGIN
    IF to_regclass('marts.mart_activity_hourly') IS NULL THEN
        RAISE EXCEPTION 'Нет marts.mart_activity_hourly: сначала выполните DAG витрин';
    END IF;

    IF to_regclass('marts.mart_dq_results') IS NULL THEN
        RAISE EXCEPTION 'Нет marts.mart_dq_results: ожидается таблица аудита DAG витрин';
    END IF;

    SELECT count(*) INTO column_count
    FROM information_schema.columns
    WHERE table_schema = 'marts'
      AND table_name = 'mart_activity_hourly'
      AND column_name IN ('day_of_week', 'day_name', 'is_weekend');

    IF column_count <> 3 THEN
        RAISE EXCEPTION
            'В marts.mart_activity_hourly нет полного набора day_of_week/day_name/is_weekend (найдено %)',
            column_count;
    END IF;

    SELECT string_agg(v, ', ' ORDER BY v)
    INTO missing_views
    FROM unnest(ARRAY[
        'bi_top15_games_by_avg_players',
        'bi_activity_dow_hour_utc',
        'bi_weekend_hourly',
        'bi_reviews_daily',
        'bi_top_genres_by_reviews',
        'bi_genre_sentiment',
        'bi_publisher_reviews_vs_online',
        'bi_dq_latest'
    ]) AS v
    WHERE to_regclass('marts.' || v) IS NULL;

    IF missing_views IS NOT NULL THEN
        RAISE EXCEPTION 'Отсутствуют BI views: %', missing_views;
    END IF;

    -- День недели в витрине должен быть ISODOW 1..7, а is_weekend — согласован.
    SELECT count(*) INTO invalid_count
    FROM marts.mart_activity_hourly
    WHERE day_of_week IS NULL
       OR day_of_week NOT BETWEEN 1 AND 7
       OR day_name IS NULL
       OR is_weekend IS NULL
       OR is_weekend IS DISTINCT FROM (day_of_week IN (6, 7));

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'Некорректные day_of_week/day_name/is_weekend в mart_activity_hourly: % строк',
            invalid_count;
    END IF;

    SELECT count(*) INTO invalid_count
    FROM marts.mart_activity_hourly
    WHERE hour_of_day NOT BETWEEN 0 AND 23 OR samples <= 0;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION 'Некорректные час или samples в mart_activity_hourly: % строк', invalid_count;
    END IF;

    SELECT count(*) INTO invalid_count FROM (
        SELECT positive_ratio FROM marts.bi_top15_games_by_avg_players
        UNION ALL
        SELECT positive_ratio FROM marts.bi_reviews_daily
        UNION ALL
        SELECT positive_ratio FROM marts.bi_top_genres_by_reviews
        UNION ALL
        SELECT positive_ratio FROM marts.bi_genre_sentiment
        UNION ALL
        SELECT positive_ratio FROM marts.bi_publisher_reviews_vs_online
    ) AS ratios
    WHERE positive_ratio IS NOT NULL
      AND (positive_ratio < 0 OR positive_ratio > 1);

    IF invalid_count > 0 THEN
        RAISE EXCEPTION 'positive_ratio вне [0,1]: % строк', invalid_count;
    END IF;

    SELECT count(*) INTO invalid_count
    FROM marts.bi_genre_sentiment
    WHERE negative_reviews < 0
       OR positive_reviews > review_count;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION 'Арифметика отзывов по жанрам нарушена: % строк', invalid_count;
    END IF;

    SELECT count(*) INTO invalid_count
    FROM marts.bi_publisher_reviews_vs_online
    WHERE negative_reviews < 0
       OR positive_reviews > review_count;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION 'Арифметика отзывов по издателям нарушена: % строк', invalid_count;
    END IF;

    SELECT count(*) INTO invalid_count
    FROM marts.bi_reviews_daily
    WHERE review_count < 0
       OR positive_reviews < 0
       OR negative_reviews < 0
       OR positive_reviews > review_count;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION 'Арифметика ежедневных отзывов нарушена: % строк', invalid_count;
    END IF;

    SELECT count(*) INTO invalid_count
    FROM marts.bi_top15_games_by_avg_players
    WHERE avg_players_weighted IS NOT NULL
      AND max_players IS NOT NULL
      AND avg_players_weighted > max_players;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION 'Средний онлайн больше максимума в топ-15: % строк', invalid_count;
    END IF;

    SELECT count(*) INTO invalid_count
    FROM marts.bi_activity_dow_hour_utc
    WHERE avg_players_weighted IS NOT NULL
      AND max_players IS NOT NULL
      AND avg_players_weighted > max_players;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION 'Средний онлайн больше максимума в тепловой карте: % строк', invalid_count;
    END IF;

    SELECT count(*) INTO invalid_count
    FROM marts.bi_weekend_hourly
    WHERE is_weekend IS NULL
       OR hour_of_day NOT BETWEEN 0 AND 23
       OR (
           avg_players_weighted IS NOT NULL
           AND max_players IS NOT NULL
           AND avg_players_weighted > max_players
       );

    IF invalid_count > 0 THEN
        RAISE EXCEPTION 'Некорректные строки будни/выходные: %', invalid_count;
    END IF;

    IF (SELECT count(*) FROM marts.bi_top15_games_by_avg_players) > 15 THEN
        RAISE EXCEPTION 'bi_top15_games_by_avg_players содержит больше 15 строк';
    END IF;

    IF (SELECT count(*) FROM marts.bi_top_genres_by_reviews) > 10 THEN
        RAISE EXCEPTION 'bi_top_genres_by_reviews содержит больше 10 строк';
    END IF;

    SELECT count(*) INTO duplicate_count
    FROM (
        SELECT day_of_week, hour_of_day
        FROM marts.bi_activity_dow_hour_utc
        GROUP BY day_of_week, hour_of_day
        HAVING count(*) > 1
    ) AS duplicates;

    IF duplicate_count > 0 THEN
        RAISE EXCEPTION 'bi_activity_dow_hour_utc содержит дубли ячеек: %', duplicate_count;
    END IF;

    SELECT count(*) INTO duplicate_count
    FROM (
        SELECT is_weekend, hour_of_day
        FROM marts.bi_weekend_hourly
        GROUP BY is_weekend, hour_of_day
        HAVING count(*) > 1
    ) AS duplicates;

    IF duplicate_count > 0 THEN
        RAISE EXCEPTION 'bi_weekend_hourly содержит дубли ячеек: %', duplicate_count;
    END IF;

    SELECT count(*) INTO duplicate_count
    FROM (
        SELECT check_name
        FROM marts.bi_dq_latest
        GROUP BY check_name
        HAVING count(*) > 1
    ) AS duplicates;

    IF duplicate_count > 0 THEN
        RAISE EXCEPTION 'bi_dq_latest содержит дубли check_name: %', duplicate_count;
    END IF;

    SELECT count(*) INTO invalid_count
    FROM marts.bi_dq_latest
    WHERE status NOT IN ('passed', 'warning', 'error');

    IF invalid_count > 0 THEN
        RAISE EXCEPTION 'bi_dq_latest содержит неизвестный status: %', invalid_count;
    END IF;
END
$$;

\echo 'BI views: число строк'
SELECT 'bi_top15_games_by_avg_players' AS view_name, count(*) AS row_count
FROM marts.bi_top15_games_by_avg_players
UNION ALL
SELECT 'bi_activity_dow_hour_utc', count(*)
FROM marts.bi_activity_dow_hour_utc
UNION ALL
SELECT 'bi_weekend_hourly', count(*)
FROM marts.bi_weekend_hourly
UNION ALL
SELECT 'bi_reviews_daily', count(*)
FROM marts.bi_reviews_daily
UNION ALL
SELECT 'bi_top_genres_by_reviews', count(*)
FROM marts.bi_top_genres_by_reviews
UNION ALL
SELECT 'bi_genre_sentiment', count(*)
FROM marts.bi_genre_sentiment
UNION ALL
SELECT 'bi_publisher_reviews_vs_online', count(*)
FROM marts.bi_publisher_reviews_vs_online
UNION ALL
SELECT 'bi_dq_latest', count(*)
FROM marts.bi_dq_latest
ORDER BY view_name;

\echo 'BI views: фактически собранные часы (обычно 0,3,6,...,21 UTC)'
SELECT hour_of_day, count(*) AS day_rows, sum(samples) AS samples
FROM marts.bi_activity_dow_hour_utc
GROUP BY hour_of_day
ORDER BY hour_of_day;

\echo 'BI views: права роли superset_reader'
DO $$
BEGIN
    IF to_regrole('superset_reader') IS NULL THEN
        RAISE EXCEPTION 'Роль superset_reader не создана; выполните 10_create_superset_reader.sql';
    END IF;

    IF NOT has_schema_privilege('superset_reader', 'marts', 'USAGE')
       OR NOT has_schema_privilege('superset_reader', 'dds', 'USAGE') THEN
        RAISE EXCEPTION 'superset_reader не имеет USAGE на marts и dds';
    END IF;

    IF NOT has_table_privilege(
        'superset_reader', 'marts.mart_activity_hourly', 'SELECT'
    ) OR NOT has_table_privilege(
        'superset_reader', 'marts.bi_activity_dow_hour_utc', 'SELECT'
    ) OR NOT has_table_privilege(
        'superset_reader', 'dds.fact_player_activity', 'SELECT'
    ) THEN
        RAISE EXCEPTION 'superset_reader не имеет ожидаемого SELECT на marts/dds';
    END IF;

    IF has_table_privilege('superset_reader', 'dds.dim_review', 'SELECT')
       OR has_table_privilege('superset_reader', 'dds.dim_user', 'SELECT')
       OR has_table_privilege('superset_reader', 'steam.ods_reviews', 'SELECT') THEN
        RAISE EXCEPTION 'superset_reader имеет доступ к PII/тексту отзывов';
    END IF;
END
$$;

\echo 'BI views: итоговая проверка пройдена.'
