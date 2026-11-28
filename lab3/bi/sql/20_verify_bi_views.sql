-- =========================================================
-- Лёгкие проверки BI-представлений.
-- Запуск:
--   Get-Content -Raw -Encoding UTF8 lab3/bi/sql/20_verify_bi_views.sql |
--     docker compose exec -T postgres psql -U airflow -d airflow -X -v ON_ERROR_STOP=1
-- Проверки только читают данные; пустые витрины не считаются ошибкой SQL.
-- =========================================================

\echo 'BI views: наличие объектов'
SELECT v AS view_name, to_regclass('marts.' || v) IS NOT NULL AS exists
FROM unnest(ARRAY[
    'bi_top15_games_by_avg_players',
    'bi_activity_dow_hour_utc',
    'bi_reviews_daily',
    'bi_top_genres_by_reviews',
    'bi_genre_sentiment',
    'bi_publisher_reviews_vs_online'
]) AS v
ORDER BY view_name;

\echo 'BI views: число строк'
SELECT 'bi_top15_games_by_avg_players' AS view_name,
       count(*) AS row_count
FROM marts.bi_top15_games_by_avg_players
UNION ALL
SELECT 'bi_activity_dow_hour_utc', count(*)
FROM marts.bi_activity_dow_hour_utc
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
ORDER BY view_name;

\echo 'BI views: доля положительных в допустимом диапазоне'
SELECT 'bi_reviews_daily' AS view_name,
       count(*) FILTER (
           WHERE positive_ratio IS NOT NULL
             AND (positive_ratio < 0 OR positive_ratio > 1)
       ) AS invalid_ratio_rows
FROM marts.bi_reviews_daily
UNION ALL
SELECT 'bi_top_genres_by_reviews',
       count(*) FILTER (
           WHERE positive_ratio IS NOT NULL
             AND (positive_ratio < 0 OR positive_ratio > 1)
       )
FROM marts.bi_top_genres_by_reviews
UNION ALL
SELECT 'bi_genre_sentiment',
       count(*) FILTER (
           WHERE positive_ratio IS NOT NULL
             AND (positive_ratio < 0 OR positive_ratio > 1)
       )
FROM marts.bi_genre_sentiment
UNION ALL
SELECT 'bi_publisher_reviews_vs_online',
       count(*) FILTER (
           WHERE positive_ratio IS NOT NULL
             AND (positive_ratio < 0 OR positive_ratio > 1)
       )
FROM marts.bi_publisher_reviews_vs_online
ORDER BY view_name;

\echo 'BI views: топ-15 не превышает лимит'
SELECT count(*) AS rows, count(*) <= 15 AS within_limit
FROM marts.bi_top15_games_by_avg_players;

\echo 'BI views: топ-10 жанров не превышает лимит'
SELECT count(*) AS rows, count(*) <= 10 AS within_limit
FROM marts.bi_top_genres_by_reviews;

\echo 'BI views: нет дублей ячеек день/час'
SELECT count(*) - count(DISTINCT (day_of_week, hour_of_day)) AS duplicate_cells
FROM marts.bi_activity_dow_hour_utc;

\echo 'BI views: отрицательных отзывов не меньше нуля'
SELECT 'bi_genre_sentiment' AS view_name,
       count(*) FILTER (WHERE negative_reviews < 0) AS invalid_rows
FROM marts.bi_genre_sentiment
UNION ALL
SELECT 'bi_publisher_reviews_vs_online',
       count(*) FILTER (WHERE negative_reviews < 0)
FROM marts.bi_publisher_reviews_vs_online
ORDER BY view_name;

\echo 'BI views: итоговая проверка (ошибка при нарушениях)'
DO $$
DECLARE
    missing_views TEXT;
    invalid_count BIGINT;
BEGIN
    SELECT string_agg(v, ', ' ORDER BY v)
    INTO missing_views
    FROM unnest(ARRAY[
        'bi_top15_games_by_avg_players',
        'bi_activity_dow_hour_utc',
        'bi_reviews_daily',
        'bi_top_genres_by_reviews',
        'bi_genre_sentiment',
        'bi_publisher_reviews_vs_online'
    ]) AS v
    WHERE to_regclass('marts.' || v) IS NULL;

    IF missing_views IS NOT NULL THEN
        RAISE EXCEPTION 'Отсутствуют BI views: %', missing_views;
    END IF;

    SELECT count(*) INTO invalid_count FROM (
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

    IF (SELECT count(*) FROM marts.bi_top15_games_by_avg_players) > 15 THEN
        RAISE EXCEPTION 'bi_top15_games_by_avg_players содержит больше 15 строк';
    END IF;

    IF (SELECT count(*) FROM marts.bi_top_genres_by_reviews) > 10 THEN
        RAISE EXCEPTION 'bi_top_genres_by_reviews содержит больше 10 строк';
    END IF;

    IF (
        SELECT count(*) - count(DISTINCT (day_of_week, hour_of_day))
        FROM marts.bi_activity_dow_hour_utc
    ) > 0 THEN
        RAISE EXCEPTION 'bi_activity_dow_hour_utc содержит дубли ячеек';
    END IF;

    IF EXISTS (SELECT 1 FROM marts.bi_genre_sentiment WHERE negative_reviews < 0)
       OR EXISTS (
           SELECT 1 FROM marts.bi_publisher_reviews_vs_online
           WHERE negative_reviews < 0
       ) THEN
        RAISE EXCEPTION 'Отрицательное число отзывов в категорийной витрине';
    END IF;
END
$$;

\echo 'BI views: итоговая проверка пройдена.'
