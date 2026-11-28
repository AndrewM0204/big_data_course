-- =========================================================
-- Проверки качества витрин ЛР3 (справочные запросы).
-- Витрины пересчитываются целиком, поэтому проверки 1-7 идут по всей таблице;
-- проверка свежести (8) — отдельно по последним закрытым UTC-суткам.
-- Проверки 1-7 выполняются в той же транзакции (REPEATABLE READ), что и сборка
-- витрин, ДО commit; при ошибке публикация целиком откатывается.
-- Реализация — в dags/build_marts_dag.py (функции DQ_*).
-- Параметр (только свежесть): %(day)s = закрытая дата (date).
-- =========================================================

-- 1. Уникальность зерна (дубликаты первичных ключей).
SELECT
    (SELECT count(*) FROM (
        SELECT activity_date, game_sk FROM marts.mart_game_daily_metrics
        GROUP BY 1, 2 HAVING count(*) > 1) d)
  + (SELECT count(*) FROM (
        SELECT activity_date, hour_of_day, game_sk FROM marts.mart_activity_hourly
        GROUP BY 1, 2, 3 HAVING count(*) > 1) d)
  + (SELECT count(*) FROM (
        SELECT activity_date, genre_id FROM marts.mart_genre_daily_metrics
        GROUP BY 1, 2 HAVING count(*) > 1) d)
  + (SELECT count(*) FROM (
        SELECT activity_date, publisher_id FROM marts.mart_publisher_daily_metrics
        GROUP BY 1, 2 HAVING count(*) > 1) d) AS duplicate_keys;

-- 2. Доля положительных отзывов в диапазоне [0, 1] и согласованность со счётчиком.
SELECT
    (SELECT count(*) FROM marts.mart_game_daily_metrics
     WHERE positive_ratio < 0 OR positive_ratio > 1
        OR (review_count = 0 AND positive_ratio IS NOT NULL)
        OR (review_count > 0 AND positive_ratio IS NULL))
  + (SELECT count(*) FROM marts.mart_genre_daily_metrics
     WHERE positive_ratio < 0 OR positive_ratio > 1
        OR (review_count = 0 AND positive_ratio IS NOT NULL)
        OR (review_count > 0 AND positive_ratio IS NULL))
  + (SELECT count(*) FROM marts.mart_publisher_daily_metrics
     WHERE positive_ratio < 0 OR positive_ratio > 1
        OR (review_count = 0 AND positive_ratio IS NOT NULL)
        OR (review_count > 0 AND positive_ratio IS NULL)) AS bad_ratio_rows;

-- 3. Число отзывов в витрине точно равно источнику (текущие факты отзывов).
SELECT
    COALESCE((SELECT sum(review_count) FROM marts.mart_game_daily_metrics), 0)::bigint AS mart_reviews,
    (SELECT count(*) FROM dds.fact_review f
     JOIN dds.dim_review r ON r.review_sk = f.review_sk
     WHERE r.is_current
       AND r.timestamp_created IS NOT NULL) AS source_reviews,
    (SELECT count(*) FROM dds.dim_review r
     LEFT JOIN dds.fact_review f ON f.review_sk = r.review_sk
     WHERE r.is_current AND r.timestamp_created IS NOT NULL
       AND f.review_sk IS NULL) AS missing_facts;

-- 4. Арифметика отзывов: counts и positive_ratio согласованы.
SELECT
    (SELECT count(*) FROM marts.mart_game_daily_metrics
     WHERE positive_reviews + negative_reviews <> review_count) AS game_count_bad,
    (SELECT count(*) FROM marts.mart_game_daily_metrics
     WHERE review_count > 0
       AND abs(positive_ratio - positive_reviews::numeric / review_count) > 0.00001) AS game_ratio_bad,
    (SELECT count(*) FROM marts.mart_genre_daily_metrics
     WHERE positive_reviews > review_count
        OR (review_count > 0
            AND abs(positive_ratio - positive_reviews::numeric / review_count) > 0.00001)) AS genre_bad,
    (SELECT count(*) FROM marts.mart_publisher_daily_metrics
     WHERE positive_reviews > review_count
        OR (review_count > 0
            AND abs(positive_ratio - positive_reviews::numeric / review_count) > 0.00001)) AS publisher_bad;

-- 5. Средний онлайн не превышает максимум и не отрицателен.
SELECT
    (SELECT count(*) FROM marts.mart_game_daily_metrics
     WHERE avg_players < 0 OR max_players < 0 OR current_players < 0
        OR avg_players > max_players)
  + (SELECT count(*) FROM marts.mart_activity_hourly
     WHERE current_players < 0 OR avg_players < 0 OR max_players < 0
        OR avg_players > max_players)
  + (SELECT count(*) FROM marts.mart_genre_daily_metrics
     WHERE avg_players < 0 OR max_players < 0 OR avg_players > max_players)
  + (SELECT count(*) FROM marts.mart_publisher_daily_metrics
     WHERE avg_players < 0 OR max_players < 0 OR avg_players > max_players) AS bad_avg_rows;

-- 6. Полнота активности: точки и samples витрин совпадают с фактом, часы корректны.
SELECT
    (SELECT count(*) FROM dds.fact_player_activity) AS source_points,
    (SELECT count(*) FROM marts.mart_activity_hourly) AS mart_points,
    (SELECT COALESCE(sum(samples), 0)::bigint FROM dds.fact_player_activity) AS source_samples,
    (SELECT COALESCE(sum(samples), 0)::bigint FROM marts.mart_game_daily_metrics) AS daily_samples,
    (SELECT count(*) FROM marts.mart_game_daily_metrics
     WHERE hours_covered < 0 OR hours_covered > 24
        OR (samples = 0 AND (avg_players IS NOT NULL OR max_players IS NOT NULL))
        OR (samples > 0 AND hours_covered = 0)) AS hours_bad;

-- 7. Сироты: ключи витрин отсутствуют в измерениях.
SELECT
    (SELECT count(*) FROM marts.mart_game_daily_metrics m
     LEFT JOIN dds.dim_game g ON g.game_sk = m.game_sk
     WHERE g.game_sk IS NULL)
  + (SELECT count(*) FROM marts.mart_activity_hourly m
     LEFT JOIN dds.dim_game g ON g.game_sk = m.game_sk
     WHERE g.game_sk IS NULL)
  + (SELECT count(*) FROM marts.mart_genre_daily_metrics m
     LEFT JOIN dds.dim_genre d ON d.genre_id = m.genre_id
     WHERE d.genre_id IS NULL)
  + (SELECT count(*) FROM marts.mart_publisher_daily_metrics m
     LEFT JOIN dds.dim_publisher d ON d.publisher_id = m.publisher_id
     WHERE d.publisher_id IS NULL) AS orphan_rows;

-- 8. Свежесть: каждая витрина имеет строки за последние закрытые UTC-сутки.
SELECT
    (SELECT count(*) FROM marts.mart_game_daily_metrics WHERE activity_date = %(day)s) AS game_rows,
    (SELECT count(*) FROM marts.mart_activity_hourly WHERE activity_date = %(day)s) AS activity_rows,
    (SELECT count(*) FROM marts.mart_genre_daily_metrics WHERE activity_date = %(day)s) AS genre_rows,
    (SELECT count(*) FROM marts.mart_publisher_daily_metrics WHERE activity_date = %(day)s) AS publisher_rows;
