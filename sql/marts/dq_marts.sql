-- =========================================================
-- Проверки качества витрин ЛР3 (1-7) — единый SQL-источник.
-- Канонический SQL: dags/build_marts_dag.py читает этот файл.
--
-- Запрос возвращает ОДНУ строку с метриками; DAG сопоставляет
-- колонки по именам и сам выставляет статусы. Проверки идут по всей
-- витрине, потому что витрины пересчитываются целиком.
--
-- Контекст выполнения: та же транзакция публикации (REPEATABLE READ),
-- до commit; при статусе error публикация откатывается. Поэтому DQ
-- видит ровно те строки, которые будут опубликованы, и не конкурирует
-- с параллельным издателем, соблюдающим общий advisory-ключ и LOCK TABLE.
--
-- Coverage отзывов (проверка 3) намеренно использует фильтр
-- r.timestamp_created IS NOT NULL — ту же границу, что и сборка
-- mart_game_daily_metrics. Текущие отзывы без даты не имеют дневного
-- зерна и не должны попадать в источник; их число возвращается
-- отдельной метрикой reviews_without_timestamp (warning, не error).
-- Параметры: нет.
-- =========================================================
SELECT
    -- 1. Уникальность зерна (дубликаты первичных ключей).
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
        GROUP BY 1, 2 HAVING count(*) > 1) d) AS duplicate_keys,

    -- 2. Доля положительных отзывов в диапазоне [0, 1] и согласованность со счётчиком.
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
        OR (review_count > 0 AND positive_ratio IS NULL)) AS bad_ratio_rows,

    -- 3. Coverage отзывов: витрина игр равна текущим фактам с непустой датой.
    COALESCE((SELECT sum(review_count) FROM marts.mart_game_daily_metrics), 0)::bigint
        AS mart_reviews,
    (SELECT count(*) FROM dds.fact_review f
     JOIN dds.dim_review r
       ON r.review_sk = f.review_sk
      AND r.is_current
     WHERE r.timestamp_created IS NOT NULL) AS source_reviews,
    (SELECT count(*) FROM dds.dim_review r
     LEFT JOIN dds.fact_review f
       ON f.review_sk = r.review_sk
     WHERE r.is_current
       AND r.timestamp_created IS NOT NULL
       AND f.review_sk IS NULL) AS current_reviews_without_fact,
    (SELECT count(*) FROM dds.dim_review r
     WHERE r.is_current
       AND r.timestamp_created IS NULL) AS reviews_without_timestamp,

    -- 4. Арифметика отзывов: counts и positive_ratio согласованы.
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
            AND abs(positive_ratio - positive_reviews::numeric / review_count) > 0.00001)) AS publisher_bad,

    -- 5. Средний онлайн не превышает максимум и не отрицателен.
    (SELECT count(*) FROM marts.mart_game_daily_metrics
     WHERE avg_players < 0 OR max_players < 0 OR current_players < 0
        OR avg_players > max_players)
  + (SELECT count(*) FROM marts.mart_activity_hourly
     WHERE current_players < 0 OR avg_players < 0 OR max_players < 0
        OR avg_players > max_players)
  + (SELECT count(*) FROM marts.mart_genre_daily_metrics
     WHERE avg_players < 0 OR max_players < 0 OR avg_players > max_players)
  + (SELECT count(*) FROM marts.mart_publisher_daily_metrics
     WHERE avg_players < 0 OR max_players < 0 OR avg_players > max_players) AS bad_avg_rows,

    -- 6. Полнота активности: точки и samples витрин совпадают с фактом, часы корректны.
    (SELECT count(*) FROM dds.fact_player_activity) AS source_points,
    (SELECT count(*) FROM marts.mart_activity_hourly) AS mart_points,
    (SELECT COALESCE(sum(samples), 0)::bigint FROM dds.fact_player_activity) AS source_samples,
    (SELECT COALESCE(sum(samples), 0)::bigint FROM marts.mart_game_daily_metrics) AS daily_samples,
    (SELECT count(*) FROM marts.mart_game_daily_metrics
     WHERE hours_covered < 0 OR hours_covered > 24
        OR (samples = 0 AND (avg_players IS NOT NULL OR max_players IS NOT NULL))
        OR (samples > 0 AND hours_covered = 0)) AS hours_bad,

    -- 7. Сироты: ключи витрин отсутствуют в измерениях.
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
