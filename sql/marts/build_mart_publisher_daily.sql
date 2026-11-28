-- =========================================================
-- Сборка marts.mart_publisher_daily_metrics.
-- Зерно: (activity_date, publisher_id).
-- Канонический SQL: dags/build_marts_dag.py читает этот файл.
--
-- Контекст выполнения: шаг транзакции DAG после сборки
-- marts.mart_game_daily_metrics (из неё берутся база зерна и games_count).
-- Файл не идемпотентен сам по себе; ручной запуск обязан соблюдать
-- тот же протокол блокировок, что и DAG (см. build_mart_game_daily.sql).
--
-- ПОЛНЫЙ пересчёт через ОДИН мост publisher, поэтому факты не перемножаются
-- (нет соединения с genre).
-- games_count — точное число игр издателя (count DISTINCT appid, union множеств).
-- Метрики повторяются для каждого издателя: сумма по издателям не общий итог.
--
-- review_count и positive_reviews считаются как count(DISTINCT review_id)
-- по текущим версиям отзывов (требование ТЗ). Явный DISTINCT защищает от
-- повторов при мостах и нескольких версиях игры.
--
-- Отзывы и активность агрегируются РАЗДЕЛЬНО до зерна (date, publisher_id)
-- и только потом соединяются: соединение двух уже агрегированных наборов
-- не размножает отзывы и снимки онлайна (no fan-out).
-- Параметры: нет.
-- =========================================================
INSERT INTO marts.mart_publisher_daily_metrics (
    activity_date, publisher_id, publisher_name, games_count, review_count,
    positive_reviews, positive_ratio, avg_players, max_players, samples, loaded_at
)
WITH review_agg AS (
    SELECT
        (r.timestamp_created AT TIME ZONE 'UTC')::date AS activity_date,
        b.publisher_id,
        count(DISTINCT r.review_id)::bigint AS review_count,
        (count(DISTINCT r.review_id) FILTER (WHERE r.voted_up))::bigint
            AS positive_reviews
    FROM dds.fact_review f
    JOIN dds.dim_review r
        ON r.review_sk = f.review_sk
       AND r.is_current
    JOIN dds.bridge_game_publisher b
        ON b.game_sk = f.game_sk
    WHERE r.timestamp_created IS NOT NULL
    GROUP BY 1, 2
),
activity_agg AS (
    SELECT
        a.activity_date,
        b.publisher_id,
        round(sum(a.avg_players * a.samples) / nullif(sum(a.samples), 0), 2)::numeric(12, 2)
            AS avg_players,
        max(a.max_players)::bigint AS max_players,
        sum(a.samples)::bigint AS samples
    FROM dds.fact_player_activity a
    JOIN dds.bridge_game_publisher b
        ON b.game_sk = a.game_sk
    GROUP BY 1, 2
),
-- game_base задаёт полный набор (date, publisher) и games_count;
-- метрики отзывов и онлайна здесь не суммируются.
game_base AS (
    SELECT
        m.activity_date,
        dp.publisher_id,
        dp.publisher_name,
        count(DISTINCT m.appid)::bigint AS games_count
    FROM marts.mart_game_daily_metrics m
    JOIN dds.bridge_game_publisher b
        ON b.game_sk = m.game_sk
    JOIN dds.dim_publisher dp
        ON dp.publisher_id = b.publisher_id
    GROUP BY 1, 2, 3
)
SELECT
    gb.activity_date,
    gb.publisher_id,
    gb.publisher_name,
    gb.games_count,
    COALESCE(rv.review_count, 0) AS review_count,
    COALESCE(rv.positive_reviews, 0) AS positive_reviews,
    CASE
        WHEN COALESCE(rv.review_count, 0) > 0
        THEN (rv.positive_reviews::numeric / rv.review_count)::numeric(6, 5)
        ELSE NULL
    END AS positive_ratio,
    act.avg_players,
    act.max_players,
    COALESCE(act.samples, 0),
    now()
FROM game_base gb
LEFT JOIN review_agg rv
    ON rv.activity_date = gb.activity_date
   AND rv.publisher_id = gb.publisher_id
LEFT JOIN activity_agg act
    ON act.activity_date = gb.activity_date
   AND act.publisher_id = gb.publisher_id;
