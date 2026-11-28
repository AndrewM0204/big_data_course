-- =========================================================
-- Сборка marts.mart_publisher_daily_metrics.
-- Зерно: (activity_date, publisher_id).
-- ПОЛНЫЙ пересчёт из marts.mart_game_daily_metrics через ОДИН мост publisher,
-- поэтому факты не перемножаются (нет соединения с genre).
-- games_count — точное число игр издателя (count DISTINCT game_sk, union множеств).
-- Метрики повторяются для каждого издателя: сумма по издателям не общий итог.
-- Параметры: нет.
-- Синхронизировано с dags/build_marts_dag.py (SQL_BUILD_PUBLISHER_DAILY).
-- =========================================================
INSERT INTO marts.mart_publisher_daily_metrics (
    activity_date, publisher_id, publisher_name, games_count, review_count,
    positive_reviews, positive_ratio, avg_players, max_players, samples, loaded_at
)
SELECT
    m.activity_date,
    dp.publisher_id,
    dp.publisher_name,
    count(DISTINCT m.appid)::bigint AS games_count,
    sum(m.review_count)::bigint AS review_count,
    sum(m.positive_reviews)::bigint AS positive_reviews,
    CASE
        WHEN sum(m.review_count) > 0
        THEN (sum(m.positive_reviews)::numeric / sum(m.review_count))::numeric(6, 5)
        ELSE NULL
    END AS positive_ratio,
    round(sum(m.avg_players * m.samples) / nullif(sum(m.samples), 0), 2)::numeric(12, 2)
        AS avg_players,
    max(m.max_players)::bigint AS max_players,
    sum(m.samples)::bigint AS samples,
    now()
FROM marts.mart_game_daily_metrics m
JOIN dds.bridge_game_publisher b
    ON b.game_sk = m.game_sk
JOIN dds.dim_publisher dp
    ON dp.publisher_id = b.publisher_id
GROUP BY m.activity_date, dp.publisher_id, dp.publisher_name;
