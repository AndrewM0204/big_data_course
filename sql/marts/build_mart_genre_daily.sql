-- =========================================================
-- Сборка marts.mart_genre_daily_metrics.
-- Зерно: (activity_date, genre_id).
-- ПОЛНЫЙ пересчёт из marts.mart_game_daily_metrics через ОДИН мост genre,
-- поэтому факты не перемножаются (нет соединения с publisher).
-- games_count — точное число игр жанра (count DISTINCT game_sk, union множеств).
-- Метрики повторяются для каждого жанра: сумма по жанрам не общий итог.
-- Параметры: нет.
-- Синхронизировано с dags/build_marts_dag.py (SQL_BUILD_GENRE_DAILY).
-- =========================================================
INSERT INTO marts.mart_genre_daily_metrics (
    activity_date, genre_id, genre_name, games_count, review_count,
    positive_reviews, positive_ratio, avg_players, max_players, samples, loaded_at
)
SELECT
    m.activity_date,
    dg.genre_id,
    dg.genre_name,
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
JOIN dds.bridge_game_genre b
    ON b.game_sk = m.game_sk
JOIN dds.dim_genre dg
    ON dg.genre_id = b.genre_id
GROUP BY m.activity_date, dg.genre_id, dg.genre_name;
