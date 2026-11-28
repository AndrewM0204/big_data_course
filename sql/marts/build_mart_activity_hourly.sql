-- =========================================================
-- Сборка marts.mart_activity_hourly.
-- Зерно: (activity_date, hour_of_day, game_sk).
-- ПОЛНЫЙ пересчёт из dds.fact_player_activity без фильтра по окну,
-- обогащённый атрибутами dds.dim_game.
-- Параметры: нет.
-- Синхронизировано с dags/build_marts_dag.py (SQL_BUILD_ACTIVITY_HOURLY).
-- =========================================================
INSERT INTO marts.mart_activity_hourly (
    activity_date, hour_of_day, game_sk, appid, game_name, is_free,
    current_players, avg_players, max_players, samples, loaded_at
)
SELECT
    a.activity_date,
    a.hour_of_day,
    a.game_sk,
    g.appid,
    g.name,
    g.is_free,
    a.current_players,
    a.avg_players,
    a.max_players,
    a.samples,
    now()
FROM dds.fact_player_activity a
JOIN dds.dim_game g
    ON g.game_sk = a.game_sk;
