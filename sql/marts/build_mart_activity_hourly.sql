-- =========================================================
-- Сборка marts.mart_activity_hourly.
-- Зерно: (activity_date, hour_of_day, game_sk).
-- Канонический SQL: dags/build_marts_dag.py читает этот файл.
--
-- Контекст выполнения: шаг транзакции DAG после DELETE всех витрин.
-- Файл не идемпотентен сам по себе; ручной запуск обязан соблюдать
-- тот же протокол блокировок, что и DAG (см. build_mart_game_daily.sql).
--
-- ПОЛНЫЙ пересчёт из dds.fact_player_activity без фильтра по окну,
-- обогащённый атрибутами dds.dim_game.
-- День недели — ISODOW (1..7, без зависимости от локали сервера),
-- название дня задано детерминированно, is_weekend — суббота/воскресенье.
--
-- Жанр сюда не добавляется: у игры несколько жанров, поэтому genre_name
-- размножил бы зерно (date, hour, game_sk). Для анализа по жанрам нужно
-- соединять эту витрину с dds.bridge_game_genre + dds.dim_genre и
-- агрегировать явно (например, в BI-представлении), не суммируя игру дважды.
-- Параметры: нет.
-- =========================================================
INSERT INTO marts.mart_activity_hourly (
    activity_date, hour_of_day, day_of_week, day_name, is_weekend,
    game_sk, appid, game_name, is_free, current_players,
    avg_players, max_players, samples, loaded_at
)
SELECT
    a.activity_date,
    a.hour_of_day,
    EXTRACT(ISODOW FROM a.activity_date)::smallint AS day_of_week,
    CASE (EXTRACT(ISODOW FROM a.activity_date))::int
        WHEN 1 THEN 'Понедельник'
        WHEN 2 THEN 'Вторник'
        WHEN 3 THEN 'Среда'
        WHEN 4 THEN 'Четверг'
        WHEN 5 THEN 'Пятница'
        WHEN 6 THEN 'Суббота'
        WHEN 7 THEN 'Воскресенье'
    END AS day_name,
    EXTRACT(ISODOW FROM a.activity_date) IN (6, 7) AS is_weekend,
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
