-- =========================================================
-- Свежесть витрин ЛР3 по источникам (проверка 8) — единый SQL-источник.
-- Канонический SQL: dags/build_marts_dag.py читает этот файл.
--
-- Сверка mart/source НЕ зависит от даты запуска и планировщика. Для каждой
-- витрины максимальная activity_date сверяется с максимальной датой
-- источника, из которого она построена:
--   * mart_game_daily_metrics — max даты онлайн-факта и текущих
--     отзывов с timestamp_created IS NOT NULL (join fact_review);
--   * mart_activity_hourly — max даты dds.fact_player_activity;
--   * genre/publisher — max дата игры с непустой связью в мосте.
-- Пустой источник и пустая витрина — это warning (нет данных для
-- публикации), а расхождение дат при непустом источнике — error.
-- Так первый запуск на неполном ODS не падает из-за календарной даты,
-- но потеря или отставание данных в витрине обнаруживаются.
--
-- Дополнительно SQL возвращает отдельные max-даты источников активности
-- и отзывов. DAG сравнивает их возраст с текущей UTC-датой и настраиваемым
-- порогом (Param source_max_age_days) и выставляет только warning, не error:
-- это сигнал о давности сбора, а не о рассинхроне витрины и источника.
-- Параметры SQL: нет; порог передаётся в DAG.
-- =========================================================
SELECT
    (SELECT max(activity_date) FROM marts.mart_game_daily_metrics)
        AS game_mart_max,
    (SELECT GREATEST(
        (SELECT max(activity_date) FROM dds.fact_player_activity),
        (SELECT max((r.timestamp_created AT TIME ZONE 'UTC')::date)
           FROM dds.fact_review f
           JOIN dds.dim_review r
             ON r.review_sk = f.review_sk
            AND r.is_current
          WHERE r.timestamp_created IS NOT NULL)
    )) AS game_source_max,

    (SELECT max(activity_date) FROM marts.mart_activity_hourly)
        AS activity_mart_max,
    (SELECT max(activity_date) FROM dds.fact_player_activity)
        AS activity_source_max,
    (SELECT max((r.timestamp_created AT TIME ZONE 'UTC')::date)
       FROM dds.fact_review f
       JOIN dds.dim_review r
         ON r.review_sk = f.review_sk
        AND r.is_current
      WHERE r.timestamp_created IS NOT NULL) AS review_source_max,

    (SELECT max(activity_date) FROM marts.mart_genre_daily_metrics)
        AS genre_mart_max,
    (SELECT max(m.activity_date)
       FROM marts.mart_game_daily_metrics m
      WHERE EXISTS (
          SELECT 1 FROM dds.bridge_game_genre b
           WHERE b.game_sk = m.game_sk)) AS genre_source_max,

    (SELECT max(activity_date) FROM marts.mart_publisher_daily_metrics)
        AS publisher_mart_max,
    (SELECT max(m.activity_date)
       FROM marts.mart_game_daily_metrics m
      WHERE EXISTS (
          SELECT 1 FROM dds.bridge_game_publisher b
           WHERE b.game_sk = m.game_sk)) AS publisher_source_max;
