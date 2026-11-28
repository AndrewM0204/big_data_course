-- =========================================================
-- BI-представления ЛР3 поверх витрин marts (PostgreSQL).
-- Источник имён: sql/marts/init_marts.sql и docs/lab3_contract.md.
--
-- Представления только читают marts и не меняют данные.
-- Средний онлайн везде взвешен по samples: это среднее по снимкам
-- отдельных игр, а не суммарный онлайн категории.
-- SCD2-версии одной игры объединяются по appid.
--
-- Поля marts.mart_activity_hourly:
--   day_of_week — ISODOW (1 = понедельник ... 7 = воскресенье);
--   day_name    — название дня из витрины;
--   is_weekend  — признак субботы/воскресенья.
-- Онлайн собирается раз в 3 часа, поэтому в представлениях есть
-- только фактически наблюдённые часы; отсутствующие часы не
-- досчитываются и не изображаются как реальные данные.
--
-- Запуск (каталог sql не смонтирован в контейнер postgres):
--   Get-Content -Raw -Encoding UTF8 lab3/bi/sql/00_create_bi_views.sql |
--     docker compose exec -T postgres psql -U airflow -d airflow -X -q -1 -v ON_ERROR_STOP=1
-- ВАЖНО: DROP VIEW удаляет и прежние GRANT. После этого файла всегда
-- выполняйте 10_create_superset_reader.sql, иначе роль потеряет доступ;
-- bootstrap.sh делает оба шага подряд.
-- =========================================================

DROP VIEW IF EXISTS marts.bi_top15_games_by_avg_players;
DROP VIEW IF EXISTS marts.bi_activity_dow_hour_utc;
DROP VIEW IF EXISTS marts.bi_weekend_hourly;
DROP VIEW IF EXISTS marts.bi_reviews_daily;
DROP VIEW IF EXISTS marts.bi_top_genres_by_reviews;
DROP VIEW IF EXISTS marts.bi_genre_sentiment;
DROP VIEW IF EXISTS marts.bi_publisher_reviews_vs_online;
DROP VIEW IF EXISTS marts.bi_dq_latest;

-- ---------------------------------------------------------
-- 1. Топ-15 игр по среднему онлайну.
-- Зерно представления: appid (SCD2-версии объединены).
-- Отзывы суммируются по всем датам и версиям: каждая текущая
-- версия отзыва попадает в витрину один раз.
-- ---------------------------------------------------------
CREATE VIEW marts.bi_top15_games_by_avg_players AS
WITH base AS (
    SELECT
        m.appid,
        m.game_sk,
        m.game_name,
        m.activity_date,
        m.avg_players,
        m.max_players,
        m.samples,
        m.review_count,
        m.positive_reviews,
        m.negative_reviews
    FROM marts.mart_game_daily_metrics m
),
name_pick AS (
    SELECT
        appid,
        (array_agg(game_name ORDER BY activity_date DESC, game_sk DESC))[1]
            AS game_name
    FROM base
    GROUP BY appid
),
online_agg AS (
    SELECT
        appid,
        count(DISTINCT game_sk)::int AS scd2_versions_with_online,
        min(activity_date) AS first_online_date,
        max(activity_date) AS last_online_date,
        sum(samples)::bigint AS samples,
        round(
            sum(avg_players * samples) / NULLIF(sum(samples), 0),
            2
        ) AS avg_players_weighted,
        max(max_players)::bigint AS max_players
    FROM base
    WHERE avg_players IS NOT NULL
      AND samples > 0
    GROUP BY appid
),
review_agg AS (
    SELECT
        appid,
        sum(review_count)::bigint AS review_count,
        sum(positive_reviews)::bigint AS positive_reviews,
        sum(negative_reviews)::bigint AS negative_reviews
    FROM base
    GROUP BY appid
)
SELECT
    o.appid,
    n.game_name,
    COALESCE(n.game_name, 'Без названия') || ' [' || o.appid::text || ']'
        AS game_label,
    o.scd2_versions_with_online,
    o.first_online_date,
    o.last_online_date,
    o.samples,
    o.avg_players_weighted,
    o.max_players,
    COALESCE(r.review_count, 0) AS review_count,
    COALESCE(r.positive_reviews, 0) AS positive_reviews,
    COALESCE(r.negative_reviews, 0) AS negative_reviews,
    CASE
        WHEN COALESCE(r.review_count, 0) > 0
        THEN round(r.positive_reviews::numeric / r.review_count, 5)
        ELSE NULL
    END AS positive_ratio
FROM online_agg o
JOIN name_pick n
    ON n.appid = o.appid
LEFT JOIN review_agg r
    ON r.appid = o.appid
ORDER BY o.avg_players_weighted DESC NULLS LAST, o.samples DESC, o.appid
LIMIT 15;

COMMENT ON VIEW marts.bi_top15_games_by_avg_players IS
    'Топ-15 игр по среднему онлайну, взвешенному по samples; SCD2-версии объединены по appid.';

-- ---------------------------------------------------------
-- 2. Активность по дню недели и часу UTC.
-- Зерно представления: (day_of_week, hour_of_day).
-- Используются поля day_of_week, day_name и is_weekend из витрины.
-- ---------------------------------------------------------
CREATE VIEW marts.bi_activity_dow_hour_utc AS
WITH base AS (
    SELECT
        activity_date,
        hour_of_day,
        day_of_week,
        day_name,
        is_weekend,
        appid,
        avg_players,
        max_players,
        samples
    FROM marts.mart_activity_hourly
    WHERE samples > 0
)
SELECT
    b.day_of_week,
    max(b.day_name) AS day_name,
    b.day_of_week::text || ' ' || COALESCE(max(b.day_name), '')
        AS day_label,
    b.hour_of_day,
    bool_or(b.is_weekend) AS is_weekend,
    round(
        sum(b.avg_players * b.samples) / NULLIF(sum(b.samples), 0),
        2
    ) AS avg_players_weighted,
    max(b.max_players)::bigint AS max_players,
    count(DISTINCT b.appid)::int AS games_count,
    count(DISTINCT b.activity_date)::int AS observed_days,
    sum(b.samples)::bigint AS samples
FROM base b
GROUP BY b.day_of_week, b.hour_of_day
ORDER BY b.day_of_week, b.hour_of_day;

COMMENT ON VIEW marts.bi_activity_dow_hour_utc IS
    'Средний онлайн по дню недели (ISODOW) и часу UTC; только фактически собранные часы, взвешивание по samples.';

-- ---------------------------------------------------------
-- 3. Будни и выходные по часам UTC.
-- Зерно представления: (is_weekend, hour_of_day).
-- Выходные определяются полем is_weekend витрины, а не пересчётом даты.
-- ---------------------------------------------------------
CREATE VIEW marts.bi_weekend_hourly AS
WITH base AS (
    SELECT
        activity_date,
        hour_of_day,
        is_weekend,
        appid,
        avg_players,
        max_players,
        samples
    FROM marts.mart_activity_hourly
    WHERE samples > 0
)
SELECT
    b.is_weekend,
    CASE
        WHEN b.is_weekend THEN 'Выходные'
        ELSE 'Будни'
    END AS day_type,
    b.hour_of_day,
    lpad(b.hour_of_day::text, 2, '0') || ':00' AS hour_label,
    round(
        sum(b.avg_players * b.samples) / NULLIF(sum(b.samples), 0),
        2
    ) AS avg_players_weighted,
    max(b.max_players)::bigint AS max_players,
    count(DISTINCT b.appid)::int AS games_count,
    count(DISTINCT b.activity_date)::int AS observed_days,
    sum(b.samples)::bigint AS samples
FROM base b
GROUP BY b.is_weekend, b.hour_of_day
ORDER BY b.is_weekend, b.hour_of_day;

COMMENT ON VIEW marts.bi_weekend_hourly IS
    'Средний онлайн по часам отдельно для будней и выходных (поле is_weekend витрины).';

-- ---------------------------------------------------------
-- 4. Динамика отзывов и доли положительных по дате создания (UTC).
-- Зерно представления: activity_date.
-- Оценка отзыва берётся текущая; это не восстановление тональности
-- на историческую дату.
-- ---------------------------------------------------------
CREATE VIEW marts.bi_reviews_daily AS
SELECT
    m.activity_date,
    sum(m.review_count)::bigint AS review_count,
    sum(m.positive_reviews)::bigint AS positive_reviews,
    sum(m.negative_reviews)::bigint AS negative_reviews,
    CASE
        WHEN sum(m.review_count) > 0
        THEN round(sum(m.positive_reviews)::numeric / sum(m.review_count), 5)
        ELSE NULL
    END AS positive_ratio,
    count(DISTINCT m.appid)::int AS games_with_reviews
FROM marts.mart_game_daily_metrics m
WHERE m.review_count > 0
GROUP BY m.activity_date
ORDER BY m.activity_date;

COMMENT ON VIEW marts.bi_reviews_daily IS
    'Ежедневная динамика текущих отзывов и доли положительных по дате создания UTC.';

-- ---------------------------------------------------------
-- 5. Топ-10 жанров по объёму отзывов.
-- Зерно представления: genre_id.
-- Игра может входить в несколько жанров, поэтому сумма по жанрам
-- не равна числу уникальных отзывов выборки.
-- max_daily_games_count — максимум игр за один день, не union за период.
-- ---------------------------------------------------------
CREATE VIEW marts.bi_top_genres_by_reviews AS
WITH genre_agg AS (
    SELECT
        g.genre_id,
        g.genre_name,
        sum(g.review_count)::bigint AS review_count,
        sum(g.positive_reviews)::bigint AS positive_reviews,
        round(
            sum(g.avg_players * g.samples) / NULLIF(sum(g.samples), 0),
            2
        ) AS avg_players_weighted,
        max(g.max_players)::bigint AS max_players,
        sum(g.samples)::bigint AS samples,
        max(g.games_count)::bigint AS max_daily_games_count,
        count(DISTINCT g.activity_date)::int AS observed_days
    FROM marts.mart_genre_daily_metrics g
    GROUP BY g.genre_id, g.genre_name
)
SELECT
    genre_id,
    genre_name,
    review_count,
    positive_reviews,
    CASE
        WHEN review_count > 0
        THEN round(positive_reviews::numeric / review_count, 5)
        ELSE NULL
    END AS positive_ratio,
    avg_players_weighted,
    max_players,
    samples,
    max_daily_games_count,
    observed_days
FROM genre_agg
ORDER BY review_count DESC, genre_id
LIMIT 10;

COMMENT ON VIEW marts.bi_top_genres_by_reviews IS
    'Топ-10 жанров по сумме отзывов; метрики онлайна неаддитивны между жанрами.';

-- ---------------------------------------------------------
-- 6. Положительные и отрицательные отзывы по жанрам.
-- Зерно представления: genre_id.
-- negative_reviews выводится как review_count - positive_reviews,
-- потому что в витрине жанров отдельной колонки отрицательных нет.
-- ---------------------------------------------------------
CREATE VIEW marts.bi_genre_sentiment AS
SELECT
    g.genre_id,
    g.genre_name,
    sum(g.review_count)::bigint AS review_count,
    sum(g.positive_reviews)::bigint AS positive_reviews,
    (sum(g.review_count) - sum(g.positive_reviews))::bigint
        AS negative_reviews,
    CASE
        WHEN sum(g.review_count) > 0
        THEN round(sum(g.positive_reviews)::numeric / sum(g.review_count), 5)
        ELSE NULL
    END AS positive_ratio,
    round(
        sum(g.avg_players * g.samples) / NULLIF(sum(g.samples), 0),
        2
    ) AS avg_players_weighted,
    max(g.max_players)::bigint AS max_players,
    sum(g.samples)::bigint AS samples,
    max(g.games_count)::bigint AS max_daily_games_count,
    count(DISTINCT g.activity_date)::int AS observed_days
FROM marts.mart_genre_daily_metrics g
GROUP BY g.genre_id, g.genre_name
ORDER BY review_count DESC, genre_id;

COMMENT ON VIEW marts.bi_genre_sentiment IS
    'Доля положительных отзывов по жанрам; отрицательные получены как review_count - positive_reviews.';

-- ---------------------------------------------------------
-- 7. Издатели: отзывы против среднего онлайна.
-- Зерно представления: publisher_id.
-- ---------------------------------------------------------
CREATE VIEW marts.bi_publisher_reviews_vs_online AS
SELECT
    p.publisher_id,
    p.publisher_name,
    sum(p.review_count)::bigint AS review_count,
    sum(p.positive_reviews)::bigint AS positive_reviews,
    (sum(p.review_count) - sum(p.positive_reviews))::bigint
        AS negative_reviews,
    CASE
        WHEN sum(p.review_count) > 0
        THEN round(sum(p.positive_reviews)::numeric / sum(p.review_count), 5)
        ELSE NULL
    END AS positive_ratio,
    round(
        sum(p.avg_players * p.samples) / NULLIF(sum(p.samples), 0),
        2
    ) AS avg_players_weighted,
    max(p.max_players)::bigint AS max_players,
    sum(p.samples)::bigint AS samples,
    max(p.games_count)::bigint AS max_daily_games_count,
    count(DISTINCT p.activity_date)::int AS observed_days
FROM marts.mart_publisher_daily_metrics p
GROUP BY p.publisher_id, p.publisher_name
HAVING sum(p.review_count) > 0 OR sum(p.samples) > 0
ORDER BY review_count DESC, publisher_id;

COMMENT ON VIEW marts.bi_publisher_reviews_vs_online IS
    'Издатели: объём отзывов и средний онлайн, взвешенный по samples; метрики неаддитивны между издателями.';

-- ---------------------------------------------------------
-- 8. Последний результат каждой проверки качества витрин.
-- Источник: marts.mart_dq_results, а не устаревшее marts.dq_results.
-- Зерно представления: check_name (последняя запись по checked_at).
-- ---------------------------------------------------------
CREATE VIEW marts.bi_dq_latest AS
WITH latest AS (
    SELECT DISTINCT ON (d.check_name)
        d.check_name,
        d.run_id,
        d.check_type,
        d.status,
        d.severity,
        d.metric_value,
        d.threshold,
        d.details,
        d.checked_at
    FROM marts.mart_dq_results d
    ORDER BY d.check_name, d.checked_at DESC, d.id DESC
)
SELECT
    check_name,
    run_id,
    check_type,
    status,
    severity,
    metric_value,
    threshold,
    details,
    checked_at,
    (status = 'error') AS is_error,
    (status = 'warning') AS is_warning,
    (status = 'passed') AS is_passed
FROM latest
ORDER BY
    CASE status
        WHEN 'error' THEN 0
        WHEN 'warning' THEN 1
        ELSE 2
    END,
    check_name;

COMMENT ON VIEW marts.bi_dq_latest IS
    'Последний результат каждой DQ-проверки витрин из marts.mart_dq_results.';
