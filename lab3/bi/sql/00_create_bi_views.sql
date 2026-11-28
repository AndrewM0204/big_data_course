-- =========================================================
-- BI-представления ЛР3 поверх витрин marts (PostgreSQL).
-- Источник имён: docs/lab3_contract.md, sql/marts/init_marts.sql.
--
-- Представления только читают marts и не меняют данные.
-- Все средние онлайна взвешены по samples и описывают среднее
-- по снимкам отдельных игр, а не суммарный онлайн категории.
-- SCD2-версии одной игры объединяются по appid.
--
-- Запуск (каталог sql не смонтирован в контейнер postgres):
--   Get-Content -Raw -Encoding UTF8 lab3/bi/sql/00_create_bi_views.sql |
--     docker compose exec -T postgres psql -U airflow -d airflow -X -q -1 -v ON_ERROR_STOP=1
-- =========================================================

DROP VIEW IF EXISTS marts.bi_top15_games_by_avg_players;
DROP VIEW IF EXISTS marts.bi_activity_dow_hour_utc;
DROP VIEW IF EXISTS marts.bi_reviews_daily;
DROP VIEW IF EXISTS marts.bi_top_genres_by_reviews;
DROP VIEW IF EXISTS marts.bi_genre_sentiment;
DROP VIEW IF EXISTS marts.bi_publisher_reviews_vs_online;

-- ---------------------------------------------------------
-- 1. Топ-15 игр по среднему онлайну.
-- Зерно представления: appid (SCD2-версии объединены).
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
-- Зерно представления: (day_of_week_num, hour_of_day).
-- Дата и час в витрине получены в UTC.
-- ---------------------------------------------------------
CREATE VIEW marts.bi_activity_dow_hour_utc AS
WITH base AS (
    SELECT
        activity_date,
        hour_of_day,
        game_sk,
        avg_players,
        max_players,
        samples,
        EXTRACT(ISODOW FROM activity_date)::int AS day_of_week_num
    FROM marts.mart_activity_hourly
)
SELECT
    day_of_week_num,
    day_of_week_num::text || '-' ||
        CASE day_of_week_num
            WHEN 1 THEN 'Пн'
            WHEN 2 THEN 'Вт'
            WHEN 3 THEN 'Ср'
            WHEN 4 THEN 'Чт'
            WHEN 5 THEN 'Пт'
            WHEN 6 THEN 'Сб'
            WHEN 7 THEN 'Вс'
        END AS day_of_week,
    hour_of_day,
    round(
        sum(avg_players * samples) / NULLIF(sum(samples), 0),
        2
    ) AS avg_players_weighted,
    max(max_players)::bigint AS max_players,
    count(DISTINCT game_sk)::int AS games_count,
    sum(samples)::bigint AS samples
FROM base
GROUP BY day_of_week_num, hour_of_day
ORDER BY day_of_week_num, hour_of_day;

COMMENT ON VIEW marts.bi_activity_dow_hour_utc IS
    'Средний онлайн по дню недели (ISODOW) и часу UTC, взвешенный по samples; max_players — максимум отдельной игры.';

-- ---------------------------------------------------------
-- 3. Динамика отзывов и доли положительных по дате создания (UTC).
-- Зерно представления: activity_date.
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
    END AS positive_ratio
FROM marts.mart_game_daily_metrics m
WHERE m.review_count > 0
GROUP BY m.activity_date
ORDER BY m.activity_date;

COMMENT ON VIEW marts.bi_reviews_daily IS
    'Ежедневная динамика текущих отзывов и доли положительных по дате создания UTC.';

-- ---------------------------------------------------------
-- 4. Топ-10 жанров по объёму отзывов.
-- Зерно представления: genre_id.
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
        max(g.games_count)::bigint AS max_daily_games_count
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
    max_daily_games_count
FROM genre_agg
ORDER BY review_count DESC, genre_id
LIMIT 10;

COMMENT ON VIEW marts.bi_top_genres_by_reviews IS
    'Топ-10 жанров по сумме отзывов; метрики онлайна повторяются для каждого жанра и не суммируются между категориями.';

-- ---------------------------------------------------------
-- 5. Положительные и отрицательные отзывы по жанрам.
-- Зерно представления: genre_id.
-- negative_reviews выводится как review_count - positive_reviews.
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
    max(g.games_count)::bigint AS max_daily_games_count
FROM marts.mart_genre_daily_metrics g
GROUP BY g.genre_id, g.genre_name
ORDER BY review_count DESC, genre_id;

COMMENT ON VIEW marts.bi_genre_sentiment IS
    'Доля положительных отзывов по жанрам; отрицательные получены как review_count - positive_reviews.';

-- ---------------------------------------------------------
-- 6. Издатели: отзывы против среднего онлайна.
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
    max(p.games_count)::bigint AS max_daily_games_count
FROM marts.mart_publisher_daily_metrics p
GROUP BY p.publisher_id, p.publisher_name
HAVING sum(p.review_count) > 0 OR sum(p.samples) > 0
ORDER BY review_count DESC, publisher_id;

COMMENT ON VIEW marts.bi_publisher_reviews_vs_online IS
    'Издатели: объём отзывов и средний онлайн, взвешенный по samples; метрики не суммируются между издателями.';
