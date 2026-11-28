-- =========================================================
-- Сборка marts.mart_game_daily_metrics.
-- Зерно: (activity_date, game_sk).
-- Канонический SQL: dags/build_marts_dag.py читает этот файл,
-- копии запроса в Python нет.
--
-- Контекст выполнения: шаг транзакции DAG после DELETE всех витрин.
-- Сам по себе файл не идемпотентен: повторный запуск без очистки
-- нарушит первичный ключ. Ручная публикация обязана соблюдать тот
-- же протокол: LOCK TABLE ... ACCESS EXCLUSIVE, advisory-ключ 48234001,
-- DELETE и INSERT в одной транзакции.
--
-- ПОЛНЫЙ пересчёт: витрина целиком строится из DDS без фильтра по окну,
-- чтобы смена current dim_review (collected_at входит в row_hash)
-- пересчитывала всю историю отзывов, а первый запуск заполнял прошлые даты.
-- Независимые факты (отзывы и онлайн) агрегируются порознь до этого
-- зерна и соединяются ПОЛНЫМ ВНЕШНИМ соединением (FULL OUTER JOIN).
--
-- Coverage отзывов: только текущие версии dds.dim_review и только
-- строки с timestamp_created IS NOT NULL. Эта же граница используется
-- в DQ-проверке review_count_source, поэтому счётчики сопоставимы.
-- Онлайн: средний онлайн взвешивается по samples.
-- Параметры: нет.
-- =========================================================
INSERT INTO marts.mart_game_daily_metrics (
    activity_date, game_sk, appid, game_name, is_free, release_date,
    review_count, positive_reviews, negative_reviews, positive_ratio,
    avg_weighted_score, sum_votes_up, sum_votes_funny, avg_playtime_at_review,
    avg_players, max_players, current_players, samples, hours_covered, loaded_at
)
WITH review_agg AS (
    SELECT
        (r.timestamp_created AT TIME ZONE 'UTC')::date AS activity_date,
        f.game_sk AS game_sk,
        count(*)::bigint AS review_count,
        count(*) FILTER (WHERE r.voted_up)::bigint AS positive_reviews,
        count(*) FILTER (WHERE NOT r.voted_up)::bigint AS negative_reviews,
        avg(r.weighted_score)::numeric(6, 5) AS avg_weighted_score,
        sum(r.votes_up)::bigint AS sum_votes_up,
        sum(r.votes_funny)::bigint AS sum_votes_funny,
        avg(r.author_playtime_at_review)::numeric(14, 2) AS avg_playtime_at_review
    FROM dds.fact_review f
    JOIN dds.dim_review r
      ON r.review_sk = f.review_sk
     AND r.is_current
    WHERE r.timestamp_created IS NOT NULL
    GROUP BY 1, 2
),
online_agg AS (
    SELECT
        a.activity_date AS activity_date,
        a.game_sk AS game_sk,
        round(sum(a.avg_players * a.samples) / nullif(sum(a.samples), 0), 2)::numeric(12, 2)
            AS avg_players,
        max(a.max_players)::bigint AS max_players,
        (array_agg(a.current_players ORDER BY a.hour_of_day DESC))[1]::bigint
            AS current_players,
        sum(a.samples)::bigint AS samples,
        count(DISTINCT a.hour_of_day)::smallint AS hours_covered
    FROM dds.fact_player_activity a
    GROUP BY 1, 2
)
SELECT
    COALESCE(rv.activity_date, onl.activity_date) AS activity_date,
    COALESCE(rv.game_sk, onl.game_sk) AS game_sk,
    g.appid, g.name, g.is_free, g.release_date,
    COALESCE(rv.review_count, 0),
    COALESCE(rv.positive_reviews, 0),
    COALESCE(rv.negative_reviews, 0),
    CASE
        WHEN COALESCE(rv.review_count, 0) > 0
        THEN (rv.positive_reviews::numeric / rv.review_count)::numeric(6, 5)
        ELSE NULL
    END,
    rv.avg_weighted_score,
    COALESCE(rv.sum_votes_up, 0),
    COALESCE(rv.sum_votes_funny, 0),
    rv.avg_playtime_at_review,
    onl.avg_players,
    onl.max_players,
    onl.current_players,
    COALESCE(onl.samples, 0),
    COALESCE(onl.hours_covered, 0),
    now()
FROM review_agg rv
FULL OUTER JOIN online_agg onl
    ON rv.activity_date = onl.activity_date
   AND rv.game_sk = onl.game_sk
JOIN dds.dim_game g
    ON g.game_sk = COALESCE(rv.game_sk, onl.game_sk);
