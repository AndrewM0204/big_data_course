-- =========================================================
-- Витрины ЛР3 (PostgreSQL, схема marts).
-- Источники: dds.fact_review, dds.dim_review (только is_current),
-- dds.fact_player_activity, dds.dim_game, мосты genre/publisher.
-- Зерно ежедневных витрин: (activity_date, game_sk).
-- Зерно часовой витрины: (activity_date, hour_of_day, game_sk).
-- Категорийные витрины строятся отдельно по каждому мосту, чтобы
-- факты не перемножались между собой (genre x publisher).
--
-- Каталог sql/ НЕ смонтирован в контейнер postgres (в /docker-entrypoint-initdb.d
-- попадает только sql/postgres). Скрипт выполняется вручную через stdin:
--   Get-Content -Raw sql/marts/init_marts.sql | docker compose exec -T postgres psql -U airflow -d airflow -v ON_ERROR_STOP=1
-- Скрипт идемпотентен (IF NOT EXISTS).
-- =========================================================
CREATE SCHEMA IF NOT EXISTS marts;

-- =========================================================
-- 1. Ежедневная витрина игр: зерно (activity_date, game_sk)
-- =========================================================
CREATE TABLE IF NOT EXISTS marts.mart_game_daily_metrics (
    activity_date          DATE        NOT NULL,
    game_sk                BIGINT      NOT NULL REFERENCES dds.dim_game(game_sk),
    appid                  BIGINT      NOT NULL,
    game_name              TEXT,
    is_free                BOOLEAN,
    release_date           DATE,
    review_count           BIGINT      NOT NULL DEFAULT 0,
    positive_reviews       BIGINT      NOT NULL DEFAULT 0,
    negative_reviews       BIGINT      NOT NULL DEFAULT 0,
    positive_ratio         NUMERIC(6, 5),
    avg_weighted_score     NUMERIC(6, 5),
    sum_votes_up           BIGINT      NOT NULL DEFAULT 0,
    sum_votes_funny        BIGINT      NOT NULL DEFAULT 0,
    avg_playtime_at_review NUMERIC(14, 2),
    avg_players            NUMERIC(12, 2),
    max_players            BIGINT,
    current_players        BIGINT,
    samples                BIGINT      NOT NULL DEFAULT 0,
    hours_covered          SMALLINT    NOT NULL DEFAULT 0,
    loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (activity_date, game_sk),
    CONSTRAINT chk_mart_game_daily_ratio
        CHECK (positive_ratio IS NULL OR (positive_ratio >= 0 AND positive_ratio <= 1)),
    CONSTRAINT chk_mart_game_daily_avg_le_max
        CHECK (avg_players IS NULL OR max_players IS NULL OR avg_players <= max_players)
);

CREATE INDEX IF NOT EXISTS idx_mart_game_daily_appid
    ON marts.mart_game_daily_metrics (appid, activity_date);

-- =========================================================
-- 2. Часовая витрина активности: зерно (activity_date, hour_of_day, game_sk)
-- =========================================================
CREATE TABLE IF NOT EXISTS marts.mart_activity_hourly (
    activity_date   DATE        NOT NULL,
    hour_of_day     SMALLINT    NOT NULL CHECK (hour_of_day BETWEEN 0 AND 23),
    game_sk         BIGINT      NOT NULL REFERENCES dds.dim_game(game_sk),
    appid           BIGINT      NOT NULL,
    game_name       TEXT,
    is_free         BOOLEAN,
    current_players BIGINT      NOT NULL,
    avg_players     NUMERIC(12, 2) NOT NULL,
    max_players     BIGINT      NOT NULL,
    samples         INTEGER     NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (activity_date, hour_of_day, game_sk),
    CONSTRAINT chk_mart_activity_hourly_avg_le_max CHECK (avg_players <= max_players)
);

CREATE INDEX IF NOT EXISTS idx_mart_activity_hourly_appid
    ON marts.mart_activity_hourly (appid, activity_date);

-- =========================================================
-- 3. Ежедневная витрина жанров: зерно (activity_date, genre_id)
-- Метрики повторяются для каждого жанра; games_count — точное
-- число игр (union множеств), а не перемножение фактов.
-- =========================================================
CREATE TABLE IF NOT EXISTS marts.mart_genre_daily_metrics (
    activity_date      DATE        NOT NULL,
    genre_id           INT         NOT NULL REFERENCES dds.dim_genre(genre_id),
    genre_name         TEXT        NOT NULL,
    games_count        BIGINT      NOT NULL DEFAULT 0,
    review_count       BIGINT      NOT NULL DEFAULT 0,
    positive_reviews   BIGINT      NOT NULL DEFAULT 0,
    positive_ratio     NUMERIC(6, 5),
    avg_players        NUMERIC(12, 2),
    max_players        BIGINT,
    samples            BIGINT      NOT NULL DEFAULT 0,
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (activity_date, genre_id),
    CONSTRAINT chk_mart_genre_daily_ratio
        CHECK (positive_ratio IS NULL OR (positive_ratio >= 0 AND positive_ratio <= 1))
);

-- =========================================================
-- 4. Ежедневная витрина издателей: зерно (activity_date, publisher_id)
-- =========================================================
CREATE TABLE IF NOT EXISTS marts.mart_publisher_daily_metrics (
    activity_date      DATE        NOT NULL,
    publisher_id       INT         NOT NULL REFERENCES dds.dim_publisher(publisher_id),
    publisher_name     TEXT        NOT NULL,
    games_count        BIGINT      NOT NULL DEFAULT 0,
    review_count       BIGINT      NOT NULL DEFAULT 0,
    positive_reviews   BIGINT      NOT NULL DEFAULT 0,
    positive_ratio     NUMERIC(6, 5),
    avg_players        NUMERIC(12, 2),
    max_players        BIGINT,
    samples            BIGINT      NOT NULL DEFAULT 0,
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (activity_date, publisher_id),
    CONSTRAINT chk_mart_publisher_daily_ratio
        CHECK (positive_ratio IS NULL OR (positive_ratio >= 0 AND positive_ratio <= 1))
);

-- =========================================================
-- 5. Результаты проверок качества витрин
-- =========================================================
CREATE TABLE IF NOT EXISTS marts.mart_dq_results (
    id           BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    check_name   TEXT        NOT NULL,
    run_id       TEXT,
    check_type   TEXT        NOT NULL,
    status       TEXT        NOT NULL CHECK (status IN ('passed', 'warning', 'error')),
    severity     TEXT,
    metric_value NUMERIC,
    threshold    NUMERIC,
    details      JSONB,
    checked_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_mart_dq_results_check_checked_at
    ON marts.mart_dq_results (check_name, checked_at DESC);

CREATE INDEX IF NOT EXISTS idx_mart_dq_results_run_id
    ON marts.mart_dq_results (run_id);
