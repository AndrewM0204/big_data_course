-- =========================================================
-- DDS-слой
-- =========================================================
CREATE SCHEMA IF NOT EXISTS dds;

-- =========================================================
-- 1. ИЗМЕРЕНИЯ (DIMENSIONS)
-- =========================================================

-- 1.1. Справочник жанров
CREATE TABLE IF NOT EXISTS dds.dim_genre (
    genre_id SERIAL PRIMARY KEY,
    genre_name TEXT UNIQUE NOT NULL
);

-- 1.2. Справочник разработчиков
CREATE TABLE IF NOT EXISTS dds.dim_developer (
    developer_id SERIAL PRIMARY KEY,
    developer_name TEXT UNIQUE NOT NULL
);

-- 1.3. Справочник издателей
CREATE TABLE IF NOT EXISTS dds.dim_publisher (
    publisher_id SERIAL PRIMARY KEY,
    publisher_name TEXT UNIQUE NOT NULL
);

-- 1.4. Измерение игр.
CREATE TABLE IF NOT EXISTS dds.dim_game (
    appid BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    is_free BOOLEAN NOT NULL,
    release_date DATE,
    short_description TEXT,

    --SCD2
    valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59'
);

-- 1.5. Игра — жанр
CREATE TABLE IF NOT EXISTS dds.bridge_game_genre (
    appid BIGINT NOT NULL REFERENCES dds.dim_game(appid) ON DELETE CASCADE,
    genre_id INT NOT NULL REFERENCES dds.dim_genre(genre_id) ON DELETE CASCADE,
    PRIMARY KEY (appid, genre_id)
);

CREATE INDEX IF NOT EXISTS idx_bridge_game_genre_genre
    ON dds.bridge_game_genre (genre_id);

-- 1.6. Игра — разработчик
CREATE TABLE IF NOT EXISTS dds.bridge_game_developer (
    appid BIGINT NOT NULL REFERENCES dds.dim_game(appid) ON DELETE CASCADE,
    developer_id INT NOT NULL REFERENCES dds.dim_developer(developer_id) ON DELETE CASCADE,
    PRIMARY KEY (appid, developer_id)
);

CREATE INDEX IF NOT EXISTS idx_bridge_game_developer_developer
    ON dds.bridge_game_developer (developer_id);

-- 1.7. Игра — издатель
CREATE TABLE IF NOT EXISTS dds.bridge_game_publisher (
    appid BIGINT NOT NULL REFERENCES dds.dim_game(appid) ON DELETE CASCADE,
    publisher_id INT NOT NULL REFERENCES dds.dim_publisher(publisher_id) ON DELETE CASCADE,
    PRIMARY KEY (appid, publisher_id)
);

CREATE INDEX IF NOT EXISTS idx_bridge_game_publisher_publisher
    ON dds.bridge_game_publisher (publisher_id);

-- 1.8. Пользователи Steam.
CREATE TABLE IF NOT EXISTS dds.dim_user (
    steamid BIGINT PRIMARY KEY,
    num_reviews INT NOT NULL DEFAULT 0,
    playtime_forever INTEGER NOT NULL DEFAULT 0,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59'
);

-- 1.9. Измерение отзывов.
CREATE TABLE IF NOT EXISTS dds.dim_review (
    review_id                   TEXT PRIMARY KEY,
    language                    VARCHAR(10),
    review_text                 TEXT,
    steam_purchase              BOOLEAN,
    received_for_free           BOOLEAN,
    written_during_early_access BOOLEAN,
    refunded                    BOOLEAN,
    timestamp_created           TIMESTAMPTZ,
    timestamp_updated           TIMESTAMPTZ,
    author_playtime_at_review   BIGINT,
    voted_up                    BOOLEAN NOT NULL,
    votes_up                    INTEGER,
    votes_funny                 INTEGER,
    weighted_score              NUMERIC(6, 5),
    comment_count               INTEGER,
    collected_at                TIMESTAMPTZ,
    --SCD2
    valid_from                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to                    TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59'
);

CREATE INDEX IF NOT EXISTS idx_dim_review_timestamp_created
    ON dds.dim_review (timestamp_created DESC);

-- =========================================================
-- 2. ФАКТЫ (FACTS)
-- =========================================================

-- 2.1. Факт отзывов: связь отзыва с игрой и пользователем.
CREATE TABLE IF NOT EXISTS dds.fact_review (
    review_id       TEXT PRIMARY KEY REFERENCES dds.dim_review(review_id) ON DELETE CASCADE,
    appid           BIGINT NOT NULL REFERENCES dds.dim_game(appid),
    author_steamid  BIGINT NOT NULL REFERENCES dds.dim_user(steamid)
);

CREATE INDEX IF NOT EXISTS idx_fact_review_appid
    ON dds.fact_review (appid);

CREATE INDEX IF NOT EXISTS idx_fact_review_author
    ON dds.fact_review (author_steamid);

-- 2.2. Факт активности игроков.
CREATE TABLE IF NOT EXISTS dds.fact_player_activity (
    activity_id     SERIAL PRIMARY KEY,
    appid           BIGINT NOT NULL REFERENCES dds.dim_game(appid),
    activity_date   DATE NOT NULL,
    hour_of_day     SMALLINT NOT NULL CHECK (hour_of_day BETWEEN 0 AND 23),
    current_players BIGINT NOT NULL CHECK (current_players >= 0),
    avg_players     NUMERIC(12, 2) NOT NULL CHECK (avg_players >= 0),
    max_players     BIGINT NOT NULL CHECK (max_players >= 0),
    samples         INTEGER NOT NULL CHECK (samples > 0),
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (appid, activity_date, hour_of_day),
    CONSTRAINT chk_fact_player_activity_max CHECK (max_players >= avg_players)
);

CREATE INDEX IF NOT EXISTS idx_fact_player_activity_appid_date
    ON dds.fact_player_activity (appid, activity_date);

CREATE INDEX IF NOT EXISTS idx_fact_player_activity_date
    ON dds.fact_player_activity (activity_date);

-- =========================================================
-- 3. STAGING (UNLOGGED)
-- =========================================================
CREATE UNLOGGED TABLE IF NOT EXISTS dds.stg_review (
    review_id                   TEXT,
    appid                       BIGINT,
    language                    TEXT,
    review_text                 TEXT,
    voted_up                    BOOLEAN,
    votes_up                    INTEGER,
    votes_funny                 INTEGER,
    weighted_score              NUMERIC(6, 5),
    comment_count               INTEGER,
    steam_purchase              BOOLEAN,
    received_for_free           BOOLEAN,
    written_during_early_access BOOLEAN,
    refunded                    BOOLEAN,
    timestamp_created           TIMESTAMPTZ,
    timestamp_updated           TIMESTAMPTZ,
    author_steamid              BIGINT,
    author_num_reviews          INTEGER,
    author_playtime_forever     INTEGER,
    author_playtime_at_review   INTEGER,
    collected_at                TIMESTAMPTZ
);

CREATE UNLOGGED TABLE IF NOT EXISTS dds.stg_player_activity (
    appid           BIGINT,
    activity_date   DATE,
    hour_of_day     SMALLINT,
    current_players BIGINT,
    avg_players     NUMERIC(12, 2),
    max_players     BIGINT,
    samples         INTEGER
);

-- =========================================================
-- 4. DATA QUALITY
-- =========================================================
CREATE TABLE IF NOT EXISTS dds.dq_results (
    id           BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    check_name   TEXT NOT NULL,
    run_id       TEXT,
    check_type   TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('passed', 'warning', 'error')),
    severity     TEXT,
    metric_value NUMERIC,
    threshold    NUMERIC,
    details      JSONB,
    checked_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dq_results_check_checked_at
    ON dds.dq_results (check_name, checked_at DESC);

CREATE INDEX IF NOT EXISTS idx_dq_results_run_id
    ON dds.dq_results (run_id);
