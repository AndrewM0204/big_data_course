-- sql/postgres/init_ods.sql
-- Инициализация ODS-слоя Steam.
-- Все сырые данные системы хранятся в PostgreSQL, схема steam.

CREATE SCHEMA IF NOT EXISTS steam;

-- ==========================================
-- КАТАЛОГ ИГР
-- ==========================================
CREATE TABLE IF NOT EXISTS steam.ods_games (
    appid             BIGINT PRIMARY KEY,
    name              TEXT,
    is_free           BOOLEAN,
    developers        TEXT,
    publishers        TEXT,
    genres            TEXT,
    release_date      TEXT,
    short_description TEXT,
    raw_json          JSONB,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ods_games_name ON steam.ods_games (name);
CREATE INDEX IF NOT EXISTS idx_ods_games_is_free ON steam.ods_games (is_free);
CREATE INDEX IF NOT EXISTS idx_ods_games_loaded_at ON steam.ods_games (loaded_at DESC);
CREATE INDEX IF NOT EXISTS idx_ods_games_raw_json ON steam.ods_games USING gin (raw_json);

-- ==========================================
-- ОТЗЫВЫ
-- ==========================================
-- Целевая ODS-таблица отзывов. Заменяет MongoDB-коллекцию steam.ods_reviews.
-- При мерже lab2_feature_b удалить дублирующий DDL steam.ods_reviews из init_dds.sql.
CREATE TABLE IF NOT EXISTS steam.ods_reviews (
    review_id                   TEXT PRIMARY KEY,
    appid                       BIGINT NOT NULL,
    language                    TEXT,
    review_text                 TEXT,
    voted_up                    BOOLEAN,
    votes_up                    INTEGER,
    votes_funny                 INTEGER,
    weighted_vote_score         NUMERIC(6, 5),
    comment_count               INTEGER,
    steam_purchase              BOOLEAN,
    received_for_free           BOOLEAN,
    written_during_early_access BOOLEAN,
    refunded                    BOOLEAN,
    timestamp_created           TIMESTAMPTZ,
    timestamp_updated           TIMESTAMPTZ,
    author_steamid              TEXT,
    author_num_reviews          INTEGER,
    author_playtime_forever     INTEGER,
    author_playtime_at_review   INTEGER,
    author_last_played          TIMESTAMPTZ,
    collected_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ods_reviews_appid
    ON steam.ods_reviews (appid);

CREATE INDEX IF NOT EXISTS idx_ods_reviews_timestamp_created
    ON steam.ods_reviews (timestamp_created DESC);

CREATE INDEX IF NOT EXISTS idx_ods_reviews_author_steamid
    ON steam.ods_reviews (author_steamid);

-- ==========================================
-- ОНЛАЙН ИГРОКОВ
-- ==========================================
-- Сырые снимки онлайна. Заменяет ClickHouse-таблицу steam.player_stats.
-- Первичный ключ повторяет логику ReplacingMergeTree: одна точка на игру и время.
CREATE TABLE IF NOT EXISTS steam.ods_player_stats (
    appid           BIGINT NOT NULL,
    current_players BIGINT NOT NULL,
    collected_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (appid, collected_at)
);

CREATE INDEX IF NOT EXISTS idx_ods_player_stats_collected_at
    ON steam.ods_player_stats (collected_at DESC);

-- ==========================================
-- СЛУЖЕБНЫЕ ТАБЛИЦЫ ODS
-- ==========================================
-- AppID, которые не удалось скачать из Steam API.
-- После 3 неудачных попыток AppID исключается из будущих батчей.
CREATE TABLE IF NOT EXISTS steam.failed_appids (
    appid       BIGINT PRIMARY KEY,
    failed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    retry_count INT NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_failed_appids_retry_count
    ON steam.failed_appids (retry_count);

-- Глобальное состояние пайплайна: чекпоинты, счётчики, флаги.
CREATE TABLE IF NOT EXISTS steam.etl_state (
    key         VARCHAR(50) PRIMARY KEY,
    value       BIGINT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO steam.etl_state (key, value)
VALUES ('total_games_loaded', 0)
ON CONFLICT (key) DO NOTHING;
