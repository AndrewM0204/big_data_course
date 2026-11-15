-- =========================================================
-- Создание схемы для детального слоя данных (DDS)
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

-- 1.3. Справочник разработчиков (Нормализация 3НФ)
CREATE TABLE IF NOT EXISTS dds.dim_developer (
    developer_id SERIAL PRIMARY KEY,
    developer_name TEXT UNIQUE NOT NULL
);

-- 1.4. Справочник издателей (Нормализация 3НФ)
CREATE TABLE IF NOT EXISTS dds.dim_publisher (
    publisher_id SERIAL PRIMARY KEY,
    publisher_name TEXT UNIQUE NOT NULL
);

-- 1.5. Измерение Игр
CREATE TABLE IF NOT EXISTS dds.dim_game (
    appid BIGINT PRIMARY KEY,            -- Бизнес-ключ (Steam AppID)
    name TEXT NOT NULL,
    is_free BOOLEAN NOT NULL,
    release_date DATE,
    short_description TEXT,

    valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59'
);

-- 1.6. Связь «игра — жанр» (многие ко многим).
CREATE TABLE IF NOT EXISTS dds.bridge_game_genre (
    appid BIGINT NOT NULL REFERENCES dds.dim_game(appid) ON DELETE CASCADE,
    genre_id INT NOT NULL REFERENCES dds.dim_genre(genre_id) ON DELETE CASCADE,
    PRIMARY KEY (appid, genre_id)
);

-- 1.7. Связь «игра — разработчик» (многие ко многим).
CREATE TABLE IF NOT EXISTS dds.bridge_game_developer (
    appid BIGINT NOT NULL REFERENCES dds.dim_game(appid) ON DELETE CASCADE,
    developer_id INT NOT NULL REFERENCES dds.dim_developer(developer_id) ON DELETE CASCADE,
    PRIMARY KEY (appid, developer_id)
);

-- 1.8. Связь «игра — издатель» (многие ко многим).
CREATE TABLE IF NOT EXISTS dds.bridge_game_publisher (
    appid BIGINT NOT NULL REFERENCES dds.dim_game(appid) ON DELETE CASCADE,
    publisher_id INT NOT NULL REFERENCES dds.dim_publisher(publisher_id) ON DELETE CASCADE,
    PRIMARY KEY (appid, publisher_id)
);

-- 1.9. Измерение Пользователей Steam
CREATE TABLE IF NOT EXISTS dds.dim_user (
    steamid BIGINT PRIMARY KEY,          -- Бизнес-ключ (SteamID64)
    num_reviews INT DEFAULT 0,
    
    valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59'
);

-- 1.10. Измерение отзывов
CREATE TABLE IF NOT EXISTS dds.dim_review (
    review_id                   TEXT PRIMARY KEY,  -- recommendationid из Steam API
    language                    VARCHAR(10),
    review_text                 TEXT,
    steam_purchase              BOOLEAN,
    received_for_free           BOOLEAN,
    written_during_early_access BOOLEAN,
    refunded                    BOOLEAN,
    timestamp_created           TIMESTAMPTZ,
    timestamp_updated           TIMESTAMPTZ,
    author_playtime_at_review   BIGINT,
    voted_up BOOLEAN NOT NULL,
    votes_up BOOLEAN,
    votes_funny INT,
    weighted_score NUMERIC(6, 5),
    comment_count INT,

    valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59'
);

CREATE INDEX IF NOT EXISTS idx_dim_review_timestamp_created
    ON dds.dim_review (timestamp_created DESC);


-- =========================================================
-- 2. ФАКТЫ (FACTS)
-- =========================================================

-- 2.1. Факт: Отзывы пользователей.
-- review_id связан с измерением отзыва, appid — с игрой, author_steamid — с пользователем.
CREATE TABLE IF NOT EXISTS dds.fact_review (
    review_id TEXT PRIMARY KEY REFERENCES dds.dim_review(review_id) ON DELETE CASCADE,
    appid BIGINT NOT NULL REFERENCES dds.dim_game(appid),
    author_steamid BIGINT NOT NULL REFERENCES dds.dim_user(steamid)
);

-- 2.2. Факт: Активность игроков
CREATE TABLE IF NOT EXISTS dds.fact_player_activity (
    activity_id SERIAL PRIMARY KEY,
    appid BIGINT NOT NULL REFERENCES dds.dim_game(appid),
    activity_date DATE NOT NULL,
    current_players BIGINT NOT NULL
);
