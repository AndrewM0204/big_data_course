-- sql/init_ods.sql
CREATE SCHEMA IF NOT EXISTS steam;

CREATE TABLE IF NOT EXISTS steam.ods_games (
    appid BIGINT PRIMARY KEY,
    name TEXT,
    is_free BOOLEAN,
    price NUMERIC(10, 2),
    developers TEXT,
    publishers TEXT,
    genres TEXT,
    release_date TEXT,
    short_description TEXT,
    raw_json JSONB,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ods_games_appid ON steam.ods_games (appid);

CREATE TABLE IF NOT EXISTS steam.etl_state (
    key VARCHAR(50) PRIMARY KEY,
    value BIGINT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO steam.etl_state (key, value)
VALUES ('total_games_loaded', 0)
ON CONFLICT (key) DO NOTHING;
