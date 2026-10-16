-- sql/postgres/init_ods.sql
-- Инициализация ODS-слоя для Steam Games Catalog ETL
CREATE SCHEMA IF NOT EXISTS steam;

CREATE TABLE IF NOT EXISTS steam.ods_games (
    appid             BIGINT PRIMARY KEY,
    name              TEXT,
    is_free           BOOLEAN,
    price             NUMERIC(10, 2),
    currency          VARCHAR(3) DEFAULT 'USD',
    developers        TEXT,
    publishers        TEXT,
    genres            TEXT,
    release_date      TEXT,
    short_description TEXT,
    raw_json          JSONB,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Индексы для аналитических запросов
CREATE INDEX IF NOT EXISTS idx_ods_games_name ON steam.ods_games (name);
CREATE INDEX IF NOT EXISTS idx_ods_games_is_free ON steam.ods_games (is_free);
CREATE INDEX IF NOT EXISTS idx_ods_games_price ON steam.ods_games (price);
CREATE INDEX IF NOT EXISTS idx_ods_games_loaded_at ON steam.ods_games (loaded_at DESC);

-- GIN-индекс для поиска по raw_json (JSONB)
CREATE INDEX IF NOT EXISTS idx_ods_games_raw_json ON steam.ods_games USING gin (raw_json);

-- Хранит appid, которые не удалось скачать из Steam API.
-- После 3 неудачных попыток appid исключается из будущих батчей,
-- чтобы не блокировать прогресс ETL.
CREATE TABLE IF NOT EXISTS steam.failed_appids (
    appid       BIGINT PRIMARY KEY,
    failed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    retry_count INT NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_failed_appids_retry_count 
    ON steam.failed_appids (retry_count);

-- Используется для хранения глобального состояния пайплайна:
-- чекпоинты, счётчики, флаги.
CREATE TABLE IF NOT EXISTS steam.etl_state (
    key         VARCHAR(50) PRIMARY KEY,
    value       BIGINT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Инициализация счётчика загруженных игр
INSERT INTO steam.etl_state (key, value)
VALUES ('total_games_loaded', 0)
ON CONFLICT (key) DO NOTHING;

-- Вьюшка для быстрого просмотра статистики по каталогу
CREATE OR REPLACE VIEW steam.v_catalog_stats AS
SELECT 
    COUNT(*) AS total_games,
    COUNT(*) FILTER (WHERE is_free = true) AS free_games,
    COUNT(*) FILTER (WHERE is_free = false) AS paid_games,
    ROUND(AVG(price) FILTER (WHERE is_free = false), 2) AS avg_price,
    MIN(price) FILTER (WHERE is_free = false) AS min_price,
    MAX(price) FILTER (WHERE is_free = false) AS max_price,
    MIN(loaded_at) AS first_loaded,
    MAX(loaded_at) AS last_loaded
FROM steam.ods_games;

-- Функция для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION steam.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггер для автоматического обновления updated_at при изменении записи
DROP TRIGGER IF EXISTS trigger_ods_games_updated_at ON steam.ods_games;
CREATE TRIGGER trigger_ods_games_updated_at
    BEFORE UPDATE ON steam.ods_games
    FOR EACH ROW
    EXECUTE FUNCTION steam.update_updated_at_column();