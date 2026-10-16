CREATE DATABASE IF NOT EXISTS steam;

CREATE TABLE IF NOT EXISTS steam.player_stats (
    appid           UInt32,
    game_name       String,
    current_players UInt64,
    collected_at    DateTime('UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(collected_at)
ORDER BY (appid, collected_at);