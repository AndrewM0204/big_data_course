CREATE DATABASE IF NOT EXISTS steam;

CREATE TABLE IF NOT EXISTS steam.market_prices (
    appid              UInt32,
    market_hash_name   String,
    item_name          String,
    sell_listings      UInt64,
    price_minor        UInt64,
    price              Decimal(18, 2),
    currency           String,
    market_total_items UInt64,
    collected_at       DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(collected_at)
ORDER BY (appid, market_hash_name, collected_at);