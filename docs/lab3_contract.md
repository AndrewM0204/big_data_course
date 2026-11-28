# Контракт ЛР3

Источник имён и структур исходных данных: `sql/postgres/init_ods.sql`, `sql/postgres/init_dds.sql` и DAG в `dags/`. Требования к ЛР3 дополнительно описаны в `ArchPlan3.md`. Секреты и учётные данные в этом документе не фиксируются.

## Источники

ODS находится в PostgreSQL, схема `steam`:

- `ods_games`: `appid` (PK), `name`, `is_free`, `developers`, `publishers`, `genres`, `release_date`, `short_description`, `raw_json`, `loaded_at`, `updated_at`.
- `ods_reviews`: `review_id` (PK), `appid`, `language`, `review_text`, `voted_up`, `votes_up`, `votes_funny`, `weighted_vote_score`, `comment_count`, `steam_purchase`, `received_for_free`, `written_during_early_access`, `refunded`, `timestamp_created`, `timestamp_updated`, `author_steamid`, `author_num_reviews`, `author_playtime_forever`, `author_playtime_at_review`, `author_last_played`, `collected_at`.
- `ods_player_stats`: `appid`, `current_players`, `collected_at`; PK (`appid`, `collected_at`).
- Служебные таблицы: `failed_appids`, `etl_state`.

## DDS

- `dds.dim_game`: PK `game_sk`; бизнес-ключ `appid`; `name`, `is_free`, `release_date`, `short_description`, `row_hash`, `valid_from`, `valid_to`, `is_current`.
- `dds.dim_user`: PK `user_sk`; бизнес-ключ `steamid`; `num_reviews`, `playtime_forever`, `first_seen_at`, `last_seen_at`, `row_hash`, `valid_from`, `valid_to`, `is_current`.
- `dds.dim_review`: PK `review_sk`; бизнес-ключ `review_id`; `language`, `review_text`, `steam_purchase`, `received_for_free`, `written_during_early_access`, `refunded`, `timestamp_created`, `timestamp_updated`, `author_playtime_at_review`, `voted_up`, `votes_up`, `votes_funny`, `weighted_score`, `comment_count`, `collected_at`, `row_hash`, `valid_from`, `valid_to`, `is_current`.
- SCD2-интервал полуоткрытый: `valid_from <= timestamp < valid_to`; уникальная текущая версия по бизнес-ключу. Факты сохраняют surrogate keys версий.
- `dds.dim_genre` (`genre_id`, `genre_name`), `dds.dim_developer` (`developer_id`, `developer_name`), `dds.dim_publisher` (`publisher_id`, `publisher_name`); соответствующие PK — идентификаторы.
- Мосты: `dds.bridge_game_genre` (`game_sk`, `genre_id`), `dds.bridge_game_developer` (`game_sk`, `developer_id`), `dds.bridge_game_publisher` (`game_sk`, `publisher_id`); PK — соответствующая пара.
- `dds.fact_review`: PK и FK `review_sk`, также `review_id`, FK `game_sk`, FK `user_sk`, `loaded_at`. Одна строка на версию отзыва; для подсчёта актуальных отзывов фильтровать `dim_review.is_current`.
- `dds.fact_player_activity`: PK `activity_id`, FK `game_sk`, `activity_date`, `hour_of_day`, `current_players`, `avg_players`, `max_players`, `samples`, `loaded_at`. Уникален (`game_sk`, `activity_date`, `hour_of_day`); дата и час получены в UTC. Средние по нескольким строкам взвешиваются по `samples`.
- `dds.stg_review`, `dds.stg_player_activity`, `dds.dq_results` — служебные таблицы.

## Оркестрация

Все ежедневные DAG ниже используют `@daily`: `steam_games_catalog_etl`, `steam_reviews_collector`, `dds_game_dimensions_dag`, `dds_review_dimensions_dag`, `dds_facts_dag`, `data_quality_checks`. `steam_player_stats_etl` использует `0 */3 * * *`; `steam_storage_benchmark` запускается вручную.

`dds_facts_dag` ждёт оба DAG измерений и интервалы `steam_player_stats_etl`; ветви завершаются `load_fact_review` и `load_fact_player_activity`, после них идёт `log_fact_run`. Витрины должны ждать успешного DAG `dds_facts_dag` за тот же logical date. При ручном запуске необходимо создать соответствующий запуск DDS с совпадающей logical date; `catchup=False` не создаёт пропущенные интервалы задним числом.

## Правила витрин

Ежедневная витрина игр имеет grain (`activity_date`, `game_sk`); независимые факты предварительно агрегируются до этого grain и соединяются полным внешним соединением. Часовая витрина имеет grain (`activity_date`, `hour_of_day`, `game_sk`). При переходе через мост метрики повторяются для каждого жанра или издателя: суммы между категориями не являются общим итогом. Рейтинг игр объединяет версии по `appid`, средний онлайн взвешивается по `samples`; исторические атрибуты сохраняются по `game_sk`. В категорийных витринах `avg_players` — среднее по снимкам отдельных игр, а `max_players` — максимум отдельной игры, не суммарный онлайн категории.
