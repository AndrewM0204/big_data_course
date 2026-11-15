import json
import logging
import os
from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import Param
from requests.auth import HTTPBasicAuth

# ==========================================
# КОНФИГУРАЦИЯ
# ==========================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_PROC_DIR = '/opt/airflow/data/processed/dds_facts'

POSTGRES_CONN_ID = "steam_postgres"
CLICKHOUSE_CONN_ID = "clickhouse_http"

# Таблицы, без которых загрузка фактов невозможна
REQUIRED_DDS_TABLES = (
    'dds.dim_date',
    'dds.dim_game',
    'dds.dim_author',
    'dds.fact_review',
    'dds.fact_player_activity',
)


# ==========================================
# ХЕЛПЕРЫ
# ==========================================
def daily_slot(dt: datetime) -> datetime:
    """Выравнивает logical date на полночь — сетка @daily DAG'ов."""
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def three_hour_slot(dt: datetime) -> datetime:
    """Выравнивает logical date на трёхчасовую сетку steam_player_stats_etl."""
    return dt.replace(hour=(dt.hour // 3) * 3, minute=0, second=0, microsecond=0)


def processed_dir(context) -> str:
    """Каталог обработанных файлов запуска на общем томе."""
    path = os.path.join(BASE_PROC_DIR, context['ds'])
    os.makedirs(path, exist_ok=True)
    return path


def parse_ts(value) -> datetime | None:
    """ISO-строка -> aware datetime в UTC."""
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(value)
    except ValueError:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def get_clickhouse_credentials() -> dict:
    """Получает credentials для ClickHouse из Airflow Connection."""
    conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
    extra = conn.extra_dejson or {}
    return {
        "host": f"http://{conn.host}:{conn.port or 8123}",
        "user": conn.login,
        "password": conn.password,
        "database": extra.get("database", "steam"),
    }


def clickhouse_query(query: str, credentials: dict | None = None) -> str:
    """Выполняет запрос к ClickHouse через HTTP-интерфейс."""
    if credentials is None:
        credentials = get_clickhouse_credentials()

    resp = requests.post(
        credentials["host"],
        params={
            "query": query,
            "database": credentials["database"],
            "date_time_input_format": "best_effort",
        },
        auth=HTTPBasicAuth(credentials["user"], credentials["password"]),
        timeout=60,
    )
    response_text = resp.text.strip()

    if not resp.ok or response_text.startswith("Code:") or "DB::Exception" in response_text:
        raise RuntimeError(f"ClickHouse error (HTTP {resp.status_code}): {response_text}")

    return response_text


# ==========================================
# ФУНКЦИИ DAG
# ==========================================
def check_prerequisites(**context) -> None:
    """
    Проверяет наличие DDS-таблиц и источников.
    Пустой steam.ods_reviews — skip, пустые измерения — явная ошибка.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()

        missing = []
        for table in REQUIRED_DDS_TABLES:
            cursor.execute("SELECT to_regclass(%s);", (table,))
            if cursor.fetchone()[0] is None:
                missing.append(table)
        if missing:
            raise AirflowException(
                f"DDS tables not found: {', '.join(missing)}. "
                "Apply sql/postgres/init_dds.sql first."
            )

        cursor.execute("SELECT count(*) FROM dds.dim_game WHERE is_current;")
        current_games = cursor.fetchone()[0]
        if current_games == 0:
            raise AirflowException(
                "dds.dim_game has no current rows. Run dds_dimensions_dag (owner A) first."
            )

        cursor.execute("SELECT to_regclass('steam.ods_reviews');")
        if cursor.fetchone()[0] is None:
            raise AirflowSkipException("steam.ods_reviews does not exist yet (owner A).")

        cursor.execute("SELECT count(*) FROM steam.ods_reviews;")
        reviews_count = cursor.fetchone()[0]
    finally:
        cursor.close()
        conn.close()

    if reviews_count == 0:
        raise AirflowSkipException("steam.ods_reviews is empty. Nothing to load into facts.")

    logger.info(f"Prerequisites OK: {current_games} current games, {reviews_count} reviews in ODS.")


def ensure_dim_date(**context) -> None:
    """Страховка: добавляет в dim_date недостающие даты окна активности."""
    lookback = int(context['params'].get('activity_lookback_days', 1))
    interval_end = context['data_interval_end'].astimezone(timezone.utc)
    window_start = (interval_end - timedelta(days=lookback)).date()
    window_end = interval_end.date()

    query = """
        INSERT INTO dds.dim_date (date_id, date, year, month, day, quarter, is_weekend)
        SELECT to_char(gs.d, 'YYYYMMDD')::int,
               gs.d::date,
               EXTRACT(YEAR FROM gs.d)::smallint,
               EXTRACT(MONTH FROM gs.d)::smallint,
               EXTRACT(DAY FROM gs.d)::smallint,
               EXTRACT(QUARTER FROM gs.d)::smallint,
               EXTRACT(ISODOW FROM gs.d) IN (6, 7)
        FROM generate_series(%s::timestamp, %s::timestamp, interval '1 day') AS gs(d)
        ON CONFLICT (date_id) DO NOTHING;
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(query, (window_start, window_end))
        inserted = cursor.rowcount
        conn.commit()
    finally:
        cursor.close()
        conn.close()

    logger.info(f"dim_date: ensured {window_start}..{window_end} ({inserted} new dates).")


def extract_reviews(**context) -> str:
    """Anti-join: забирает отзывы ODS, которых ещё нет в dds.fact_review."""
    reviews_limit = int(context['params'].get('reviews_limit', 50000))

    query = """
        SELECT r.review_id, r.appid, r.language, r.review_text, r.voted_up,
               r.votes_up, r.votes_funny, r.weighted_vote_score,
               r.timestamp_created, r.author_steamid, r.author_num_reviews,
               r.author_playtime_forever
        FROM steam.ods_reviews r
        WHERE NOT EXISTS (
            SELECT 1 FROM dds.fact_review f WHERE f.review_id = r.review_id
        )
        ORDER BY r.timestamp_created NULLS LAST
        LIMIT %s;
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(query, (reviews_limit,))
        rows = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    if not rows:
        raise AirflowSkipException("No new reviews in steam.ods_reviews.")

    reviews = []
    for row in rows:
        ts = row[8]
        if ts is not None and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        reviews.append({
            "review_id": str(row[0]),
            "appid": row[1],
            "language": row[2],
            "review_text": row[3],
            "voted_up": row[4],
            "votes_up": row[5],
            "votes_funny": row[6],
            "weighted_score": float(row[7]) if row[7] is not None else None,
            "timestamp_created": ts.astimezone(timezone.utc).isoformat() if ts else None,
            "author_steamid": str(row[9]) if row[9] else None,
            "author_num_reviews": row[10],
            "author_playtime_forever": row[11],
        })

    out_file = os.path.join(processed_dir(context), 'reviews_extracted.json')
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(reviews, f, ensure_ascii=False)

    logger.info(f"Extracted {len(reviews)} new reviews (limit {reviews_limit}).")
    return out_file


def transform_reviews(extracted_file: str, **context) -> str:
    """Дедуп по review_id и фильтрация мусора; пишет статистику до/после."""
    min_text_length = int(context['params'].get('min_text_length', 20))
    now = datetime.now(timezone.utc)

    with open(extracted_file, 'r', encoding='utf-8') as f:
        reviews = json.load(f)

    stats = {"input": len(reviews), "dedup_dropped": 0, "filtered": 0, "output": 0}
    seen = set()
    prepared = []

    for review in reviews:
        review_id = review.get('review_id')
        if not review_id or review_id in seen:
            stats['dedup_dropped'] += 1
            continue
        seen.add(review_id)

        ts = parse_ts(review.get('timestamp_created'))
        votes_up = review.get('votes_up') or 0
        votes_funny = review.get('votes_funny') or 0
        text = review.get('review_text') or ''

        if (
            ts is None
            or ts > now
            or review.get('appid') is None
            or votes_up < 0
            or votes_funny < 0
            or len(text) < min_text_length
        ):
            stats['filtered'] += 1
            continue

        prepared.append({
            "review_id": review_id,
            "appid": int(review['appid']),
            "date_id": int(ts.strftime('%Y%m%d')),
            "language": review.get('language'),
            "voted_up": review.get('voted_up'),
            "votes_up": votes_up,
            "votes_funny": votes_funny,
            "weighted_score": review.get('weighted_score'),
            "timestamp_created": ts.isoformat(),
            "author_steamid": review.get('author_steamid'),
            "author_num_reviews": review.get('author_num_reviews') or 0,
            "author_playtime_forever": review.get('author_playtime_forever') or 0,
        })

    stats['output'] = len(prepared)
    out_dir = processed_dir(context)
    out_file = os.path.join(out_dir, 'reviews_prepared.json')
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(prepared, f, ensure_ascii=False)

    stats_file = os.path.join(out_dir, 'reviews_stats.json')
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    logger.info(f"Reviews transform: {stats}")
    return out_file


def load_fact_review(prepared_file: str, **context) -> dict:
    """Транзакция: staging -> JOIN с dim_game (is_current) -> upsert в fact_review."""
    with open(prepared_file, 'r', encoding='utf-8') as f:
        rows = json.load(f)

    if not rows:
        logger.info("No prepared reviews; fact_review is not changed.")
        return {"staged": 0, "inserted": 0, "unmatched_appid": 0, "unmatched_date": 0}

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE dds.stg_review;")
        cursor.executemany(
            """
            INSERT INTO dds.stg_review (
                review_id, appid, date_id, language, voted_up, votes_up,
                votes_funny, weighted_score, timestamp_created,
                author_steamid, author_num_reviews, author_playtime_forever
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            [
                (
                    row['review_id'], row['appid'], row['date_id'], row['language'],
                    row['voted_up'], row['votes_up'], row['votes_funny'],
                    row['weighted_score'], datetime.fromisoformat(row['timestamp_created']),
                    row['author_steamid'], row['author_num_reviews'], row['author_playtime_forever'],
                )
                for row in rows
            ],
        )

        cursor.execute("SELECT count(*) FROM dds.stg_review;")
        staged = cursor.fetchone()[0]

        # Страховка: авторы, пришедшие с отзывами, должны существовать в измерении
        cursor.execute(
            """
            INSERT INTO dds.dim_author (steamid, num_reviews, playtime_forever)
            SELECT DISTINCT ON (author_steamid)
                   author_steamid,
                   COALESCE(author_num_reviews, 0),
                   COALESCE(author_playtime_forever, 0)
            FROM dds.stg_review
            WHERE author_steamid IS NOT NULL AND author_steamid <> ''
            ORDER BY author_steamid, author_num_reviews DESC NULLS LAST
            ON CONFLICT (steamid) DO UPDATE SET
                num_reviews = GREATEST(dds.dim_author.num_reviews, EXCLUDED.num_reviews),
                playtime_forever = GREATEST(dds.dim_author.playtime_forever, EXCLUDED.playtime_forever),
                last_seen_at = now();
            """
        )

        cursor.execute(
            """
            SELECT count(*)
            FROM dds.stg_review s
            WHERE NOT EXISTS (
                SELECT 1 FROM dds.dim_game g
                WHERE g.appid = s.appid AND g.is_current
            );
            """
        )
        unmatched_appid = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM dds.stg_review s
            WHERE NOT EXISTS (
                SELECT 1 FROM dds.dim_date d WHERE d.date_id = s.date_id
            );
            """
        )
        unmatched_date = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO dds.fact_review (
                review_id, game_sk, author_steamid, date_id, language, voted_up,
                votes_up, votes_funny, weighted_score, timestamp_created, loaded_at
            )
            SELECT s.review_id, g.game_sk, a.steamid, s.date_id, s.language, s.voted_up,
                   s.votes_up, s.votes_funny, s.weighted_score, s.timestamp_created, now()
            FROM dds.stg_review s
            JOIN dds.dim_game g ON g.appid = s.appid AND g.is_current
            JOIN dds.dim_date d ON d.date_id = s.date_id
            LEFT JOIN dds.dim_author a ON a.steamid = s.author_steamid
            ON CONFLICT (review_id) DO UPDATE SET
                game_sk = EXCLUDED.game_sk,
                author_steamid = EXCLUDED.author_steamid,
                date_id = EXCLUDED.date_id,
                language = EXCLUDED.language,
                voted_up = EXCLUDED.voted_up,
                votes_up = EXCLUDED.votes_up,
                votes_funny = EXCLUDED.votes_funny,
                weighted_score = EXCLUDED.weighted_score,
                timestamp_created = EXCLUDED.timestamp_created,
                loaded_at = now();
            """
        )
        inserted = cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    counters = {
        "staged": staged,
        "inserted": inserted,
        "unmatched_appid": unmatched_appid,
        "unmatched_date": unmatched_date,
    }
    logger.info(f"fact_review load: {counters}")
    return counters


def aggregate_player_activity(**context) -> str:
    """Агрегирует онлайн из ClickHouse (FINAL) по дням и часам UTC."""
    lookback = int(context['params'].get('activity_lookback_days', 1))
    window_end = context['data_interval_end'].astimezone(timezone.utc)
    window_start = window_end - timedelta(days=lookback)

    query = f"""
        SELECT appid,
               toYYYYMMDD(collected_at) AS date_id,
               toHour(collected_at) AS hour,
               avg(current_players) AS avg_players,
               max(current_players) AS max_players,
               count() AS samples
        FROM steam.player_stats FINAL
        WHERE collected_at >= toDateTime('{window_start:%Y-%m-%d %H:%M:%S}', 'UTC')
          AND collected_at < toDateTime('{window_end:%Y-%m-%d %H:%M:%S}', 'UTC')
        GROUP BY appid, date_id, hour
        ORDER BY appid, date_id, hour
        FORMAT JSONEachRow
    """

    response_text = clickhouse_query(query)
    ch_rows = [json.loads(line) for line in response_text.splitlines() if line.strip()]
    if not ch_rows:
        raise AirflowSkipException(
            f"No player stats in ClickHouse for [{window_start}, {window_end})."
        )

    prepared = [
        {
            "appid": int(row['appid']),
            "date_id": int(row['date_id']),
            "hour": int(row['hour']),
            "avg_players": round(float(row['avg_players']), 2),
            "max_players": int(row['max_players']),
            "samples": int(row['samples']),
        }
        for row in ch_rows
    ]

    out_file = os.path.join(processed_dir(context), 'activity_prepared.json')
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(prepared, f, ensure_ascii=False)

    logger.info(f"Aggregated {len(prepared)} hourly points for [{window_start}, {window_end}).")
    return out_file


def load_fact_player_activity(prepared_file: str, **context) -> dict:
    """Транзакция: staging -> JOIN с dim_game/dim_date -> upsert в fact_player_activity."""
    with open(prepared_file, 'r', encoding='utf-8') as f:
        rows = json.load(f)

    if not rows:
        logger.info("No prepared activity; fact_player_activity is not changed.")
        return {"staged": 0, "inserted": 0, "unmatched_game": 0, "unmatched_date": 0}

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE dds.stg_player_activity;")
        cursor.executemany(
            """
            INSERT INTO dds.stg_player_activity (
                appid, date_id, hour, avg_players, max_players, samples
            ) VALUES (%s, %s, %s, %s, %s, %s);
            """,
            [
                (
                    row['appid'], row['date_id'], row['hour'],
                    row['avg_players'], row['max_players'], row['samples'],
                )
                for row in rows
            ],
        )

        cursor.execute("SELECT count(*) FROM dds.stg_player_activity;")
        staged = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM dds.stg_player_activity s
            WHERE NOT EXISTS (
                SELECT 1 FROM dds.dim_game g
                WHERE g.appid = s.appid AND g.is_current
            );
            """
        )
        unmatched_game = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM dds.stg_player_activity s
            WHERE NOT EXISTS (
                SELECT 1 FROM dds.dim_date d WHERE d.date_id = s.date_id
            );
            """
        )
        unmatched_date = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO dds.fact_player_activity (
                game_sk, date_id, hour, avg_players, max_players, samples, loaded_at
            )
            SELECT g.game_sk, s.date_id, s.hour, s.avg_players, s.max_players, s.samples, now()
            FROM dds.stg_player_activity s
            JOIN dds.dim_game g ON g.appid = s.appid AND g.is_current
            JOIN dds.dim_date d ON d.date_id = s.date_id
            ON CONFLICT (game_sk, date_id, hour) DO UPDATE SET
                avg_players = EXCLUDED.avg_players,
                max_players = EXCLUDED.max_players,
                samples = EXCLUDED.samples,
                loaded_at = now();
            """
        )
        inserted = cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    counters = {
        "staged": staged,
        "inserted": inserted,
        "unmatched_game": unmatched_game,
        "unmatched_date": unmatched_date,
    }
    logger.info(f"fact_player_activity load: {counters}")
    return counters


def log_fact_run(**context) -> str:
    """Сводка запуска: счётчики «до/после» в лог и JSON-файл."""
    ti = context['ti']
    out_dir = processed_dir(context)

    reviews_stats = {}
    stats_file = os.path.join(out_dir, 'reviews_stats.json')
    if os.path.exists(stats_file):
        with open(stats_file, 'r', encoding='utf-8') as f:
            reviews_stats = json.load(f)

    summary = {
        "ds": context['ds'],
        "run_id": context['run_id'],
        "reviews": reviews_stats,
        "fact_review": ti.xcom_pull(task_ids='load_fact_review'),
        "fact_player_activity": ti.xcom_pull(task_ids='load_fact_player_activity'),
    }

    out_file = os.path.join(out_dir, 'run_summary.json')
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    logger.info(f"DDS facts run summary: {json.dumps(summary, ensure_ascii=False)}")
    return out_file


# ==========================================
# ОПРЕДЕЛЕНИЕ DAG
# ==========================================
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

with DAG(
    dag_id='dds_facts_dag',
    default_args=default_args,
    description='Load DDS facts: reviews and player activity with surrogate keys',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['steam', 'dds', 'facts'],
    params={
        'reviews_limit': Param(
            50000,
            type='integer',
            minimum=1,
            maximum=500000,
            description='Max new reviews to load into dds.fact_review per run'
        ),
        'min_text_length': Param(
            20,
            type='integer',
            minimum=0,
            maximum=1000,
            description='Minimal cleaned review text length to keep a review'
        ),
        'activity_lookback_days': Param(
            1,
            type='integer',
            minimum=1,
            maximum=30,
            description='Closed UTC days of ClickHouse player stats to re-aggregate'
        ),
    },
    doc_md="""
### DDS Facts DAG

Загружает факты DDS из ODS и ClickHouse:
- `steam.ods_reviews` -> очистка/дедуп -> `dds.fact_review` (JOIN на `dim_game` по `is_current`);
- `steam.player_stats` (ClickHouse, `FINAL`) -> часовая агрегация -> `dds.fact_player_activity`.

**Зависимости:** сенсоры ждут `steam_games_catalog_etl`, `steam_reviews_collector`,
`steam_player_stats_etl` и `dds_dimensions_dag` (зона А) на своих logical date.

**Идемпотентность:** upsert по `review_id` и `(game_sk, date_id, hour)`.
Промежуточные файлы — на общем томе `/opt/airflow/data`, в XCom передаются пути.

**Параметры:** `reviews_limit`, `min_text_length`, `activity_lookback_days`.
"""
) as dag:

    wait_for_ods_catalog = ExternalTaskSensor(
        task_id='wait_for_ods_catalog',
        external_dag_id='steam_games_catalog_etl',
        external_task_id=None,
        execution_date_fn=daily_slot,
        allowed_states=['success'],
        mode='reschedule',
        poke_interval=60,
        timeout=timedelta(hours=6),
        check_existence=True,
    )

    wait_for_ods_reviews = ExternalTaskSensor(
        task_id='wait_for_ods_reviews',
        external_dag_id='steam_reviews_collector',
        external_task_id=None,
        execution_date_fn=daily_slot,
        allowed_states=['success'],
        mode='reschedule',
        poke_interval=60,
        timeout=timedelta(hours=6),
        check_existence=True,
    )

    wait_for_ods_player_stats = ExternalTaskSensor(
        task_id='wait_for_ods_player_stats',
        external_dag_id='steam_player_stats_etl',
        external_task_id=None,
        execution_date_fn=three_hour_slot,
        allowed_states=['success'],
        mode='reschedule',
        poke_interval=60,
        timeout=timedelta(hours=6),
        check_existence=True,
    )

    wait_for_dds_dimensions = ExternalTaskSensor(
        task_id='wait_for_dds_dimensions',
        external_dag_id='dds_dimensions_dag',
        external_task_id=None,
        execution_date_fn=daily_slot,
        allowed_states=['success'],
        mode='reschedule',
        poke_interval=60,
        timeout=timedelta(hours=6),
        check_existence=True,
    )

    check_prerequisites = PythonOperator(
        task_id='check_prerequisites',
        python_callable=check_prerequisites,
    )

    ensure_dim_date = PythonOperator(
        task_id='ensure_dim_date',
        python_callable=ensure_dim_date,
    )

    extract_reviews = PythonOperator(
        task_id='extract_reviews',
        python_callable=extract_reviews,
    )

    transform_reviews = PythonOperator(
        task_id='transform_reviews',
        python_callable=transform_reviews,
        op_kwargs={
            'extracted_file': "{{ ti.xcom_pull(task_ids='extract_reviews') }}"
        },
    )

    load_fact_review = PythonOperator(
        task_id='load_fact_review',
        python_callable=load_fact_review,
        op_kwargs={
            'prepared_file': "{{ ti.xcom_pull(task_ids='transform_reviews') }}"
        },
    )

    aggregate_player_activity = PythonOperator(
        task_id='aggregate_player_activity',
        python_callable=aggregate_player_activity,
    )

    load_fact_player_activity = PythonOperator(
        task_id='load_fact_player_activity',
        python_callable=load_fact_player_activity,
        op_kwargs={
            'prepared_file': "{{ ti.xcom_pull(task_ids='aggregate_player_activity') }}"
        },
    )

    log_fact_run = PythonOperator(
        task_id='log_fact_run',
        python_callable=log_fact_run,
        trigger_rule='none_failed',
    )

    [
        wait_for_ods_catalog,
        wait_for_ods_reviews,
        wait_for_ods_player_stats,
        wait_for_dds_dimensions,
    ] >> check_prerequisites

    check_prerequisites >> ensure_dim_date

    ensure_dim_date >> extract_reviews >> transform_reviews >> load_fact_review
    ensure_dim_date >> aggregate_player_activity >> load_fact_player_activity

    [load_fact_review, load_fact_player_activity] >> log_fact_run
