import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import Param
from airflow.sdk.exceptions import AirflowException

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_PROC_DIR = "/opt/airflow/data/processed/dds_facts"
POSTGRES_CONN_ID = "steam_postgres"

REQUIRED_DDS_TABLES = (
    "dds.dim_game",
    "dds.dim_user",
    "dds.dim_review",
    "dds.fact_review",
    "dds.fact_player_activity",
)


def daily_slot(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def _as_utc(value) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def three_hour_slots(dt: datetime, **context) -> list[datetime]:
    end = _as_utc(context.get("data_interval_end") or dt)
    lookback = int((context.get("params") or {}).get("activity_lookback_days", 1))
    start = (end - timedelta(days=lookback)).replace(
        hour=((end - timedelta(days=lookback)).hour // 3) * 3,
        minute=0,
        second=0,
        microsecond=0,
    )

    slots = []
    current = _as_utc(start)
    end = _as_utc(end)
    while current < end:
        slots.append(current)
        current = _as_utc(current + timedelta(hours=3))
    return slots


def processed_dir(context) -> str:
    path = os.path.join(BASE_PROC_DIR, context["ds"])
    os.makedirs(path, exist_ok=True)
    return path


def parse_ts(value) -> datetime | None:
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(value)
    except ValueError:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def get_connection():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()


def player_stats_ready(**context) -> bool:
    slots = three_hour_slots(
        context.get("logical_date") or context.get("data_interval_end"),
        **context,
    )
    if not slots:
        return False

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT count(*)
            FROM unnest(%s::timestamptz[]) AS expected(slot)
            WHERE EXISTS (
                SELECT 1
                FROM steam.ods_player_stats s
                WHERE s.collected_at >= expected.slot
                  AND s.collected_at < expected.slot + interval '3 hours'
            );
            """,
            (slots,),
        )
        covered = cursor.fetchone()[0]
    finally:
        cursor.close()
        conn.close()

    logger.info(
        "Player stats window %s..%s: %s/%s slots covered.",
        slots[0].isoformat(),
        slots[-1].isoformat(),
        covered,
        len(slots),
    )
    return covered == len(slots)


def check_prerequisites(**context) -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
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
            raise AirflowException("dds.dim_game has no current rows.")

        cursor.execute("SELECT count(*) FROM dds.dim_review WHERE is_current;")
        current_reviews = cursor.fetchone()[0]
        if current_reviews == 0:
            logger.warning("dds.dim_review has no current rows; review facts will be empty.")
    finally:
        cursor.close()
        conn.close()

    logger.info(
        f"Prerequisites OK: current_games={current_games}, current_reviews={current_reviews}."
    )


def extract_reviews(**context) -> str:
    reviews_limit = int(context["params"].get("reviews_limit", 50000))

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT review_id, appid, language, review_text, voted_up,
                   votes_up, votes_funny, weighted_vote_score,
                   timestamp_created, author_steamid, author_num_reviews,
                   author_playtime_forever
            FROM steam.ods_reviews
            ORDER BY timestamp_created NULLS LAST
            LIMIT %s;
            """,
            (reviews_limit,),
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

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

    out_file = os.path.join(processed_dir(context), "reviews_extracted.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(reviews, f, ensure_ascii=False)

    logger.info(f"Extracted {len(reviews)} reviews (limit {reviews_limit}).")
    return out_file


def transform_reviews(extracted_file: str, **context) -> str:
    now = datetime.now(timezone.utc)

    with open(extracted_file, "r", encoding="utf-8") as f:
        reviews = json.load(f)

    stats = {"input": len(reviews), "dedup_dropped": 0, "filtered": 0, "output": 0}
    seen = set()
    prepared = []

    for review in reviews:
        review_id = review.get("review_id")
        if not review_id or review_id in seen:
            stats["dedup_dropped"] += 1
            continue
        seen.add(review_id)

        ts = parse_ts(review.get("timestamp_created"))
        votes_up = review.get("votes_up") or 0
        votes_funny = review.get("votes_funny") or 0
        author_steamid = review.get("author_steamid") or ""

        if (
            ts is None
            or ts > now
            or review.get("appid") is None
            or votes_up < 0
            or votes_funny < 0
            or not str(author_steamid).isdigit()
        ):
            stats["filtered"] += 1
            continue

        prepared.append({
            "review_id": review_id,
            "appid": int(review["appid"]),
            "language": review.get("language"),
            "voted_up": review.get("voted_up"),
            "votes_up": votes_up,
            "votes_funny": votes_funny,
            "weighted_score": review.get("weighted_score"),
            "timestamp_created": ts.isoformat(),
            "author_steamid": int(author_steamid),
            "author_num_reviews": review.get("author_num_reviews") or 0,
            "author_playtime_forever": review.get("author_playtime_forever") or 0,
        })

    stats["output"] = len(prepared)
    out_dir = processed_dir(context)
    out_file = os.path.join(out_dir, "reviews_prepared.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(prepared, f, ensure_ascii=False)

    stats_file = os.path.join(out_dir, "reviews_stats.json")
    with open(stats_file, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    logger.info(f"Reviews transform: {stats}")
    return out_file


def load_fact_review(prepared_file: str, **context) -> dict:
    with open(prepared_file, "r", encoding="utf-8") as f:
        rows = json.load(f)

    if not rows:
        logger.info("No prepared reviews; fact_review is not changed.")
        return {"staged": 0, "inserted": 0}

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("TRUNCATE dds.stg_review;")
        cursor.executemany(
            """
            INSERT INTO dds.stg_review (
                review_id, appid, language, voted_up, votes_up, votes_funny,
                weighted_score, timestamp_created, author_steamid,
                author_num_reviews, author_playtime_forever
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            [
                (
                    row["review_id"], row["appid"], row["language"], row["voted_up"],
                    row["votes_up"], row["votes_funny"], row["weighted_score"],
                    datetime.fromisoformat(row["timestamp_created"]),
                    row["author_steamid"], row["author_num_reviews"],
                    row["author_playtime_forever"],
                )
                for row in rows
            ],
        )

        cursor.execute("SELECT count(*) FROM dds.stg_review;")
        staged = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO dds.fact_review (review_sk, review_id, game_sk, user_sk)
            SELECT r.review_sk, r.review_id, g.game_sk, u.user_sk
            FROM dds.stg_review s
            JOIN dds.dim_review r
              ON r.review_id = s.review_id AND r.is_current
            JOIN dds.dim_game g
              ON g.appid = s.appid
             AND COALESCE(s.timestamp_created, now()) >= g.valid_from
             AND COALESCE(s.timestamp_created, now()) < g.valid_to
            JOIN dds.dim_user u
              ON u.steamid = s.author_steamid
             AND COALESCE(s.timestamp_created, now()) >= u.valid_from
             AND COALESCE(s.timestamp_created, now()) < u.valid_to
            ON CONFLICT (review_sk) DO UPDATE SET
                review_id = EXCLUDED.review_id,
                game_sk = EXCLUDED.game_sk,
                user_sk = EXCLUDED.user_sk,
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

    counters = {"staged": staged, "inserted": inserted}
    logger.info(f"fact_review load: {counters}")
    return counters


def aggregate_player_activity(**context) -> str:
    lookback = int(context["params"].get("activity_lookback_days", 1))
    interval_end = context.get("data_interval_end") or datetime.now(timezone.utc)
    window_end = interval_end.astimezone(timezone.utc)
    window_start = window_end - timedelta(days=lookback)

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT
                g.game_sk,
                (s.collected_at AT TIME ZONE 'UTC')::date AS activity_date,
                EXTRACT(HOUR FROM s.collected_at AT TIME ZONE 'UTC')::smallint AS hour_of_day,
                (array_agg(s.current_players ORDER BY s.collected_at DESC))[1]::bigint
                    AS current_players,
                round(avg(s.current_players), 2)::numeric(12, 2) AS avg_players,
                max(s.current_players)::bigint AS max_players,
                count(*)::int AS samples
            FROM steam.ods_player_stats s
            JOIN dds.dim_game g
              ON g.appid = s.appid
             AND s.collected_at >= g.valid_from
             AND s.collected_at < g.valid_to
            WHERE s.collected_at >= %s
              AND s.collected_at < %s
            GROUP BY g.game_sk, activity_date, hour_of_day
            ORDER BY g.game_sk, activity_date, hour_of_day;
            """,
            (window_start, window_end),
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    prepared = [
        {
            "game_sk": int(game_sk),
            "activity_date": activity_date.isoformat(),
            "hour_of_day": int(hour_of_day),
            "current_players": int(current_players),
            "avg_players": float(avg_players),
            "max_players": int(max_players),
            "samples": int(samples),
        }
        for game_sk, activity_date, hour_of_day, current_players, avg_players, max_players, samples in rows
    ]

    out_file = os.path.join(processed_dir(context), "activity_prepared.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(prepared, f, ensure_ascii=False)

    logger.info(
        f"Aggregated {len(prepared)} hourly points for [{window_start}, {window_end})."
    )
    return out_file


def load_fact_player_activity(prepared_file: str, **context) -> dict:
    with open(prepared_file, "r", encoding="utf-8") as f:
        rows = json.load(f)

    if not rows:
        logger.info("No prepared activity; fact_player_activity is not changed.")
        return {"inserted": 0}

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.executemany(
            """
            INSERT INTO dds.fact_player_activity (
                game_sk, activity_date, hour_of_day, current_players,
                avg_players, max_players, samples
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_sk, activity_date, hour_of_day) DO UPDATE SET
                current_players = EXCLUDED.current_players,
                avg_players = EXCLUDED.avg_players,
                max_players = EXCLUDED.max_players,
                samples = EXCLUDED.samples,
                loaded_at = now();
            """,
            [
                (
                    row["game_sk"], row["activity_date"], row["hour_of_day"],
                    row["current_players"], row["avg_players"], row["max_players"],
                    row["samples"],
                )
                for row in rows
            ],
        )
        inserted = cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info(f"fact_player_activity load: inserted={inserted}.")
    return {"inserted": inserted}


def log_fact_run(**context) -> str:
    ti = context["ti"]
    out_dir = processed_dir(context)

    reviews_stats = {}
    stats_file = os.path.join(out_dir, "reviews_stats.json")
    if os.path.exists(stats_file):
        with open(stats_file, "r", encoding="utf-8") as f:
            reviews_stats = json.load(f)

    summary = {
        "ds": context["ds"],
        "run_id": context["run_id"],
        "reviews": reviews_stats,
        "fact_review": ti.xcom_pull(task_ids="load_fact_review"),
        "fact_player_activity": ti.xcom_pull(task_ids="load_fact_player_activity"),
    }

    out_file = os.path.join(out_dir, "run_summary.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    logger.info(f"DDS facts run summary: {json.dumps(summary, ensure_ascii=False)}")
    return out_file


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="dds_facts_dag",
    default_args=default_args,
    description="Load DDS facts using SCD2 surrogate keys and PostgreSQL ODS",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["steam", "dds", "facts", "scd2"],
    params={
        "reviews_limit": Param(
            50000,
            type="integer",
            minimum=1,
            maximum=500000,
            description="Max reviews to process per run",
        ),
        "activity_lookback_days": Param(
            1,
            type="integer",
            minimum=1,
            maximum=30,
            description="Closed UTC days of player stats to re-aggregate",
        ),
    },
    doc_md="""
### DDS Facts

Загружает:
- `dds.fact_review` из `steam.ods_reviews` с `review_sk`, `game_sk`, `user_sk`;
- `dds.fact_player_activity` из `steam.ods_player_stats` с `game_sk` as-of версии игры.

**Зависимости:** `dds_game_dimensions_dag`, `dds_review_dimensions_dag`,
`steam_player_stats_etl`.

**Идемпотентность:** upsert по `review_sk` и `(game_sk, activity_date, hour_of_day)`.
""",
) as dag:

    wait_for_game_dimensions = ExternalTaskSensor(
        task_id="wait_for_game_dimensions",
        external_dag_id="dds_game_dimensions_dag",
        external_task_id=None,
        execution_date_fn=daily_slot,
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=60,
        timeout=timedelta(hours=6),
        check_existence=True,
    )

    wait_for_review_dimensions = ExternalTaskSensor(
        task_id="wait_for_review_dimensions",
        external_dag_id="dds_review_dimensions_dag",
        external_task_id=None,
        execution_date_fn=daily_slot,
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=60,
        timeout=timedelta(hours=6),
        check_existence=True,
    )

    wait_for_ods_player_stats = PythonSensor(
        task_id="wait_for_ods_player_stats",
        python_callable=player_stats_ready,
        mode="reschedule",
        poke_interval=300,
        timeout=timedelta(hours=6),
    )

    check_prerequisites_task = PythonOperator(
        task_id="check_prerequisites",
        python_callable=check_prerequisites,
    )

    extract_reviews_task = PythonOperator(
        task_id="extract_reviews",
        python_callable=extract_reviews,
    )

    transform_reviews_task = PythonOperator(
        task_id="transform_reviews",
        python_callable=transform_reviews,
        op_kwargs={
            "extracted_file": "{{ ti.xcom_pull(task_ids='extract_reviews') }}"
        },
    )

    load_fact_review_task = PythonOperator(
        task_id="load_fact_review",
        python_callable=load_fact_review,
        op_kwargs={
            "prepared_file": "{{ ti.xcom_pull(task_ids='transform_reviews') }}"
        },
    )

    aggregate_player_activity_task = PythonOperator(
        task_id="aggregate_player_activity",
        python_callable=aggregate_player_activity,
    )

    load_fact_player_activity_task = PythonOperator(
        task_id="load_fact_player_activity",
        python_callable=load_fact_player_activity,
        op_kwargs={
            "prepared_file": "{{ ti.xcom_pull(task_ids='aggregate_player_activity') }}"
        },
    )

    log_fact_run_task = PythonOperator(
        task_id="log_fact_run",
        python_callable=log_fact_run,
        trigger_rule="none_failed",
    )

    [
        wait_for_game_dimensions,
        wait_for_review_dimensions,
        wait_for_ods_player_stats,
    ] >> check_prerequisites_task

    check_prerequisites_task >> extract_reviews_task >> transform_reviews_task >> load_fact_review_task
    check_prerequisites_task >> aggregate_player_activity_task >> load_fact_player_activity_task

    [load_fact_review_task, load_fact_player_activity_task] >> log_fact_run_task
