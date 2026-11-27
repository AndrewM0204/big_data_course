import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk.exceptions import AirflowException

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

POSTGRES_CONN_ID = "steam_postgres"
BASE_DIR = "/opt/airflow/data/processed/marts"
MARTS = (
    "marts.mart_game_daily_metrics",
    "marts.mart_genre_daily_metrics",
    "marts.mart_activity_hourly",
)


def daily_slot(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def get_connection():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()


def out_dir(context) -> str:
    path = os.path.join(BASE_DIR, context["ds"])
    os.makedirs(path, exist_ok=True)
    return path


def ensure_marts_schema(**context) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS marts;")
        missing = []
        for mart in MARTS:
            cursor.execute("SELECT to_regclass(%s);", (mart,))
            if cursor.fetchone()[0] is None:
                missing.append(mart)
        if missing:
            raise AirflowException(
                "Marts tables are missing: " + ", ".join(missing)
                + ". Apply sql/marts/init_marts.sql first."
            )
        conn.commit()
    finally:
        cursor.close()
        conn.close()

    logger.info("Marts schema is ready.")


def refresh_game_mart(**context) -> int:
    sql = """
    WITH review_metrics AS (
        SELECT
            (r.timestamp_created AT TIME ZONE 'UTC')::date AS activity_date,
            f.game_sk,
            count(*)::bigint AS review_count,
            count(*) FILTER (WHERE r.voted_up)::bigint AS positive_reviews,
            round(avg(r.weighted_score), 5)::numeric(8, 5) AS avg_weighted_score
        FROM dds.fact_review f
        JOIN dds.dim_review r
          ON r.review_sk = f.review_sk
        WHERE r.is_current
          AND r.timestamp_created IS NOT NULL
        GROUP BY 1, 2
    ),
    activity_metrics AS (
        SELECT
            fa.activity_date,
            fa.game_sk,
            round(
                sum(fa.avg_players * fa.samples) / NULLIF(sum(fa.samples), 0), 2
            )::numeric(12, 2) AS avg_players,
            max(fa.max_players)::bigint AS max_players,
            sum(fa.samples)::bigint AS samples
        FROM dds.fact_player_activity fa
        GROUP BY 1, 2
    )
    INSERT INTO marts.mart_game_daily_metrics (
        activity_date, game_sk, appid, game_name, is_free,
        review_count, positive_reviews, positive_ratio,
        avg_weighted_score, avg_players, max_players, samples
    )
    SELECT
        COALESCE(r.activity_date, a.activity_date),
        COALESCE(r.game_sk, a.game_sk),
        g.appid,
        g.name,
        g.is_free,
        COALESCE(r.review_count, 0),
        COALESCE(r.positive_reviews, 0),
        CASE
            WHEN COALESCE(r.review_count, 0) > 0
            THEN round(
                COALESCE(r.positive_reviews, 0)::numeric / r.review_count, 4
            )
            ELSE NULL
        END,
        r.avg_weighted_score,
        a.avg_players,
        a.max_players,
        COALESCE(a.samples, 0)
    FROM review_metrics r
    FULL OUTER JOIN activity_metrics a
      ON a.activity_date = r.activity_date
     AND a.game_sk = r.game_sk
    JOIN dds.dim_game g
      ON g.game_sk = COALESCE(r.game_sk, a.game_sk);
    """

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("TRUNCATE marts.mart_game_daily_metrics;")
        cursor.execute(sql)
        cursor.execute("SELECT count(*) FROM marts.mart_game_daily_metrics;")
        count = cursor.fetchone()[0]
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info(f"mart_game_daily_metrics refreshed: {count} rows.")
    return count


def refresh_genre_mart(**context) -> int:
    sql = """
    WITH review_metrics AS (
        SELECT
            (r.timestamp_created AT TIME ZONE 'UTC')::date AS activity_date,
            bg.genre_id,
            count(*)::bigint AS review_count,
            count(*) FILTER (WHERE r.voted_up)::bigint AS positive_reviews,
            round(avg(r.weighted_score), 5)::numeric(8, 5) AS avg_weighted_score,
            count(DISTINCT f.game_sk)::bigint AS games_count
        FROM dds.fact_review f
        JOIN dds.dim_review r
          ON r.review_sk = f.review_sk
        JOIN dds.bridge_game_genre bg
          ON bg.game_sk = f.game_sk
        WHERE r.is_current
          AND r.timestamp_created IS NOT NULL
        GROUP BY 1, 2
    ),
    activity_metrics AS (
        SELECT
            fa.activity_date,
            bg.genre_id,
            round(
                sum(fa.avg_players * fa.samples) / NULLIF(sum(fa.samples), 0), 2
            )::numeric(12, 2) AS avg_players,
            max(fa.max_players)::bigint AS max_players,
            sum(fa.samples)::bigint AS samples,
            count(DISTINCT fa.game_sk)::bigint AS games_count
        FROM dds.fact_player_activity fa
        JOIN dds.bridge_game_genre bg
          ON bg.game_sk = fa.game_sk
        GROUP BY 1, 2
    )
    INSERT INTO marts.mart_genre_daily_metrics (
        activity_date, genre_id, genre_name,
        review_count, positive_reviews, positive_ratio, avg_weighted_score,
        games_count, avg_players, max_players, samples
    )
    SELECT
        COALESCE(r.activity_date, a.activity_date),
        COALESCE(r.genre_id, a.genre_id),
        dg.genre_name,
        COALESCE(r.review_count, 0),
        COALESCE(r.positive_reviews, 0),
        CASE
            WHEN COALESCE(r.review_count, 0) > 0
            THEN round(
                COALESCE(r.positive_reviews, 0)::numeric / r.review_count, 4
            )
            ELSE NULL
        END,
        r.avg_weighted_score,
        GREATEST(COALESCE(r.games_count, 0), COALESCE(a.games_count, 0)),
        a.avg_players,
        a.max_players,
        COALESCE(a.samples, 0)
    FROM review_metrics r
    FULL OUTER JOIN activity_metrics a
      ON a.activity_date = r.activity_date
     AND a.genre_id = r.genre_id
    JOIN dds.dim_genre dg
      ON dg.genre_id = COALESCE(r.genre_id, a.genre_id);
    """

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("TRUNCATE marts.mart_genre_daily_metrics;")
        cursor.execute(sql)
        cursor.execute("SELECT count(*) FROM marts.mart_genre_daily_metrics;")
        count = cursor.fetchone()[0]
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info(f"mart_genre_daily_metrics refreshed: {count} rows.")
    return count


def refresh_activity_mart(**context) -> int:
    sql = """
    INSERT INTO marts.mart_activity_hourly (
        activity_date, hour_of_day, day_of_week, day_name, is_weekend,
        game_sk, appid, game_name, avg_players, max_players, samples
    )
    SELECT
        fa.activity_date,
        fa.hour_of_day,
        EXTRACT(ISODOW FROM fa.activity_date)::smallint,
        to_char(fa.activity_date, 'FMDAY'),
        EXTRACT(ISODOW FROM fa.activity_date) IN (6, 7),
        fa.game_sk,
        g.appid,
        g.name,
        fa.avg_players,
        fa.max_players,
        fa.samples
    FROM dds.fact_player_activity fa
    JOIN dds.dim_game g
      ON g.game_sk = fa.game_sk;
    """

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("TRUNCATE marts.mart_activity_hourly;")
        cursor.execute(sql)
        cursor.execute("SELECT count(*) FROM marts.mart_activity_hourly;")
        count = cursor.fetchone()[0]
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info(f"mart_activity_hourly refreshed: {count} rows.")
    return count


def validate_marts(**context) -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        counts = {}
        for mart in MARTS:
            cursor.execute(f"SELECT count(*) FROM {mart};")
            counts[mart] = cursor.fetchone()[0]

        if any(count == 0 for count in counts.values()):
            raise AirflowException(f"One or more marts are empty: {counts}")

        cursor.execute(
            """
            SELECT count(*)
            FROM marts.mart_game_daily_metrics
            WHERE positive_ratio < 0 OR positive_ratio > 1
               OR avg_players < 0
               OR max_players < avg_players;
            """
        )
        game_anomalies = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM marts.mart_genre_daily_metrics
            WHERE positive_ratio < 0 OR positive_ratio > 1
               OR avg_players < 0
               OR max_players < avg_players;
            """
        )
        genre_anomalies = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM marts.mart_activity_hourly
            WHERE avg_players < 0
               OR max_players < avg_players
               OR samples <= 0
               OR hour_of_day NOT BETWEEN 0 AND 23;
            """
        )
        activity_anomalies = cursor.fetchone()[0]
    finally:
        cursor.close()
        conn.close()

    anomalies = game_anomalies + genre_anomalies + activity_anomalies
    if anomalies:
        raise AirflowException(f"Marts validation failed: {anomalies} anomalies.")

    result = {
        "counts": counts,
        "game_anomalies": game_anomalies,
        "genre_anomalies": genre_anomalies,
        "activity_anomalies": activity_anomalies,
    }
    logger.info(f"Marts validation passed: {result}")
    return result


def log_mart_run(**context) -> str:
    ti = context["ti"]
    summary = {
        "ds": context["ds"],
        "run_id": context["run_id"],
        "game_rows": ti.xcom_pull(task_ids="refresh_game_mart"),
        "genre_rows": ti.xcom_pull(task_ids="refresh_genre_mart"),
        "activity_rows": ti.xcom_pull(task_ids="refresh_activity_mart"),
        "validation": ti.xcom_pull(task_ids="validate_marts"),
    }

    path = os.path.join(out_dir(context), "run_summary.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    logger.info(f"Marts run summary: {json.dumps(summary, ensure_ascii=False)}")
    return path


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="build_marts_dag",
    default_args=default_args,
    description="Build Superset data marts from DDS facts",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["steam", "dds", "marts", "superset"],
    doc_md="""
### Build Marts

Строит витрины для Superset:
- `marts.mart_game_daily_metrics`;
- `marts.mart_genre_daily_metrics`;
- `marts.mart_activity_hourly`.

**Запуск:** после `dds_facts_dag` за тот же logical date.
**Обновление:** полное, через `TRUNCATE` + `INSERT` в транзакции.
""",
) as dag:

    wait_for_dds_facts = ExternalTaskSensor(
        task_id="wait_for_dds_facts",
        external_dag_id="dds_facts_dag",
        external_task_id=None,
        execution_date_fn=daily_slot,
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=60,
        timeout=timedelta(hours=6),
        check_existence=True,
    )

    ensure_marts_schema_task = PythonOperator(
        task_id="ensure_marts_schema",
        python_callable=ensure_marts_schema,
    )

    refresh_game_mart_task = PythonOperator(
        task_id="refresh_game_mart",
        python_callable=refresh_game_mart,
    )

    refresh_genre_mart_task = PythonOperator(
        task_id="refresh_genre_mart",
        python_callable=refresh_genre_mart,
    )

    refresh_activity_mart_task = PythonOperator(
        task_id="refresh_activity_mart",
        python_callable=refresh_activity_mart,
    )

    validate_marts_task = PythonOperator(
        task_id="validate_marts",
        python_callable=validate_marts,
    )

    log_mart_run_task = PythonOperator(
        task_id="log_mart_run",
        python_callable=log_mart_run,
        trigger_rule="none_failed",
    )

    wait_for_dds_facts >> ensure_marts_schema_task
    ensure_marts_schema_task >> [
        refresh_game_mart_task,
        refresh_genre_mart_task,
        refresh_activity_mart_task,
    ] >> validate_marts_task >> log_mart_run_task
