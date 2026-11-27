import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk.exceptions import AirflowFailException

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

POSTGRES_CONN_ID = "steam_postgres"
BASE_DIR = "/opt/airflow/data/processed/data_quality_marts"

CHECK_TASKS = (
    "dq_mart_volume",
    "dq_mart_integrity",
    "dq_mart_coverage",
)


def daily_slot(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def pg_query(query: str, params=None):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchone()
    finally:
        cursor.close()
        conn.close()


def record_result(
    context,
    check_name: str,
    check_type: str,
    status: str,
    severity: str,
    metric_value,
    threshold,
    details: dict,
) -> dict:
    result = {
        "check_name": check_name,
        "check_type": check_type,
        "status": status,
        "severity": severity,
        "metric_value": metric_value,
        "threshold": threshold,
        "details": details,
    }

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO marts.dq_results (
                check_name, run_id, check_type, status, severity,
                metric_value, threshold, details
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb);
            """,
            (
                check_name,
                context["run_id"],
                check_type,
                status,
                severity,
                metric_value,
                threshold,
                json.dumps(details, ensure_ascii=False, default=str),
            ),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()

    context["ti"].xcom_push(key=check_name, value=result)
    return result


def dq_mart_volume(**context) -> dict:
    check_name = "volume"
    counts = {}
    for table in (
        "marts.mart_game_daily_metrics",
        "marts.mart_genre_daily_metrics",
        "marts.mart_activity_hourly",
    ):
        counts[table] = pg_query(f"SELECT count(*) FROM {table};")[0]

    empty = [table for table, count in counts.items() if count == 0]
    details = {"counts": counts}

    if empty:
        record_result(context, check_name, "volume", "error", "high", 0, 0, details)
        raise AirflowFailException(f"Empty marts: {', '.join(empty)}")

    result = record_result(
        context, check_name, "volume", "passed", "info", sum(counts.values()), 0, details
    )
    logger.info(f"mart volume passed: {counts}")
    return result


def dq_mart_integrity(**context) -> dict:
    check_name = "integrity"
    anomalies = pg_query(
        """
        SELECT
            (SELECT count(*) FROM marts.mart_game_daily_metrics
              WHERE positive_ratio < 0 OR positive_ratio > 1
                 OR positive_reviews > review_count
                 OR avg_players < 0
                 OR max_players < avg_players
                 OR samples < 0)
          + (SELECT count(*) FROM marts.mart_genre_daily_metrics
              WHERE positive_ratio < 0 OR positive_ratio > 1
                 OR positive_reviews > review_count
                 OR avg_players < 0
                 OR max_players < avg_players
                 OR samples < 0)
          + (SELECT count(*) FROM marts.mart_activity_hourly
              WHERE hour_of_day NOT BETWEEN 0 AND 23
                 OR day_of_week NOT BETWEEN 1 AND 7
                 OR avg_players < 0
                 OR max_players < avg_players
                 OR samples <= 0);
        """
    )[0]

    details = {"anomalies": anomalies}
    if anomalies:
        record_result(context, check_name, "integrity", "error", "high", anomalies, 0, details)
        raise AirflowFailException(f"Mart integrity: {anomalies} invalid rows")

    result = record_result(
        context, check_name, "integrity", "passed", "info", 0, 0, details
    )
    logger.info("mart integrity passed")
    return result


def dq_mart_coverage(**context) -> dict:
    check_name = "coverage"

    orphan_games = pg_query(
        """
        SELECT count(*)
        FROM (SELECT DISTINCT game_sk FROM marts.mart_game_daily_metrics) m
        LEFT JOIN dds.dim_game g ON g.game_sk = m.game_sk
        WHERE g.game_sk IS NULL;
        """
    )[0]

    orphan_genres = pg_query(
        """
        SELECT count(*)
        FROM (SELECT DISTINCT genre_id FROM marts.mart_genre_daily_metrics) m
        LEFT JOIN dds.dim_genre g ON g.genre_id = m.genre_id
        WHERE g.genre_id IS NULL;
        """
    )[0]

    mart_samples, dds_samples = pg_query(
        """
        SELECT
            COALESCE((SELECT sum(samples) FROM marts.mart_activity_hourly), 0),
            COALESCE((SELECT sum(samples) FROM dds.fact_player_activity), 0);
        """
    )

    mart_reviews, dds_reviews = pg_query(
        """
        SELECT
            COALESCE((SELECT sum(review_count) FROM marts.mart_game_daily_metrics), 0),
            (SELECT count(*)
             FROM dds.fact_review f
             JOIN dds.dim_review r ON r.review_sk = f.review_sk
             WHERE r.is_current);
        """
    )

    details = {
        "orphan_games": orphan_games,
        "orphan_genres": orphan_genres,
        "mart_samples": mart_samples,
        "dds_samples": dds_samples,
        "mart_reviews": mart_reviews,
        "dds_reviews": dds_reviews,
    }

    if orphan_games or orphan_genres:
        record_result(context, check_name, "coverage", "error", "high", orphan_games + orphan_genres, 0, details)
        raise AirflowFailException(
            f"Mart coverage: orphan games={orphan_games}, orphan genres={orphan_genres}"
        )

    if mart_samples != dds_samples or mart_reviews != dds_reviews:
        record_result(context, check_name, "coverage", "error", "high", None, 0, details)
        raise AirflowFailException(
            f"Mart coverage mismatch: samples {mart_samples}/{dds_samples}, "
            f"reviews {mart_reviews}/{dds_reviews}"
        )

    result = record_result(
        context, check_name, "coverage", "passed", "info", 0, 0, details
    )
    logger.info(f"mart coverage passed: {details}")
    return result


def write_summary(**context) -> str:
    ti = context["ti"]
    results = []
    for task_id in CHECK_TASKS:
        check_name = task_id.removeprefix("dq_mart_")
        result = ti.xcom_pull(task_ids=task_id, key=check_name)
        if result is None:
            result = {
                "check_name": check_name,
                "check_type": "unknown",
                "status": "error",
                "severity": "high",
                "metric_value": None,
                "threshold": None,
                "details": {"reason": "check task did not produce a result"},
            }
        results.append(result)

    summary = {
        "ds": context["ds"],
        "run_id": context["run_id"],
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "status_counts": {
            status: sum(1 for r in results if r["status"] == status)
            for status in ("passed", "warning", "error")
        },
        "results": results,
    }

    path = os.path.join(BASE_DIR, context["ds"])
    os.makedirs(path, exist_ok=True)
    out_file = os.path.join(path, "results.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)

    logger.info(f"Mart DQ summary: {json.dumps(summary['status_counts'], ensure_ascii=False)}")
    return out_file


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="data_quality_marts_dag",
    default_args=default_args,
    description="Data Quality checks for Superset marts",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["steam", "marts", "data_quality", "superset"],
    doc_md="""
### Data Quality Marts

Проверяет витрины:
- непустые `marts.*`;
- корректность `positive_ratio`, `avg_players`, `max_players`, `samples`;
- отсутствие orphan `game_sk` и `genre_id`;
- согласованность объёмов с DDS:
  - `sum(samples)` в `mart_activity_hourly` и `fact_player_activity`;
  - `sum(review_count)` в `mart_game_daily_metrics` и текущими `fact_review`.

**Результаты:** `marts.dq_results` и JSON в `data/processed/data_quality_marts/<ds>/results.json`.
""",
) as dag:

    wait_for_build_marts = ExternalTaskSensor(
        task_id="wait_for_build_marts",
        external_dag_id="build_marts_dag",
        external_task_id=None,
        execution_date_fn=daily_slot,
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=60,
        timeout=timedelta(hours=6),
        check_existence=True,
    )

    dq_mart_volume = PythonOperator(
        task_id="dq_mart_volume",
        python_callable=dq_mart_volume,
    )

    dq_mart_integrity = PythonOperator(
        task_id="dq_mart_integrity",
        python_callable=dq_mart_integrity,
    )

    dq_mart_coverage = PythonOperator(
        task_id="dq_mart_coverage",
        python_callable=dq_mart_coverage,
    )

    write_summary_task = PythonOperator(
        task_id="write_summary",
        python_callable=write_summary,
        trigger_rule="all_done",
    )

    checks = [dq_mart_volume, dq_mart_integrity, dq_mart_coverage]
    wait_for_build_marts >> checks >> write_summary_task
