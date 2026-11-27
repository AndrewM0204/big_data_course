import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import Param
from airflow.sdk.exceptions import AirflowFailException

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_DQ_DIR = "/opt/airflow/data/processed/data_quality"
POSTGRES_CONN_ID = "steam_postgres"
ACTIVITY_LOOKBACK_DAYS = 1
MAX_PLAYERS_LIMIT = 5_000_000

CHECK_TASKS = (
    "dq_volume_reviews",
    "dq_volume_activity",
    "dq_anomaly_reviews",
    "dq_anomaly_activity",
    "dq_orphans",
    "dq_scd_integrity",
)


def daily_slot(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def pg_query(query: str, params=None, fetch_one: bool = True):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchone() if fetch_one else cursor.fetchall()
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
            INSERT INTO dds.dq_results (
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


def dq_volume_reviews(**context) -> dict:
    check_name = "volume_reviews"
    tolerance = float(context["params"].get("volume_tolerance_pct", 5.0)) / 100.0
    threshold = round(1 - tolerance, 4)

    if pg_query("SELECT to_regclass('steam.ods_reviews');")[0] is None:
        return record_result(
            context, check_name, "volume", "warning", "medium", None, threshold,
            {"reason": "steam.ods_reviews does not exist"},
        )

    ods_count = pg_query("SELECT count(*) FROM steam.ods_reviews;")[0]
    fact_count = pg_query(
        """
        SELECT count(*)
        FROM dds.fact_review f
        JOIN dds.dim_review r ON r.review_sk = f.review_sk
        WHERE r.is_current;
        """
    )[0]
    details = {"ods_count": ods_count, "fact_count": fact_count}

    if ods_count == 0:
        details["reason"] = "steam.ods_reviews is empty"
        return record_result(
            context, check_name, "volume", "warning", "medium", None, threshold, details
        )

    ratio = round(fact_count / ods_count, 4)
    if ratio >= threshold and fact_count <= ods_count:
        result = record_result(
            context, check_name, "volume", "passed", "info", ratio, threshold, details
        )
        logger.info(f"volume_reviews passed: ratio={ratio}")
        return result

    record_result(
        context, check_name, "volume", "error", "high", ratio, threshold, details
    )
    raise AirflowFailException(
        f"volume_reviews: ratio={ratio} is below {threshold} or fact exceeds ODS"
    )


def dq_volume_activity(**context) -> dict:
    check_name = "volume_activity"
    min_coverage = float(context["params"].get("activity_min_coverage", 0.95))

    interval_end = context.get("data_interval_end") or datetime.now(timezone.utc)
    window_end = interval_end.astimezone(timezone.utc)
    window_start = window_end - timedelta(days=ACTIVITY_LOOKBACK_DAYS)

    ods_points = pg_query(
        """
        SELECT count(*)
        FROM steam.ods_player_stats
        WHERE collected_at >= %s AND collected_at < %s;
        """,
        (window_start, window_end),
    )[0]

    fact_samples, fact_points = pg_query(
        """
        SELECT COALESCE(sum(samples), 0), count(*)
        FROM dds.fact_player_activity
        WHERE activity_date >= %s AND activity_date < %s;
        """,
        (window_start.date(), window_end.date()),
    )

    details = {
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "ods_points": ods_points,
        "fact_points": fact_points,
        "fact_samples": fact_samples,
    }

    if ods_points == 0:
        details["reason"] = "no player stats in ODS for the window"
        return record_result(
            context, check_name, "volume", "warning", "medium", None, min_coverage, details
        )

    coverage = round(fact_samples / ods_points, 4)
    if coverage >= min_coverage and fact_samples <= ods_points:
        result = record_result(
            context, check_name, "volume", "passed", "info", coverage, min_coverage, details
        )
        logger.info(f"volume_activity passed: coverage={coverage}")
        return result

    record_result(
        context, check_name, "volume", "error", "high", coverage, min_coverage, details
    )
    raise AirflowFailException(
        f"volume_activity: coverage={coverage} is below {min_coverage} or exceeds ODS"
    )


def dq_anomaly_reviews(**context) -> dict:
    check_name = "anomaly_reviews"
    threshold = 0

    anomalies = pg_query(
        """
        SELECT count(*)
        FROM dds.dim_review
        WHERE is_current
          AND (
              votes_up < 0
              OR votes_funny < 0
              OR weighted_score < -1
              OR weighted_score > 1
              OR timestamp_created > now()
          );
        """
    )[0]

    details = {"anomalies": anomalies}
    if anomalies == 0:
        result = record_result(
            context, check_name, "anomaly", "passed", "info", 0, threshold, details
        )
        logger.info("anomaly_reviews passed")
        return result

    record_result(context, check_name, "anomaly", "error", "high", anomalies, threshold, details)
    raise AirflowFailException(f"anomaly_reviews: {anomalies} invalid rows in dds.dim_review")


def dq_anomaly_activity(**context) -> dict:
    check_name = "anomaly_activity"
    threshold = 0

    anomalies = pg_query(
        """
        SELECT count(*)
        FROM dds.fact_player_activity
        WHERE avg_players < 0
           OR max_players < avg_players
           OR max_players > %s;
        """,
        (MAX_PLAYERS_LIMIT,),
    )[0]

    details = {"anomalies": anomalies, "max_players_limit": MAX_PLAYERS_LIMIT}
    if anomalies == 0:
        result = record_result(
            context, check_name, "anomaly", "passed", "info", 0, threshold, details
        )
        logger.info("anomaly_activity passed")
        return result

    record_result(context, check_name, "anomaly", "error", "high", anomalies, threshold, details)
    raise AirflowFailException(
        f"anomaly_activity: {anomalies} invalid rows in dds.fact_player_activity"
    )


def dq_orphans(**context) -> dict:
    check_name = "orphans"
    threshold = 0
    coverage_threshold = 0.95

    orphan_count = pg_query(
        """
        SELECT
            (SELECT count(*) FROM dds.fact_review f
              LEFT JOIN dds.dim_review r ON r.review_sk = f.review_sk
              WHERE r.review_sk IS NULL)
          + (SELECT count(*) FROM dds.fact_review f
              LEFT JOIN dds.dim_game g ON g.game_sk = f.game_sk
              WHERE g.game_sk IS NULL)
          + (SELECT count(*) FROM dds.fact_review f
              LEFT JOIN dds.dim_user u ON u.user_sk = f.user_sk
              WHERE u.user_sk IS NULL)
          + (SELECT count(*) FROM dds.fact_player_activity fa
              LEFT JOIN dds.dim_game g ON g.game_sk = fa.game_sk
              WHERE g.game_sk IS NULL);
        """
    )[0]

    coverage = None
    if pg_query("SELECT to_regclass('steam.ods_reviews');")[0] is not None:
        ods_appids, matched_appids = pg_query(
            """
            SELECT count(*), count(*) FILTER (WHERE g.appid IS NOT NULL)
            FROM (SELECT DISTINCT appid FROM steam.ods_reviews WHERE appid IS NOT NULL) r
            LEFT JOIN dds.dim_game g ON g.appid = r.appid AND g.is_current;
            """
        )
        if ods_appids:
            coverage = round(matched_appids / ods_appids, 4)

    details = {"orphan_count": orphan_count, "dim_game_coverage": coverage}

    if orphan_count > 0:
        record_result(
            context, check_name, "orphan", "error", "high", orphan_count, threshold, details
        )
        raise AirflowFailException(f"dq_orphans: {orphan_count} orphan rows in DDS facts")

    if coverage is not None and coverage < coverage_threshold:
        details["reason"] = "not all ods_reviews.appid have a current dim_game row"
        result = record_result(
            context, check_name, "orphan", "warning", "medium", 0, threshold, details
        )
        logger.warning(f"dq_orphans warning: dim_game coverage={coverage}")
        return result

    result = record_result(
        context, check_name, "orphan", "passed", "info", 0, threshold, details
    )
    logger.info("dq_orphans passed")
    return result


def dq_scd_integrity(**context) -> dict:
    check_name = "scd_integrity"
    threshold = 0

    anomalies = pg_query(
        """
        SELECT
            (SELECT count(*) FROM (
                SELECT appid FROM dds.dim_game WHERE is_current
                GROUP BY appid HAVING count(*) > 1
            ) d)
          + (SELECT count(*) FROM (
                SELECT steamid FROM dds.dim_user WHERE is_current
                GROUP BY steamid HAVING count(*) > 1
            ) d)
          + (SELECT count(*) FROM (
                SELECT review_id FROM dds.dim_review WHERE is_current
                GROUP BY review_id HAVING count(*) > 1
            ) d)
          + (SELECT count(*) FROM dds.dim_game WHERE valid_to <= valid_from)
          + (SELECT count(*) FROM dds.dim_user WHERE valid_to <= valid_from)
          + (SELECT count(*) FROM dds.dim_review WHERE valid_to <= valid_from)
          + (SELECT count(*) FROM dds.dim_game a
              JOIN dds.dim_game b
                ON a.appid = b.appid AND a.game_sk < b.game_sk
               AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to)
          + (SELECT count(*) FROM dds.dim_user a
              JOIN dds.dim_user b
                ON a.steamid = b.steamid AND a.user_sk < b.user_sk
               AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to)
          + (SELECT count(*) FROM dds.dim_review a
              JOIN dds.dim_review b
                ON a.review_id = b.review_id AND a.review_sk < b.review_sk
               AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to);
        """
    )[0]

    details = {"anomalies": anomalies}
    if anomalies == 0:
        result = record_result(
            context, check_name, "scd", "passed", "info", 0, threshold, details
        )
        logger.info("scd_integrity passed")
        return result

    record_result(context, check_name, "scd", "error", "high", anomalies, threshold, details)
    raise AirflowFailException(f"scd_integrity: {anomalies} SCD2 invariant violations")


def write_dq_summary(**context) -> str:
    ti = context["ti"]
    results = []

    for task_id in CHECK_TASKS:
        check_name = task_id.removeprefix("dq_")
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

    out_dir = os.path.join(BASE_DQ_DIR, context["ds"])
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "results.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    logger.info(f"DQ summary: {json.dumps(summary['status_counts'], ensure_ascii=False)}")
    return out_file


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="data_quality_checks",
    default_args=default_args,
    description="Data Quality checks for SCD2 DDS dimensions and facts",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["steam", "dds", "data_quality", "scd2"],
    params={
        "volume_tolerance_pct": Param(
            5.0,
            type="number",
            minimum=0.0,
            maximum=100.0,
            description="Allowed share of reviews lost between ODS and fact_review, percent",
        ),
        "activity_min_coverage": Param(
            0.95,
            type="number",
            minimum=0.0,
            maximum=1.0,
            description="Minimal share of ODS activity points present in fact_player_activity",
        ),
    },
    doc_md="""
### Data Quality Checks

Проверки DDS:
- `dq_volume_reviews` — покрытие ODS-отзывов текущими фактами;
- `dq_volume_activity` — покрытие ODS-точек онлайна суммой `samples`;
- `dq_anomaly_reviews` — отрицательные голоса, score вне `[-1, 1]`, будущие даты;
- `dq_anomaly_activity` — отрицательный онлайн, `max < avg`, онлайн выше порога;
- `dq_orphans` — сироты фактов и покрытие `appid`;
- `dq_scd_integrity` — одна текущая версия, корректные интервалы, отсутствие пересечений.

**Результаты:** `dds.dq_results` и JSON-отчёт `data/processed/data_quality/<ds>/results.json`.
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

    dq_volume_reviews = PythonOperator(
        task_id="dq_volume_reviews",
        python_callable=dq_volume_reviews,
    )

    dq_volume_activity = PythonOperator(
        task_id="dq_volume_activity",
        python_callable=dq_volume_activity,
    )

    dq_anomaly_reviews = PythonOperator(
        task_id="dq_anomaly_reviews",
        python_callable=dq_anomaly_reviews,
    )

    dq_anomaly_activity = PythonOperator(
        task_id="dq_anomaly_activity",
        python_callable=dq_anomaly_activity,
    )

    dq_orphans = PythonOperator(
        task_id="dq_orphans",
        python_callable=dq_orphans,
    )

    dq_scd_integrity = PythonOperator(
        task_id="dq_scd_integrity",
        python_callable=dq_scd_integrity,
    )

    write_dq_summary = PythonOperator(
        task_id="write_dq_summary",
        python_callable=write_dq_summary,
        trigger_rule="all_done",
    )

    checks = [
        dq_volume_reviews,
        dq_volume_activity,
        dq_anomaly_reviews,
        dq_anomaly_activity,
        dq_orphans,
        dq_scd_integrity,
    ]

    wait_for_dds_facts >> checks >> write_dq_summary
