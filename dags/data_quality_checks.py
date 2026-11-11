import json
import logging
import os
from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException
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

BASE_DQ_DIR = '/opt/airflow/data/processed/data_quality'

POSTGRES_CONN_ID = "steam_postgres"
CLICKHOUSE_CONN_ID = "clickhouse_http"

# Окно сверки активности: те же закрытые сутки UTC, что у dds_facts_dag
ACTIVITY_LOOKBACK_DAYS = 1

# Верхняя граница правдоподобного онлайна (выше пика Steam)
MAX_PLAYERS_LIMIT = 5_000_000

# Задачи проверок: имена задач совпадают с ключами XCom для сводки
CHECK_TASKS = (
    'dq_volume_reviews',
    'dq_volume_activity',
    'dq_anomaly_reviews',
    'dq_anomaly_activity',
    'dq_orphans',
)


# ==========================================
# ХЕЛПЕРЫ
# ==========================================
def daily_slot(dt: datetime) -> datetime:
    """Выравнивает logical date на полночь — сетка @daily DAG'ов."""
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


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


def clickhouse_query(query: str) -> str:
    """Выполняет запрос к ClickHouse через HTTP-интерфейс."""
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


def pg_query(query: str, params=None, fetch_one: bool = True):
    """Выполняет запрос к Postgres и возвращает одну строку или все строки."""
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
    """Пишет результат проверки в dds.dq_results и кладёт его в XCom."""
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
                context['run_id'],
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

    context['ti'].xcom_push(key=check_name, value=result)
    return result


# ==========================================
# ПРОВЕРКИ
# ==========================================
def dq_volume_reviews(**context) -> dict:
    """Объём: доля отзывов, дошедших из ODS в fact_review."""
    check_name = 'volume_reviews'
    tolerance = float(context['params'].get('volume_tolerance_pct', 5.0)) / 100.0
    threshold = round(1 - tolerance, 4)

    if pg_query("SELECT to_regclass('steam.ods_reviews');")[0] is None:
        details = {"reason": "steam.ods_reviews does not exist (owner A)"}
        return record_result(
            context, check_name, 'volume', 'warning', 'medium', None, threshold, details
        )

    ods_count = pg_query("SELECT count(*) FROM steam.ods_reviews;")[0]
    fact_count = pg_query("SELECT count(*) FROM dds.fact_review;")[0]
    details = {"ods_count": ods_count, "fact_count": fact_count}

    if ods_count == 0:
        details["reason"] = "steam.ods_reviews is empty"
        return record_result(
            context, check_name, 'volume', 'warning', 'medium', None, threshold, details
        )

    ratio = round(fact_count / ods_count, 4)
    if ratio >= threshold and fact_count <= ods_count:
        result = record_result(
            context, check_name, 'volume', 'passed', 'info', ratio, threshold, details
        )
        logger.info(f"volume_reviews passed: ratio={ratio}")
        return result

    result = record_result(
        context, check_name, 'volume', 'error', 'high', ratio, threshold, details
    )
    raise AirflowFailException(
        f"volume_reviews: ratio={ratio} is below {threshold} or fact_review exceeds ods_reviews"
    )


def dq_volume_activity(**context) -> dict:
    """Покрытие: сумма samples факта против часовых точек ClickHouse за окно."""
    check_name = 'volume_activity'
    min_coverage = float(context['params'].get('activity_min_coverage', 0.95))

    window_end = context['data_interval_end'].astimezone(timezone.utc)
    window_start = window_end - timedelta(days=ACTIVITY_LOOKBACK_DAYS)
    start_id = int(window_start.strftime('%Y%m%d'))
    end_id = int(window_end.strftime('%Y%m%d'))

    # ReplacingMergeTree: без FINAL дубли завышают count.
    # sum(samples) факта — это тоже число сырых точек, метрики сопоставимы
    ch_query = f"""
        SELECT count()
        FROM steam.player_stats FINAL
        WHERE collected_at >= toDateTime('{window_start:%Y-%m-%d %H:%M:%S}', 'UTC')
          AND collected_at < toDateTime('{window_end:%Y-%m-%d %H:%M:%S}', 'UTC')
        FORMAT JSONEachRow
    """
    ch_response = clickhouse_query(ch_query)
    ch_points = 0
    if ch_response:
        line = next((line for line in ch_response.splitlines() if line.strip()), None)
        if line:
            ch_points = int(json.loads(line)['count()'])

    sum_samples, fact_points = pg_query(
        """
        SELECT COALESCE(sum(samples), 0), count(*)
        FROM dds.fact_player_activity
        WHERE date_id >= %s AND date_id < %s;
        """,
        (start_id, end_id),
    )

    details = {
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "clickhouse_points": ch_points,
        "fact_points": fact_points,
        "fact_samples": sum_samples,
    }

    if ch_points == 0:
        details["reason"] = "no player stats in ClickHouse for the window"
        return record_result(
            context, check_name, 'volume', 'warning', 'medium', None, min_coverage, details
        )

    coverage = round(sum_samples / ch_points, 4)
    if coverage >= min_coverage:
        result = record_result(
            context, check_name, 'volume', 'passed', 'info', coverage, min_coverage, details
        )
        logger.info(f"volume_activity passed: coverage={coverage}")
        return result

    record_result(
        context, check_name, 'volume', 'error', 'high', coverage, min_coverage, details
    )
    raise AirflowFailException(
        f"volume_activity: coverage={coverage} is below {min_coverage}"
    )


def dq_anomaly_reviews(**context) -> dict:
    """Аномалии отзывов: отрицательные голоса, score вне [-1, 1], будущие даты."""
    check_name = 'anomaly_reviews'
    threshold = 0

    anomalies = pg_query(
        """
        SELECT count(*)
        FROM dds.fact_review
        WHERE votes_up < 0
           OR votes_funny < 0
           OR weighted_score < -1
           OR weighted_score > 1
           OR timestamp_created > now()
           OR date_id > to_char(now() AT TIME ZONE 'UTC', 'YYYYMMDD')::int;
        """
    )[0]

    details = {"anomalies": anomalies}
    if anomalies == 0:
        result = record_result(
            context, check_name, 'anomaly', 'passed', 'info', 0, threshold, details
        )
        logger.info("anomaly_reviews passed")
        return result

    record_result(context, check_name, 'anomaly', 'error', 'high', anomalies, threshold, details)
    raise AirflowFailException(f"anomaly_reviews: {anomalies} invalid rows in dds.fact_review")


def dq_anomaly_activity(**context) -> dict:
    """Аномалии активности: отрицательные значения, max < avg, онлайн выше порога."""
    check_name = 'anomaly_activity'
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
            context, check_name, 'anomaly', 'passed', 'info', 0, threshold, details
        )
        logger.info("anomaly_activity passed")
        return result

    record_result(context, check_name, 'anomaly', 'error', 'high', anomalies, threshold, details)
    raise AirflowFailException(
        f"anomaly_activity: {anomalies} invalid rows in dds.fact_player_activity"
    )


def dq_orphans(**context) -> dict:
    """Сироты фактов и покрытие appid из ODS текущими версиями dim_game."""
    check_name = 'orphans'
    threshold = 0
    coverage_threshold = 0.95

    orphan_count = pg_query(
        """
        SELECT
            (SELECT count(*) FROM dds.fact_review f
              LEFT JOIN dds.dim_game g ON g.game_sk = f.game_sk
              WHERE g.game_sk IS NULL)
          + (SELECT count(*) FROM dds.fact_review f
              LEFT JOIN dds.dim_date d ON d.date_id = f.date_id
              WHERE d.date_id IS NULL)
          + (SELECT count(*) FROM dds.fact_review f
              LEFT JOIN dds.dim_author a ON a.steamid = f.author_steamid
              WHERE f.author_steamid IS NOT NULL AND a.steamid IS NULL)
          + (SELECT count(*) FROM dds.fact_player_activity fa
              LEFT JOIN dds.dim_game g ON g.game_sk = fa.game_sk
              WHERE g.game_sk IS NULL)
          + (SELECT count(*) FROM dds.fact_player_activity fa
              LEFT JOIN dds.dim_date d ON d.date_id = fa.date_id
              WHERE d.date_id IS NULL);
        """
    )[0]

    coverage = None
    if pg_query("SELECT to_regclass('steam.ods_reviews');")[0] is not None:
        ods_appids, matched_appids = pg_query(
            """
            SELECT count(*), count(*) FILTER (WHERE g.game_sk IS NOT NULL)
            FROM (SELECT DISTINCT appid FROM steam.ods_reviews WHERE appid IS NOT NULL) r
            LEFT JOIN dds.dim_game g ON g.appid = r.appid AND g.is_current;
            """
        )
        if ods_appids:
            coverage = round(matched_appids / ods_appids, 4)

    details = {"orphan_count": orphan_count, "dim_game_coverage": coverage}

    if orphan_count > 0:
        record_result(
            context, check_name, 'orphan', 'error', 'high', orphan_count, threshold, details
        )
        raise AirflowFailException(f"dq_orphans: {orphan_count} orphan rows in DDS facts")

    if coverage is not None and coverage < coverage_threshold:
        details["reason"] = "not all ods_reviews.appid have a current dim_game row"
        result = record_result(
            context, check_name, 'orphan', 'warning', 'medium', 0, threshold, details
        )
        logger.warning(f"dq_orphans warning: dim_game coverage={coverage}")
        return result

    result = record_result(
        context, check_name, 'orphan', 'passed', 'info', 0, threshold, details
    )
    logger.info("dq_orphans passed")
    return result


def write_dq_summary(**context) -> str:
    """Собирает результаты всех проверок в JSON-отчёт на общем томе."""
    ti = context['ti']
    results = []

    for task_id in CHECK_TASKS:
        check_name = task_id.removeprefix('dq_')
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
        "ds": context['ds'],
        "run_id": context['run_id'],
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "status_counts": {
            status: sum(1 for r in results if r['status'] == status)
            for status in ('passed', 'warning', 'error')
        },
        "results": results,
    }

    out_dir = os.path.join(BASE_DQ_DIR, context['ds'])
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, 'results.json')
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    logger.info(f"DQ summary: {json.dumps(summary['status_counts'], ensure_ascii=False)}")
    return out_file


# ==========================================
# ОПРЕДЕЛЕНИЕ DAG
# ==========================================
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    dag_id='data_quality_checks',
    default_args=default_args,
    description='Data Quality checks for DDS facts (reviews, player activity)',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['steam', 'dds', 'data_quality'],
    params={
        'volume_tolerance_pct': Param(
            5.0,
            type='number',
            minimum=0.0,
            maximum=100.0,
            description='Allowed share of reviews lost between ODS and fact_review, percent'
        ),
        'activity_min_coverage': Param(
            0.95,
            type='number',
            minimum=0.0,
            maximum=1.0,
            description='Minimal share of ClickHouse activity samples present in fact_player_activity'
        ),
    },
    doc_md="""
### Data Quality Checks

Проверки DDS-фактов после `dds_facts_dag`:
- `dq_volume_reviews` — доля отзывов, дошедших из `steam.ods_reviews` в `dds.fact_review`;
- `dq_volume_activity` — покрытие часовых точек ClickHouse суммой `samples` факта;
- `dq_anomaly_reviews` — отрицательные голоса, score вне `[-1, 1]`, будущие даты;
- `dq_anomaly_activity` — отрицательный онлайн, `max < avg`, онлайн выше порога;
- `dq_orphans` — сироты фактов и покрытие `appid` текущими версиями `dim_game`.

**Результаты:** строки в `dds.dq_results` и JSON-отчёт
`data/processed/data_quality/<ds>/results.json`. Провал уровня `error` роняет задачу.

**Параметры:** `volume_tolerance_pct`, `activity_min_coverage`.
"""
) as dag:

    wait_for_dds_facts = ExternalTaskSensor(
        task_id='wait_for_dds_facts',
        external_dag_id='dds_facts_dag',
        external_task_id=None,
        execution_date_fn=daily_slot,
        allowed_states=['success'],
        mode='reschedule',
        poke_interval=60,
        timeout=timedelta(hours=6),
        check_existence=True,
    )

    dq_volume_reviews = PythonOperator(
        task_id='dq_volume_reviews',
        python_callable=dq_volume_reviews,
    )

    dq_volume_activity = PythonOperator(
        task_id='dq_volume_activity',
        python_callable=dq_volume_activity,
    )

    dq_anomaly_reviews = PythonOperator(
        task_id='dq_anomaly_reviews',
        python_callable=dq_anomaly_reviews,
    )

    dq_anomaly_activity = PythonOperator(
        task_id='dq_anomaly_activity',
        python_callable=dq_anomaly_activity,
    )

    dq_orphans = PythonOperator(
        task_id='dq_orphans',
        python_callable=dq_orphans,
    )

    write_dq_summary = PythonOperator(
        task_id='write_dq_summary',
        python_callable=write_dq_summary,
        trigger_rule='ALL_DONE',
    )

    checks = [
        dq_volume_reviews,
        dq_volume_activity,
        dq_anomaly_reviews,
        dq_anomaly_activity,
        dq_orphans,
    ]

    wait_for_dds_facts >> checks >> write_dq_summary
