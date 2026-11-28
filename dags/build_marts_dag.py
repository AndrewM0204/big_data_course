import json
import logging
import os
from datetime import date, datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk.exceptions import AirflowException, AirflowFailException

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_MART_DIR = "/opt/airflow/data/processed/marts"
POSTGRES_CONN_ID = "steam_postgres"

# Единый ключ advisory-блокировки. Все авторизованные издатели витрин обязаны
# брать ИМЕННО этот ключ, иначе протокол публикации не соблюдается.
PUBLISH_LOCK_KEY = 48234001

REQUIRED_DDS_TABLES = (
    "dds.dim_game",
    "dds.dim_review",
    "dds.fact_review",
    "dds.fact_player_activity",
    "dds.dim_genre",
    "dds.dim_publisher",
    "dds.bridge_game_genre",
    "dds.bridge_game_publisher",
)

MART_TABLES = (
    "marts.mart_game_daily_metrics",
    "marts.mart_activity_hourly",
    "marts.mart_genre_daily_metrics",
    "marts.mart_publisher_daily_metrics",
)

# SQL сборки и проверок дублирует файлы sql/marts/*.sql (справочники для ручного
# просмотра и запуска). Каталог sql/ не смонтирован в контейнеры Airflow, поэтому
# для выполнения единственной канонической копией служит этот модуль.
# Витрины пересчитываются ЦЕЛИКОМ на каждом запуске (без окна), чтобы смена
# current dim_review пересчитывала историю и первый запуск заполнял прошлые даты.
SQL_BUILD_GAME_DAILY = """
INSERT INTO marts.mart_game_daily_metrics (
    activity_date, game_sk, appid, game_name, is_free, release_date,
    review_count, positive_reviews, negative_reviews, positive_ratio,
    avg_weighted_score, sum_votes_up, sum_votes_funny, avg_playtime_at_review,
    avg_players, max_players, current_players, samples, hours_covered, loaded_at
)
WITH review_agg AS (
    SELECT
        (r.timestamp_created AT TIME ZONE 'UTC')::date AS activity_date,
        f.game_sk AS game_sk,
        count(*)::bigint AS review_count,
        count(*) FILTER (WHERE r.voted_up)::bigint AS positive_reviews,
        count(*) FILTER (WHERE NOT r.voted_up)::bigint AS negative_reviews,
        avg(r.weighted_score)::numeric(6, 5) AS avg_weighted_score,
        sum(r.votes_up)::bigint AS sum_votes_up,
        sum(r.votes_funny)::bigint AS sum_votes_funny,
        avg(r.author_playtime_at_review)::numeric(14, 2) AS avg_playtime_at_review
    FROM dds.fact_review f
    JOIN dds.dim_review r
      ON r.review_sk = f.review_sk
     AND r.is_current
    WHERE r.timestamp_created IS NOT NULL
    GROUP BY 1, 2
),
online_agg AS (
    SELECT
        a.activity_date AS activity_date,
        a.game_sk AS game_sk,
        round(sum(a.avg_players * a.samples) / nullif(sum(a.samples), 0), 2)::numeric(12, 2)
            AS avg_players,
        max(a.max_players)::bigint AS max_players,
        (array_agg(a.current_players ORDER BY a.hour_of_day DESC))[1]::bigint
            AS current_players,
        sum(a.samples)::bigint AS samples,
        count(DISTINCT a.hour_of_day)::smallint AS hours_covered
    FROM dds.fact_player_activity a
    GROUP BY 1, 2
)
SELECT
    COALESCE(rv.activity_date, onl.activity_date) AS activity_date,
    COALESCE(rv.game_sk, onl.game_sk) AS game_sk,
    g.appid, g.name, g.is_free, g.release_date,
    COALESCE(rv.review_count, 0),
    COALESCE(rv.positive_reviews, 0),
    COALESCE(rv.negative_reviews, 0),
    CASE
        WHEN COALESCE(rv.review_count, 0) > 0
        THEN (rv.positive_reviews::numeric / rv.review_count)::numeric(6, 5)
        ELSE NULL
    END,
    rv.avg_weighted_score,
    COALESCE(rv.sum_votes_up, 0),
    COALESCE(rv.sum_votes_funny, 0),
    rv.avg_playtime_at_review,
    onl.avg_players,
    onl.max_players,
    onl.current_players,
    COALESCE(onl.samples, 0),
    COALESCE(onl.hours_covered, 0),
    now()
FROM review_agg rv
FULL OUTER JOIN online_agg onl
    ON rv.activity_date = onl.activity_date
   AND rv.game_sk = onl.game_sk
JOIN dds.dim_game g
    ON g.game_sk = COALESCE(rv.game_sk, onl.game_sk);
"""

SQL_BUILD_ACTIVITY_HOURLY = """
INSERT INTO marts.mart_activity_hourly (
    activity_date, hour_of_day, game_sk, appid, game_name, is_free,
    current_players, avg_players, max_players, samples, loaded_at
)
SELECT
    a.activity_date, a.hour_of_day, a.game_sk, g.appid, g.name, g.is_free,
    a.current_players, a.avg_players, a.max_players, a.samples, now()
FROM dds.fact_player_activity a
JOIN dds.dim_game g
    ON g.game_sk = a.game_sk;
"""

SQL_BUILD_GENRE_DAILY = """
INSERT INTO marts.mart_genre_daily_metrics (
    activity_date, genre_id, genre_name, games_count, review_count,
    positive_reviews, positive_ratio, avg_players, max_players, samples, loaded_at
)
SELECT
    m.activity_date,
    dg.genre_id,
    dg.genre_name,
    count(DISTINCT m.appid)::bigint AS games_count,
    sum(m.review_count)::bigint AS review_count,
    sum(m.positive_reviews)::bigint AS positive_reviews,
    CASE
        WHEN sum(m.review_count) > 0
        THEN (sum(m.positive_reviews)::numeric / sum(m.review_count))::numeric(6, 5)
        ELSE NULL
    END AS positive_ratio,
    round(sum(m.avg_players * m.samples) / nullif(sum(m.samples), 0), 2)::numeric(12, 2)
        AS avg_players,
    max(m.max_players)::bigint AS max_players,
    sum(m.samples)::bigint AS samples,
    now()
FROM marts.mart_game_daily_metrics m
JOIN dds.bridge_game_genre b
    ON b.game_sk = m.game_sk
JOIN dds.dim_genre dg
    ON dg.genre_id = b.genre_id
GROUP BY m.activity_date, dg.genre_id, dg.genre_name;
"""

SQL_BUILD_PUBLISHER_DAILY = """
INSERT INTO marts.mart_publisher_daily_metrics (
    activity_date, publisher_id, publisher_name, games_count, review_count,
    positive_reviews, positive_ratio, avg_players, max_players, samples, loaded_at
)
SELECT
    m.activity_date,
    dp.publisher_id,
    dp.publisher_name,
    count(DISTINCT m.appid)::bigint AS games_count,
    sum(m.review_count)::bigint AS review_count,
    sum(m.positive_reviews)::bigint AS positive_reviews,
    CASE
        WHEN sum(m.review_count) > 0
        THEN (sum(m.positive_reviews)::numeric / sum(m.review_count))::numeric(6, 5)
        ELSE NULL
    END AS positive_ratio,
    round(sum(m.avg_players * m.samples) / nullif(sum(m.samples), 0), 2)::numeric(12, 2)
        AS avg_players,
    max(m.max_players)::bigint AS max_players,
    sum(m.samples)::bigint AS samples,
    now()
FROM marts.mart_game_daily_metrics m
JOIN dds.bridge_game_publisher b
    ON b.game_sk = m.game_sk
JOIN dds.dim_publisher dp
    ON dp.publisher_id = b.publisher_id
GROUP BY m.activity_date, dp.publisher_id, dp.publisher_name;
"""

# Аудит DQ. Успешные проверки пишутся в транзакции публикации, неудачные — отдельно.
AUDIT_INSERT = """
INSERT INTO marts.mart_dq_results (
    check_name, run_id, check_type, status, severity,
    metric_value, threshold, details
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb);
"""

# SQL проверок качества. Дублируют sql/marts/dq_marts.sql.
# Проверки 1-7 идут по всей витрине, свежесть (8) — по закрытым суткам.
DQ_UNIQUENESS = """
SELECT
    (SELECT count(*) FROM (
        SELECT activity_date, game_sk FROM marts.mart_game_daily_metrics
        GROUP BY 1, 2 HAVING count(*) > 1) d)
  + (SELECT count(*) FROM (
        SELECT activity_date, hour_of_day, game_sk FROM marts.mart_activity_hourly
        GROUP BY 1, 2, 3 HAVING count(*) > 1) d)
  + (SELECT count(*) FROM (
        SELECT activity_date, genre_id FROM marts.mart_genre_daily_metrics
        GROUP BY 1, 2 HAVING count(*) > 1) d)
  + (SELECT count(*) FROM (
        SELECT activity_date, publisher_id FROM marts.mart_publisher_daily_metrics
        GROUP BY 1, 2 HAVING count(*) > 1) d) AS duplicate_keys;
"""

DQ_RATIO = """
SELECT
    (SELECT count(*) FROM marts.mart_game_daily_metrics
     WHERE positive_ratio < 0 OR positive_ratio > 1
        OR (review_count = 0 AND positive_ratio IS NOT NULL)
        OR (review_count > 0 AND positive_ratio IS NULL))
  + (SELECT count(*) FROM marts.mart_genre_daily_metrics
     WHERE positive_ratio < 0 OR positive_ratio > 1
        OR (review_count = 0 AND positive_ratio IS NOT NULL)
        OR (review_count > 0 AND positive_ratio IS NULL))
  + (SELECT count(*) FROM marts.mart_publisher_daily_metrics
     WHERE positive_ratio < 0 OR positive_ratio > 1
        OR (review_count = 0 AND positive_ratio IS NOT NULL)
        OR (review_count > 0 AND positive_ratio IS NULL)) AS bad_ratio_rows;
"""

DQ_REVIEW_SOURCE = """
SELECT
    COALESCE((SELECT sum(review_count) FROM marts.mart_game_daily_metrics), 0)::bigint AS mart_reviews,
    (SELECT count(*) FROM dds.fact_review f
     JOIN dds.dim_review r ON r.review_sk = f.review_sk
     WHERE r.is_current
       AND r.timestamp_created IS NOT NULL) AS source_reviews,
    (SELECT count(*) FROM dds.dim_review r
     LEFT JOIN dds.fact_review f ON f.review_sk = r.review_sk
     WHERE r.is_current AND r.timestamp_created IS NOT NULL
       AND f.review_sk IS NULL) AS missing_facts;
"""

DQ_REVIEW_ARITHMETIC = """
SELECT
    (SELECT count(*) FROM marts.mart_game_daily_metrics
     WHERE positive_reviews + negative_reviews <> review_count) AS game_count_bad,
    (SELECT count(*) FROM marts.mart_game_daily_metrics
     WHERE review_count > 0
       AND abs(positive_ratio - positive_reviews::numeric / review_count) > 0.00001) AS game_ratio_bad,
    (SELECT count(*) FROM marts.mart_genre_daily_metrics
     WHERE positive_reviews > review_count
        OR (review_count > 0
            AND abs(positive_ratio - positive_reviews::numeric / review_count) > 0.00001)) AS genre_bad,
    (SELECT count(*) FROM marts.mart_publisher_daily_metrics
     WHERE positive_reviews > review_count
        OR (review_count > 0
            AND abs(positive_ratio - positive_reviews::numeric / review_count) > 0.00001)) AS publisher_bad;
"""

DQ_AVG_MAX = """
SELECT
    (SELECT count(*) FROM marts.mart_game_daily_metrics
     WHERE avg_players < 0 OR max_players < 0 OR current_players < 0
        OR avg_players > max_players)
  + (SELECT count(*) FROM marts.mart_activity_hourly
     WHERE current_players < 0 OR avg_players < 0 OR max_players < 0
        OR avg_players > max_players)
  + (SELECT count(*) FROM marts.mart_genre_daily_metrics
     WHERE avg_players < 0 OR max_players < 0 OR avg_players > max_players)
  + (SELECT count(*) FROM marts.mart_publisher_daily_metrics
     WHERE avg_players < 0 OR max_players < 0 OR avg_players > max_players) AS bad_avg_rows;
"""

DQ_ACTIVITY_COMPLETENESS = """
SELECT
    (SELECT count(*) FROM dds.fact_player_activity) AS source_points,
    (SELECT count(*) FROM marts.mart_activity_hourly) AS mart_points,
    (SELECT COALESCE(sum(samples), 0)::bigint FROM dds.fact_player_activity) AS source_samples,
    (SELECT COALESCE(sum(samples), 0)::bigint FROM marts.mart_game_daily_metrics) AS daily_samples,
    (SELECT count(*) FROM marts.mart_game_daily_metrics
     WHERE hours_covered < 0 OR hours_covered > 24
        OR (samples = 0 AND (avg_players IS NOT NULL OR max_players IS NOT NULL))
        OR (samples > 0 AND hours_covered = 0)) AS hours_bad;
"""

DQ_ORPHANS = """
SELECT
    (SELECT count(*) FROM marts.mart_game_daily_metrics m
     LEFT JOIN dds.dim_game g ON g.game_sk = m.game_sk
     WHERE g.game_sk IS NULL)
  + (SELECT count(*) FROM marts.mart_activity_hourly m
     LEFT JOIN dds.dim_game g ON g.game_sk = m.game_sk
     WHERE g.game_sk IS NULL)
  + (SELECT count(*) FROM marts.mart_genre_daily_metrics m
     LEFT JOIN dds.dim_genre d ON d.genre_id = m.genre_id
     WHERE d.genre_id IS NULL)
  + (SELECT count(*) FROM marts.mart_publisher_daily_metrics m
     LEFT JOIN dds.dim_publisher d ON d.publisher_id = m.publisher_id
     WHERE d.publisher_id IS NULL) AS orphan_rows;
"""

DQ_FRESHNESS = """
SELECT
    (SELECT count(*) FROM marts.mart_game_daily_metrics WHERE activity_date = %(day)s) AS game_rows,
    (SELECT count(*) FROM marts.mart_activity_hourly WHERE activity_date = %(day)s) AS activity_rows,
    (SELECT count(*) FROM marts.mart_genre_daily_metrics WHERE activity_date = %(day)s) AS genre_rows,
    (SELECT count(*) FROM marts.mart_publisher_daily_metrics WHERE activity_date = %(day)s) AS publisher_rows;
"""


def closed_date(context) -> date:
    # Закрытые сутки — последняя дата в UTC до конца интервала планировщика.
    end = context.get("data_interval_end") or datetime.now(timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    else:
        end = end.astimezone(timezone.utc)
    return (end - timedelta(days=1)).date()


def processed_dir(context) -> str:
    path = os.path.join(BASE_MART_DIR, context["ds"])
    os.makedirs(path, exist_ok=True)
    return path


def get_connection():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()


def check_prerequisites(**context) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        missing = []
        for table in REQUIRED_DDS_TABLES + MART_TABLES:
            cursor.execute("SELECT to_regclass(%s);", (table,))
            if cursor.fetchone()[0] is None:
                missing.append(table)
        if missing:
            raise AirflowException(
                f"Required tables not found: {', '.join(missing)}. "
                "Apply sql/postgres/init_dds.sql and sql/marts/init_marts.sql first "
                "(init_marts.sql — через stdin, каталог sql/marts не смонтирован в postgres)."
            )

        cursor.execute("SELECT count(*) FROM dds.dim_game WHERE is_current;")
        current_games = cursor.fetchone()[0]
        if current_games == 0:
            raise AirflowException("dds.dim_game has no current rows.")
    finally:
        cursor.close()
        conn.close()

    logger.info(f"Mart prerequisites OK: current_games={current_games}.")


def acquire_publish_locks(cursor) -> None:
    # Сначала LOCK TABLE, до создания снимка REPEATABLE READ, затем advisory lock.
    cursor.execute(
        "LOCK TABLE " + ", ".join(MART_TABLES) + " IN ACCESS EXCLUSIVE MODE;"
    )
    cursor.execute("SELECT pg_advisory_xact_lock(%s);", (PUBLISH_LOCK_KEY,))


def _result(check_name, check_type, metric_value, threshold, status=None,
            severity="high", details=None) -> dict:
    if status is None:
        status = "passed" if metric_value is not None and metric_value <= threshold else "error"
    if status == "passed":
        severity = "info"
    elif status == "warning":
        severity = "medium"
    return {
        "check_name": check_name,
        "check_type": check_type,
        "status": status,
        "severity": severity,
        "metric_value": metric_value,
        "threshold": threshold,
        "details": details or {},
    }


def run_dq_checks(cursor, closed: date) -> list[dict]:
    # Проверки выполняются в текущей транзакции, до commit публикации.
    # 1-7 — по всей витрине (полный пересчёт); 8 — свежесть за закрытые сутки.
    results = []

    cursor.execute(DQ_UNIQUENESS)
    duplicates = cursor.fetchone()[0]
    results.append(_result("uniqueness", "uniqueness", duplicates, 0))

    cursor.execute(DQ_RATIO)
    bad_ratio = cursor.fetchone()[0]
    results.append(_result("ratio", "consistency", bad_ratio, 0))

    cursor.execute(DQ_REVIEW_SOURCE)
    mart_reviews, source_reviews, missing_facts = cursor.fetchone()
    results.append(_result(
        "review_count_source", "volume", mart_reviews, source_reviews,
        status="passed" if mart_reviews == source_reviews else "error",
        details={"mart_reviews": mart_reviews, "source_reviews": source_reviews},
    ))
    results.append(_result("current_reviews_without_fact", "completeness", missing_facts, 0))

    cursor.execute(DQ_REVIEW_ARITHMETIC)
    game_count_bad, game_ratio_bad, genre_bad, publisher_bad = cursor.fetchone()
    arith_bad = game_count_bad + game_ratio_bad + genre_bad + publisher_bad
    results.append(_result(
        "review_arithmetic", "consistency", arith_bad, 0,
        details={
            "game_count_bad": game_count_bad, "game_ratio_bad": game_ratio_bad,
            "genre_bad": genre_bad, "publisher_bad": publisher_bad,
        },
    ))

    cursor.execute(DQ_AVG_MAX)
    bad_avg = cursor.fetchone()[0]
    results.append(_result("avg_le_max", "consistency", bad_avg, 0))

    cursor.execute(DQ_ACTIVITY_COMPLETENESS)
    source_points, mart_points, source_samples, daily_samples, hours_bad = cursor.fetchone()
    activity_ok = (mart_points == source_points
                   and daily_samples == source_samples
                   and hours_bad == 0)
    results.append(_result(
        "activity_completeness", "volume", None, 1,
        status="passed" if activity_ok else "error",
        details={
            "source_points": source_points, "mart_points": mart_points,
            "source_samples": source_samples, "daily_samples": daily_samples,
            "hours_bad": hours_bad,
        },
    ))

    cursor.execute(DQ_ORPHANS)
    orphans = cursor.fetchone()[0]
    results.append(_result("orphans", "referential", orphans, 0))

    cursor.execute(DQ_FRESHNESS, {"day": closed})
    counts = dict(zip(MART_TABLES, cursor.fetchone()))
    for table, count in counts.items():
        results.append(_result(
            f"freshness_{table.rsplit('.', 1)[-1]}", "freshness", count, 1,
            status="passed" if count > 0 else "error",
            details={"closed_date": str(closed), "table": table},
        ))
    return results


def _write_dq_rows(cursor, results: list[dict], run_id: str) -> None:
    if not results:
        return
    cursor.executemany(
        AUDIT_INSERT,
        [
            (
                r["check_name"], run_id, r["check_type"], r["status"], r["severity"],
                r["metric_value"], r["threshold"],
                json.dumps(r["details"], ensure_ascii=False, default=str),
            )
            for r in results
        ],
    )


def audit_failed_dq(results: list[dict], run_id: str) -> None:
    # Аудит неудачных проверок — отдельным соединением ПОСЛЕ rollback публикации.
    # Ошибку аудита логируем, но исходную ошибку сборки не проглатываем.
    if not results:
        return
    try:
        conn = get_connection()
        cursor = conn.cursor()
        try:
            _write_dq_rows(cursor, results, run_id)
            conn.commit()
        finally:
            cursor.close()
            conn.close()
    except Exception as exc:
        logger.error(f"Failed to persist failed mart DQ audit: {exc}")


def rebuild_marts(**context) -> dict:
    closed = closed_date(context)
    run_id = context["run_id"]

    conn = get_connection()
    # REPEATABLE READ: стабильный снимок для чтения, DQ видят ровно те данные,
    # что записаны в этой же транзакции публикации.
    conn.set_session(isolation_level="REPEATABLE READ")
    counters = {}
    results = []
    error = None
    cursor = conn.cursor()
    try:
        acquire_publish_locks(cursor)

        # Полный идемпотентный пересчёт: витрины очищаются целиком и собираются
        # заново из DDS в одной транзакции (атомарная публикация).
        for table in MART_TABLES:
            cursor.execute(f"DELETE FROM {table};")

        cursor.execute(SQL_BUILD_GAME_DAILY)
        counters["mart_game_daily_metrics"] = cursor.rowcount
        cursor.execute(SQL_BUILD_ACTIVITY_HOURLY)
        counters["mart_activity_hourly"] = cursor.rowcount
        cursor.execute(SQL_BUILD_GENRE_DAILY)
        counters["mart_genre_daily_metrics"] = cursor.rowcount
        cursor.execute(SQL_BUILD_PUBLISHER_DAILY)
        counters["mart_publisher_daily_metrics"] = cursor.rowcount

        # Проверки качества — до commit, на данных текущей транзакции.
        results = run_dq_checks(cursor, closed)
        failed = [r for r in results if r["status"] == "error"]
        if failed:
            raise AirflowFailException(
                "Mart DQ failed: " + ", ".join(r["check_name"] for r in failed)
            )

        # Аудит успешных проверок — в той же транзакции, что и публикация.
        _write_dq_rows(cursor, results, run_id)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        if error is None:
            error = exc
    finally:
        cursor.close()
        conn.close()

    if error is not None:
        # Публикация отменена: аудит неудачных проверок пишем отдельно.
        if not results:
            results = [_result("build_failed", "execution", None, 0, status="error")]
        audit_failed_dq(results, run_id)

    summary = {
        "ds": context["ds"],
        "run_id": run_id,
        "full_rebuild": True,
        "closed_date": str(closed),
        "published": error is None,
        "counters": counters,
        "dq_status_counts": {
            status: sum(1 for r in results if r["status"] == status)
            for status in ("passed", "warning", "error")
        },
        "dq_results": results,
    }
    out_file = os.path.join(processed_dir(context), "results.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    if error is not None:
        raise error

    logger.info(
        f"Marts fully rebuilt: {counters}, dq={summary['dq_status_counts']}."
    )
    return summary


def log_mart_run(**context) -> str:
    ti = context["ti"]
    summary = ti.xcom_pull(task_ids="rebuild_marts") or {}
    out_file = os.path.join(processed_dir(context), "run_summary.json")
    payload = {
        "ds": context["ds"],
        "run_id": context["run_id"],
        "full_rebuild": summary.get("full_rebuild"),
        "closed_date": summary.get("closed_date"),
        "counters": summary.get("counters"),
        "dq_status_counts": summary.get("dq_status_counts"),
        "published": summary.get("published"),
    }
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info(f"Mart run summary: {json.dumps(payload, ensure_ascii=False)}")
    return out_file


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="build_marts_dag",
    default_args=default_args,
    description="Сборка и публикация витрин ЛР3 из фактов и измерений DDS",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["steam", "marts", "lab3", "postgres"],
    doc_md="""
### Витрины (ЛР3)

Строит и атомарно публикует витрины в схеме `marts`:
- `marts.mart_game_daily_metrics` — зерно `(activity_date, game_sk)`, независимые
  факты отзывов и онлайна агрегируются порознь и соединяются `FULL OUTER JOIN`
  (`review_count`, `positive_reviews`, `positive_ratio`, `avg_players`, `max_players`, `samples`);
- `marts.mart_activity_hourly` — зерно `(activity_date, hour_of_day, game_sk)`
  (`avg_players`, `max_players`, `samples`);
- `marts.mart_genre_daily_metrics` и `marts.mart_publisher_daily_metrics` — категории
  по одному мосту, `games_count` как точный union множеств по `appid`, факты не перемножаются.

**Зависимость:** ждёт успешного `dds_facts_dag` за тот же logical date (без сдвига).

**Полный пересчёт:** каждый запуск очищает витрины целиком и собирает их заново из DDS
в одной транзакции (`REPEATABLE READ`) под advisory-блокировкой и `LOCK TABLE` в
фиксированном порядке. Проверки 1-7 идут по всей витрине, при ошибке публикация
откатывается.

**Аудит DQ:** успешные проверки пишутся в `marts.mart_dq_results` в той же транзакции;
неудачные — отдельным соединением после rollback, без проглатывания исходной ошибки.

**Свежесть:** каждая витрина должна содержать строки за последние закрытые UTC-сутки
(`data_interval_end - 1 день`), иначе публикация откатывается.

**Ограничение:** другие издатели должны брать блокировки таблиц в том же порядке,
а затем тот же advisory-ключ. `LOCK TABLE ... ACCESS EXCLUSIVE` блокирует читателей
на время пересборки.

**Результаты:** `marts.mart_dq_results` и JSON `data/processed/marts/<ds>/results.json`.
""",
) as dag:

    wait_for_dds_facts = ExternalTaskSensor(
        task_id="wait_for_dds_facts",
        external_dag_id="dds_facts_dag",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=timedelta(hours=6),
        check_existence=True,
    )

    check_prerequisites_task = PythonOperator(
        task_id="check_prerequisites",
        python_callable=check_prerequisites,
    )

    rebuild_marts_task = PythonOperator(
        task_id="rebuild_marts",
        python_callable=rebuild_marts,
    )

    log_mart_run_task = PythonOperator(
        task_id="log_mart_run",
        python_callable=log_mart_run,
    )

    wait_for_dds_facts >> check_prerequisites_task
    check_prerequisites_task >> rebuild_marts_task
    rebuild_marts_task >> log_mart_run_task
