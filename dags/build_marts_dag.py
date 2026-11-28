import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import Param
from airflow.sdk.exceptions import AirflowException, AirflowFailException

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_MART_DIR = "/opt/airflow/data/processed/marts"
POSTGRES_CONN_ID = "steam_postgres"

# Порог давности источников активности/отзывов в днях. Возраст считается
# от текущей UTC-даты; превышение даёт warning, но не блокирует публикацию.
DEFAULT_SOURCE_MAX_AGE_DAYS = 2

# Единственный источник SQL — каталог sql/marts, смонтированный в контейнеры
# Airflow. Копий запросов в Python нет, поэтому DAG и .sql не расходятся.
SQL_DIR = "/opt/airflow/sql/marts"

# Единый ключ advisory-блокировки. Все авторизованные издатели витрин обязаны
# брать ИМЕННО этот ключ, иначе протокол публикации не соблюдается.
PUBLISH_LOCK_KEY = 48234001

# Порядок важен: жанры и издатели читают mart_game_daily_metrics.
BUILD_SQL_FILES = {
    "marts.mart_game_daily_metrics": "build_mart_game_daily.sql",
    "marts.mart_activity_hourly": "build_mart_activity_hourly.sql",
    "marts.mart_genre_daily_metrics": "build_mart_genre_daily.sql",
    "marts.mart_publisher_daily_metrics": "build_mart_publisher_daily.sql",
}
MART_TABLES = tuple(BUILD_SQL_FILES)

# Единая таблица аудита DQ. Не удаляется при пересборке и не входит
# в LOCK TABLE витрин: сюда пишутся и успешные, и неудачные проверки.
AUDIT_TABLE = "marts.mart_dq_results"

DQ_SQL_FILE = "dq_marts.sql"
DQ_FRESHNESS_SQL_FILE = "dq_freshness.sql"
DQ_AUDIT_INSERT_FILE = "insert_dq_results.sql"

SQL_FILES = tuple(BUILD_SQL_FILES.values()) + (
    DQ_SQL_FILE,
    DQ_FRESHNESS_SQL_FILE,
    DQ_AUDIT_INSERT_FILE,
)

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

# Колонки, добавленные/участвующие после слияния веток. На старом томе
# CREATE TABLE IF NOT EXISTS их не создаёт, поэтому проверяем схему до сборки.
REQUIRED_MART_COLUMNS = {
    "marts.mart_game_daily_metrics": ("avg_weighted_score",),
    "marts.mart_activity_hourly": ("day_of_week", "day_name", "is_weekend"),
    "marts.mart_genre_daily_metrics": ("avg_weighted_score",),
}

# Сопоставление витрины и префикса колонок в dq_freshness.sql.
FRESHNESS_CHECKS = (
    ("mart_game_daily_metrics", "game"),
    ("mart_activity_hourly", "activity"),
    ("mart_genre_daily_metrics", "genre"),
    ("mart_publisher_daily_metrics", "publisher"),
)


def sql_path(filename: str) -> str:
    return os.path.join(SQL_DIR, filename)


def load_sql(filename: str) -> str:
    # Читаем в момент выполнения таска: при отсутствии монтирования
    # пользователь получает понятную ошибку, а не падение парсинга DAG.
    path = sql_path(filename)
    try:
        with open(path, "r", encoding="utf-8") as f:
            sql = f.read().strip()
    except OSError as exc:
        raise AirflowException(
            f"Cannot read mart SQL file {path}: {exc}. "
            "Mount ./sql into the Airflow containers at /opt/airflow/sql (read-only)."
        ) from exc
    if not sql:
        raise AirflowException(f"Mart SQL file is empty: {path}.")
    return sql


def run_ds(context) -> str:
    # ds — дата logical date. У ручного запуска без logical date она может
    # отсутствовать; фиксированный каталог не ломает запись результатов.
    return context.get("ds") or "manual"


def get_source_max_age_days(context) -> int:
    # Порог настраивается Param в UI/CLI; при отсутствии берём безопасный
    # дефолт. Значение не влияет на успешность публикации, только на warning.
    raw = (context.get("params") or {}).get(
        "source_max_age_days", DEFAULT_SOURCE_MAX_AGE_DAYS
    )
    try:
        value = int(getattr(raw, "value", raw))
    except (TypeError, ValueError):
        value = DEFAULT_SOURCE_MAX_AGE_DAYS
    return max(0, value)


def processed_dir(context) -> str:
    path = os.path.join(BASE_MART_DIR, run_ds(context))
    os.makedirs(path, exist_ok=True)
    return path


def get_connection():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()


def check_prerequisites(**context) -> None:
    missing_files = [
        name for name in SQL_FILES if not os.path.isfile(sql_path(name))
    ]
    if missing_files:
        raise AirflowException(
            f"Mart SQL files are missing in {SQL_DIR}: {', '.join(missing_files)}. "
            "Mount ./sql into the Airflow containers at /opt/airflow/sql (read-only)."
        )

    conn = get_connection()
    cursor = conn.cursor()
    try:
        missing_tables = []
        for table in REQUIRED_DDS_TABLES + MART_TABLES + (AUDIT_TABLE,):
            cursor.execute("SELECT to_regclass(%s);", (table,))
            if cursor.fetchone()[0] is None:
                missing_tables.append(table)
        if missing_tables:
            raise AirflowException(
                f"Required tables not found: {', '.join(missing_tables)}. "
                "Apply sql/postgres/init_dds.sql and sql/marts/init_marts.sql first."
            )

        missing_columns = []
        for table, columns in REQUIRED_MART_COLUMNS.items():
            schema, name = table.split(".", 1)
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema::text = %s
                  AND table_name::text = %s
                  AND column_name::text = ANY(%s::text[]);
                """,
                (schema, name, list(columns)),
            )
            found = {row[0] for row in cursor.fetchall()}
            missing_columns.extend(
                f"{table}.{column}" for column in columns if column not in found
            )
        if missing_columns:
            raise AirflowException(
                "Marts schema is outdated, missing columns: "
                + ", ".join(missing_columns)
                + ". Re-apply sql/marts/init_marts.sql on a fresh volume or "
                  "DROP SCHEMA marts CASCADE before rerunning the DAG."
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
    # ВАЖНО: сначала LOCK TABLE, затем advisory. SELECT pg_advisory_xact_lock
    # сам является запросом и может зафиксировать snapshot REPEATABLE READ;
    # если взять advisory раньше табличных блокировок, снимок может быть
    # зафиксирован до ожидания, и после него публикация увидит устаревшие
    # данные. Здесь первый запрос к данным (DELETE) выполняется уже после
    # обоих локов, поэтому снимок согласован с порядком блокировок.
    # ACCESS EXCLUSIVE удерживается до commit/rollback, читатели Superset
    # ждут завершения публикации и не видят частично пересобранных данных.
    # Advisory-ключ сериализует только издателей, соблюдающих протокол.
    cursor.execute(
        "LOCK TABLE " + ", ".join(MART_TABLES) + " IN ACCESS EXCLUSIVE MODE;"
    )
    cursor.execute("SELECT pg_advisory_xact_lock(%s);", (PUBLISH_LOCK_KEY,))


def _fetch_one_dict(cursor) -> dict:
    row = cursor.fetchone()
    if row is None:
        raise AirflowException("DQ query returned no rows.")
    columns = [description[0] for description in cursor.description]
    return dict(zip(columns, row))


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


def _freshness_result(table: str, mart_max, source_max) -> dict:
    # Свежесть по источникам, без привязки к дате запуска: пустой источник
    # и пустая витрина — warning, расхождение дат — error.
    if mart_max is None and source_max is None:
        status = "warning"
        reason = "no source rows and no mart rows"
    elif mart_max == source_max:
        status = "passed"
        reason = None
    else:
        status = "error"
        reason = "mart max(activity_date) does not match source"

    lag_days = None
    if mart_max is not None and source_max is not None:
        lag_days = (source_max - mart_max).days

    details = {
        "table": table,
        "mart_max": mart_max.isoformat() if mart_max else None,
        "source_max": source_max.isoformat() if source_max else None,
    }
    if reason:
        details["reason"] = reason

    return _result(
        f"freshness_{table.rsplit('.', 1)[-1]}", "freshness",
        lag_days, 0, status=status, details=details,
    )


def _source_age_result(check_name, source, source_max, today, max_age_days) -> dict:
    # Возраст источника считается от текущей UTC-даты, а не logical_date.
    # Это только warning: давность сбора не должна блокировать публикацию.
    if source_max is None:
        status = "warning"
        age_days = None
        reason = "no source rows"
    else:
        age_days = (today - source_max).days
        if age_days < 0:
            status = "warning"
            reason = "source date is in the future"
        elif age_days > max_age_days:
            status = "warning"
            reason = "source is older than the configured threshold"
        else:
            status = "passed"
            reason = None

    details = {
        "source": source,
        "source_max": source_max.isoformat() if source_max else None,
        "today_utc": today.isoformat(),
        "threshold_days": max_age_days,
    }
    if reason:
        details["reason"] = reason

    return _result(
        check_name, "freshness",
        age_days, max_age_days, status=status, details=details,
    )


def run_dq_checks(cursor, source_max_age_days: int) -> list[dict]:
    # Проверки выполняются в текущей транзакции, до commit публикации.
    # 1-7 — по всей витрине; freshness — сверка mart/source; age — warning
    # о давности источников активности и отзывов относительно UTC today.
    cursor.execute(load_sql(DQ_SQL_FILE))
    metrics = _fetch_one_dict(cursor)

    arithmetic_bad = (
        metrics["game_count_bad"] + metrics["game_ratio_bad"]
        + metrics["genre_bad"] + metrics["publisher_bad"]
    )
    activity_ok = (
        metrics["mart_points"] == metrics["source_points"]
        and metrics["daily_samples"] == metrics["source_samples"]
        and metrics["hours_bad"] == 0
    )

    results = [
        _result("uniqueness", "uniqueness", metrics["duplicate_keys"], 0),
        _result("ratio", "consistency", metrics["bad_ratio_rows"], 0),
        _result(
            "review_count_source", "coverage",
            metrics["mart_reviews"], metrics["source_reviews"],
            status=(
                "passed"
                if metrics["mart_reviews"] == metrics["source_reviews"]
                else "error"
            ),
            details={
                "mart_reviews": metrics["mart_reviews"],
                "source_reviews": metrics["source_reviews"],
                "source_filter": (
                    "dim_review.is_current AND timestamp_created IS NOT NULL"
                ),
            },
        ),
        _result(
            "current_reviews_without_fact", "completeness",
            metrics["current_reviews_without_fact"], 0,
            status=(
                "warning"
                if metrics["current_reviews_without_fact"]
                else "passed"
            ),
            details={
                "reason": (
                    "DDS applies reviews_limit and filters, so a current "
                    "review may have no fact row; warning does not block "
                    "publication"
                ),
            },
        ),
        _result(
            "reviews_without_timestamp", "coverage",
            metrics["reviews_without_timestamp"], 0,
            status=(
                "warning" if metrics["reviews_without_timestamp"] else "passed"
            ),
            details={
                "reason": (
                    "current reviews without timestamp_created are excluded "
                    "from the daily grain"
                ),
            },
        ),
        _result(
            "review_arithmetic", "consistency", arithmetic_bad, 0,
            details={
                "game_count_bad": metrics["game_count_bad"],
                "game_ratio_bad": metrics["game_ratio_bad"],
                "genre_bad": metrics["genre_bad"],
                "publisher_bad": metrics["publisher_bad"],
            },
        ),
        _result("avg_le_max", "consistency", metrics["bad_avg_rows"], 0),
        _result(
            "activity_completeness", "volume", None, 1,
            status="passed" if activity_ok else "error",
            details={
                "source_points": metrics["source_points"],
                "mart_points": metrics["mart_points"],
                "source_samples": metrics["source_samples"],
                "daily_samples": metrics["daily_samples"],
                "hours_bad": metrics["hours_bad"],
            },
        ),
        _result("orphans", "referential", metrics["orphan_rows"], 0),
    ]

    cursor.execute(load_sql(DQ_FRESHNESS_SQL_FILE))
    freshness = _fetch_one_dict(cursor)
    for table, prefix in FRESHNESS_CHECKS:
        results.append(
            _freshness_result(
                table,
                freshness[f"{prefix}_mart_max"],
                freshness[f"{prefix}_source_max"],
            )
        )

    # Возраст источников — отдельные warning по активности и отзывам.
    today = datetime.now(timezone.utc).date()
    results.append(
        _source_age_result(
            "freshness_activity_source_age", "activity",
            freshness["activity_source_max"], today, source_max_age_days,
        )
    )
    results.append(
        _source_age_result(
            "freshness_reviews_source_age", "reviews",
            freshness["review_source_max"], today, source_max_age_days,
        )
    )
    return results


def _write_dq_rows(cursor, results: list[dict], run_id: str) -> None:
    if not results:
        return
    cursor.executemany(
        load_sql(DQ_AUDIT_INSERT_FILE),
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
    # Аудит неудачных проверок — отдельным соединением ПОСЛЕ rollback
    # публикации. Ошибку аудита логируем, но исходную ошибку сборки
    # не проглатываем.
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
    run_id = context["run_id"]
    source_max_age_days = get_source_max_age_days(context)

    conn = get_connection()
    # REPEATABLE READ: стабильный снимок DDS на всю публикацию. DQ видят
    # ровно те строки, которые записаны этой же транзакцией, а не изменения
    # параллельных ETL. DELETE + INSERT + DQ + audit входят в одну
    # транзакцию: commit публикует всё сразу, rollback не оставляет
    # полупустых витрин и не пишет успешный аудит.
    counters = {}
    results = []
    error = None
    cursor = conn.cursor()
    try:
        # Works for both psycopg2 and psycopg3 and must be the first
        # statement in the transaction.
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
        acquire_publish_locks(cursor)

        # Полный идемпотентный пересчёт: витрины очищаются целиком и
        # собираются заново из DDS в одной транзакции. ACCESS EXCLUSIVE
        # из acquire_publish_locks удерживается до конца транзакции, поэтому
        # DELETE не виден читателям отдельно от последующего INSERT.
        for table in MART_TABLES:
            cursor.execute(f"DELETE FROM {table};")

        # SQL читается из sql/marts: DAG не содержит копий запросов.
        for table, filename in BUILD_SQL_FILES.items():
            cursor.execute(load_sql(filename))
            counters[table.rsplit(".", 1)[-1]] = cursor.rowcount

        # Проверки качества — до commit, на данных текущей транзакции.
        results = run_dq_checks(cursor, source_max_age_days)
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
        error = exc
    finally:
        cursor.close()
        conn.close()

    if error is not None:
        # Публикация отменена. build_failed добавляется ВСЕГДА, даже если
        # часть проверок прошла: результаты откатанного запуска не должны
        # выглядеть как успешная публикация. Аудит идёт отдельной
        # транзакцией, чтобы пережить rollback публикации.
        failure = _result(
            "build_failed", "execution", None, 0, status="error",
            details={
                "published": False,
                "reason": "publication rolled back",
                "error_type": type(error).__name__,
            },
        )
        results = [
            r for r in results if r["check_name"] != "build_failed"
        ] + [failure]
        # Ни одна строка откатанного запуска не должна выглядеть опубликованной.
        for row in results:
            details = dict(row.get("details") or {})
            details["published"] = False
            row["details"] = details
        audit_failed_dq(results, run_id)

    summary = {
        "ds": run_ds(context),
        "run_id": run_id,
        "full_rebuild": True,
        "published": error is None,
        "source_max_age_days": source_max_age_days,
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
        "ds": run_ds(context),
        "run_id": context["run_id"],
        "full_rebuild": summary.get("full_rebuild"),
        "counters": summary.get("counters"),
        "dq_status_counts": summary.get("dq_status_counts"),
        "published": summary.get("published"),
        "source_max_age_days": summary.get("source_max_age_days"),
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
    params={
        "source_max_age_days": Param(
            DEFAULT_SOURCE_MAX_AGE_DAYS,
            type="integer",
            minimum=0,
            maximum=30,
            description=(
                "Порог давности источников активности/отзывов в днях до warning"
            ),
        ),
    },
    doc_md="""
### Витрины (ЛР3)

Строит и атомарно публикует витрины в схеме `marts`:
- `marts.mart_game_daily_metrics` — зерно `(activity_date, game_sk)`, независимые
  факты отзывов и онлайна агрегируются порознь и соединяются `FULL OUTER JOIN`;
- `marts.mart_activity_hourly` — зерно `(activity_date, hour_of_day, game_sk)`,
  содержит `day_of_week` (ISODOW), `day_name` и `is_weekend`; `genre_name`
  намеренно не добавлен, чтобы не размножить зерно: срез по жанрам делается
  через `dds.bridge_game_genre` + `dds.dim_genre` (или BI-представление)
  с явной агрегацией по `appid`/`game_sk`;
- `marts.mart_genre_daily_metrics` и `marts.mart_publisher_daily_metrics` —
  категории по одному мосту, `games_count` как точный union множеств по `appid`;
  у жанров есть `avg_weighted_score`, посчитанный по исходным отзывам без fanout.

**Единственный источник SQL:** файлы `sql/marts/*.sql`; DAG читает их из
`/opt/airflow/sql/marts`. Compose должен смонтировать `./sql` в
`/opt/airflow/sql` (read-only). Копий запросов в Python нет.

**Зависимость:** ждёт успешного `dds_facts_dag` за тот же logical date.

**Транзакции и блокировки:** каждый запуск выполняет `DELETE` и `INSERT` всех
витрин, DQ и запись успешного аудита в одной транзакции `REPEATABLE READ`.
Сначала берутся `LOCK TABLE ... ACCESS EXCLUSIVE` в фиксированном порядке,
затем общий advisory-ключ: `SELECT pg_advisory_xact_lock` сам является
запросом и при обратном порядке мог бы зафиксировать snapshot до табличных
блокировок. Читатели Superset ждут commit/rollback и не видят частично
пересобранных данных; advisory-ключ сериализует только издателей,
соблюдающих протокол.

**Аудит DQ:** все проверки пишутся в единую `marts.mart_dq_results`; успешные —
в транзакции публикации, неудачные — отдельным соединением после rollback.
В аудит отказа ВСЕГДА добавляется `build_failed` с `published=false`, даже если
часть проверок прошла: passed-результаты откатанного запуска не означают
успешную публикацию.

**Свежесть:** не зависит от logical_date. `max(activity_date)` каждой витрины
сверяется с максимумом её источника; пустой источник и пустая витрина — warning,
расхождение дат — error. Дополнительно два warning о возрасте источников
активности и отзывов относительно текущей UTC-даты; порог настраивается
Param `source_max_age_days` (по умолчанию 2 дня), это не error.
Coverage отзывов использует `timestamp_created IS NOT NULL`; проверка
`current_reviews_without_fact` — warning, потому что DDS применяет
`reviews_limit` и фильтры.

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
