import hashlib
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk.exceptions import AirflowException, AirflowSkipException

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

POSTGRES_CONN_ID = "steam_postgres"


def daily_slot(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def source_hash(*parts) -> str:
    payload = json.dumps(parts, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def get_connection():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()


def check_ods_reviews(**context) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT count(*) FROM steam.ods_reviews;")
        count = cursor.fetchone()[0]
    finally:
        cursor.close()
        conn.close()

    if count == 0:
        raise AirflowSkipException("steam.ods_reviews is empty.")

    logger.info(f"steam.ods_reviews contains {count} rows.")


def sync_dim_user(**context) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT
                author_steamid,
                MAX(COALESCE(author_num_reviews, 0)),
                MAX(COALESCE(author_playtime_forever, 0))
            FROM steam.ods_reviews
            WHERE author_steamid IS NOT NULL
              AND author_steamid ~ '^[0-9]+$'
            GROUP BY author_steamid;
            """
        )
        rows = cursor.fetchall()
        if not rows:
            logger.warning("No numeric author_steamid values found in ODS reviews.")
            return

        cursor.execute(
            """
            CREATE TEMP TABLE tmp_user_source (
                steamid BIGINT,
                num_reviews INT,
                playtime_forever INT,
                row_hash TEXT
            ) ON COMMIT DROP;
            """
        )

        values = [
            (int(steamid), int(num_reviews), int(playtime), source_hash(int(num_reviews), int(playtime)))
            for steamid, num_reviews, playtime in rows
        ]
        cursor.executemany(
            "INSERT INTO tmp_user_source (steamid, num_reviews, playtime_forever, row_hash) "
            "VALUES (%s, %s, %s, %s);",
            values,
        )

        cursor.execute(
            """
            UPDATE dds.dim_user d
            SET is_current = false, valid_to = now()
            FROM tmp_user_source s
            WHERE d.steamid = s.steamid
              AND d.is_current
              AND d.row_hash <> s.row_hash;
            """
        )
        closed = cursor.rowcount

        cursor.execute(
            """
            UPDATE dds.dim_user d
            SET last_seen_at = now()
            FROM tmp_user_source s
            WHERE d.steamid = s.steamid
              AND d.is_current
              AND d.row_hash = s.row_hash;
            """
        )
        touched = cursor.rowcount

        cursor.execute(
            """
            INSERT INTO dds.dim_user (
                steamid, num_reviews, playtime_forever,
                first_seen_at, last_seen_at, row_hash,
                valid_from, valid_to, is_current
            )
            SELECT
                s.steamid,
                s.num_reviews,
                s.playtime_forever,
                COALESCE(
                    (SELECT MIN(u.first_seen_at) FROM dds.dim_user u WHERE u.steamid = s.steamid),
                    now()
                ),
                now(),
                s.row_hash,
                CASE
                    WHEN EXISTS (SELECT 1 FROM dds.dim_user u WHERE u.steamid = s.steamid)
                    THEN now()
                    ELSE '-infinity'::timestamptz
                END,
                'infinity'::timestamptz,
                true
            FROM tmp_user_source s
            WHERE NOT EXISTS (
                SELECT 1 FROM dds.dim_user d
                WHERE d.steamid = s.steamid AND d.is_current
            );
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

    logger.info(
        f"SCD2 dim_user: inserted={inserted}, closed={closed}, untouched={touched}."
    )


def sync_dim_review(**context) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT review_id, language, review_text, steam_purchase,
                   received_for_free, written_during_early_access, refunded,
                   timestamp_created, timestamp_updated, author_playtime_at_review,
                   voted_up, votes_up, votes_funny, weighted_vote_score,
                   comment_count, collected_at
            FROM steam.ods_reviews;
            """
        )
        raw_rows = cursor.fetchall()
        if not raw_rows:
            raise AirflowSkipException("steam.ods_reviews is empty.")

        cursor.execute(
            """
            CREATE TEMP TABLE tmp_review_source (
                review_id TEXT,
                language VARCHAR(10),
                review_text TEXT,
                steam_purchase BOOLEAN,
                received_for_free BOOLEAN,
                written_during_early_access BOOLEAN,
                refunded BOOLEAN,
                timestamp_created TIMESTAMPTZ,
                timestamp_updated TIMESTAMPTZ,
                author_playtime_at_review BIGINT,
                voted_up BOOLEAN,
                votes_up INTEGER,
                votes_funny INTEGER,
                weighted_score NUMERIC(6, 5),
                comment_count INTEGER,
                collected_at TIMESTAMPTZ,
                row_hash TEXT
            ) ON COMMIT DROP;
            """
        )

        values = []
        for row in raw_rows:
            (
                review_id, language, review_text, steam_purchase, received_for_free,
                written_during_early_access, refunded, timestamp_created,
                timestamp_updated, author_playtime_at_review, voted_up,
                votes_up, votes_funny, weighted_score, comment_count, collected_at,
            ) = row
            row_hash = source_hash(
                language, review_text, steam_purchase, received_for_free,
                written_during_early_access, refunded,
                timestamp_created.isoformat() if timestamp_created else None,
                timestamp_updated.isoformat() if timestamp_updated else None,
                author_playtime_at_review, voted_up, votes_up, votes_funny,
                weighted_score, comment_count,
                collected_at.isoformat() if collected_at else None,
            )
            values.append((
                review_id, language, review_text, steam_purchase, received_for_free,
                written_during_early_access, refunded, timestamp_created,
                timestamp_updated, author_playtime_at_review, voted_up,
                votes_up, votes_funny, weighted_score, comment_count, collected_at,
                row_hash,
            ))

        cursor.executemany(
            """
            INSERT INTO tmp_review_source (
                review_id, language, review_text, steam_purchase, received_for_free,
                written_during_early_access, refunded, timestamp_created,
                timestamp_updated, author_playtime_at_review, voted_up,
                votes_up, votes_funny, weighted_score, comment_count, collected_at,
                row_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            values,
        )

        cursor.execute(
            """
            UPDATE dds.dim_review d
            SET is_current = false, valid_to = now()
            FROM tmp_review_source s
            WHERE d.review_id = s.review_id
              AND d.is_current
              AND d.row_hash <> s.row_hash;
            """
        )
        closed = cursor.rowcount

        cursor.execute(
            """
            INSERT INTO dds.dim_review (
                review_id, language, review_text, steam_purchase, received_for_free,
                written_during_early_access, refunded, timestamp_created,
                timestamp_updated, author_playtime_at_review, voted_up,
                votes_up, votes_funny, weighted_score, comment_count, collected_at,
                row_hash, valid_from, valid_to, is_current
            )
            SELECT
                s.review_id, s.language, s.review_text, s.steam_purchase,
                s.received_for_free, s.written_during_early_access, s.refunded,
                s.timestamp_created, s.timestamp_updated, s.author_playtime_at_review,
                COALESCE(s.voted_up, false), s.votes_up, s.votes_funny,
                s.weighted_score, s.comment_count, s.collected_at,
                s.row_hash,
                CASE
                    WHEN EXISTS (SELECT 1 FROM dds.dim_review d WHERE d.review_id = s.review_id)
                    THEN now()
                    ELSE '-infinity'::timestamptz
                END,
                'infinity'::timestamptz,
                true
            FROM tmp_review_source s
            WHERE NOT EXISTS (
                SELECT 1 FROM dds.dim_review d
                WHERE d.review_id = s.review_id AND d.is_current
            );
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

    logger.info(f"SCD2 dim_review: inserted={inserted}, closed={closed}.")


def validate_review_dimensions(**context) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT count(*)
            FROM (
                SELECT review_id
                FROM dds.dim_review
                WHERE is_current
                GROUP BY review_id
                HAVING count(*) > 1
            ) duplicates;
            """
        )
        duplicate_reviews = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM (
                SELECT steamid
                FROM dds.dim_user
                WHERE is_current
                GROUP BY steamid
                HAVING count(*) > 1
            ) duplicates;
            """
        )
        duplicate_users = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM dds.dim_review
            WHERE valid_to <= valid_from;
            """
        )
        invalid_review_intervals = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM dds.dim_user
            WHERE valid_to <= valid_from;
            """
        )
        invalid_user_intervals = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM dds.dim_review a
            JOIN dds.dim_review b
              ON a.review_id = b.review_id
             AND a.review_sk < b.review_sk
             AND a.valid_from < b.valid_to
             AND b.valid_from < a.valid_to;
            """
        )
        overlapping_reviews = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM steam.ods_reviews o
            LEFT JOIN dds.dim_review r
              ON r.review_id = o.review_id AND r.is_current
            WHERE r.review_sk IS NULL;
            """
        )
        missing_reviews = cursor.fetchone()[0]

        cursor.execute("SELECT count(*) FROM dds.dim_review WHERE is_current;")
        reviews = cursor.fetchone()[0]

        cursor.execute("SELECT count(*) FROM dds.dim_user WHERE is_current;")
        users = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM steam.ods_reviews
            WHERE author_steamid IS NULL
               OR author_steamid !~ '^[0-9]+$';
            """
        )
        invalid_authors = cursor.fetchone()[0]
    finally:
        cursor.close()
        conn.close()

    if reviews == 0:
        raise AirflowException("dds.dim_review has no current rows.")
    if duplicate_reviews:
        raise AirflowException(f"{duplicate_reviews} review_ids have multiple current versions.")
    if duplicate_users:
        raise AirflowException(f"{duplicate_users} steamids have multiple current versions.")
    if invalid_review_intervals:
        raise AirflowException(f"{invalid_review_intervals} dim_review rows have invalid intervals.")
    if invalid_user_intervals:
        raise AirflowException(f"{invalid_user_intervals} dim_user rows have invalid intervals.")
    if overlapping_reviews:
        raise AirflowException(f"{overlapping_reviews} dim_review version pairs overlap.")
    if missing_reviews:
        raise AirflowException(f"{missing_reviews} ODS reviews are missing in dds.dim_review.")

    if invalid_authors:
        logger.warning(f"{invalid_authors} reviews have non-numeric author_steamid.")

    logger.info(
        f"SCD2 review dimensions validation passed: reviews={reviews}, users={users}, "
        f"invalid_authors={invalid_authors}."
    )


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="dds_review_dimensions_dag",
    default_args=default_args,
    description="SCD2 dimensions for users and reviews",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["steam", "dds", "dimensions", "scd2", "reviews"],
    doc_md="""
### DDS Review and User Dimensions (SCD2)

Загружает:
- `dds.dim_user` с историей версий;
- `dds.dim_review` с историей версий.

Изменение метрик пользователя или содержимого/голосов отзыва закрывает текущую
версию и создаёт новую. Неизменённые записи не версионируются.

**Запуск:** после `steam_reviews_collector` за тот же logical date.
""",
) as dag:

    wait_for_ods_reviews = ExternalTaskSensor(
        task_id="wait_for_ods_reviews",
        external_dag_id="steam_reviews_collector",
        external_task_id=None,
        execution_date_fn=daily_slot,
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=60,
        timeout=timedelta(hours=6),
        check_existence=True,
    )

    check_ods_reviews_task = PythonOperator(
        task_id="check_ods_reviews",
        python_callable=check_ods_reviews,
    )

    sync_dim_user_task = PythonOperator(
        task_id="sync_dim_user",
        python_callable=sync_dim_user,
    )

    sync_dim_review_task = PythonOperator(
        task_id="sync_dim_review",
        python_callable=sync_dim_review,
    )

    validate_review_dimensions_task = PythonOperator(
        task_id="validate_review_dimensions",
        python_callable=validate_review_dimensions,
    )

    wait_for_ods_reviews >> check_ods_reviews_task
    check_ods_reviews_task >> [sync_dim_user_task, sync_dim_review_task]
    [sync_dim_user_task, sync_dim_review_task] >> validate_review_dimensions_task
