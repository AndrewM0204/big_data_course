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
    """Выравнивает logical date на полночь — сетку @daily DAG'ов."""
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


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


def load_dim_user(**context) -> None:
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

        values = [
            (int(steamid), int(num_reviews), int(playtime_forever))
            for steamid, num_reviews, playtime_forever in rows
        ]

        cursor.executemany(
            """
            INSERT INTO dds.dim_user (
                steamid, num_reviews, playtime_forever, last_seen_at
            ) VALUES (%s, %s, %s, now())
            ON CONFLICT (steamid) DO UPDATE SET
                num_reviews = GREATEST(dds.dim_user.num_reviews, EXCLUDED.num_reviews),
                playtime_forever = GREATEST(
                    dds.dim_user.playtime_forever, EXCLUDED.playtime_forever
                ),
                last_seen_at = now();
            """,
            values,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info(f"Loaded {len(values)} users into dds.dim_user.")


def load_dim_review(**context) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO dds.dim_review (
                review_id, language, review_text, steam_purchase,
                received_for_free, written_during_early_access, refunded,
                timestamp_created, timestamp_updated, author_playtime_at_review,
                voted_up, votes_up, votes_funny, weighted_score,
                comment_count, collected_at
            )
            SELECT
                review_id,
                language,
                review_text,
                COALESCE(steam_purchase, false),
                COALESCE(received_for_free, false),
                COALESCE(written_during_early_access, false),
                COALESCE(refunded, false),
                timestamp_created,
                timestamp_updated,
                author_playtime_at_review,
                COALESCE(voted_up, false),
                votes_up,
                votes_funny,
                weighted_vote_score,
                comment_count,
                COALESCE(collected_at, now())
            FROM steam.ods_reviews
            ON CONFLICT (review_id) DO UPDATE SET
                language = EXCLUDED.language,
                review_text = EXCLUDED.review_text,
                steam_purchase = EXCLUDED.steam_purchase,
                received_for_free = EXCLUDED.received_for_free,
                written_during_early_access = EXCLUDED.written_during_early_access,
                refunded = EXCLUDED.refunded,
                timestamp_created = EXCLUDED.timestamp_created,
                timestamp_updated = EXCLUDED.timestamp_updated,
                author_playtime_at_review = EXCLUDED.author_playtime_at_review,
                voted_up = EXCLUDED.voted_up,
                votes_up = EXCLUDED.votes_up,
                votes_funny = EXCLUDED.votes_funny,
                weighted_score = EXCLUDED.weighted_score,
                comment_count = EXCLUDED.comment_count,
                collected_at = EXCLUDED.collected_at;
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

    logger.info(f"Loaded {inserted} reviews into dds.dim_review.")


def validate_review_dimensions(**context) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT count(*) FROM dds.dim_review;")
        reviews = cursor.fetchone()[0]

        cursor.execute("SELECT count(*) FROM dds.dim_user;")
        users = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM steam.ods_reviews o
            LEFT JOIN dds.dim_review r ON r.review_id = o.review_id
            WHERE r.review_id IS NULL;
            """
        )
        missing_reviews = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM steam.ods_reviews o
            WHERE o.author_steamid IS NOT NULL
              AND o.author_steamid ~ '^[0-9]+$'
              AND NOT EXISTS (
                  SELECT 1 FROM dds.dim_user u
                  WHERE u.steamid = o.author_steamid::bigint
              );
            """
        )
        missing_users = cursor.fetchone()[0]

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
        raise AirflowException("dds.dim_review is empty after load.")
    if missing_reviews:
        raise AirflowException(
            f"{missing_reviews} ODS reviews are missing in dds.dim_review."
        )

    if missing_users:
        logger.warning(f"{missing_users} reviews reference missing dds.dim_user rows.")
    if invalid_authors:
        logger.warning(f"{invalid_authors} reviews have non-numeric author_steamid.")

    logger.info(
        f"Review dimensions: reviews={reviews}, users={users}, "
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
    description="Load user and review DDS dimensions from ODS reviews",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["steam", "dds", "dimensions", "reviews", "users"],
    doc_md="""
### DDS Review and User Dimensions

Загружает измерения из `steam.ods_reviews`:
- `dds.dim_user` — уникальные авторы отзывов;
- `dds.dim_review` — описательные атрибуты и меры отзывов.

**Запуск:** после успешного завершения `steam_reviews_collector` за тот же logical date.
**Идемпотентность:** upsert по `steamid` и `review_id`.
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

    load_dim_user_task = PythonOperator(
        task_id="load_dim_user",
        python_callable=load_dim_user,
    )

    load_dim_review_task = PythonOperator(
        task_id="load_dim_review",
        python_callable=load_dim_review,
    )

    validate_review_dimensions_task = PythonOperator(
        task_id="validate_review_dimensions",
        python_callable=validate_review_dimensions,
    )

    wait_for_ods_reviews >> check_ods_reviews_task
    check_ods_reviews_task >> [load_dim_user_task, load_dim_review_task]
    [load_dim_user_task, load_dim_review_task] >> validate_review_dimensions_task
