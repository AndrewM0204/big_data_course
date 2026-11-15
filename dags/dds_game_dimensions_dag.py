import logging
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk.exceptions import AirflowException, AirflowSkipException

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

POSTGRES_CONN_ID = "steam_postgres"

RELEASE_DATE_FORMATS = (
    "%d %b, %Y",
    "%d %B, %Y",
    "%b %d, %Y",
    "%B %d, %Y",
    "%d %b %Y",
    "%d %B %Y",
    "%Y-%m-%d",
)


def daily_slot(dt: datetime) -> datetime:
    """Выравнивает logical date на полночь — сетку @daily DAG'ов."""
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def parse_release_date(value: str | None) -> date | None:
    """Парсит типовые форматы даты Steam, иначе возвращает None."""
    if not value:
        return None
    text = value.strip()
    for fmt in RELEASE_DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def split_csv(value: str | None) -> list[str]:
    """Разбивает строку ODS на уникальные значения через запятую."""
    if not value:
        return []
    return sorted({part.strip() for part in value.split(",") if part.strip()})


def get_connection():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()


def check_ods_games(**context) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT count(*) FROM steam.ods_games;")
        count = cursor.fetchone()[0]
    finally:
        cursor.close()
        conn.close()

    if count == 0:
        raise AirflowSkipException("steam.ods_games is empty.")

    logger.info(f"steam.ods_games contains {count} rows.")


def load_dim_game(**context) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT appid, name, is_free, release_date, short_description
            FROM steam.ods_games;
            """
        )
        rows = cursor.fetchall()
        if not rows:
            raise AirflowSkipException("steam.ods_games is empty.")

        values = [
            (
                int(appid),
                name or "Unknown",
                bool(is_free),
                parse_release_date(release_date),
                short_description,
            )
            for appid, name, is_free, release_date, short_description in rows
        ]

        cursor.executemany(
            """
            INSERT INTO dds.dim_game (
                appid, name, is_free, release_date, short_description
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (appid) DO UPDATE SET
                name = EXCLUDED.name,
                is_free = EXCLUDED.is_free,
                release_date = EXCLUDED.release_date,
                short_description = EXCLUDED.short_description;
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

    logger.info(f"Loaded {len(values)} games into dds.dim_game.")


def sync_game_lookup(
    source_column: str,
    temp_table: str,
    dimension_table: str,
    dimension_id: str,
    dimension_name: str,
    bridge_table: str,
    bridge_column: str,
) -> int:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"CREATE TEMP TABLE {temp_table} (appid BIGINT, value TEXT) ON COMMIT DROP;"
        )
        cursor.execute(f"SELECT appid, {source_column} FROM steam.ods_games;")

        pairs = []
        for appid, raw_value in cursor.fetchall():
            values = split_csv(raw_value)
            if not values:
                pairs.append((int(appid), None))
            else:
                pairs.extend((int(appid), value) for value in values)

        if not pairs:
            conn.commit()
            return 0

        cursor.executemany(
            f"INSERT INTO {temp_table} (appid, value) VALUES (%s, %s);",
            pairs,
        )

        cursor.execute(
            f"""
            INSERT INTO {dimension_table} ({dimension_name})
            SELECT DISTINCT value
            FROM {temp_table}
            WHERE value IS NOT NULL
            ON CONFLICT ({dimension_name}) DO NOTHING;
            """
        )

        cursor.execute(
            f"""
            DELETE FROM {bridge_table} b
            USING (SELECT DISTINCT appid FROM {temp_table}) t
            WHERE b.appid = t.appid;
            """
        )

        cursor.execute(
            f"""
            INSERT INTO {bridge_table} (appid, {bridge_column})
            SELECT t.appid, d.{dimension_id}
            FROM {temp_table} t
            JOIN {dimension_table} d ON d.{dimension_name} = t.value
            ON CONFLICT DO NOTHING;
            """
        )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    return len(pairs)


def load_dim_genre(**context) -> None:
    count = sync_game_lookup(
        source_column="genres",
        temp_table="tmp_game_genre",
        dimension_table="dds.dim_genre",
        dimension_id="genre_id",
        dimension_name="genre_name",
        bridge_table="dds.bridge_game_genre",
        bridge_column="genre_id",
    )
    logger.info(f"Synchronized {count} game-genre rows.")


def load_dim_developer(**context) -> None:
    count = sync_game_lookup(
        source_column="developers",
        temp_table="tmp_game_developer",
        dimension_table="dds.dim_developer",
        dimension_id="developer_id",
        dimension_name="developer_name",
        bridge_table="dds.bridge_game_developer",
        bridge_column="developer_id",
    )
    logger.info(f"Synchronized {count} game-developer rows.")


def load_dim_publisher(**context) -> None:
    count = sync_game_lookup(
        source_column="publishers",
        temp_table="tmp_game_publisher",
        dimension_table="dds.dim_publisher",
        dimension_id="publisher_id",
        dimension_name="publisher_name",
        bridge_table="dds.bridge_game_publisher",
        bridge_column="publisher_id",
    )
    logger.info(f"Synchronized {count} game-publisher rows.")


def validate_game_dimensions(**context) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT count(*) FROM dds.dim_game;")
        games = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM steam.ods_games o
            LEFT JOIN dds.dim_game g ON g.appid = o.appid
            WHERE g.appid IS NULL;
            """
        )
        missing_games = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM steam.ods_games o
            WHERE o.genres IS NOT NULL
              AND o.genres <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM dds.bridge_game_genre b WHERE b.appid = o.appid
              );
            """
        )
        missing_genres = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM steam.ods_games o
            WHERE o.developers IS NOT NULL
              AND o.developers <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM dds.bridge_game_developer b WHERE b.appid = o.appid
              );
            """
        )
        missing_developers = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM steam.ods_games o
            WHERE o.publishers IS NOT NULL
              AND o.publishers <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM dds.bridge_game_publisher b WHERE b.appid = o.appid
              );
            """
        )
        missing_publishers = cursor.fetchone()[0]

        cursor.execute("SELECT count(*) FROM dds.dim_genre;")
        genres = cursor.fetchone()[0]
        cursor.execute("SELECT count(*) FROM dds.dim_developer;")
        developers = cursor.fetchone()[0]
        cursor.execute("SELECT count(*) FROM dds.dim_publisher;")
        publishers = cursor.fetchone()[0]
    finally:
        cursor.close()
        conn.close()

    if games == 0:
        raise AirflowException("dds.dim_game is empty after load.")
    if missing_games:
        raise AirflowException(f"{missing_games} ODS games are missing in dds.dim_game.")

    if missing_genres:
        logger.warning(f"{missing_genres} games have no genre bridge rows.")
    if missing_developers:
        logger.warning(f"{missing_developers} games have no developer bridge rows.")
    if missing_publishers:
        logger.warning(f"{missing_publishers} games have no publisher bridge rows.")

    logger.info(
        f"Game dimensions: games={games}, genres={genres}, "
        f"developers={developers}, publishers={publishers}."
    )


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="dds_game_dimensions_dag",
    default_args=default_args,
    description="Load game, genre, developer and publisher DDS dimensions",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["steam", "dds", "dimensions", "games"],
    doc_md="""
### DDS Game Dimensions

Загружает измерения, зависящие от каталога Steam:
- `dds.dim_game` из `steam.ods_games`;
- `dds.dim_genre` и `dds.bridge_game_genre`;
- `dds.dim_developer` и `dds.bridge_game_developer`;
- `dds.dim_publisher` и `dds.bridge_game_publisher`.

**Запуск:** после успешного завершения `steam_games_catalog_etl` за тот же logical date.
**Идемпотентность:** upsert по бизнес-ключам и полная пересинхронизация bridge-таблиц
для игр из текущего ODS-снимка.
""",
) as dag:

    wait_for_ods_catalog = ExternalTaskSensor(
        task_id="wait_for_ods_catalog",
        external_dag_id="steam_games_catalog_etl",
        external_task_id=None,
        execution_date_fn=daily_slot,
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=60,
        timeout=timedelta(hours=6),
        check_existence=True,
    )

    check_ods_games_task = PythonOperator(
        task_id="check_ods_games",
        python_callable=check_ods_games,
    )

    load_dim_game_task = PythonOperator(
        task_id="load_dim_game",
        python_callable=load_dim_game,
    )

    load_dim_genre_task = PythonOperator(
        task_id="load_dim_genre",
        python_callable=load_dim_genre,
    )

    load_dim_developer_task = PythonOperator(
        task_id="load_dim_developer",
        python_callable=load_dim_developer,
    )

    load_dim_publisher_task = PythonOperator(
        task_id="load_dim_publisher",
        python_callable=load_dim_publisher,
    )

    validate_game_dimensions_task = PythonOperator(
        task_id="validate_game_dimensions",
        python_callable=validate_game_dimensions,
    )

    wait_for_ods_catalog >> check_ods_games_task >> load_dim_game_task
    load_dim_game_task >> [
        load_dim_genre_task,
        load_dim_developer_task,
        load_dim_publisher_task,
    ] >> validate_game_dimensions_task
