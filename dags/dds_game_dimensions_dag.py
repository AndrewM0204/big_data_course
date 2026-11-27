import hashlib
import json
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
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def parse_release_date(value: str | None) -> date | None:
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
    if not value:
        return []
    return sorted({part.strip() for part in value.split(",") if part.strip()})


def source_hash(*parts) -> str:
    payload = json.dumps(parts, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def get_connection():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()


def ensure_lookup(cursor, table: str, id_column: str, name_column: str, names: set[str]) -> dict[str, int]:
    if not names:
        return {}
    cursor.executemany(
        f"INSERT INTO {table} ({name_column}) VALUES (%s) ON CONFLICT ({name_column}) DO NOTHING;",
        [(name,) for name in sorted(names)],
    )
    cursor.execute(
        f"SELECT {id_column}, {name_column} FROM {table} WHERE {name_column} = ANY(%s);",
        (list(names),),
    )
    return {name: dimension_id for dimension_id, name in cursor.fetchall()}


def sync_bridges(cursor, table: str, fk_column: str, game_sk: int, dimension_ids: set[int]) -> None:
    cursor.execute(f"DELETE FROM {table} WHERE game_sk = %s;", (game_sk,))
    if dimension_ids:
        cursor.executemany(
            f"INSERT INTO {table} (game_sk, {fk_column}) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
            [(game_sk, dimension_id) for dimension_id in sorted(dimension_ids)],
        )


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


def sync_game_dimensions(**context) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT appid, name, is_free, release_date, short_description,
                   genres, developers, publishers
            FROM steam.ods_games;
            """
        )
        raw_rows = cursor.fetchall()
        if not raw_rows:
            raise AirflowSkipException("steam.ods_games is empty.")

        source = {}
        for appid, name, is_free, release_raw, description, genres_raw, developers_raw, publishers_raw in raw_rows:
            genres = split_csv(genres_raw)
            developers = split_csv(developers_raw)
            publishers = split_csv(publishers_raw)
            release_date = parse_release_date(release_raw)
            payload = (
                int(appid),
                name or "Unknown",
                bool(is_free),
                release_date.isoformat() if release_date else None,
                description,
                genres,
                developers,
                publishers,
            )
            source[int(appid)] = {
                "name": name or "Unknown",
                "is_free": bool(is_free),
                "release_date": release_date,
                "short_description": description,
                "genres": genres,
                "developers": developers,
                "publishers": publishers,
                "row_hash": source_hash(*payload),
            }

        genre_map = ensure_lookup(
            cursor, "dds.dim_genre", "genre_id", "genre_name",
            {value for row in source.values() for value in row["genres"]},
        )
        developer_map = ensure_lookup(
            cursor, "dds.dim_developer", "developer_id", "developer_name",
            {value for row in source.values() for value in row["developers"]},
        )
        publisher_map = ensure_lookup(
            cursor, "dds.dim_publisher", "publisher_id", "publisher_name",
            {value for row in source.values() for value in row["publishers"]},
        )

        cursor.execute(
            """
            SELECT game_sk, appid, row_hash
            FROM dds.dim_game
            WHERE is_current;
            """
        )
        current = {
            int(appid): (int(game_sk), row_hash)
            for game_sk, appid, row_hash in cursor.fetchall()
        }

        inserted_versions = 0
        changed_versions = 0
        unchanged_versions = 0

        for appid, row in source.items():
            cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s));", (str(appid),))
            current_row = current.get(appid)

            if current_row is None:
                cursor.execute(
                    """
                    INSERT INTO dds.dim_game (
                        appid, name, is_free, release_date, short_description,
                        row_hash, valid_from, valid_to, is_current
                    ) VALUES (%s, %s, %s, %s, %s, %s, '-infinity', 'infinity', true)
                    RETURNING game_sk;
                    """,
                    (
                        appid, row["name"], row["is_free"], row["release_date"],
                        row["short_description"], row["row_hash"],
                    ),
                )
                game_sk = cursor.fetchone()[0]
                inserted_versions += 1
            else:
                game_sk, current_hash = current_row
                if current_hash != row["row_hash"]:
                    cursor.execute(
                        """
                        SELECT game_sk, row_hash
                        FROM dds.dim_game
                        WHERE appid = %s AND is_current
                        FOR UPDATE;
                        """,
                        (appid,),
                    )
                    locked = cursor.fetchone()
                    if locked and locked[1] != row["row_hash"]:
                        cursor.execute(
                            """
                            UPDATE dds.dim_game
                            SET is_current = false, valid_to = now()
                            WHERE game_sk = %s;
                            """,
                            (locked[0],),
                        )
                        cursor.execute(
                            """
                            INSERT INTO dds.dim_game (
                                appid, name, is_free, release_date, short_description,
                                row_hash, valid_from, valid_to, is_current
                            ) VALUES (%s, %s, %s, %s, %s, %s, now(), 'infinity', true)
                            RETURNING game_sk;
                            """,
                            (
                                appid, row["name"], row["is_free"], row["release_date"],
                                row["short_description"], row["row_hash"],
                            ),
                        )
                        game_sk = cursor.fetchone()[0]
                        changed_versions += 1
                    elif locked:
                        game_sk = locked[0]
                        unchanged_versions += 1
                else:
                    unchanged_versions += 1

            sync_bridges(
                cursor, "dds.bridge_game_genre", "genre_id", game_sk,
                {genre_map[name] for name in row["genres"] if name in genre_map},
            )
            sync_bridges(
                cursor, "dds.bridge_game_developer", "developer_id", game_sk,
                {developer_map[name] for name in row["developers"] if name in developer_map},
            )
            sync_bridges(
                cursor, "dds.bridge_game_publisher", "publisher_id", game_sk,
                {publisher_map[name] for name in row["publishers"] if name in publisher_map},
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info(
        f"SCD2 dim_game: inserted={inserted_versions}, changed={changed_versions}, "
        f"unchanged={unchanged_versions}."
    )


def validate_game_dimensions(**context) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT count(*) FROM dds.dim_game WHERE is_current;")
        current_games = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM (
                SELECT appid
                FROM dds.dim_game
                WHERE is_current
                GROUP BY appid
                HAVING count(*) > 1
            ) duplicates;
            """
        )
        duplicate_current = cursor.fetchone()[0]

        cursor.execute("SELECT count(*) FROM dds.dim_game WHERE valid_to <= valid_from;")
        invalid_intervals = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM dds.dim_game a
            JOIN dds.dim_game b
              ON a.appid = b.appid
             AND a.game_sk < b.game_sk
             AND a.valid_from < b.valid_to
             AND b.valid_from < a.valid_to;
            """
        )
        overlapping_intervals = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM steam.ods_games o
            LEFT JOIN dds.dim_game g
              ON g.appid = o.appid AND g.is_current
            WHERE g.game_sk IS NULL;
            """
        )
        missing_games = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT count(*)
            FROM dds.bridge_game_genre b
            LEFT JOIN dds.dim_game g ON g.game_sk = b.game_sk
            LEFT JOIN dds.dim_genre d ON d.genre_id = b.genre_id
            WHERE g.game_sk IS NULL OR d.genre_id IS NULL;
            """
        )
        broken_genre_bridges = cursor.fetchone()[0]
    finally:
        cursor.close()
        conn.close()

    if current_games == 0:
        raise AirflowException("dds.dim_game has no current rows.")
    if duplicate_current:
        raise AirflowException(f"{duplicate_current} appids have multiple current versions.")
    if invalid_intervals:
        raise AirflowException(f"{invalid_intervals} dim_game rows have invalid intervals.")
    if overlapping_intervals:
        raise AirflowException(f"{overlapping_intervals} dim_game version pairs overlap.")
    if missing_games:
        raise AirflowException(f"{missing_games} ODS games are missing in dds.dim_game.")
    if broken_genre_bridges:
        raise AirflowException(f"{broken_genre_bridges} broken game-genre bridges.")

    logger.info(f"SCD2 dim_game validation passed: {current_games} current games.")


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
    description="SCD2 dimensions for games, genres, developers and publishers",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["steam", "dds", "dimensions", "scd2", "games"],
    doc_md="""
### DDS Game Dimensions (SCD2)

Загружает:
- `dds.dim_game` с историей версий и `valid_from` / `valid_to` / `is_current`;
- `dds.dim_genre`, `dds.dim_developer`, `dds.dim_publisher`;
- bridge-таблицы, привязанные к `game_sk`.

Изменение атрибутов игры или её жанров/разработчиков/издателей закрывает текущую
версию и создаёт новую. Неизменённые игры не версионируются.

**Запуск:** после `steam_games_catalog_etl` за тот же logical date.
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

    sync_game_dimensions_task = PythonOperator(
        task_id="sync_game_dimensions",
        python_callable=sync_game_dimensions,
    )

    validate_game_dimensions_task = PythonOperator(
        task_id="validate_game_dimensions",
        python_callable=validate_game_dimensions,
    )

    wait_for_ods_catalog >> check_ods_games_task
    check_ods_games_task >> sync_game_dimensions_task
    sync_game_dimensions_task >> validate_game_dimensions_task
