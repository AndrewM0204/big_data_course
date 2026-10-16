import json
import logging
import os
import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from requests.auth import HTTPBasicAuth

# ==========================================
# КОНФИГУРАЦИЯ
# ==========================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_RAW_DIR = '/opt/airflow/data/raw/steam_player_stats'

# Официальный эндпоинт Steam для получения текущего онлайна
PLAYER_STATS_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"

# Airflow Connections
POSTGRES_CONN_ID = "steam_postgres"
CLICKHOUSE_CONN_ID = "clickhouse_http"


# ==========================================
# ХЕЛПЕРЫ
# ==========================================
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


def clickhouse_query(
    query: str,
    data: bytes | None = None,
    credentials: dict | None = None,
) -> str:
    """Выполняет запрос к ClickHouse через HTTP-интерфейс."""
    if credentials is None:
        credentials = get_clickhouse_credentials()
    
    params = {
        "query": query,
        "database": credentials["database"],
        "date_time_input_format": "best_effort",
    }
    
    resp = requests.post(
        credentials["host"],
        params=params,
        data=data,
        auth=HTTPBasicAuth(credentials["user"], credentials["password"]),
        timeout=60,
    )
    response_text = resp.text.strip()
    
    if not resp.ok or response_text.startswith("Code:") or "DB::Exception" in response_text:
        raise RuntimeError(f"ClickHouse error (HTTP {resp.status_code}): {response_text}")
        
    return response_text


def clickhouse_insert(rows: list, table: str, credentials: dict | None = None) -> int:
    """Пакетная вставка строк в ClickHouse в формате JSONEachRow."""
    if not rows:
        return 0
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
    clickhouse_query(f"INSERT INTO {table} FORMAT JSONEachRow", data=body.encode("utf-8"), credentials=credentials)
    return len(rows)


# ==========================================
# ФУНКЦИИ DAG
# ==========================================
def select_target_games(**context) -> str:
    """
    Выбирает игры для сбора статистики онлайна.
    """
    ds = context['ds']
    batch_size = int(context['params'].get('batch_size', 200))

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT appid, name FROM steam.ods_games;")
        games_data = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    if not games_data:
        raise AirflowSkipException(
            "steam.ods_games is empty. Run steam_games_catalog_etl first."
        )

    target_games = [{"appid": row[0], "name": row[1]} for row in games_data[:batch_size]]

    logger.info(f"Selected {len(target_games)} games for player stats collection on {ds}.")

    target_dir = os.path.join(BASE_RAW_DIR, ds)
    os.makedirs(target_dir, exist_ok=True)
    file_path = os.path.join(target_dir, 'target_games.json')
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(target_games, f, ensure_ascii=False)

    return file_path


def fetch_player_stats(target_file: str, **context) -> str:
    """
    Для каждой целевой игры запрашивает текущий онлайн через Steam API.
    Передаёт collected_at из Airflow context для точного времени сбора.
    """
    ds = context['ds']
    # Используем data_interval_start как точное время начала интервала сбора
    collected_at = context['data_interval_start'].strftime("%Y-%m-%d %H:%M:%S")
    
    request_delay = float(context['params'].get('request_delay', 0.5))

    with open(target_file, 'r', encoding='utf-8') as f:
        target_games = json.load(f)

    if not target_games:
        logger.info("Empty target list, skipping fetch.")
        empty_file = os.path.join(os.path.dirname(target_file), 'player_stats_raw.json')
        with open(empty_file, 'w') as f:
            json.dump({}, f)
        return empty_file

    raw_stats = {}
    total = len(target_games)

    for i, game in enumerate(target_games, start=1):
        appid = game['appid']
        params = {"appid": appid}

        try:
            resp = requests.get(PLAYER_STATS_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            response_data = data.get('response', {})
            if response_data.get('result') == 1:
                raw_stats[appid] = {
                    "player_count": response_data.get('player_count', 0),
                    "game_name": game['name'],
                    "collected_at": collected_at,  # Передаём время сбора
                }
            else:
                logger.warning(f"No player stats for appid {appid} (result != 1).")

        except requests.exceptions.RequestException as e:
            logger.error(f"Network error for appid {appid}: {e}")

        if i % 20 == 0:
            logger.info(f"Progress: {i}/{total} games processed.")

        time.sleep(request_delay)

    # Сохраняем сырые данные
    out_dir = os.path.join(BASE_RAW_DIR, ds, 'stats')
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, 'player_stats_raw.json')
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(raw_stats, f, ensure_ascii=False)

    logger.info(f"Saved player stats for {len(raw_stats)}/{total} games.")
    return out_file


def transform_and_load_clickhouse(stats_file: str, **context) -> None:
    """Трансформация сырых данных и пакетная загрузка в ClickHouse с дедупликацией."""
    credentials = get_clickhouse_credentials()
    
    with open(stats_file, 'r', encoding='utf-8') as f:
        raw_stats = json.load(f)

    if not raw_stats:
        logger.info("No player stats data to load.")
        return

    rows = []
    for appid, data in raw_stats.items():
        rows.append({
            "appid": int(appid),
            "game_name": data.get('game_name', 'Unknown'),
            "current_players": int(data.get('player_count', 0)),
            "collected_at": data.get('collected_at'),  # Передаём время сбора из raw данных
        })

    if not rows:
        logger.warning("No valid rows parsed from player stats data.")
        return

    inserted = clickhouse_insert(rows, "player_stats", credentials=credentials)
    logger.info(f"Inserted {inserted} rows into steam.player_stats.")


# ==========================================
# ОПРЕДЕЛЕНИЕ DAG
# ==========================================
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    dag_id='steam_player_stats_etl',
    default_args=default_args,
    description='Collect Steam player statistics and load into ClickHouse',
    schedule='0 */3 * * *',  # Каждые 3 часа
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['steam', 'player_stats', 'clickhouse', 'time_series'],
    params={
        'batch_size': Param(
            200,
            type='integer',
            minimum=1,
            maximum=1000,
            description='Number of games to collect player stats for per run'
        ),
        'request_delay': Param(
            0.5,
            type='number',
            minimum=0.1,
            maximum=5.0,
            description='Delay between API requests (seconds)'
        ),
    },
    doc_md="""
### Steam Player Stats ETL

Собирает текущую статистику онлайна игроков для игр из каталога (`steam.ods_games`) 
и загружает в ClickHouse для анализа временных рядов.

**Расписание:** каждые 3 часа (`0 */3 * * *`).
"""
) as dag:

    task_select = PythonOperator(
        task_id='select_target_games',
        python_callable=select_target_games,
    )

    task_fetch = PythonOperator(
        task_id='fetch_player_stats',
        python_callable=fetch_player_stats,
        op_kwargs={
            'target_file': "{{ ti.xcom_pull(task_ids='select_target_games') }}"
        },
    )

    task_load = PythonOperator(
        task_id='transform_and_load_clickhouse',
        python_callable=transform_and_load_clickhouse,
        op_kwargs={
            'stats_file': "{{ ti.xcom_pull(task_ids='fetch_player_stats') }}"
        },
    )

    task_select >> task_fetch >> task_load