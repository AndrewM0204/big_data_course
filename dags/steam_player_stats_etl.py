import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

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
            "steam.ods_games is empty."
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
    Передаёт фактическое UTC-время сбора.
    """
    ds = context['ds']
    collected_at = datetime.now(timezone.utc).isoformat()
    
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
                    "collected_at": collected_at,
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


def transform_player_stats(stats_file: str, **context) -> str:
    """
    Task 3 (Transform): Читает сырые данные и готовит их для PostgreSQL.
    """
    with open(stats_file, 'r', encoding='utf-8') as f:
        raw_stats = json.load(f)

    if not raw_stats:
        logger.info("No player stats data to transform.")
        out_file = os.path.join(os.path.dirname(stats_file), 'prepared_stats.json')
        with open(out_file, 'w') as f:
            json.dump([], f)
        return out_file

    rows = []
    for appid, data in raw_stats.items():
        rows.append({
            "appid": int(appid),
            "game_name": data.get('game_name', 'Unknown'),
            "current_players": int(data.get('player_count', 0)),
            "collected_at": data.get('collected_at'),
        })

    logger.info(f"Transformed {len(rows)} player stats records.")
    
    out_file = os.path.join(os.path.dirname(stats_file), 'prepared_stats.json')
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    
    return out_file

def load_to_postgres(prepared_file: str, **context) -> None:
    """
    Task 4 (Load): Загружает подготовленные данные в PostgreSQL.
    """
    with open(prepared_file, 'r', encoding='utf-8') as f:
        rows = json.load(f)

    if not rows:
        logger.info("No data to load into PostgreSQL.")
        return

    values = []
    for row in rows:
        collected_at = row.get('collected_at')
        if isinstance(collected_at, str):
            collected_at = datetime.fromisoformat(collected_at)
        if collected_at is None:
            collected_at = datetime.now(timezone.utc)
        elif collected_at.tzinfo is None:
            collected_at = collected_at.replace(tzinfo=timezone.utc)
        values.append((
            int(row['appid']),
            row.get('game_name'),
            int(row['current_players']),
            collected_at,
        ))

    query = """
    INSERT INTO steam.ods_player_stats (
        appid, game_name, current_players, collected_at
    ) VALUES (%s, %s, %s, %s)
    ON CONFLICT (appid, collected_at) DO UPDATE SET
        game_name = EXCLUDED.game_name,
        current_players = EXCLUDED.current_players;
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        for start in range(0, len(values), 1000):
            cursor.executemany(query, values[start:start + 1000])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info(f"Loaded {len(values)} rows into steam.ods_player_stats.")


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
    description='Collect Steam player statistics and load into PostgreSQL',
    schedule='0 */3 * * *',  # Каждые 3 часа
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['steam', 'player_stats', 'postgres', 'time_series'],
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
и загружает в PostgreSQL `steam.ods_player_stats` для анализа временных рядов.

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

    task_transform = PythonOperator(
        task_id='transform_player_stats',
        python_callable=transform_player_stats,
        op_kwargs={
            'stats_file': "{{ ti.xcom_pull(task_ids='fetch_player_stats') }}"
        },
    )

    task_load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        op_kwargs={
            'prepared_file': "{{ ti.xcom_pull(task_ids='transform_player_stats') }}"
        },
    )

    task_select >> task_fetch >> task_transform >> task_load