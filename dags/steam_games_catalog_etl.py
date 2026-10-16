import json
import logging
import os
import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import (
    PythonOperator,
    ShortCircuitOperator,
)

# ==========================================
# КОНФИГУРАЦИЯ
# ==========================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_RAW_DIR = '/opt/airflow/data/raw/steam_catalog'
BASE_PROC_DIR = '/opt/airflow/data/processed/steam_catalog'

STEAM_APP_LIST_URL = "https://api.steampowered.com/IStoreService/GetAppList/v1/"
STEAM_APP_DETAILS_URL = "https://store.steampowered.com/api/appdetails/"

POSTGES_CONNECTION_ID = "steam_postgres"

# ==========================================
# ФУНКЦИИ DAG
# ==========================================

def check_global_limit(**context) -> bool:
    """
    ShortCircuit: Проверяет, не превышен ли глобальный лимит игр (10000).
    Если да — DAG успешно завершается, не выполняя следующие таски.
    """
    max_games = context['params'].get('max_games_total', 10000)
    
    hook = PostgresHook(postgres_conn_id=POSTGES_CONNECTION_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM steam.ods_games;")
    current_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    
    logger.info(f"Current games in DB: {current_count}, Max allowed: {max_games}")
    
    if current_count >= max_games:
        logger.info(f"Global limit reached ({current_count} >= {max_games}). Stopping DAG.")
        return False  # Прерывает выполнение следующих тасок
        
    return True  # Продолжаем выполнение


def extract_master_app_list(**context) -> str:
    """
    Task 1: Скачивает список AppID из нового Steam API (IStoreService) с пагинацией.
    Требует наличия переменной STEAM_API_KEY в .env файле.
    """
    ds = context['ds']
    target_dir = os.path.join(BASE_RAW_DIR, ds)
    os.makedirs(target_dir, exist_ok=True)
    file_path = os.path.join(target_dir, 'master_app_list.json')

    # Читаем API ключ из переменных окружения контейнера
    api_key = os.environ.get('STEAM_API_KEY')
    if not api_key:
        raise ValueError("STEAM_API_KEY не найден в переменных окружения. Проверьте .env файл и перезапустите контейнеры.")

    logger.info("Fetching master app list from Steam IStoreService API...")
    
    all_appids = []
    last_appid = 0
    has_more = True
    
    try:
        while has_more:
            params = {
                "key": api_key,
                "format": "json",
                "max_results": 50000, # Запрашиваем максимально допустимый батч
                "last_appid": last_appid # Пагинация: продолжаем с прошлого места
            }
            response = requests.get(STEAM_APP_LIST_URL, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            resp_data = data.get('response', {})
            apps = resp_data.get('apps', [])
            
            for app in apps:
                all_appids.append(app['appid'])
                
            has_more = resp_data.get('have_more_results', False)
            last_appid = resp_data.get('last_appid', 0)
            
            logger.info(f"Fetched {len(apps)} apps. Total so far: {len(all_appids)}. Have more: {has_more}")
            time.sleep(1) # Небольшая пауза между страницами пагинации
            
        logger.info(f"Fetched {len(all_appids)} total appids from Steam.")
        
        with open(file_path, 'w') as f:
            json.dump(all_appids, f)
            
        return file_path
        
    except Exception as e:
        logger.error(f"Failed to fetch master app list: {e}")
        raise


def select_unprocessed_batch(master_file: str, **context) -> str:
    """
    Task 2: Anti-Join паттерн.
    1. Читает master list
    2. Запрашивает из БД уже загруженные appid
    3. Вычисляет разницу (только новые игры)
    4. Берет первые N игр (batch_size)
    """
    ds = context['ds']
    batch_size = context['params'].get('batch_size', 100)
    max_games = context['params'].get('max_games_total', 10000)
    
    # 1. Читаем master list
    with open(master_file, 'r') as f:
        all_appids: list[int] = json.load(f)
    
    # 2. Запрашиваем уже загруженные игры из БД (быстро, т.к. PK индекс)
    hook = PostgresHook(postgres_conn_id=POSTGES_CONNECTION_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT appid FROM steam.ods_games;")
    existing_appids: set[int] = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT COUNT(*) FROM steam.ods_games;")
    current_count = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    logger.info(f"Already loaded in DB: {len(existing_appids)} games")
    
    # 3. Anti-Join: вычитаем уже загруженные из master list
    unprocessed_appids = [appid for appid in all_appids if appid not in existing_appids]
    logger.info(f"Unprocessed games remaining: {len(unprocessed_appids)}")
    
    # 4. Учитываем глобальный лимит: сколько еще можно загрузить
    remaining_capacity = max_games - current_count
    effective_batch_size = min(batch_size, remaining_capacity, len(unprocessed_appids))
    
    if effective_batch_size <= 0:
        logger.warning("No games to process (capacity exhausted or all loaded)")
        # Создаем пустой файл, чтобы следующие таски корректно отработали
        effective_batch_size = 0
        daily_batch = []
    else:
        daily_batch = unprocessed_appids[:effective_batch_size]
        logger.info(f"Selected batch of {len(daily_batch)} new games for processing")
    
    # Сохраняем батч
    target_dir = os.path.join(BASE_PROC_DIR, ds)
    os.makedirs(target_dir, exist_ok=True)
    batch_file_path = os.path.join(target_dir, 'daily_batch.json')
    
    with open(batch_file_path, 'w') as f:
        json.dump(daily_batch, f)
        
    return batch_file_path


def fetch_game_details(batch_file_path: str, **context) -> str:
    """
    Task 3: Скачивает детали по выбранному батчу с rate-limiting.
    """
    ds = context['ds']
    with open(batch_file_path, 'r') as f:
        appids = json.load(f)
    
    if not appids:
        logger.info("Empty batch, skipping fetch.")
        empty_file = os.path.join(os.path.dirname(batch_file_path), 'raw_details.json')
        with open(empty_file, 'w') as f:
            json.dump({}, f)
        return empty_file
        
    raw_details = {}
    total = len(appids)
    
    for i, appid in enumerate(appids):
        url = f"{STEAM_APP_DETAILS_URL}?appids={appid}&l=english"
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            
            if str(appid) in data and data[str(appid)]['success']:
                raw_details[appid] = data[str(appid)]['data']
            else:
                logger.warning(f"AppID {appid} not found or unsuccessful.")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error for appid {appid}: {e}")
            
        if (i + 1) % 10 == 0:
            logger.info(f"Progress: {i + 1}/{total} games processed...")
        time.sleep(1.2)  # Rate limiting
        
    target_dir = os.path.join(BASE_RAW_DIR, ds, 'details')
    os.makedirs(target_dir, exist_ok=True)
    details_file = os.path.join(target_dir, 'raw_details.json')
    
    with open(details_file, 'w', encoding='utf-8') as f:
        json.dump(raw_details, f, ensure_ascii=False)
        
    logger.info(f"Saved details for {len(raw_details)} games.")
    return details_file


def transform_and_load_to_postgres(details_file: str, **context):
    """
    Task 4: Трансформация и Bulk Upsert в PostgreSQL.
    """
    with open(details_file, 'r', encoding='utf-8') as f:
        raw_details = json.load(f)
    
    if not raw_details:
        logger.info("No data to load.")
        return
        
    prepared_data = []
    for appid, game in raw_details.items():
        is_free = game.get('is_free', False)
        
        price_cents = game.get('price_overview', {}).get('final', 0) if not is_free else 0
        price = float(price_cents) / 100.0
        
        developers = ", ".join(game.get('developers', [])) or None
        publishers = ", ".join(game.get('publishers', [])) or None
        genres = ", ".join([g['description'] for g in game.get('genres', [])]) or None
        release_date = game.get('release_date', {}).get('date') or None
        short_desc = game.get('short_description', '')
        
        raw_json_str = json.dumps(game)
        
        prepared_data.append((
            int(appid), game.get('name'), is_free, price, developers, 
            publishers, genres, release_date, short_desc, raw_json_str
        ))
        
    if not prepared_data:
        logger.warning("No valid data to load.")
        return

    hook = PostgresHook(postgres_conn_id=POSTGES_CONNECTION_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    query = """
     INSERT INTO steam.ods_games (
         appid, name, is_free, price, developers, publishers, 
         genres, release_date, short_description, raw_json
     ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
     ON CONFLICT (appid) DO UPDATE SET
         name = EXCLUDED.name,
         is_free = EXCLUDED.is_free,
         price = EXCLUDED.price,
         developers = EXCLUDED.developers,
         publishers = EXCLUDED.publishers,
         genres = EXCLUDED.genres,
         release_date = EXCLUDED.release_date,
         short_description = EXCLUDED.short_description,
         raw_json = EXCLUDED.raw_json,
         updated_at = CURRENT_TIMESTAMP;
 """
    
    try:
        cursor.executemany(query, prepared_data)
        conn.commit()
        logger.info(f"Successfully upserted {len(prepared_data)} records into steam.ods_games.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Database error during upsert: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


# ==========================================
# ОПРЕДЕЛЕНИЕ DAG
# ==========================================
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    dag_id='steam_games_catalog_etl',
    default_args=default_args,
    description='Incremental ETL for Steam Games Catalog (ODS Layer)',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['steam', 'ods', 'catalog', 'incremental'],
    params={
        'batch_size': Param(
            100, 
            type='integer', 
            description='Number of NEW games to process per run'
        ),
        'max_games_total': Param(
            10000, 
            type='integer', 
            description='Global limit: stop loading after reaching this number'
        ),
    },
    doc_md="""
    ### Steam Games Catalog ETL (Incremental)
    
    **Алгоритм работы:**
    1. Получает полный список AppID из Steam API
    2. Запрашивает из БД уже загруженные игры
    3. Вычисляет разницу (только новые игры)
    4. Берет батч из `batch_size` новых игр
    5. Проверяет глобальный лимит `max_games_total`
    6. Если лимит достигнут — DAG корректно завершается
    
    **Параметры:**
    - `batch_size`: размер батча за один запуск (по умолчанию 100)
    - `max_games_total`: глобальный лимит игр (по умолчанию 10000)
    """
) as dag:

    # Task 0: ShortCircuit - проверка глобального лимита
    task_check_limit = ShortCircuitOperator(
        task_id='check_global_limit',
        python_callable=check_global_limit,
    )

    # Task 1: Extract master list
    task_extract_list = PythonOperator(
        task_id='extract_master_app_list',
        python_callable=extract_master_app_list,
    )

    # Task 2: Anti-Join и выбор батча
    task_select_batch = PythonOperator(
        task_id='select_unprocessed_batch',
        python_callable=select_unprocessed_batch,
        op_kwargs={
            'master_file': "{{ ti.xcom_pull(task_ids='extract_master_app_list') }}"
        },
    )

    # Task 3: Fetch details
    task_fetch_details = PythonOperator(
        task_id='fetch_game_details',
        python_callable=fetch_game_details,
        op_kwargs={
            'batch_file_path': "{{ ti.xcom_pull(task_ids='select_unprocessed_batch') }}"
        },
    )

    # Task 4: Transform & Load
    task_load_db = PythonOperator(
        task_id='transform_and_load_to_postgres',
        python_callable=transform_and_load_to_postgres,
        op_kwargs={
            'details_file': "{{ ti.xcom_pull(task_ids='fetch_game_details') }}"
        },
    )

    # Pipeline Dependencies
    task_check_limit >> task_extract_list >> task_select_batch >> task_fetch_details >> task_load_db