import json
import logging
import os
import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import (
    PythonOperator,
    ShortCircuitOperator,
)
from airflow.sdk import Param

# ==========================================
# КОНФИГУРАЦИЯ
# ==========================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_RAW_DIR = '/opt/airflow/data/raw/steam_catalog'
BASE_PROC_DIR = '/opt/airflow/data/processed/steam_catalog'

STEAM_APP_LIST_URL = "https://api.steampowered.com/IStoreService/GetAppList/v1/"
STEAM_APP_DETAILS_URL = "https://store.steampowered.com/api/appdetails/"

POSTGRES_CONNECTION_ID = "steam_postgres"

# ==========================================
# ФУНКЦИИ DAG
# ==========================================

def check_global_limit(**context) -> bool:
    """
    ShortCircuit: Проверяет, не превышен ли глобальный лимит игр.
    Читает результат из XCom от SQLExecuteQueryOperator.
    """
    max_games = context['params'].get('max_games_total', 10000)
    
    # Получаем COUNT(*) из предыдущего SQLExecuteQueryOperator
    count_result = context['ti'].xcom_pull(task_ids='count_existing_games')
    if count_result and len(count_result) > 0:
        current_count = count_result[0][0]
    else:
        current_count = 0
    
    logger.info(f"Current games in DB: {current_count}, Max allowed: {max_games}")
    
    if current_count >= max_games:
        logger.info(f"Global limit reached ({current_count} >= {max_games}). Stopping DAG.")
        return False
        
    return True


def extract_master_app_list(**context) -> str:
    """
    Скачивает список AppID из Steam API (IStoreService) с пагинацией.
    """
    ds = context['ds']
    target_dir = os.path.join(BASE_RAW_DIR, ds)
    os.makedirs(target_dir, exist_ok=True)
    file_path = os.path.join(target_dir, 'master_app_list.json')

    api_key = os.environ.get('STEAM_API_KEY')
    if not api_key:
        raise ValueError("STEAM_API_KEY does not found in the enviroment.")

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
    Anti-Join паттерн.
    Читает существующие appids из XCom,
    вычисляет разницу и формирует батч.
    """
    ds = context['ds']
    batch_size = context['params'].get('batch_size', 100)
    max_games = context['params'].get('max_games_total', 10000)
    
    # 1. Читаем master list
    with open(master_file, 'r') as f:
        all_appids: list[int] = json.load(f)
    
    # 2. Получаем уже загруженные игры из XCom
    existing_rows = context['ti'].xcom_pull(task_ids='get_existing_appids') or []
    existing_appids: set[int] = {row[0] for row in existing_rows}
    
    failed_rows = context['ti'].xcom_pull(task_ids='get_failed_appids') or []
    failed_appids: set[int] = {row[0] for row in failed_rows}
    
    # Получаем текущий count из XCom
    count_result = context['ti'].xcom_pull(task_ids='count_existing_games')
    current_count = count_result[0][0] if count_result else 0
    
    # 3. Anti-Join: вычитаем уже загруженные из master list
    unprocessed_appids = [
        appid for appid in all_appids 
        if appid not in existing_appids and appid not in failed_appids
    ]
    logger.info(f"Unprocessed games remaining: {len(unprocessed_appids)}")
    
    # 4. Учитываем глобальный лимит
    remaining_capacity = max_games - current_count
    effective_batch_size = min(batch_size, remaining_capacity, len(unprocessed_appids))
    
    if effective_batch_size <= 0:
        logger.warning("No games to process (capacity exhausted or all loaded)")
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
    
    failed_appids = []

    for i, appid in enumerate(appids):
        url = f"{STEAM_APP_DETAILS_URL}?appids={appid}&l=english&cc=us"
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if str(appid) in data and data[str(appid)]['success']:
                raw_details[appid] = data[str(appid)]['data']
            else:
                logger.warning(f"AppID {appid} not found or unsuccessful.")
                failed_appids.append(appid)
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error for appid {appid}: {e}")
            failed_appids.append(appid)
            
        if (i + 1) % 10 == 0:
            logger.info(f"Progress: {i + 1}/{total} games processed...")
        time.sleep(1.2)

    target_dir = os.path.join(BASE_RAW_DIR, ds, 'details')
    os.makedirs(target_dir, exist_ok=True)
    details_file = os.path.join(target_dir, 'raw_details.json')
    
    with open(details_file, 'w', encoding='utf-8') as f:
        json.dump(raw_details, f, ensure_ascii=False)

    if failed_appids:
            failed_file = os.path.join(target_dir, 'failed_appids.json')
            with open(failed_file, 'w') as f:
                json.dump(failed_appids, f)
            logger.warning(f"Saved {len(failed_appids)} failed appids to {failed_file}")

    logger.info(f"Saved details for {len(raw_details)} games.")
    return details_file


def transform_game_details(details_file: str, **context) -> str:
    """
    Парсит сырые данные и готовит их для загрузки в БД.
    """
    with open(details_file, 'r', encoding='utf-8') as f:
        raw_details = json.load(f)
    
    if not raw_details:
        logger.info("No data to transform.")
        out_file = os.path.join(os.path.dirname(details_file), 'prepared_data.json')
        with open(out_file, 'w') as f:
            json.dump([], f)
        return out_file
        
    prepared_data = []
    for appid, game in raw_details.items():
        is_free = game.get('is_free', False)
        
        price_overview = game.get('price_overview') or {}
        price_cents = price_overview.get('final', 0) if not is_free else 0
        price = float(price_cents) / 100.0
        currency = price_overview.get('currency', 'USD') if not is_free else 'USD'
        
        developers = ", ".join(game.get('developers') or []) or None
        publishers = ", ".join(game.get('publishers') or []) or None
        genres = ", ".join([g.get('description', '') for g in (game.get('genres') or [])]) or None
        
        release_date_obj = game.get('release_date') or {}
        release_date = release_date_obj.get('date') or None
        
        short_desc = game.get('short_description', '')
        raw_json_str = json.dumps(game)
        
        prepared_data.append({
            'appid': int(appid),
            'name': game.get('name'),
            'is_free': is_free,
            'price': price,
            'developers': developers,
            'publishers': publishers,
            'genres': genres,
            'release_date': release_date,
            'short_description': short_desc,
            'raw_json': raw_json_str,
            'currency': currency
        })
        
    logger.info(f"Transformed {len(prepared_data)} games into prepared format.")
    
    out_file = os.path.join(os.path.dirname(details_file), 'prepared_data.json')
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(prepared_data, f, ensure_ascii=False, indent=2)
        
    return out_file

def load_to_postgres(prepared_file: str, **context):
    """
    Загружает подготовленные данные в PostgreSQL (Bulk Upsert).
    """
    
    with open(prepared_file, 'r', encoding='utf-8') as f:
        prepared_data = json.load(f)
    
    if not prepared_data:
        logger.info("No data to load.")
        return
        
    failed_file = os.path.join(os.path.dirname(prepared_file), 'failed_appids.json')
    failed_appids = []
    if os.path.exists(failed_file):
        with open(failed_file, 'r') as f:
            failed_appids = json.load(f)
        logger.info(f"Found {len(failed_appids)} failed appids to save")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
    conn = hook.get_conn()
    
    query = """
    INSERT INTO steam.ods_games (
        appid, name, is_free, price, developers, publishers, 
        genres, release_date, short_description, raw_json, currency
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        currency = EXCLUDED.currency,
        updated_at = CURRENT_TIMESTAMP;
    """
    
    data_tuples = [
        (
            row['appid'], row['name'], row['is_free'], row['price'],
            row['developers'], row['publishers'], row['genres'],
            row['release_date'], row['short_description'],
            row['raw_json'], row['currency']
        )
        for row in prepared_data
    ]
    
    try:
        cursor = conn.cursor()
        cursor.executemany(query, data_tuples)
        conn.commit()
        logger.info(f"Successfully upserted {len(data_tuples)} records into steam.ods_games.")
        
        if failed_appids:
            try:
                params_list = [(appid,) for appid in failed_appids]
                
                cursor.executemany("""
                    INSERT INTO steam.failed_appids (appid, failed_at, retry_count)
                    VALUES (%s, CURRENT_TIMESTAMP, 1)
                    ON CONFLICT (appid) DO UPDATE SET 
                        retry_count = steam.failed_appids.retry_count + 1,
                        failed_at = CURRENT_TIMESTAMP
                """, params_list)
                
                conn.commit()
                logger.info(f"Saved {len(failed_appids)} failed appids to steam.failed_appids")
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to save failed_appids: {e}")
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

    # Проверка глобального лимита
    task_count_games = SQLExecuteQueryOperator(
        task_id='count_existing_games',
        conn_id=POSTGRES_CONNECTION_ID,
        sql='SELECT COUNT(*) FROM steam.ods_games;',
    )

    task_check_limit = ShortCircuitOperator(
        task_id='check_global_limit',
        python_callable=check_global_limit,
    )

    # Task 1: Extract master list
    task_extract_list = PythonOperator(
        task_id='extract_master_app_list',
        python_callable=extract_master_app_list,
    )

    #Task 2: Anti-Join и выбор батча
    task_get_existing = SQLExecuteQueryOperator(
        task_id='get_existing_appids',
        conn_id=POSTGRES_CONNECTION_ID,
        sql='SELECT appid FROM steam.ods_games;',
    )

    # Получение failed appids
    task_get_failed = SQLExecuteQueryOperator(
        task_id='get_failed_appids',
        conn_id=POSTGRES_CONNECTION_ID,
        sql='SELECT appid FROM steam.failed_appids WHERE retry_count >= 3;',
    )

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

    # Task 4: transform
    task_transform = PythonOperator(
        task_id='transform_game_details',
        python_callable=transform_game_details,
        op_kwargs={
            'details_file': "{{ ti.xcom_pull(task_ids='fetch_game_details') }}"
        },
    )

    # Task 5: load
    task_load_db = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        op_kwargs={
            'prepared_file': "{{ ti.xcom_pull(task_ids='transform_game_details') }}"
        },
    )

    # Pipeline Dependencies
    task_count_games >> task_check_limit
    task_check_limit >> task_extract_list
    task_check_limit >> task_get_existing
    task_check_limit >> task_get_failed

    [task_extract_list, task_get_existing, task_get_failed] >> task_select_batch
    task_select_batch >> task_fetch_details >> task_transform >> task_load_db