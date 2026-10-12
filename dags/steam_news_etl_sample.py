import json
import os
from datetime import datetime

import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient

# --- Конфигурация подключений ---
MONGO_URI = 'mongodb://root:example@mongo:27017/'
MONGO_DB = 'steam_ods'

def extract_steam_news(**kwargs):
    """Extract: Получаем последние новости Dota 2 из Steam API."""
    api_key = os.environ.get('STEAM_API_KEY', '')
    
    url = "https://api.steampowered.com/ISteamNews/GetNewsForApp/v0002/"
    params = {
        'appid': 570, # Dota 2
        'count': 5,
        'maxlength': 300,
        'format': 'json',
        'key': api_key
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    # Сохраняем во временный файл внутри контейнера для передачи между тасками
    ds_nodash = kwargs['ds_nodash']
    file_path = f"/opt/airflow/data/steam_news_{ds_nodash}.json"
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
        
    print(f"✅ Extracted data saved to {file_path}")
    return file_path

def load_to_mongo(**kwargs):
    """Load 1: Загрузка сырых JSON документов в MongoDB."""
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='extract_news')
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db['raw_news']
    
    news_items = data.get('appnews', {}).get('newsitems', [])
    for item in news_items:
        item['loaded_at'] = datetime.utcnow().isoformat()
        item['source'] = 'steam_api'
        
    if news_items:
        collection.insert_many(news_items)
        print(f"✅ Loaded {len(news_items)} items to MongoDB")

def load_to_postgres(**kwargs):
    """Load 2: Загрузка структурированных данных в PostgreSQL."""
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='extract_news')
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    news_items = data.get('appnews', {}).get('newsitems', [])
    
    conn = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow")
    cursor = conn.cursor()
    
    # Создаем таблицу, если её нет
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS steam_news_ods (
            gid VARCHAR(255) PRIMARY KEY,
            title VARCHAR(255),
            url TEXT,
            author VARCHAR(100),
            feedlabel VARCHAR(100),
            published_at TIMESTAMP,
            contents TEXT,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Используем ON CONFLICT DO NOTHING для идемпотентности (защита от дублей)
    for item in news_items:
        cursor.execute("""
            INSERT INTO steam_news_ods (gid, title, url, author, feedlabel, published_at, contents)
            VALUES (%s, %s, %s, %s, %s, to_timestamp(%s), %s)
            ON CONFLICT (gid) DO NOTHING
        """, (
            item.get('gid'), item.get('title'), item.get('url'), 
            item.get('author'), item.get('feedlabel'), 
            item.get('date'), item.get('contents')
        ))
        
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Loaded {len(news_items)} items to PostgreSQL")

# --- Настройки DAG ---
default_args = {'owner': 'airflow', 'retries': 1}

with DAG(
    dag_id='steam_news_etl_sample',
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule=None, # <-- ИСПРАВЛЕНО: в Airflow 3.x используется schedule вместо schedule_interval
    catchup=False,
    tags=['steam', 'ods', 'sample'],
) as dag:

    extract_task = PythonOperator(task_id='extract_news', python_callable=extract_steam_news)
    load_mongo_task = PythonOperator(task_id='load_to_mongo', python_callable=load_to_mongo)
    load_postgres_task = PythonOperator(task_id='load_to_postgres', python_callable=load_to_postgres)

    # Extract запускается первым, затем параллельно работают две загрузки
    extract_task >> [load_mongo_task, load_postgres_task]