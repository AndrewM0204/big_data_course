import csv
import json
import logging
import os
import random
import statistics
import time
from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from airflow.task.trigger_rule import TriggerRule
from psycopg2.extras import execute_values
from pymongo import MongoClient
from requests.auth import HTTPBasicAuth

# ==========================================
# КОНФИГУРАЦИЯ
# ==========================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_RAW_DIR = '/opt/airflow/data/raw/steam_benchmark'
BASE_PROC_DIR = '/opt/airflow/data/processed/steam_benchmark'

POSTGRES_CONN_ID = "steam_postgres"
MONGO_CONN_ID = "steam_mongo"
CLICKHOUSE_CONN_ID = "clickhouse_http"

# Изолированные объекты бенчмарка: ODS-таблицы (steam.ods_games, steam.ods_reviews,
# steam.player_stats) этот DAG не трогает
PG_TABLE = "bench.reviews"
MONGO_DB_NAME = "steam_bench"
MONGO_COLLECTION = "reviews"
CH_TABLE = "steam.bench_reviews"

DATASET_SEED = 42        # Фиксированный seed: прогоны воспроизводимы
APPID_POOL_SIZE = 500    # Число уникальных appid в синтетике (для GROUP BY)
APPID_BASE = 1000

WORDS = [
    "game", "fun", "great", "boring", "story", "graphics", "multiplayer", "bug", "crash",
    "price", "worth", "update", "combat", "music", "level", "map", "quest", "online",
    "легко", "интересно", "скучно", "отличная", "игра", "баги", "сюжет", "графика",
]
LANGUAGES = ["english", "russian", "schinese", "german", "french"]


# ==========================================
# ГЕНЕРАЦИЯ ДАТАСЕТА
# ==========================================
def generate_dataset_rows(n: int, seed: int = DATASET_SEED) -> list[dict]:
    """
    Детерминированно генерирует n синтетических отзывов той же структуры,
    что и steam.ods_reviews (упрощенный набор полей для бенчмарка).
    """
    rng = random.Random(seed)
    base_ts = 1704067200  # 2024-01-01 UTC
    span = 3 * 365 * 86400
    rows = []
    for i in range(n):
        appid = APPID_BASE + rng.randrange(APPID_POOL_SIZE)
        rows.append({
            "review_id": str(1_000_000_000 + i),
            "appid": appid,
            "game_name": f"Game {appid}",
            "language": rng.choice(LANGUAGES),
            "voted_up": rng.random() < 0.7,
            "votes_up": int(rng.expovariate(1 / 20)),
            "weighted_vote_score": round(rng.random(), 4),
            "review_text": " ".join(rng.choice(WORDS) for _ in range(rng.randint(5, 40))),
            "timestamp": base_ts + rng.randrange(span),
        })
    return rows


# ==========================================
# ХЕЛПЕРЫ ПОДКЛЮЧЕНИЯ
# ==========================================
def get_mongo_client() -> MongoClient:
    """Клиент MongoDB по Airflow Connection (учетные данные не логируются)."""
    conn = BaseHook.get_connection(MONGO_CONN_ID)
    extra = conn.extra_dejson or {}
    return MongoClient(
        host=conn.host,
        port=conn.port or 27017,
        username=conn.login,
        password=conn.password,
        authSource=extra.get('authSource', 'admin'),
        serverSelectionTimeoutMS=10000,
    )


def get_clickhouse_credentials() -> dict:
    """Credentials ClickHouse из Airflow Connection."""
    conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
    extra = conn.extra_dejson or {}
    return {
        "host": f"http://{conn.host}:{conn.port or 8123}",
        "user": conn.login,
        "password": conn.password,
        "database": extra.get("database", "steam"),
    }


def clickhouse_query(query: str, data: bytes | None = None, credentials: dict | None = None) -> str:
    """Запрос к ClickHouse через HTTP-интерфейс; ошибки сервера поднимаются исключением."""
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
        timeout=600,
    )
    text = resp.text.strip()
    if not resp.ok or text.startswith("Code:") or "DB::Exception" in text:
        raise RuntimeError(f"ClickHouse error (HTTP {resp.status_code}): {text}")
    return text


# ==========================================
# АДАПТЕРЫ СХРАНИЛИЩ
# Единый интерфейс: prepare (вне замера), reset, insert (замеряется), count, size, query.
# ==========================================
class PostgresBench:
    name = "postgres"

    def __init__(self):
        self.conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        with self.conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS bench;")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {PG_TABLE} (
                    review_id           TEXT PRIMARY KEY,
                    appid               BIGINT NOT NULL,
                    game_name           TEXT,
                    language            TEXT,
                    voted_up            BOOLEAN NOT NULL,
                    votes_up            INT NOT NULL,
                    weighted_vote_score DOUBLE PRECISION,
                    review_text         TEXT,
                    timestamp_created   TIMESTAMPTZ NOT NULL
                );
            """)
        self.conn.commit()

    def prepare(self, rows):
        return [
            (r["review_id"], r["appid"], r["game_name"], r["language"], r["voted_up"],
             r["votes_up"], r["weighted_vote_score"], r["review_text"],
             datetime.fromtimestamp(r["timestamp"], tz=timezone.utc))
            for r in rows
        ]

    def reset(self):
        with self.conn.cursor() as cur:
            cur.execute(f"TRUNCATE {PG_TABLE};")
        self.conn.commit()

    def insert(self, payload, batch):
        sql = f"""INSERT INTO {PG_TABLE} (review_id, appid, game_name, language, voted_up,
                  votes_up, weighted_vote_score, review_text, timestamp_created) VALUES %s"""
        with self.conn.cursor() as cur:
            for start in range(0, len(payload), batch):
                execute_values(cur, sql, payload[start:start + batch], page_size=batch)
        self.conn.commit()

    def count(self):
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {PG_TABLE};")
            return cur.fetchone()[0]

    def size(self):
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT pg_total_relation_size('{PG_TABLE}');")
            return cur.fetchone()[0]

    def query(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT appid, count(*), avg(weighted_vote_score)
                FROM {PG_TABLE} WHERE voted_up GROUP BY appid;
            """)
            return cur.fetchall()

    def close(self):
        self.conn.close()


class MongoBench:
    name = "mongodb"

    def __init__(self):
        self.client = get_mongo_client()
        self.db = self.client[MONGO_DB_NAME]
        self.coll = self.db[MONGO_COLLECTION]

    def prepare(self, rows):
        return [
            {
                "_id": r["review_id"],
                "appid": r["appid"],
                "game_name": r["game_name"],
                "language": r["language"],
                "voted_up": r["voted_up"],
                "votes_up": r["votes_up"],
                "weighted_vote_score": r["weighted_vote_score"],
                "review_text": r["review_text"],
                "timestamp_created": datetime.fromtimestamp(r["timestamp"], tz=timezone.utc),
            }
            for r in rows
        ]

    def reset(self):
        # drop сбрасывает и данные, и статистику хранения между прогонами
        self.coll.drop()

    def insert(self, payload, batch):
        for start in range(0, len(payload), batch):
            self.coll.insert_many(payload[start:start + batch], ordered=False)

    def count(self):
        return self.coll.count_documents({})

    def size(self):
        return int(self.db.command("collStats", MONGO_COLLECTION)["storageSize"])

    def query(self):
        return list(self.coll.aggregate([
            {"$match": {"voted_up": True}},
            {"$group": {"_id": "$appid", "n": {"$sum": 1}, "avg": {"$avg": "$weighted_vote_score"}}},
        ]))

    def close(self):
        self.client.close()


class ClickHouseBench:
    name = "clickhouse"

    def __init__(self):
        self.credentials = get_clickhouse_credentials()
        clickhouse_query(f"""
            CREATE TABLE IF NOT EXISTS {CH_TABLE} (
                review_id           String,
                appid               UInt32,
                game_name           String,
                language            String,
                voted_up            UInt8,
                votes_up            UInt32,
                weighted_vote_score Float64,
                review_text         String,
                timestamp_created   DateTime('UTC')
            )
            ENGINE = MergeTree
            ORDER BY tuple();
        """, credentials=self.credentials)

    def prepare(self, rows):
        return [
            {
                "review_id": r["review_id"],
                "appid": r["appid"],
                "game_name": r["game_name"],
                "language": r["language"],
                "voted_up": 1 if r["voted_up"] else 0,
                "votes_up": r["votes_up"],
                "weighted_vote_score": r["weighted_vote_score"],
                "review_text": r["review_text"],
                "timestamp_created": datetime.fromtimestamp(r["timestamp"], tz=timezone.utc)
                .strftime("%Y-%m-%d %H:%M:%S"),
            }
            for r in rows
        ]

    def reset(self):
        clickhouse_query(f"TRUNCATE TABLE {CH_TABLE}", credentials=self.credentials)

    def insert(self, payload, batch):
        for start in range(0, len(payload), batch):
            body = "\n".join(json.dumps(r, ensure_ascii=False) for r in payload[start:start + batch])
            clickhouse_query(
                f"INSERT INTO {CH_TABLE} FORMAT JSONEachRow",
                data=body.encode("utf-8"),
                credentials=self.credentials,
            )

    def count(self):
        return int(clickhouse_query(f"SELECT count() FROM {CH_TABLE}", credentials=self.credentials))

    def size(self):
        # Сумма активных частей; слияния не форсируем, чтобы не искажать замер
        text = clickhouse_query(
            "SELECT sum(bytes_on_disk) FROM system.parts "
            "WHERE database = 'steam' AND table = 'bench_reviews' AND active",
            credentials=self.credentials,
        )
        return int(text or 0)

    def query(self):
        return clickhouse_query(
            f"SELECT appid, count(), avg(weighted_vote_score) FROM {CH_TABLE} "
            "WHERE voted_up = 1 GROUP BY appid",
            credentials=self.credentials,
        )

    def close(self):
        pass


ENGINES = {
    "postgres": PostgresBench,
    "mongodb": MongoBench,
    "clickhouse": ClickHouseBench,
}


# ==========================================
# ФУНКЦИИ DAG
# ==========================================
def prepare_dataset(**context) -> str:
    """Генерирует датасет на максимальный размер из params.sizes и сохраняет JSONL."""
    ds = context['ds']
    max_size = max(int(s) for s in context['params']['sizes'])

    logger.info(f"Generating {max_size} synthetic reviews (seed={DATASET_SEED}).")
    rows = generate_dataset_rows(max_size)

    target_dir = os.path.join(BASE_RAW_DIR, ds)
    os.makedirs(target_dir, exist_ok=True)
    file_path = os.path.join(target_dir, 'dataset.jsonl')
    with open(file_path, 'w', encoding='utf-8') as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return file_path


def run_benchmark(engine: str, dataset_file: str, **context) -> str:
    """
    Для каждого размера N и каждого прогона: очистка, замер вставки N записей,
    проверка числа строк, размер хранилища, замер агрегирующего запроса.
    Результаты всех прогонов пишутся в JSON.
    """
    ds = context['ds']
    params = context['params']
    sizes = sorted(int(s) for s in params['sizes'])
    runs = int(params['runs'])
    batch = int(params['batch_size'])

    max_size = sizes[-1]
    with open(dataset_file, 'r', encoding='utf-8') as f:
        rows = []
        for i, line in enumerate(f):
            if i >= max_size:
                break
            rows.append(json.loads(line))

    adapter = ENGINES[engine]()
    results = []
    try:
        for n in sizes:
            for run in range(1, runs + 1):
                adapter.reset()
                # Подготовка данных вне замера: замеряем только работу хранилища
                payload = adapter.prepare(rows[:n])

                t0 = time.perf_counter()
                adapter.insert(payload, batch)
                insert_sec = time.perf_counter() - t0

                stored = adapter.count()
                if stored != n:
                    raise RuntimeError(f"{engine}: expected {n} rows, stored {stored}")

                storage_bytes = adapter.size()

                q0 = time.perf_counter()
                adapter.query()
                query_sec = time.perf_counter() - q0

                result = {
                    "engine": engine,
                    "rows": n,
                    "run": run,
                    "insert_sec": round(insert_sec, 4),
                    "rows_per_sec": round(n / insert_sec, 2),
                    "storage_bytes": storage_bytes,
                    "query_sec": round(query_sec, 4),
                }
                results.append(result)
                logger.info(f"{result}")
    finally:
        adapter.close()

    target_dir = os.path.join(BASE_PROC_DIR, ds)
    os.makedirs(target_dir, exist_ok=True)
    out_file = os.path.join(target_dir, f'{engine}_runs.json')
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    return out_file


def cleanup_benchmark_tables(**context) -> None:
    """
    Удаляет таблицы/коллекции бенчмарка во всех трех хранилищах.
    Результаты замеров уже сохранены в JSON/CSV, поэтому данные в БД больше не нужны.
    Выполняется даже при падении предыдущих тасков (trigger_rule=all_done).
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(f"DROP TABLE IF EXISTS {PG_TABLE};")
    logger.info(f"Dropped PostgreSQL table {PG_TABLE}.")

    client = get_mongo_client()
    try:
        client[MONGO_DB_NAME].drop_collection(MONGO_COLLECTION)
        logger.info(f"Dropped MongoDB collection {MONGO_DB_NAME}.{MONGO_COLLECTION}.")
    finally:
        client.close()

    clickhouse_query(f"DROP TABLE IF EXISTS {CH_TABLE} SYNC", credentials=get_clickhouse_credentials())
    logger.info(f"Dropped ClickHouse table {CH_TABLE}.")


def aggregate_results(postgres_file: str, mongo_file: str, clickhouse_file: str, **context) -> str:
    """Собирает результаты всех прогонов в CSV и считает медианы по (engine, rows)."""
    ds = context['ds']
    all_runs = []
    for path in (postgres_file, mongo_file, clickhouse_file):
        with open(path, 'r', encoding='utf-8') as f:
            all_runs.extend(json.load(f))

    target_dir = os.path.join(BASE_PROC_DIR, ds)
    runs_csv = os.path.join(target_dir, 'benchmark_runs.csv')
    summary_csv = os.path.join(target_dir, 'benchmark_summary.csv')

    run_fields = ["engine", "rows", "run", "insert_sec", "rows_per_sec", "storage_bytes", "query_sec"]
    with open(runs_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=run_fields)
        writer.writeheader()
        writer.writerows(all_runs)

    groups: dict[tuple, list] = {}
    for r in all_runs:
        groups.setdefault((r["engine"], r["rows"]), []).append(r)

    summary_fields = ["engine", "rows", "runs", "insert_sec_median", "rows_per_sec_median",
                      "storage_bytes_median", "query_sec_median"]
    with open(summary_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=summary_fields)
        writer.writeheader()
        for (engine, rows), items in sorted(groups.items(), key=lambda kv: (kv[0][1], kv[0][0])):
            writer.writerow({
                "engine": engine,
                "rows": rows,
                "runs": len(items),
                "insert_sec_median": round(statistics.median(i["insert_sec"] for i in items), 4),
                "rows_per_sec_median": round(statistics.median(i["rows_per_sec"] for i in items), 2),
                "storage_bytes_median": int(statistics.median(i["storage_bytes"] for i in items)),
                "query_sec_median": round(statistics.median(i["query_sec"] for i in items), 4),
            })

    logger.info(f"Benchmark summary written to {summary_csv}")
    return summary_csv


# ==========================================
# ОПРЕДЕЛЕНИЕ DAG
# ==========================================
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    # Повтор целого прогона исказил бы замеры, поэтому без retries
    'retries': 0,
    'execution_timeout': timedelta(hours=3),
}

with DAG(
    dag_id='steam_storage_benchmark',
    default_args=default_args,
    description='Benchmark PostgreSQL, MongoDB and ClickHouse on synthetic review inserts',
    schedule=None,  # Ручной запуск: бенчмарк не должен идти по расписанию
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['steam', 'benchmark', 'storage'],
    params={
        'sizes': Param(
            [10000, 50000, 100000],
            type='array',
            description='Dataset sizes (number of rows) to insert per run'
        ),
        'runs': Param(
            3,
            type='integer',
            minimum=1,
            maximum=10,
            description='Repetitions per (engine, size); summary uses median'
        ),
        'batch_size': Param(
            5000,
            type='integer',
            minimum=100,
            maximum=50000,
            description='Rows per insert batch (same for all engines)'
        ),
    },
    doc_md="""
    ### Storage Benchmark: PostgreSQL vs MongoDB vs ClickHouse

    Сравнивает три хранилища ODS-слоя на синтетических отзывах одинаковой структуры.

    **Что измеряется для каждого (engine, size, run):**
    - `insert_sec`, `rows_per_sec` — время вставки N записей батчами (без подготовки данных);
    - `storage_bytes` — размер на диске: `pg_total_relation_size`, `collStats.storageSize`,
      `system.parts.bytes_on_disk` (без принудительного слияния);
    - `query_sec` — агрегация: количество и средняя оценка по appid для положительных отзывов.

    **Изоляция:** `bench.reviews` (PostgreSQL), `steam_bench.reviews` (MongoDB),
    `steam.bench_reviews` (ClickHouse). ODS-таблицы не затрагиваются.

    **Ограничения, которые нужно учитывать при интерпретации:**
    - Ключи: PostgreSQL — `PRIMARY KEY (review_id)`, MongoDB — `_id` = `review_id`,
      ClickHouse — `MergeTree ORDER BY tuple()` (без ключа сортировки). Сравнение не полностью
      эквивалентно: индекс PostgreSQL и `_id` MongoDB участвуют в вставке, у ClickHouse этой
      нагрузки нет, но и агрегация по `appid` выполняется без пропуска по индексу.
    - Вторичных индексов, кроме первичных ключей, не создаётся ни в одном хранилище.
    - Данные синтетические, одинаковые для всех хранилищ (seed=42).
    - Сетевые накладные расходы клиента входят в замер, потому что все три клиента ходят по сети в контейнерах.

    **Результаты:** `data/processed/steam_benchmark/<ds>/benchmark_summary.csv` (медианы)
    и `benchmark_runs.csv` (все прогоны).

    **Очистка:** последний таск `cleanup_benchmark_tables` удаляет таблицы/коллекции
    бенчмарка во всех трех хранилищах (`trigger_rule=all_done`, выполняется и при падении замеров).
    Сохраняются только файлы с результатами.
    """
) as dag:

    task_prepare = PythonOperator(
        task_id='prepare_dataset',
        python_callable=prepare_dataset,
    )

    task_bench_pg = PythonOperator(
        task_id='bench_postgres',
        python_callable=run_benchmark,
        op_kwargs={
            'engine': 'postgres',
            'dataset_file': "{{ ti.xcom_pull(task_ids='prepare_dataset') }}",
        },
    )

    task_bench_mongo = PythonOperator(
        task_id='bench_mongodb',
        python_callable=run_benchmark,
        op_kwargs={
            'engine': 'mongodb',
            'dataset_file': "{{ ti.xcom_pull(task_ids='prepare_dataset') }}",
        },
    )

    task_bench_ch = PythonOperator(
        task_id='bench_clickhouse',
        python_callable=run_benchmark,
        op_kwargs={
            'engine': 'clickhouse',
            'dataset_file': "{{ ti.xcom_pull(task_ids='prepare_dataset') }}",
        },
    )

    task_aggregate = PythonOperator(
        task_id='aggregate_results',
        python_callable=aggregate_results,
        op_kwargs={
            'postgres_file': "{{ ti.xcom_pull(task_ids='bench_postgres') }}",
            'mongo_file': "{{ ti.xcom_pull(task_ids='bench_mongodb') }}",
            'clickhouse_file': "{{ ti.xcom_pull(task_ids='bench_clickhouse') }}",
        },
    )

    # Таблицы бенчмарка удаляем в конце при любом исходе (включая падение замеров)
    task_cleanup = PythonOperator(
        task_id='cleanup_benchmark_tables',
        python_callable=cleanup_benchmark_tables,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Движки идут последовательно: параллельные вставки мешали бы замерам друг друга
    task_prepare >> task_bench_pg >> task_bench_mongo >> task_bench_ch >> task_aggregate >> task_cleanup
