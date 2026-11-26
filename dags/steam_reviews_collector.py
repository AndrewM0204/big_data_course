import html
import json
import logging
import os
import re
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

BASE_RAW_DIR = '/opt/airflow/data/raw/steam_reviews'
BASE_PROC_DIR = '/opt/airflow/data/processed/steam_reviews'

# Актуальный эндпоинт (старый store.steampowered.com/appreviews помечен как устаревший)
STEAM_REVIEWS_URL = "https://api.steampowered.com/IUserReviewsService/GetAppReviews/v1/"

# Enum-значения IUserReviewsService
FILTER_RECENT = 1          # k_EUserReviewsAppReviewsFilter_Recent: стабильная пагинация по cursor
REVIEW_TYPE_ALL = 0        # k_EUserReviewsReviewType_All
PURCHASE_TYPE_ALL = 1      # k_EUserReviewsPurchaseType_All

NUM_PER_PAGE = 100         # Максимум API за одну страницу
MAX_RETRIES_429 = 3        # Повторы при HTTP 429 с экспоненциальной задержкой

POSTGRES_CONN_ID = "steam_postgres"

# Управляющие символы, кроме \t, \n, \r
CONTROL_CHARS_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')
HTML_TAG_RE = re.compile(r'<[^>]+>')
WHITESPACE_RE = re.compile(r'\s+')


# ==========================================
# ХЕЛПЕРЫ
# ==========================================
def _ts_to_iso(ts) -> str | None:
    """Unix timestamp -> ISO-строка UTC. Для пустого значения возвращает None."""
    if not ts:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()


def clean_review_text(text: str) -> str:
    """Удаляет HTML-теги, HTML-сущности и управляющие символы, схлопывает пробелы."""
    if not text:
        return ""
    text = html.unescape(text)
    text = HTML_TAG_RE.sub(' ', text)
    text = CONTROL_CHARS_RE.sub('', text)
    return WHITESPACE_RE.sub(' ', text).strip()


def fetch_app_reviews_page(appid: int, cursor: str, max_retries: int = MAX_RETRIES_429) -> dict:
    """
    Запрашивает одну страницу отзывов.
    Параметры IUserReviewsService передаются JSON-строкой в input_json.
    При HTTP 429 повторяет запрос с экспоненциальной задержкой.
    """
    input_json = {
        "appid": appid,
        "filter": FILTER_RECENT,
        "languages": ["all"],
        "review_type": REVIEW_TYPE_ALL,
        "purchase_type": PURCHASE_TYPE_ALL,
        "num_per_page": NUM_PER_PAGE,
        "cursor": cursor,
    }

    for attempt in range(max_retries + 1):
        resp = requests.get(
            STEAM_REVIEWS_URL,
            params={"input_json": json.dumps(input_json)},
            timeout=15,
        )
        if resp.status_code == 429 and attempt < max_retries:
            delay = 2 ** (attempt + 1)
            logger.warning(f"HTTP 429 for appid {appid}, retry in {delay}s (attempt {attempt + 1})")
            time.sleep(delay)
            continue
        resp.raise_for_status()
        data = resp.json()
        return data.get('response', data)

    raise requests.exceptions.RetryError(f"Rate limit persists for appid {appid}")


# ==========================================
# ФУНКЦИИ DAG
# ==========================================
def select_top_games(**context) -> str:
    """
    Выбирает топ-N игр из каталога steam.ods_games по числу рекомендаций
    (raw_json.recommendations.total). Без значения — в конец, затем по appid.
    """
    ds = context['ds']
    top_n = int(context['params'].get('top_n', 100))

    query = """
        SELECT appid, name
        FROM steam.ods_games
        ORDER BY
            (raw_json #>> '{recommendations,total}')::bigint DESC NULLS LAST,
            appid
        LIMIT %s;
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(query, (top_n,))
        rows = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    if not rows:
        raise AirflowSkipException("steam.ods_games is empty. Run steam_games_catalog_etl first.")

    top_games = [{"appid": int(row[0]), "name": row[1]} for row in rows]
    logger.info(f"Selected top {len(top_games)} games by recommendations for {ds}.")

    target_dir = os.path.join(BASE_RAW_DIR, ds)
    os.makedirs(target_dir, exist_ok=True)
    file_path = os.path.join(target_dir, 'top_games.json')
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(top_games, f, ensure_ascii=False)

    return file_path


def fetch_reviews(top_games_file: str, **context) -> str:
    """
    Для каждой игры листает отзывы по cursor, не более max_pages_per_game страниц.
    Сохраняет сырые ответы API: {appid: [review, ...]}.
    """
    ds = context['ds']
    params = context['params']
    max_pages = int(params.get('max_pages_per_game', 3))
    request_delay = float(params.get('request_delay', 1.5))

    with open(top_games_file, 'r', encoding='utf-8') as f:
        top_games = json.load(f)

    raw_reviews = {}
    failed_appids = []
    total = len(top_games)

    for i, game in enumerate(top_games, start=1):
        appid = game['appid']
        reviews = []
        cursor = '*'
        seen_cursors = set()

        try:
            for _ in range(max_pages):
                page = fetch_app_reviews_page(appid, cursor)
                page_reviews = page.get('reviews', [])
                next_cursor = page.get('cursor')

                reviews.extend(page_reviews)

                # Конец выдачи: пустая страница, cursor не сменился или повторился
                if not page_reviews or not next_cursor or next_cursor in seen_cursors or next_cursor == cursor:
                    break
                seen_cursors.add(next_cursor)
                cursor = next_cursor
                time.sleep(request_delay)

            raw_reviews[appid] = reviews
        except (requests.exceptions.RequestException, ValueError) as e:
            # ValueError покрывает ошибки разбора JSON
            logger.error(f"Failed to fetch reviews for appid {appid}: {e}")
            failed_appids.append(appid)

        if i % 10 == 0:
            logger.info(f"Progress: {i}/{total} games processed.")

        time.sleep(request_delay)

    target_dir = os.path.join(BASE_RAW_DIR, ds)
    os.makedirs(target_dir, exist_ok=True)
    out_file = os.path.join(target_dir, 'reviews_raw.json')
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(raw_reviews, f, ensure_ascii=False)

    if failed_appids:
        with open(os.path.join(target_dir, 'failed_appids.json'), 'w') as f:
            json.dump(failed_appids, f)
        logger.warning(f"Failed to fetch reviews for {len(failed_appids)} games.")

    total_reviews = sum(len(v) for v in raw_reviews.values())
    logger.info(f"Saved {total_reviews} raw reviews for {len(raw_reviews)}/{total} games.")
    return out_file


def transform_reviews(raw_file: str, **context) -> str:
    """
    Очищает текст, переводит Unix timestamp в datetime UTC,
    добавляет appid. Пишет JSONL.
    """
    ds = context['ds']

    with open(raw_file, 'r', encoding='utf-8') as f:
        raw_reviews = json.load(f)

    out_dir = os.path.join(BASE_PROC_DIR, ds)
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, 'reviews_clean.jsonl')

    count = 0
    with open(out_file, 'w', encoding='utf-8') as out:
        for appid_str, reviews in raw_reviews.items():
            appid = int(appid_str)
            for rv in reviews:
                rec_id = rv.get('recommendationid')
                if rec_id is None:
                    continue
                author = rv.get('author') or {}
                doc = {
                    "review_id": str(rec_id),  # uint64 — храним строкой, без потери точности
                    "appid": appid,
                    "language": rv.get('language'),
                    "review_text": clean_review_text(rv.get('review', '')),
                    "voted_up": bool(rv.get('voted_up', False)),
                    "votes_up": int(rv.get('votes_up', 0)),
                    "votes_funny": int(rv.get('votes_funny', 0)),
                    "weighted_vote_score": float(rv.get('weighted_vote_score', 0) or 0),
                    "comment_count": int(rv.get('comment_count', 0)),
                    "steam_purchase": bool(rv.get('steam_purchase', False)),
                    "received_for_free": bool(rv.get('received_for_free', False)),
                    "written_during_early_access": bool(rv.get('written_during_early_access', False)),
                    "refunded": bool(rv.get('refunded', False)),
                    "timestamp_created": _ts_to_iso(rv.get('timestamp_created')),
                    "timestamp_updated": _ts_to_iso(rv.get('timestamp_updated')),
                    "author_steamid": str(author.get('steamid', '')) or None,
                    "author_num_reviews": int(author.get('num_reviews', 0) or 0),
                    "author_playtime_forever": int(author.get('playtime_forever', 0) or 0),
                    "author_playtime_at_review": int(author.get('playtime_at_review', 0) or 0),
                    "author_last_played": _ts_to_iso(author.get('last_played')),
                }
                out.write(json.dumps(doc, ensure_ascii=False) + "\n")
                count += 1

    logger.info(f"Transformed {count} reviews into {out_file}.")
    return out_file


def load_reviews_postgres(clean_file: str, **context) -> None:
    """
    Загрузка отзывов в PostgreSQL (steam.ods_reviews).
    Дубликаты внутри одного запуска отбрасываются, повторная загрузка обновляет
    существующую запись по review_id.
    """
    docs = {}
    with open(clean_file, 'r', encoding='utf-8') as f:
        for line in f:
            doc = json.loads(line)
            for key in ('timestamp_created', 'timestamp_updated', 'author_last_played'):
                if doc.get(key):
                    doc[key] = datetime.fromisoformat(doc[key])
            docs.setdefault(doc['review_id'], doc)

    if not docs:
        logger.info("No reviews to load into PostgreSQL.")
        return

    query = """
    INSERT INTO steam.ods_reviews (
        review_id, appid, language, review_text, voted_up,
        votes_up, votes_funny, weighted_vote_score, comment_count,
        steam_purchase, received_for_free, written_during_early_access, refunded,
        timestamp_created, timestamp_updated,
        author_steamid, author_num_reviews, author_playtime_forever,
        author_playtime_at_review, author_last_played
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (review_id) DO UPDATE SET
        appid = EXCLUDED.appid,
        language = EXCLUDED.language,
        review_text = EXCLUDED.review_text,
        voted_up = EXCLUDED.voted_up,
        votes_up = EXCLUDED.votes_up,
        votes_funny = EXCLUDED.votes_funny,
        weighted_vote_score = EXCLUDED.weighted_vote_score,
        comment_count = EXCLUDED.comment_count,
        steam_purchase = EXCLUDED.steam_purchase,
        received_for_free = EXCLUDED.received_for_free,
        written_during_early_access = EXCLUDED.written_during_early_access,
        refunded = EXCLUDED.refunded,
        timestamp_created = EXCLUDED.timestamp_created,
        timestamp_updated = EXCLUDED.timestamp_updated,
        author_steamid = EXCLUDED.author_steamid,
        author_num_reviews = EXCLUDED.author_num_reviews,
        author_playtime_forever = EXCLUDED.author_playtime_forever,
        author_playtime_at_review = EXCLUDED.author_playtime_at_review,
        author_last_played = EXCLUDED.author_last_played;
    """

    rows = [
        (
            doc['review_id'], doc['appid'], doc.get('language'),
            doc.get('review_text'), doc.get('voted_up'), doc.get('votes_up'),
            doc.get('votes_funny'), doc.get('weighted_vote_score'), doc.get('comment_count'),
            doc.get('steam_purchase'), doc.get('received_for_free'),
            doc.get('written_during_early_access'), doc.get('refunded'),
            doc.get('timestamp_created'), doc.get('timestamp_updated'),
            doc.get('author_steamid'), doc.get('author_num_reviews'),
            doc.get('author_playtime_forever'), doc.get('author_playtime_at_review'),
            doc.get('author_last_played'),
        )
        for doc in docs.values()
    ]

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        for start in range(0, len(rows), 1000):
            cursor.executemany(query, rows[start:start + 1000])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info(f"Loaded {len(rows)} reviews into steam.ods_reviews.")


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
    dag_id='steam_reviews_collector',
    default_args=default_args,
    description='Collect Steam user reviews for top games into PostgreSQL (ODS Layer)',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['steam', 'ods', 'reviews', 'postgres'],
    params={
        'top_n': Param(
            100,
            type='integer',
            minimum=1,
            maximum=100,
            description='Number of top games (by recommendations) to collect reviews for'
        ),
        'max_pages_per_game': Param(
            3,
            type='integer',
            minimum=1,
            maximum=50,
            description='Max review pages per game per run (100 reviews per page)'
        ),
        'request_delay': Param(
            1.5,
            type='number',
            minimum=0.5,
            maximum=10.0,
            description='Delay between API requests (seconds)'
        ),
    },
    doc_md="""
    ### Steam Reviews Collector

    Собирает пользовательские отзывы для топ-N игр из `steam.ods_games`
    через `IUserReviewsService/GetAppReviews` и загружает в PostgreSQL `steam.ods_reviews`.

    **Идемпотентность:** upsert по `review_id` = `recommendationid`.

    **Параметры:** `top_n`, `max_pages_per_game`, `request_delay`.
    """
) as dag:

    task_select = PythonOperator(
        task_id='select_top_games',
        python_callable=select_top_games,
    )

    task_fetch = PythonOperator(
        task_id='fetch_reviews',
        python_callable=fetch_reviews,
        op_kwargs={
            'top_games_file': "{{ ti.xcom_pull(task_ids='select_top_games') }}"
        },
    )

    task_transform = PythonOperator(
        task_id='transform_reviews',
        python_callable=transform_reviews,
        op_kwargs={
            'raw_file': "{{ ti.xcom_pull(task_ids='fetch_reviews') }}"
        },
    )

    task_load = PythonOperator(
        task_id='load_reviews_postgres',
        python_callable=load_reviews_postgres,
        op_kwargs={
            'clean_file': "{{ ti.xcom_pull(task_ids='transform_reviews') }}"
        },
    )

    task_select >> task_fetch >> task_transform >> task_load
