#!/usr/bin/env python3
"""Создание datasets, charts и dashboard в Superset по API.

Скрипт рассчитан на запуск внутри контейнера Superset:
    python /tmp/bootstrap_superset.py

Секреты читаются только из окружения и не печатаются:
  SUPERSET_ADMIN_USERNAME / SUPERSET_ADMIN_PASSWORD
  SUPERSET_READER_PASSWORD (или готовый SUPERSET_MARTS_URI)

Внимание: совместимость полей REST API и типов графиков с Superset 4.1.2
здесь не проверялась запуском. Ошибки API выводятся с маскированием секретов,
скрипт завершается ненулевым кодом, если что-то не создано.
"""

import http.cookiejar
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


BASE_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088").rstrip("/")
ADMIN_USER = (
    os.environ.get("SUPERSET_ADMIN_USERNAME")
    or os.environ.get("LR3_SUPERSET_ADMIN_USER")
    or os.environ.get("SUPERSET_ADMIN_USER")
)
ADMIN_PASSWORD = (
    os.environ.get("SUPERSET_ADMIN_PASSWORD")
    or os.environ.get("LR3_SUPERSET_ADMIN_PASSWORD")
)
READER_USER = os.environ.get("SUPERSET_READER_USER", "superset_reader")
if READER_USER != "superset_reader":
    raise SystemExit(
        "SUPERSET_READER_USER поддерживает только superset_reader: "
        "SQL-гранты и роль создаются с этим фиксированным именем."
    )
READER_PASSWORD = os.environ.get("SUPERSET_READER_PASSWORD", "")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "airflow")
DATABASE_NAME = os.environ.get("SUPERSET_DATABASE_NAME", "steam-marts")
DASHBOARD_NAME = os.environ.get(
    "SUPERSET_DASHBOARD_NAME", "Steam Analytics (ЛР3)"
)
DASHBOARD_SLUG = os.environ.get("SUPERSET_DASHBOARD_SLUG", "steam-analytics-lr3")
UPDATE_EXISTING = os.environ.get("SUPERSET_UPDATE_EXISTING", "0") == "1"

TOKEN = ""
CSRF_TOKEN = ""
COOKIE_JAR = http.cookiejar.CookieJar()
OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(COOKIE_JAR))


def build_marts_uri():
    """Собирает URI только для чтения; готовый URI можно передать снаружи."""
    ready = os.environ.get("SUPERSET_MARTS_URI", "").strip()
    if ready:
        return ready
    if not READER_PASSWORD:
        raise SystemExit(
            "Не задан SUPERSET_READER_PASSWORD (или SUPERSET_MARTS_URI)."
        )
    user = urllib.parse.quote(READER_USER, safe="")
    password = urllib.parse.quote(READER_PASSWORD, safe="")
    database = urllib.parse.quote(POSTGRES_DB, safe="")
    return "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
        user, password, POSTGRES_HOST, POSTGRES_PORT, database
    )


MARTS_URI = build_marts_uri()


def redact(text):
    """Убирает из сообщений пароль, JWT и URI подключения."""
    text = str(text)
    for secret in (ADMIN_PASSWORD, READER_PASSWORD, MARTS_URI, TOKEN):
        if secret:
            text = text.replace(secret, "***")
    return text[:2000]


class ApiError(RuntimeError):
    def __init__(self, status, message):
        super().__init__("HTTP {}: {}".format(status, message))
        self.status = status


def api(method, path, body=None, expected=(200,), auth=True):
    url = BASE_URL + path
    data = None
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    headers = {"Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    if auth and TOKEN:
        headers["Authorization"] = "Bearer " + TOKEN
    if method not in ("GET", "HEAD") and CSRF_TOKEN:
        headers["X-CSRFToken"] = CSRF_TOKEN
        headers["Referer"] = BASE_URL + "/"

    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with OPENER.open(request, timeout=60) as response:
            status = response.status
            raw = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        status = exc.code
        raw = exc.read().decode("utf-8", errors="replace")

    parsed = None
    if raw:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = raw

    if status not in expected:
        raise ApiError(status, redact(parsed))
    return status, parsed


def login():
    global TOKEN, CSRF_TOKEN
    if not ADMIN_USER or not ADMIN_PASSWORD:
        raise SystemExit(
            "Не заданы SUPERSET_ADMIN_USERNAME и SUPERSET_ADMIN_PASSWORD."
        )
    _, body = api(
        "POST",
        "/api/v1/security/login",
        {
            "username": ADMIN_USER,
            "password": ADMIN_PASSWORD,
            "provider": "db",
            "refresh": False,
        },
        expected=(200,),
        auth=False,
    )
    token = (body or {}).get("access_token")
    if not token:
        raise SystemExit("Superset не вернул access_token.")
    TOKEN = token
    try:
        _, csrf = api(
            "GET", "/api/v1/security/csrf_token/", expected=(200,), auth=True
        )
        CSRF_TOKEN = (csrf or {}).get("result") or ""
    except ApiError:
        CSRF_TOKEN = ""
    print("Superset: вход выполнен.")


def find_entity(endpoint, column, value):
    filters = [{"col": column, "opr": "eq", "value": value}]
    query = urllib.parse.quote(
        json.dumps(
            {"filters": filters, "page_size": 100},
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )
    _, body = api("GET", endpoint + "?q=" + query)
    for item in (body or {}).get("result", []):
        if item.get(column) == value:
            return item
    return None


def entity_id(body):
    if isinstance(body, dict):
        if isinstance(body.get("id"), int):
            return body["id"]
        result = body.get("result")
        if isinstance(result, dict) and isinstance(result.get("id"), int):
            return result["id"]
    raise RuntimeError("Ответ API не содержит id: " + redact(body))


def ensure_database():
    existing = find_entity("/api/v1/database/", "database_name", DATABASE_NAME)
    payload = {
        "database_name": DATABASE_NAME,
        "sqlalchemy_uri": MARTS_URI,
        "expose_in_sqllab": True,
    }
    if existing:
        database_id = entity_id(existing)
        # Пароль роли мог измениться; URI существующего подключения обновляем.
        api(
            "PUT",
            "/api/v1/database/{}".format(database_id),
            payload,
            expected=(200, 201),
        )
        print("Superset: подключение '{}' обновлено.".format(DATABASE_NAME))
        return database_id
    try:
        _, body = api("POST", "/api/v1/database/", payload, expected=(200, 201))
    except ApiError as exc:
        existing = find_entity("/api/v1/database/", "database_name", DATABASE_NAME)
        if existing:
            database_id = entity_id(existing)
            api(
                "PUT",
                "/api/v1/database/{}".format(database_id),
                payload,
                expected=(200, 201),
            )
            print("Superset: подключение '{}' обновлено.".format(DATABASE_NAME))
            return database_id
        raise exc
    print("Superset: создано подключение '{}'.".format(DATABASE_NAME))
    return entity_id(body)


def dataset_database_id(item):
    database = item.get("database")
    if isinstance(database, dict):
        return database.get("id")
    if isinstance(database, int):
        return database
    return None


def refresh_dataset(dataset_id):
    """Просит Superset перечитать колонки представления; API может не поддерживать."""
    try:
        api(
            "POST",
            "/api/v1/dataset/{}/refresh".format(dataset_id),
            {},
            expected=(200, 201),
        )
        print("Superset: dataset {} обновлён из БД.".format(dataset_id))
    except ApiError as exc:
        if exc.status not in (404, 405):
            print(
                "Superset: обновление dataset {} не удалось: {}".format(
                    dataset_id, redact(exc)
                ),
                file=sys.stderr,
            )


def ensure_dataset(database_id, table_name):
    existing = find_entity("/api/v1/dataset/", "table_name", table_name)
    existing_db = dataset_database_id(existing) if existing else None
    if (
        existing
        and existing.get("schema") in (None, "marts")
        and existing_db in (None, database_id)
    ):
        dataset_id = entity_id(existing)
        refresh_dataset(dataset_id)
        return dataset_id
    payload = {
        "database": database_id,
        "schema": "marts",
        "table_name": table_name,
    }
    try:
        _, body = api("POST", "/api/v1/dataset/", payload, expected=(200, 201))
    except ApiError as exc:
        existing = find_entity("/api/v1/dataset/", "table_name", table_name)
        existing_db = dataset_database_id(existing) if existing else None
        if (
            existing
            and existing.get("schema") in (None, "marts")
            and existing_db in (None, database_id)
        ):
            dataset_id = entity_id(existing)
            refresh_dataset(dataset_id)
            return dataset_id
        raise exc
    print("Superset: dataset marts.{} создан.".format(table_name))
    return entity_id(body)


def ensure_dashboard():
    existing = find_entity("/api/v1/dashboard/", "dashboard_title", DASHBOARD_NAME)
    payload = {
        "dashboard_title": DASHBOARD_NAME,
        "slug": DASHBOARD_SLUG,
        "published": True,
    }
    if existing:
        dashboard_id = entity_id(existing)
        api(
            "PUT",
            "/api/v1/dashboard/{}".format(dashboard_id),
            payload,
            expected=(200, 201),
        )
        print("Superset: dashboard обновлён.")
        return dashboard_id
    try:
        _, body = api("POST", "/api/v1/dashboard/", payload, expected=(200, 201))
    except ApiError as exc:
        existing = find_entity(
            "/api/v1/dashboard/", "dashboard_title", DASHBOARD_NAME
        )
        if existing:
            return entity_id(existing)
        raise exc
    print("Superset: dashboard создан.")
    return entity_id(body)


def chart_datasource_id(item):
    datasource = item.get("datasource_id") or item.get("datasource")
    if isinstance(datasource, dict):
        return datasource.get("id")
    if isinstance(datasource, int):
        return datasource
    if isinstance(datasource, str):
        prefix = datasource.split("__", 1)[0]
        if prefix.isdigit():
            return int(prefix)
    return None


def ensure_chart(definition, datasource_id, dashboard_id):
    name = definition["name"]
    existing = find_entity("/api/v1/chart/", "slice_name", name)
    if existing:
        chart_id = entity_id(existing)
        existing_datasource = chart_datasource_id(existing)
        if existing_datasource not in (None, datasource_id):
            raise RuntimeError(
                "chart '{}' относится к другому datasource".format(name)
            )
        if UPDATE_EXISTING:
            payload = chart_payload(definition, datasource_id, dashboard_id)
            api(
                "PUT",
                "/api/v1/chart/{}".format(chart_id),
                payload,
                expected=(200, 201),
            )
            print("Superset: chart '{}' обновлён.".format(name))
        else:
            print("Superset: chart '{}' уже есть.".format(name))
        return chart_id

    payload = chart_payload(definition, datasource_id, dashboard_id)
    try:
        _, body = api("POST", "/api/v1/chart/", payload, expected=(200, 201))
    except ApiError as exc:
        if exc.status in (400, 422) and "dashboards" in payload:
            payload.pop("dashboards", None)
            _, body = api("POST", "/api/v1/chart/", payload, expected=(200, 201))
        else:
            raise exc
    print("Superset: chart '{}' создан.".format(name))
    return entity_id(body)


def chart_payload(definition, datasource_id, dashboard_id):
    params = dict(definition["params"])
    params["datasource"] = "{}__table".format(datasource_id)
    params["viz_type"] = definition["viz_type"]
    return {
        "slice_name": definition["name"],
        "viz_type": definition["viz_type"],
        "datasource_id": datasource_id,
        "datasource_type": "table",
        "params": json.dumps(params, ensure_ascii=False),
        "description": definition.get("description", ""),
        "dashboards": [dashboard_id],
    }


def update_dashboard_layout(dashboard_id, chart_ids):
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {
            "type": "ROOT",
            "id": "ROOT_ID",
            "children": ["GRID_ID"],
        },
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [],
            "parents": ["ROOT_ID"],
            "meta": {},
        },
    }
    rows = []
    for index in range(0, len(chart_ids), 2):
        row_id = "ROW-{}".format(index // 2)
        nodes = []
        for chart_id in chart_ids[index : index + 2]:
            node_id = "CHART-{}".format(chart_id)
            position[node_id] = {
                "type": "CHART",
                "id": node_id,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {
                    "chartId": chart_id,
                    "width": 6,
                    "height": 50,
                    "uuid": str(uuid.uuid4()),
                },
            }
            nodes.append(node_id)
        position[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": nodes,
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        rows.append(row_id)
    position["GRID_ID"]["children"] = rows

    payload = {
        "position_json": json.dumps(position, ensure_ascii=False),
        "json_metadata": json.dumps(
            {"refresh_frequency": 0, "expanded_slices": {}}, ensure_ascii=False
        ),
        "published": True,
    }
    api(
        "PUT",
        "/api/v1/dashboard/{}".format(dashboard_id),
        payload,
        expected=(200, 201),
    )
    print("Superset: layout dashboard обновлён ({} chart).".format(len(chart_ids)))


def metric(column, aggregate, label):
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column},
        "aggregate": aggregate,
        "label": label,
        "hasCustomLabel": True,
        "optionName": "metric_{}_{}".format(column, aggregate.lower()),
    }


DATASETS = [
    "bi_top15_games_by_avg_players",
    "bi_activity_dow_hour_utc",
    "bi_weekend_hourly",
    "bi_reviews_daily",
    "bi_top_genres_by_reviews",
    "bi_genre_sentiment",
    "bi_publisher_reviews_vs_online",
    "bi_dq_latest",
]

CHARTS = [
    {
        "name": "Топ-15 игр по среднему онлайну",
        "table": "bi_top15_games_by_avg_players",
        "viz_type": "echarts_timeseries_bar",
        "description": (
            "SCD2-версии объединены по appid; средний онлайн взвешен по samples."
        ),
        "params": {
            "x_axis": "game_label",
            "metrics": [
                metric(
                    "avg_players_weighted",
                    "MAX",
                    "Средний онлайн (взвеш. по samples)",
                )
            ],
            "groupby": [],
            "row_limit": 15,
            "order_desc": True,
            "orientation": "horizontal",
            "show_legend": False,
            "rich_tooltip": True,
            "y_axis_format": ",.0f",
        },
    },
    {
        "name": "Активность: день недели и час UTC",
        "table": "bi_activity_dow_hour_utc",
        "viz_type": "heatmap",
        "description": (
            "Только фактически собранные часы (обычно каждые 3 часа); "
            "средний онлайн взвешен по samples и не является суммой онлайна игр."
        ),
        "params": {
            "all_columns_x": "day_label",
            "all_columns_y": "hour_of_day",
            "metric": metric(
                "avg_players_weighted",
                "MAX",
                "Средний онлайн (взвеш. по samples)",
            ),
            "linear_color_scheme": "blue_white_yellow",
            "normalize_across": "heatmap",
            "show_legend": True,
            "show_values": True,
            "row_limit": 10000,
            "xscale_interval": -1,
            "yscale_interval": -1,
        },
    },
    {
        "name": "Будни и выходные по часам UTC",
        "table": "bi_weekend_hourly",
        "viz_type": "echarts_timeseries_bar",
        "description": (
            "Используется поле is_weekend витрины; шкала — средний онлайн "
            "по снимкам отдельных игр, а не суммарный онлайн."
        ),
        "params": {
            "x_axis": "hour_label",
            "metrics": [
                metric(
                    "avg_players_weighted",
                    "MAX",
                    "Средний онлайн (взвеш. по samples)",
                )
            ],
            "groupby": ["day_type"],
            "row_limit": 100,
            "order_desc": False,
            "show_legend": True,
            "rich_tooltip": True,
            "y_axis_format": ",.0f",
        },
    },
    {
        "name": "Динамика отзывов и доли положительных",
        "table": "bi_reviews_daily",
        "viz_type": "mixed_timeseries",
        "description": (
            "Столбцы — число текущих отзывов, линия — доля положительных. "
            "Оценка отзыва текущая, а не восстановленная на историческую дату."
        ),
        "params": {
            "x_axis": "activity_date",
            "time_grain_sqla": "P1D",
            "metrics": [
                metric("review_count", "MAX", "Отзывы"),
            ],
            "metrics_b": [
                metric("positive_ratio", "MAX", "Доля положительных"),
            ],
            "seriesType": "bar",
            "seriesTypeB": "line",
            "y_axis_format": ",d",
            "y_axis_format_secondary": ".1%",
            "show_legend": True,
            "rich_tooltip": True,
            "row_limit": 10000,
        },
    },
    {
        "name": "Доля топ-10 жанров по отзывам",
        "table": "bi_top_genres_by_reviews",
        "viz_type": "pie",
        "description": (
            "Доли в сумме отзывов по жанрам, а не уникальные отзывы: "
            "одна игра может входить в несколько жанров."
        ),
        "params": {
            "groupby": ["genre_name"],
            "metric": metric("review_count", "MAX", "Отзывы"),
            "row_limit": 10,
            "show_labels": True,
            "labels_outside": True,
            "donut": False,
            "show_legend": True,
        },
    },
    {
        "name": "Положительные и отрицательные отзывы по жанрам",
        "table": "bi_genre_sentiment",
        "viz_type": "echarts_timeseries_bar",
        "description": (
            "Состав отзывов по жанрам; отрицательные = review_count - positive_reviews. "
            "Суммы по жанрам не являются общим числом уникальных отзывов."
        ),
        "params": {
            "x_axis": "genre_name",
            "metrics": [
                metric("positive_reviews", "MAX", "Положительные"),
                metric("negative_reviews", "MAX", "Отрицательные"),
            ],
            "groupby": [],
            "stack": "Stack",
            "row_limit": 10,
            "order_desc": True,
            "show_legend": True,
            "rich_tooltip": True,
            "y_axis_format": ",d",
        },
    },
    {
        "name": "Издатели: отзывы и средний онлайн",
        "table": "bi_publisher_reviews_vs_online",
        "viz_type": "bubble",
        "description": (
            "Размер пузыря — samples; средний онлайн взвешен по samples. "
            "Метрики не суммируются между издателями."
        ),
        "params": {
            "entity": "publisher_name",
            "series": "publisher_name",
            "x": metric("review_count", "MAX", "Отзывы"),
            "y": metric(
                "avg_players_weighted",
                "MAX",
                "Средний онлайн (взвеш.)",
            ),
            "size": metric("samples", "MAX", "Снимки"),
            "max_bubble_size": "50",
            "row_limit": 500,
            "show_legend": False,
        },
    },
    {
        "name": "Качество витрин: последние DQ-проверки",
        "table": "bi_dq_latest",
        "viz_type": "table",
        "description": (
            "Последний результат каждой проверки из marts.mart_dq_results. "
            "Таблица показывает статус, порог и фактическое значение."
        ),
        "params": {
            "all_columns": [
                "check_name",
                "status",
                "severity",
                "metric_value",
                "threshold",
                "checked_at",
            ],
            "row_limit": 30,
            "show_cell_bars": True,
            "include_search": True,
            "page_length": 20,
        },
    },
]


def main():
    login()
    database_id = ensure_database()

    dataset_ids = {}
    for table_name in DATASETS:
        dataset_ids[table_name] = ensure_dataset(database_id, table_name)

    dashboard_id = ensure_dashboard()
    chart_ids = []
    for definition in CHARTS:
        datasource_id = dataset_ids[definition["table"]]
        chart_ids.append(ensure_chart(definition, datasource_id, dashboard_id))

    update_dashboard_layout(dashboard_id, chart_ids)
    print("Готово: dashboard id={}, charts={}.".format(dashboard_id, len(chart_ids)))


if __name__ == "__main__":
    try:
        main()
    except ApiError as exc:
        print("Ошибка Superset API: {}".format(redact(exc)), file=sys.stderr)
        raise SystemExit(1)
    except Exception as exc:
        print("Ошибка: {}".format(redact(exc)), file=sys.stderr)
        raise SystemExit(1)
