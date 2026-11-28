#!/usr/bin/env python3
"""Применение декларации lineage из lineage.json к OpenMetadata.

Скрипт создаёт pipeline service/pipelines и связи table->table,
pipeline->table. Опционально создаёт dashboard service/dashboard и
связи dashboard->table (--with-dashboard).

Совместимость с OpenMetadata 1.8.0 не проверялась запуском: при ошибке
API скрипт печатает статус и завершается ненулевым кодом.
"""

import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

SERVER = os.environ.get("OM_SERVER_URL", "http://lr3-openmetadata:8585/api").rstrip(
    "/"
)
EMAIL = os.environ.get("OM_ADMIN_EMAIL", "")
PASSWORD = os.environ.get("OM_ADMIN_PASSWORD", "")
POSTGRES_SERVICE = os.environ.get("OM_POSTGRES_SERVICE", "steam-postgres")
POSTGRES_DATABASE = os.environ.get("OM_POSTGRES_DB", "airflow")

TOKEN = ""


def redact(text):
    text = str(text)
    for secret in (PASSWORD, TOKEN):
        if secret:
            text = text.replace(secret, "***")
    return text[:2000]


def call(method, path, payload=None, expected=(200,)):
    url = SERVER + path
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if TOKEN:
        headers["Authorization"] = "Bearer " + TOKEN
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            status = response.status
            raw = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        status = exc.code
        raw = exc.read().decode("utf-8", errors="replace")
    body = None
    if raw:
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = raw
    if status not in expected:
        raise RuntimeError("HTTP {} {}: {}".format(status, path, redact(body)))
    return status, body


def login():
    global TOKEN
    if not EMAIL or not PASSWORD:
        raise SystemExit("Не заданы OM_ADMIN_EMAIL и OM_ADMIN_PASSWORD.")
    _, body = call(
        "POST",
        "/v1/users/login",
        {
            "email": EMAIL,
            "password": base64.b64encode(PASSWORD.encode("utf-8")).decode("ascii"),
        },
        expected=(200,),
    )
    token = (body or {}).get("accessToken")
    if not token:
        raise SystemExit("OpenMetadata не вернул accessToken.")
    TOKEN = token
    print("OpenMetadata: вход выполнен.")


def get_or_create(path, name, payload, label):
    status, body = call("GET", path + urllib.parse.quote(name, safe=""), expected=(200, 404))
    if status == 200:
        return body
    status, body = call("POST", payload["endpoint"], payload["body"], expected=(200, 201, 409))
    if status == 409:
        _, body = call("GET", path + urllib.parse.quote(name, safe=""), expected=(200,))
    print("OpenMetadata: {} '{}' готов.".format(label, name))
    return body


def ensure_pipeline_service(declaration):
    service = declaration["pipeline_service"]
    name = service["name"]
    payload = {
        "endpoint": "/v1/services/pipelineServices",
        "body": {
            "name": name,
            "displayName": service.get("display_name", name),
            "description": service.get("description", ""),
            "serviceType": "Airflow",
        },
    }
    get_or_create(
        "/v1/services/pipelineServices/name/", name, payload, "pipeline service"
    )


def ensure_pipelines(declaration):
    service_name = declaration["pipeline_service"]["name"]
    for pipeline in declaration["pipelines"]:
        name = pipeline["name"]
        payload = {
            "endpoint": "/v1/pipelines",
            "body": {
                "name": name,
                "displayName": name,
                "description": pipeline.get("description", ""),
                "service": service_name,
                "sourceUrl": pipeline.get("source_url", ""),
            },
        }
        fqn = "{}.{}".format(service_name, name)
        get_or_create("/v1/pipelines/name/", fqn, payload, "pipeline")


def table_fqn(name):
    return "{}.{}.{}".format(POSTGRES_SERVICE, POSTGRES_DATABASE, name)


def get_entity(kind, name):
    """Возвращает id сущности по FQN; kind: table, pipeline или dashboard."""
    if kind == "table":
        fqn = table_fqn(name)
        path = "/v1/tables/name/"
    elif kind == "pipeline":
        fqn = name
        path = "/v1/pipelines/name/"
    elif kind == "dashboard":
        fqn = name
        path = "/v1/dashboards/name/"
    else:
        raise ValueError("Неизвестный тип сущности: " + kind)
    status, body = call("GET", path + urllib.parse.quote(fqn, safe=""), expected=(200, 404))
    if status != 200 or not body:
        return None
    return body.get("id")


def apply_edge(from_kind, from_id, to_id, description):
    payload = {
        "edge": {
            "fromEntity": {"id": from_id, "type": from_kind},
            "toEntity": {"id": to_id, "type": "table"},
            "description": description,
        }
    }
    status, body = call("PUT", "/v1/lineage", payload, expected=(200, 201, 409))
    if status == 409:
        return False
    return True


def ensure_dashboard(declaration):
    dashboard = declaration["dashboard"]
    service_name = dashboard["service_name"]
    service_payload = {
        "endpoint": "/v1/services/dashboardServices",
        "body": {
            "name": service_name,
            "displayName": service_name,
            "description": "Superset-дашборды Steam",
            "serviceType": "Superset",
        },
    }
    get_or_create(
        "/v1/services/dashboardServices/name/",
        service_name,
        service_payload,
        "dashboard service",
    )

    name = dashboard["name"]
    fqn = "{}.{}".format(service_name, name)
    payload = {
        "endpoint": "/v1/dashboards",
        "body": {
            "name": name,
            "displayName": dashboard.get("display_name", name),
            "description": "Дашборд ЛР3: Steam Analytics",
            "service": service_name,
            "sourceUrl": dashboard.get("source_url", ""),
        },
    }
    entity = get_or_create(
        "/v1/dashboards/name/", fqn, payload, "dashboard"
    )
    return entity.get("id") if entity else None


def main():
    parser = argparse.ArgumentParser(description="Lineage ЛР3 в OpenMetadata")
    parser.add_argument(
        "declaration",
        nargs="?",
        default=os.path.join(os.path.dirname(__file__), "lineage.json"),
        help="путь к lineage.json",
    )
    parser.add_argument(
        "--with-dashboard",
        action="store_true",
        help="создать dashboard service/dashboard и связи dashboard->table",
    )
    args = parser.parse_args()

    with open(args.declaration, encoding="utf-8") as handle:
        declaration = json.load(handle)

    global POSTGRES_SERVICE, POSTGRES_DATABASE
    POSTGRES_SERVICE = declaration.get("postgres_service", POSTGRES_SERVICE)
    POSTGRES_DATABASE = declaration.get("postgres_database", POSTGRES_DATABASE)

    login()
    ensure_pipeline_service(declaration)
    ensure_pipelines(declaration)

    table_names = set()
    for source, target in declaration["table_edges"]:
        table_names.add(source)
        table_names.add(target)
    for _, target in declaration["pipeline_table_edges"]:
        table_names.add(target)

    table_ids = {}
    missing = []
    for name in sorted(table_names):
        entity_id = get_entity("table", name)
        if entity_id is None:
            missing.append(name)
        else:
            table_ids[name] = entity_id

    if missing:
        print(
            "OpenMetadata: не найдены таблицы (сначала выполните ingestion): "
            + ", ".join(missing),
            file=sys.stderr,
        )

    failures = 0
    accepted = 0
    for source, target in declaration["table_edges"]:
        if source not in table_ids or target not in table_ids:
            continue
        try:
            if apply_edge(
                "table",
                table_ids[source],
                table_ids[target],
                "{} -> {}".format(source, target),
            ):
                accepted += 1
        except RuntimeError as exc:
            failures += 1
            print("Lineage {} -> {}: {}".format(source, target, exc), file=sys.stderr)

    for pipeline, target in declaration["pipeline_table_edges"]:
        pipeline_fqn = "{}.{}".format(
            declaration["pipeline_service"]["name"], pipeline
        )
        pipeline_id = get_entity("pipeline", pipeline_fqn)
        if pipeline_id is None or target not in table_ids:
            continue
        try:
            if apply_edge(
                "pipeline",
                pipeline_id,
                table_ids[target],
                "{} -> {}".format(pipeline, target),
            ):
                accepted += 1
        except RuntimeError as exc:
            failures += 1
            print(
                "Lineage {} -> {}: {}".format(pipeline, target, exc),
                file=sys.stderr,
            )

    dashboard = declaration.get("dashboard", {})
    if args.with_dashboard and dashboard:
        try:
            dashboard_id = ensure_dashboard(declaration)
        except RuntimeError as exc:
            failures += 1
            print("Dashboard: {}".format(exc), file=sys.stderr)
            dashboard_id = None
        if dashboard_id:
            for table_name in dashboard.get("table_edges", []):
                table_id = table_ids.get(table_name)
                if table_id is None:
                    failures += 1
                    print(
                        "Dashboard -> {}: таблица не найдена.".format(table_name),
                        file=sys.stderr,
                    )
                    continue
                try:
                    if apply_edge(
                        "dashboard",
                        dashboard_id,
                        table_id,
                        "dashboard -> {}".format(table_name),
                    ):
                        accepted += 1
                except RuntimeError as exc:
                    failures += 1
                    print(
                        "Dashboard -> {}: {}".format(table_name, exc),
                        file=sys.stderr,
                    )

    print(
        "Lineage: связей принято {}, ошибок {}, отсутствующих таблиц {}.".format(
            accepted, failures, len(missing)
        )
    )
    if missing or failures:
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as exc:
        print("Ошибка OpenMetadata: {}".format(redact(exc)), file=sys.stderr)
        raise SystemExit(1)
