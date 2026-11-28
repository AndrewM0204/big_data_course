#!/usr/bin/env python3
"""Применение декларации lineage из lineage.json к OpenMetadata.

Скрипт создаёт pipeline service/pipelines, связи table->table и
pipeline->table, а также (по умолчанию) dashboard service/dashboard
и связи dashboard->table.

Совместимость с OpenMetadata 1.8.0 не проверялась запуском: при ошибке
API скрипт печатает статус и завершается ненулевым кодом.
"""

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

SERVER = os.environ.get(
    "OM_SERVER_URL", "http://openmetadata-server:8585/api"
).rstrip("/")
EMAIL = (
    os.environ.get("OM_ADMIN_EMAIL")
    or os.environ.get("OPENMETADATA_ADMIN_EMAIL")
    or "admin@open-metadata.org"
)
PASSWORD = (
    os.environ.get("OM_ADMIN_PASSWORD")
    or os.environ.get("OPENMETADATA_ADMIN_PASSWORD")
    or ""
)
POSTGRES_SERVICE = os.environ.get("OM_POSTGRES_SERVICE", "steam-postgres")
POSTGRES_DATABASE = os.environ.get("OM_POSTGRES_DB", "airflow")

TOKEN = ""


def redact(text):
    text = str(text)
    for secret in (
        PASSWORD,
        TOKEN,
        os.environ.get("OM_POSTGRES_PASSWORD", ""),
    ):
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
        raise SystemExit("Не заданы OM_ADMIN_EMAIL/OM_ADMIN_PASSWORD.")
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


def get_entity(kind, fqn):
    """Возвращает id сущности по FQN; kind: table, pipeline, dashboard и сервисы."""
    paths = {
        "table": "/v1/tables/name/",
        "pipeline": "/v1/pipelines/name/",
        "dashboard": "/v1/dashboards/name/",
        "pipelineService": "/v1/services/pipelineServices/name/",
        "dashboardService": "/v1/services/dashboardServices/name/",
    }
    if kind not in paths:
        raise ValueError("Неизвестный тип сущности: " + kind)
    status, body = call(
        "GET", paths[kind] + urllib.parse.quote(fqn, safe=""), expected=(200, 404)
    )
    if status != 200 or not body:
        return None
    return body.get("id")


def get_or_create(kind, fqn, endpoint, payload, label):
    entity_id = get_entity(kind, fqn)
    if entity_id:
        return entity_id
    status, body = call(
        "POST", endpoint, payload, expected=(200, 201, 400, 409, 422)
    )
    if status in (200, 201) and isinstance(body, dict):
        return body.get("id")
    if status == 409:
        return get_entity(kind, fqn)
    raise RuntimeError(
        "OpenMetadata: не удалось создать {} '{}': HTTP {} {}".format(
            label, fqn, status, redact(body)
        )
    )


def create_with_service(kind, fqn, endpoint, payload, service_kind, service_fqn, label):
    """Пытается создать сущность; при отказе service-строки шлёт service-ссылку."""
    entity_id = get_entity(kind, fqn)
    if entity_id:
        return entity_id
    status, body = call(
        "POST", endpoint, payload, expected=(200, 201, 400, 409, 422)
    )
    if status in (200, 201) and isinstance(body, dict):
        return body.get("id")
    if status == 409:
        return get_entity(kind, fqn)
    if status in (400, 422):
        service_id = get_entity(service_kind, service_fqn)
        if service_id:
            retry = dict(payload)
            retry["service"] = {"id": service_id, "type": service_kind}
            status, body = call(
                "POST", endpoint, retry, expected=(200, 201, 400, 409, 422)
            )
            if status in (200, 201) and isinstance(body, dict):
                return body.get("id")
            if status == 409:
                return get_entity(kind, fqn)
    raise RuntimeError(
        "OpenMetadata: не удалось создать {} '{}': HTTP {} {}".format(
            label, fqn, status, redact(body)
        )
    )


def ensure_pipeline_service(service):
    name = service["name"]
    payload = {
        "name": name,
        "displayName": service.get("display_name", name),
        "description": service.get("description", ""),
        "serviceType": "Airflow",
    }
    get_or_create(
        "pipelineService",
        name,
        "/v1/services/pipelineServices",
        payload,
        "pipeline service",
    )


def ensure_pipelines(declaration):
    service_name = declaration["pipeline_service"]["name"]
    for pipeline in declaration["pipelines"]:
        name = pipeline["name"]
        fqn = "{}.{}".format(service_name, name)
        payload = {
            "name": name,
            "displayName": name,
            "description": pipeline.get("description", ""),
            "service": service_name,
            "sourceUrl": pipeline.get("source_url", ""),
        }
        create_with_service(
            "pipeline",
            fqn,
            "/v1/pipelines",
            payload,
            "pipelineService",
            service_name,
            "pipeline",
        )


def table_fqn(name):
    return "{}.{}.{}".format(POSTGRES_SERVICE, POSTGRES_DATABASE, name)


def apply_edge(from_kind, from_id, to_id, description):
    payload = {
        "edge": {
            "fromEntity": {"id": from_id, "type": from_kind},
            "toEntity": {"id": to_id, "type": "table"},
            "description": description,
        }
    }
    status, _ = call("PUT", "/v1/lineage", payload, expected=(200, 201, 409))
    return status != 409


def ensure_dashboard(declaration):
    dashboard = declaration["dashboard"]
    service_name = dashboard["service_name"]
    service_payload = {
        "name": service_name,
        "displayName": service_name,
        "description": "Superset-дашборды Steam",
        "serviceType": "Superset",
    }
    get_or_create(
        "dashboardService",
        service_name,
        "/v1/services/dashboardServices",
        service_payload,
        "dashboard service",
    )

    name = dashboard["name"]
    fqn = "{}.{}".format(service_name, name)
    payload = {
        "name": name,
        "displayName": dashboard.get("display_name", name),
        "description": "Дашборд ЛР3: Steam Analytics",
        "service": service_name,
        "sourceUrl": dashboard.get("source_url", ""),
    }
    return create_with_service(
        "dashboard",
        fqn,
        "/v1/dashboards",
        payload,
        "dashboardService",
        service_name,
        "dashboard",
    )


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Lineage ЛР3 в OpenMetadata")
    parser.add_argument(
        "declaration",
        nargs="?",
        default=os.environ.get(
            "OM_LINEAGE_DECLARATION",
            "/lineage/lineage.json",
        ),
        help="путь к lineage.json",
    )
    parser.add_argument(
        "--with-dashboard",
        action="store_true",
        help="создать dashboard service/dashboard и связи dashboard->table",
    )
    parser.add_argument(
        "--no-dashboard",
        action="store_true",
        help="не создавать dashboard и связи к BI-вью",
    )
    args = parser.parse_args()

    with open(args.declaration, encoding="utf-8") as handle:
        declaration = json.load(handle)

    global POSTGRES_SERVICE, POSTGRES_DATABASE
    POSTGRES_SERVICE = declaration.get("postgres_service", POSTGRES_SERVICE)
    POSTGRES_DATABASE = declaration.get("postgres_database", POSTGRES_DATABASE)

    login()
    ensure_pipeline_service(declaration["pipeline_service"])
    ensure_pipelines(declaration)

    table_names = set()
    for source, target in declaration["table_edges"]:
        table_names.add(source)
        table_names.add(target)
    for _, target in declaration["pipeline_table_edges"]:
        table_names.add(target)

    dashboard = declaration.get("dashboard", {})
    env_dashboard = os.environ.get("OM_WITH_DASHBOARD", "1") != "0"
    dashboard_enabled = (
        bool(dashboard.get("enabled"))
        and not args.no_dashboard
        and (env_dashboard or args.with_dashboard)
    )
    if dashboard_enabled:
        table_names.update(dashboard.get("table_edges", []))

    table_ids = {}
    missing = []
    for name in sorted(table_names):
        entity_id = get_entity("table", table_fqn(name))
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
    skipped = 0

    for source, target in declaration["table_edges"]:
        if source not in table_ids or target not in table_ids:
            skipped += 1
            continue
        try:
            if apply_edge(
                "table",
                table_ids[source],
                table_ids[target],
                "{} -> {}".format(source, target),
            ):
                accepted += 1
            else:
                skipped += 1
        except RuntimeError as exc:
            failures += 1
            print("Lineage {} -> {}: {}".format(source, target, exc), file=sys.stderr)

    for pipeline, target in declaration["pipeline_table_edges"]:
        pipeline_fqn = "{}.{}".format(
            declaration["pipeline_service"]["name"], pipeline
        )
        pipeline_id = get_entity("pipeline", pipeline_fqn)
        if pipeline_id is None or target not in table_ids:
            skipped += 1
            continue
        try:
            if apply_edge(
                "pipeline",
                pipeline_id,
                table_ids[target],
                "{} -> {}".format(pipeline, target),
            ):
                accepted += 1
            else:
                skipped += 1
        except RuntimeError as exc:
            failures += 1
            print(
                "Lineage {} -> {}: {}".format(pipeline, target, exc), file=sys.stderr
            )

    if dashboard_enabled:
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
                    skipped += 1
                    continue
                try:
                    if apply_edge(
                        "dashboard",
                        dashboard_id,
                        table_id,
                        "dashboard -> {}".format(table_name),
                    ):
                        accepted += 1
                    else:
                        skipped += 1
                except RuntimeError as exc:
                    failures += 1
                    print(
                        "Dashboard -> {}: {}".format(table_name, exc),
                        file=sys.stderr,
                    )

    print(
        "Lineage: связей принято {}, пропущено {}, ошибок {}, "
        "отсутствующих таблиц {}.".format(
            accepted, skipped, failures, len(missing)
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
