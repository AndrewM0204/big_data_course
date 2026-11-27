import base64
import json
import os
import urllib.error
import urllib.parse
import urllib.request

SERVER = os.environ.get("OM_SERVER_URL", "http://openmetadata-server:8585/api").rstrip("/")
EMAIL = os.environ.get("OM_ADMIN_EMAIL", "admin@open-metadata.org")
PASSWORD = os.environ.get("OM_ADMIN_PASSWORD", "admin")

PIPELINES = [
    "steam_games_catalog_etl",
    "steam_player_stats_etl",
    "steam_reviews_collector",
    "dds_game_dimensions_dag",
    "dds_review_dimensions_dag",
    "dds_facts_dag",
    "data_quality_checks",
    "build_marts_dag",
    "data_quality_marts_dag",
]

PIPELINE_EDGES = [
    ("steam_games_catalog_etl", "steam.ods_games"),
    ("steam_player_stats_etl", "steam.ods_player_stats"),
    ("steam_reviews_collector", "steam.ods_reviews"),
    ("dds_game_dimensions_dag", "dds.dim_game"),
    ("dds_game_dimensions_dag", "dds.dim_genre"),
    ("dds_game_dimensions_dag", "dds.dim_developer"),
    ("dds_game_dimensions_dag", "dds.dim_publisher"),
    ("dds_review_dimensions_dag", "dds.dim_user"),
    ("dds_review_dimensions_dag", "dds.dim_review"),
    ("dds_facts_dag", "dds.fact_review"),
    ("dds_facts_dag", "dds.fact_player_activity"),
    ("data_quality_checks", "dds.dq_results"),
    ("build_marts_dag", "marts.mart_game_daily_metrics"),
    ("build_marts_dag", "marts.mart_genre_daily_metrics"),
    ("build_marts_dag", "marts.mart_activity_hourly"),
    ("data_quality_marts_dag", "marts.dq_results"),
]


TABLE_EDGES = [
    ("steam.ods_games", "dds.dim_game"),
    ("steam.ods_games", "dds.dim_genre"),
    ("steam.ods_games", "dds.dim_developer"),
    ("steam.ods_games", "dds.dim_publisher"),
    ("dds.dim_game", "dds.bridge_game_genre"),
    ("dds.dim_game", "dds.bridge_game_developer"),
    ("dds.dim_game", "dds.bridge_game_publisher"),
    ("steam.ods_reviews", "dds.dim_user"),
    ("steam.ods_reviews", "dds.dim_review"),
    ("dds.dim_review", "dds.fact_review"),
    ("dds.dim_game", "dds.fact_review"),
    ("dds.dim_user", "dds.fact_review"),
    ("steam.ods_player_stats", "dds.fact_player_activity"),
    ("dds.dim_game", "dds.fact_player_activity"),
    ("dds.dim_game", "marts.mart_game_daily_metrics"),
    ("dds.fact_review", "marts.mart_game_daily_metrics"),
    ("dds.fact_player_activity", "marts.mart_game_daily_metrics"),
    ("dds.dim_genre", "marts.mart_genre_daily_metrics"),
    ("dds.bridge_game_genre", "marts.mart_genre_daily_metrics"),
    ("dds.fact_review", "marts.mart_genre_daily_metrics"),
    ("dds.fact_player_activity", "marts.mart_genre_daily_metrics"),
    ("dds.dim_game", "marts.mart_activity_hourly"),
    ("dds.fact_player_activity", "marts.mart_activity_hourly"),
]


def call(method: str, path: str, token: str | None = None, payload=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        f"{SERVER}{path}", data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body) if body else None
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = {"message": body}
        return exc.code, parsed


def login() -> str:
    encoded = base64.b64encode(PASSWORD.encode()).decode()
    status, body = call(
        "POST",
        "/v1/users/login",
        payload={"email": EMAIL, "password": encoded},
    )
    if status != 200 or not body or "accessToken" not in body:
        raise RuntimeError(f"OpenMetadata login failed: {status} {body}")
    return body["accessToken"]


def ensure_pipeline_service(token: str) -> None:
    status, _ = call(
        "GET", "/v1/services/pipelineServices/name/steam-airflow", token
    )
    if status == 200:
        print("Pipeline service steam-airflow exists.")
        return

    status, body = call(
        "POST",
        "/v1/services/pipelineServices",
        token,
        {
            "name": "steam-airflow",
            "description": "Airflow DAGs used by the Steam data platform",
            "serviceType": "Airflow",
        },
    )
    if status not in (200, 201):
        raise RuntimeError(f"Pipeline service creation failed: {status} {body}")
    print("Created pipeline service steam-airflow.")


def ensure_pipelines(token: str) -> None:
    for name in PIPELINES:
        fqn = f"steam-airflow.{name}"
        status, _ = call(
            "GET",
            "/v1/pipelines/name/" + urllib.parse.quote(fqn, safe=""),
            token,
        )
        if status == 200:
            continue

        status, body = call(
            "POST",
            "/v1/pipelines",
            token,
            {
                "name": name,
                "displayName": name,
                "description": f"Airflow DAG {name}",
                "service": "steam-airflow",
                "sourceUrl": f"http://localhost:8080/dags/{name}",
            },
        )
        if status not in (200, 201, 409):
            print(f"Pipeline {name} creation failed: {status} {body}")
        else:
            print(f"Pipeline {name} available.")


def entity_id(token: str, kind: str, name: str) -> str:
    if kind == "table":
        fqn = f"steam-postgres.airflow.{name}"
        path = "/v1/tables/name/"
    else:
        fqn = f"steam-airflow.{name}"
        path = "/v1/pipelines/name/"

    status, body = call(
        "GET",
        path + urllib.parse.quote(fqn, safe=""),
        token,
    )
    if status != 200 or not body:
        raise RuntimeError(f"{kind} {fqn} not found: {status} {body}")
    return body["id"]


def ensure_lineage(token: str) -> None:
    ids = {}
    for table in {table for pair in TABLE_EDGES for table in pair}:
        ids[("table", table)] = entity_id(token, "table", table)
    for pipeline, table in PIPELINE_EDGES:
        ids[("pipeline", pipeline)] = entity_id(token, "pipeline", pipeline)
        ids[("table", table)] = entity_id(token, "table", table)

    created = 0
    total = len(TABLE_EDGES) + len(PIPELINE_EDGES)

    for from_kind, from_name, to_table in [
        *(("table", a, b) for a, b in TABLE_EDGES),
        *(("pipeline", p, t) for p, t in PIPELINE_EDGES),
    ]:
        status, body = call(
            "PUT",
            "/v1/lineage",
            token,
            {
                "edge": {
                    "fromEntity": {
                        "id": ids[(from_kind, from_name)],
                        "type": from_kind,
                    },
                    "toEntity": {
                        "id": ids[("table", to_table)],
                        "type": "table",
                    },
                    "description": f"{from_name} -> {to_table}",
                }
            },
        )
        if status in (200, 201):
            created += 1
        elif status == 409:
            pass
        else:
            print(
                f"Lineage {from_name} -> {to_table} failed: {status} {body}"
            )

    print(
        f"Lineage edges ready: {total} "
        f"(table={len(TABLE_EDGES)}, pipeline={len(PIPELINE_EDGES)}, accepted={created})."
    )


def main() -> None:
    token = login()
    ensure_pipeline_service(token)
    ensure_pipelines(token)
    ensure_lineage(token)


if __name__ == "__main__":
    main()
