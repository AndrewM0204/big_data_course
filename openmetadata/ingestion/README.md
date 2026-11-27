# OpenMetadata ingestion

## PostgreSQL

```bash
docker compose --profile ingestion run --rm openmetadata-ingestion
```

Ingests `steam`, `dds` and `marts` schemas into the `steam-postgres` service.

## Airflow

OpenMetadata 1.8.0 Airflow connector expects the Airflow 2.x metadata schema
(`serialized_dag.fileloc` and related columns). The project uses Airflow 3.3,
so the connector is not used.

`airflow.yaml.disabled` is kept as a reference. Airflow DAGs are registered
through `lineage.py` as pipeline entities of the `steam-airflow` service.

## Lineage

```bash
docker compose --profile lineage run --rm openmetadata-lineage
```

The script:

- creates/updates the `steam-airflow` pipeline service;
- registers the main DAGs as pipelines;
- creates 23 table-level lineage edges ODS -> DDS -> marts;
- creates 16 pipeline -> table lineage edges for the main DAGs.
