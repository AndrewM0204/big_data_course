# Metadata-слой ЛР3 (OpenMetadata)

Каталог содержит воспроизводимую декларацию lineage и скрипты ingestion для
OpenMetadata. Инфраструктура OpenMetadata и Airflow находится в
`docker-compose.lr3.yml` и здесь не изменяется.

## Фактические имена и версии

Из `docker-compose.lr3.yml`:

- OpenMetadata: сервис `lr3-openmetadata`, UI/API `http://127.0.0.1:8585`,
  образ `openmetadata/server:1.8.0`.
- OpenSearch: `lr3-opensearch`.
- PostgreSQL метаданных OpenMetadata: `lr3-openmetadata-db`.
- CLI ingestion: `lr3-ingestion`, образ `openmetadata/ingestion:1.8.0`,
  профиль `ingestion`. Контейнер по умолчанию спит; скрипты запускаются через
  `docker compose run`.
- Источник данных: PostgreSQL `postgres`, БД `airflow`, схемы `steam`, `dds`,
  `marts`.
- Airflow: `airflow-apiserver`, UI `http://localhost:8080`, Airflow 3.3.2.

Важно: конфигурация OpenMetadata в compose помечена автором как не проверенная
запуском. Фактическая совместимость API 1.8.0 с рецептом и lineage-скриптом
здесь не проверялась.

## Состав

| Путь | Назначение |
|---|---|
| `ingestion/postgres_recipe.yaml.tmpl` | Рецепт PostgreSQL ingestion без секретов |
| `ingestion/ingest_postgres.sh` | Подстановка env, вход в OM, `metadata ingest` |
| `sql/create_om_reader.sql` | Роль `om_reader` только для чтения `steam/dds/marts` |
| `lineage/lineage.json` | Декларация pipeline-сущностей и рёбер lineage |
| `lineage/apply_lineage.py` | Создание сервисов, pipeline и рёбер через API OM |
| `airflow/airflow_recipe.yaml.disabled` | Плейсхолдер Airflow-рецепта, не используется |
| `bootstrap.sh` | Роль, ingestion и lineage одной командой |
| `env.example` | Шаблон переменных; реальные секреты не коммитить |

## Запуск

В локальном env задайте администратора OpenMetadata и read-only роль
PostgreSQL:

```dotenv
OM_ADMIN_EMAIL=...
OM_ADMIN_PASSWORD=...
OM_POSTGRES_USER=om_reader
OM_POSTGRES_PASSWORD=...
```

Полный metadata-этап:

```bash
bash lab3/metadata/bootstrap.sh
```

Создать также dashboard-сущность и связи к BI-вью:

```bash
bash lab3/metadata/bootstrap.sh --with-dashboard
```

Не создавать/обновлять `om_reader` (если роль уже есть):

```bash
bash lab3/metadata/bootstrap.sh --no-reader
```

Порядок шагов внутри `bootstrap.sh`:

1. проверяет compose, PostgreSQL, сервис `lr3-openmetadata` и `lr3-ingestion`;
2. создаёт роль `om_reader` с `SELECT` на `steam,dds,marts`;
3. запускает PostgreSQL ingestion: `steam`, `dds`, `marts` и представления;
4. создаёт pipeline service `steam-airflow`, pipeline-сущности DAG-ов и
   table-level lineage;
5. при `--with-dashboard` создаёт `steam-superset` / `steam_analytics_lr3` и
   связи dashboard → BI-вью.

## Lineage

`lineage/lineage.json` содержит фактические имена из
`docs/lab3_contract.md` и `sql/marts/init_marts.sql`:

- ODS → DDS: `steam.ods_games`, `steam.ods_reviews`,
  `steam.ods_player_stats` → измерения, мосты и факты `dds.*`;
- DDS → marts: `dds.dim_game`, `dds.fact_review`,
  `dds.fact_player_activity`, мосты жанров/издателей → четыре витрины;
- marts → BI: `marts.mart_*` → шесть `marts.bi_*` из `lab3/bi`;
- pipeline → table: восемь DAG-ов, включая `build_marts_dag`,
  `data_quality_checks`, `dds_*` и ODS-сборщики;
- опционально dashboard → шесть BI-вью.

Сервисный FQN таблиц: `<OM_POSTGRES_SERVICE>.<OM_POSTGRES_DB>.<schema.table>`
по умолчанию `steam-postgres.airflow.marts...`. Pipeline FQN:
`steam-airflow.<dag_id>`. Если ingestion зарегистрирует сервис под другим
именем, задайте `OM_POSTGRES_SERVICE` и перезапустите lineage.

Проверка в UI: `http://127.0.0.1:8585` → Databases → `steam-postgres` →
схемы `steam`, `dds`, `marts`; Pipelines → `steam-airflow`; Lineage для
таблиц и DAG-ов.

## Airflow recipe

`airflow/airflow_recipe.yaml.disabled` — **плейсхолдер, не использовать**.
OpenMetadata-коннектор Airflow ориентирован на метаданные Airflow 2.x
(в частности, `serialized_dag.fileloc`). Проект использует Airflow 3.3.2;
совместимость коннектора OpenMetadata 1.8.0 с Airflow 3.3.2 не проверена и
не заявляется. В референсе `origin/lab3_feature_a` для коннектора 1.8.0
отмечена несовместимость с Airflow 3 (ожидается `serialized_dag.fileloc`).
DAG-и регистрируются как pipeline-сущности декларативно через
`lineage/lineage.json` и `lineage/apply_lineage.py`.

## Ограничения и честные оговорки

- Docker daemon, сеть и проектные сервисы не использовались. Ingestion и
  lineage **не исполнялись**; проверены синтаксис shell/Python, JSON/YAML и
  имена.
- OpenMetadata 1.8.0 и его REST API не проверялись на совместимость с
  `apply_lineage.py`. При ошибке скрипт печатает статус и завершается
  ненулевым кодом; отсутствующие таблицы перечисляются явно.
- Создание dashboard service/dashboard и рёбер dashboard → table — самый
  рискованный участок: в OpenMetadata обычно используется цепочка
  dashboard → chart → dataset → table. Прямое ребро dashboard → table является
  упрощением и может быть отклонено API; это не скрывается.
- `metadata ingest` может писать подробный лог. Скрипт маскирует пароль и JWT
  в перехваченном stdout/stderr, но не управляет файлами логов внутри образа.
  Секреты в репозиторий не записываются.
- Рецепт использует `includeViews: true`, поэтому BI-представления должны
  попасть в каталог. Если ingestion их не увидел, lineage сообщит об
  отсутствующих таблицах.
- `om_reader` намеренно получает `SELECT` на `steam/dds/marts`: коннектору
  PostgreSQL Metadata нужен доступ к каталогу, а в ODS/DDS есть персональные
  данные (`author_steamid`) и текст отзывов. OpenMetadata сохраняет метаданные,
  но роль широкая. Для строгого минимума уберите `SELECT` с
  `steam.ods_reviews`, `dds.dim_review` и `dds.dim_user`, если профилирование
  и выборка данных не используются, и проверьте ingestion.
- Фактические результаты ingestion, число созданных рёбер и скриншоты
  появятся только после реального запуска.

## Что проверено локально

- `bash -n` для `bootstrap.sh` и `ingest_postgres.sh`,
  `python -m py_compile` для `apply_lineage.py`.
- JSON-декларация разбирается, YAML-шаблон разбирается как YAML;
  имена таблиц и DAG-ов сверены с контрактом и витринами.
- SQL роли `om_reader` применён на временном локальном PostgreSQL 15:
  роль получила `USAGE/SELECT` на `steam/dds/marts`.
- `docker compose config` (без Docker daemon) подтвердил сервисы
  `lr3-openmetadata`, `lr3-ingestion` и профили `lr3`, `ingestion`.
- OpenMetadata, ingestion и lineage-скрипт не запускались.
