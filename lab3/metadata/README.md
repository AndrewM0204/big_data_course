# Metadata-слой ЛР3 (OpenMetadata)

Каталог содержит оркестрацию ingestion и декларацию lineage для
OpenMetadata. Исполняемые файлы ingestion/lineage лежат в
`openmetadata/ingestion/`, потому что именно этот каталог смонтирован в
контейнеры базового `docker-compose.yaml`.

## Фактические имена и версии

Из базового `docker-compose.yaml`:

- OpenMetadata: сервис `openmetadata-server`, UI/API `http://127.0.0.1:8585`,
  образ `openmetadata/server:1.8.0`, healthcheck на порту 8586.
- OpenSearch: `opensearch`, образ 2.19.0.
- PostgreSQL метаданных OpenMetadata: `openmetadata-db`.
- Миграции: `openmetadata-migrate` (завершается до старта сервера).
- CLI ingestion: `openmetadata-ingestion`, образ
  `openmetadata/ingestion:1.8.0`, профиль `ingestion`, каталог
  `./openmetadata/ingestion` смонтирован в `/ingestion`.
- Lineage: `openmetadata-lineage`, тот же образ, профиль `lineage`.
- Источник данных: PostgreSQL `postgres`, БД `airflow`, схемы `steam`,
  `dds`, `marts`.
- Airflow: `airflow-apiserver`, UI `http://localhost:8080`, Airflow 3.3.2.

Конфигурация OpenMetadata в compose не проверена запуском: фактическая
совместимость API 1.8.0 с рецептом и lineage-скриптом здесь не
заявляется.

## Состав

| Путь | Назначение |
|---|---|
| `bootstrap.sh` | Роль, PostgreSQL ingestion и lineage одной командой |
| `env.example` | Шаблон переменных; реальные секреты не коммитить |
| `sql/create_om_reader.sql` | Роль `om_reader` только для чтения `steam/dds/marts` |
| `lineage/lineage.json` | Каноническая декларация pipeline-сущностей и рёбер |
| `airflow/airflow_recipe.yaml.disabled` | Плейсхолдер Airflow-рецепта, не используется |
| `../../openmetadata/ingestion/` | Исполняемые скрипты ingestion и lineage |

## Запуск

В локальном `.env` задайте администратора OpenMetadata и read-only роль
PostgreSQL:

```dotenv
OM_ADMIN_EMAIL=admin@open-metadata.org
OM_ADMIN_PASSWORD=CHANGE_ME
OM_POSTGRES_USER=om_reader
OM_POSTGRES_PASSWORD=CHANGE_ME
OM_WITH_DASHBOARD=1
```

Стандартные `admin@open-metadata.org` / `admin` — известные значения по
умолчанию; для защиты задайте свои и передайте их одновременно в compose и
`.env`. Bootstrap отклоняет значения с `CHANGE_ME`/`placeholder`.

Полный metadata-этап:

```bash
bash lab3/metadata/bootstrap.sh
```

Не создавать dashboard и связи к BI-вью:

```bash
bash lab3/metadata/bootstrap.sh --no-dashboard
```

Не создавать/обновлять `om_reader` (если роль уже есть):

```bash
bash lab3/metadata/bootstrap.sh --no-reader
```

Порядок шагов внутри `bootstrap.sh`:

1. разбирает базовый compose и включает профили `ingestion`/`lineage`;
2. проверяет PostgreSQL и healthcheck `openmetadata-server`;
3. создаёт роль `om_reader` с `SELECT` на `steam,dds,marts`
   (`OM_POSTGRES_USER` при этом должен быть `om_reader`; другая роль
   допустима только с `--no-reader`, когда она уже создана отдельно);
4. запускает `openmetadata-ingestion` (PostgreSQL ingestion);
5. запускает `openmetadata-lineage` с канонической декларацией
   `lab3/metadata/lineage/lineage.json`;
6. по умолчанию создаёт dashboard service `steam-superset`, dashboard
   `steam_analytics_lr3` и связи к восьми BI-вью.

Прямой запуск сервисов без bootstrap:

```bash
docker compose --profile ingestion run --rm openmetadata-ingestion
docker compose --profile lineage run --rm openmetadata-lineage
```

Для прямого запуска переменные `OM_*` должны быть в `.env` или переданы
через `-e`; Compose монтирует каталог с единственной декларацией
`lab3/metadata/lineage/lineage.json` в `/lineage`.

## Lineage

`lineage/lineage.json` содержит фактические имена из
`docs/lab3_contract.md`, `sql/marts/init_marts.sql` и `lab3/bi`:

- ODS → DDS: `steam.ods_games`, `steam.ods_reviews`,
  `steam.ods_player_stats` → измерения, мосты и факты `dds.*`;
- DDS → marts: `dds.dim_game`, `dds.fact_review`,
  `dds.fact_player_activity`, мосты жанров/издателей → четыре витрины;
- marts → BI: `marts.mart_*` и `marts.mart_dq_results` → восемь
  `marts.bi_*`;
- pipeline → table: восемь DAG-ов, включая `build_marts_dag` (витрины и
  DQ) и `data_quality_checks` (DQ DDS);
- dashboard → BI-вью: секция `dashboard` включена (`enabled: true`),
  восемь связей к представлениям Superset.

Отдельный DAG `data_quality_marts_dag` из варианта A в интегрированный
код не входит: DQ витрин выполняется внутри `build_marts_dag` и пишет в
`marts.mart_dq_results`. Поэтому в lineage нет несуществующего DAG-а;
связь `build_marts_dag → mart_dq_results` отражает фактическую запись.

Сервисный FQN таблиц: `<OM_POSTGRES_SERVICE>.<OM_POSTGRES_DB>.<schema.table>`
по умолчанию `steam-postgres.airflow.marts...`. Pipeline FQN:
`steam-airflow.<dag_id>`. Если ingestion зарегистрирует сервис под другим
именем, задайте `OM_POSTGRES_SERVICE` и перезапустите lineage.

Проверка в UI: `http://127.0.0.1:8585` → Databases → `steam-postgres` →
схемы `steam`, `dds`, `marts`; Pipelines → `steam-airflow`; Lineage для
таблиц, DAG-ов и dashboard.

## Airflow recipe

`airflow/airflow_recipe.yaml.disabled` — **плейсхолдер, не использовать**.
Коннектор OpenMetadata Airflow ориентирован на метаданные Airflow 2.x
(в частности, `serialized_dag.fileloc`). Проект использует Airflow 3.3.2;
совместимость коннектора OpenMetadata 1.8.0 с Airflow 3.3.2 не проверена
и не заявляется. DAG-и регистрируются как pipeline-сущности декларативно.

## Ограничения и честные оговорки

- Docker daemon, сеть и проектные сервисы не использовались. Ingestion и
  lineage **не исполнялись**; проверены синтаксис shell/Python, JSON/YAML
  и имена. `docker compose config` без запуска daemon подтверждает
  сервисы и профили.
- OpenMetadata 1.8.0 и его REST API не проверялись на совместимость с
  `lineage.py`. При ошибке скрипт печатает статус и завершается
  ненулевым кодом; отсутствующие таблицы перечисляются явно.
- Создание dashboard service/dashboard и рёбер dashboard → table — самый
  рискованный участок: в OpenMetadata обычно используется цепочка
  dashboard → chart → dataset → table. Прямое ребро dashboard → table
  является упрощением и может быть отклонено API; это не скрывается.
- `metadata ingest` может писать подробный лог. Скрипт маскирует пароль и
  JWT в перехваченном stdout/stderr, но не управляет файлами логов внутри
  образа. Секреты в репозиторий не записываются.
- `includeViews: true` означает, что BI-представления должны попасть в
  каталог. Если ingestion их не увидел, lineage сообщит об отсутствующих
  таблицах.
- `om_reader` намеренно получает `SELECT` на `steam/dds/marts`: коннектору
  PostgreSQL Metadata нужен доступ к каталогу, а в ODS/DDS есть
  персональные данные (`author_steamid`) и текст отзывов. OpenMetadata
  сохраняет метаданные, но роль широкая. Для строгого минимума уберите
  `SELECT` с `steam.ods_reviews`, `dds.dim_review` и `dds.dim_user`, если
  профилирование и выборка данных не используются, и проверьте ingestion.
- Фактические результаты ingestion, число созданных рёбер и скриншоты
  появятся только после реального запуска.

## Что проверено статически

- `bash -n` для `bootstrap.sh` и `openmetadata/ingestion/ingest-postgres.sh`.
- `python -m py_compile` для `openmetadata/ingestion/lineage.py`.
- JSON-декларация разбирается, YAML-шаблон разбирается как YAML;
  имена таблиц и DAG-ов сверены с контрактом и витринами.
- `docker compose config --services --profiles` подтверждает
  `openmetadata-server`, `openmetadata-ingestion`, `openmetadata-lineage`
  и профили `ingestion`, `lineage`.
- OpenMetadata, ingestion и lineage-скрипт не запускались.
