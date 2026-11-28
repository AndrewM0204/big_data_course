# OpenMetadata ingestion и lineage

Каталог смонтирован в контейнеры `openmetadata-ingestion` и
`openmetadata-lineage` из базового `docker-compose.yaml`. Здесь лежат
исполняемые файлы, поэтому пути и имена совпадают с entrypoint сервисов.

## PostgreSQL ingestion

```bash
docker compose --profile ingestion run --rm openmetadata-ingestion
```

Сервис `openmetadata-ingestion` монтирует этот каталог в `/ingestion` и
запускает `ingest-postgres.sh`. Скрипт:

1. входит в OpenMetadata от администратора;
2. подставляет в `postgres_recipe.yaml.tmpl` значения из окружения;
3. запускает `metadata ingest` для схем `steam`, `dds`, `marts`;
4. удаляет временный рецепт и маскирует пароли/JWT в выводе.

Обязательные переменные: `OM_ADMIN_EMAIL`, `OM_ADMIN_PASSWORD`,
`OM_POSTGRES_USER`, `OM_POSTGRES_PASSWORD`. Остальные значения имеют
значения по умолчанию: `OM_SERVER_URL=http://openmetadata-server:8585/api`,
`OM_POSTGRES_HOST=postgres`, `OM_POSTGRES_PORT=5432`,
`OM_POSTGRES_DB=airflow`, `OM_POSTGRES_SERVICE=steam-postgres`.

Пароль роли чтения создаётся и обновляется в
`lab3/metadata/sql/create_om_reader.sql`. Роль `om_reader` намеренно
получает `SELECT` на `steam`, `dds`, `marts`: коннектору метаданных нужен
каталог таблиц. В ODS/DDS есть текст отзывов и `steamid`, поэтому доступ
к самому OpenMetadata должен быть ограничен.

## Lineage

```bash
docker compose --profile lineage run --rm openmetadata-lineage
```

Сервис запускает `lineage.py` с декларацией из `/lineage/lineage.json`.
Единственный файл декларации — `lab3/metadata/lineage/lineage.json`;
Compose монтирует его каталог в контейнер. Для запуска вне Compose
укажите путь через `OM_LINEAGE_DECLARATION` или аргумент скрипта.

Декларация содержит:

- ODS → DDS: `steam.ods_games`, `steam.ods_reviews`,
  `steam.ods_player_stats` → измерения, мосты и факты `dds.*`;
- DDS → marts: `dds.dim_game`, `dds.fact_review`,
  `dds.fact_player_activity`, мосты жанров/издателей → четыре витрины;
- marts → BI: `marts.mart_*` и `marts.mart_dq_results` → восемь
  представлений `marts.bi_*`;
- pipeline → table: восемь DAG-ов, включая `build_marts_dag` и
  `data_quality_checks`;
- dashboard → BI-вью: секция `dashboard` включена (`enabled: true`),
  сервис `steam-superset`, дашборд `steam_analytics_lr3`, восемь связей.

Связи `dashboard → table` — упрощение: в OpenMetadata обычно используется
цепочка dashboard → chart → dataset → table. API 1.8.0 может отклонить
такое ребро; скрипт сообщит об ошибке и завершится ненулевым кодом, не
выдавая частичный результат за успешный. Это не проверено запуском.

## Airflow

`airflow.yaml.disabled` — плейсхолдер. Коннектор OpenMetadata 1.8.0
ориентирован на метаданные Airflow 2.x (`serialized_dag.fileloc`), а
проект использует Airflow 3.3.2. Совместимость не проверена и не
заявляется. DAG-и регистрируются как pipeline-сущности декларативно.

## Ограничения

- Docker daemon, сеть и проектные сервисы при подготовке не запускались.
  Ingestion, lineage и OpenMetadata API **не исполнялись**; проверены
  синтаксис, JSON/YAML и имена.
- OpenMetadata 1.8.0 и его REST API не сверялись с `lineage.py` запуском.
  Наиболее рискованные места: `service` в payload pipeline/dashboard и
  прямые рёбра dashboard → table.
- `metadata ingest` может писать подробный лог. Скрипт маскирует пароль и
  JWT в перехваченном stdout/stderr, но не управляет файлами логов внутри
  образа.
- Фактические результаты ingestion, число созданных рёбер и скриншоты
  появятся только после реального запуска.
