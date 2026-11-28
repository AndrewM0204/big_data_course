# BI-слой ЛР3 (Superset)

Каталог содержит только BI-артефакты: SQL-представления, безопасную роль чтения
и скрипт создания объектов Superset. Витрины `marts.*`, DDS и инфраструктура
находятся в других каталогах и здесь не изменяются.

## Фактические имена инфраструктуры

Из `docker-compose.yaml` и `docker-compose.lr3.yml`:

- PostgreSQL: сервис `postgres`, БД `airflow`, схемы `dds` и `marts`.
- Superset: сервис `lr3-superset`, UI `http://127.0.0.1:8088`, образ `lr3-superset:4.1.2`.
- Метаданные Superset: отдельный сервис `lr3-superset-db` (БД `lab3_superset`),
  поэтому BI не пишет в метаданные Airflow.
- Администратор Superset задаётся `LR3_SUPERSET_ADMIN_USER` и
  `LR3_SUPERSET_ADMIN_PASSWORD` в env-файле compose.

## Состав

| Путь | Назначение |
|---|---|
| `sql/00_create_bi_views.sql` | Шесть представлений `marts.bi_*` |
| `sql/10_create_superset_reader.sql` | Роль `superset_reader` только для чтения |
| `sql/20_verify_bi_views.sql` | Проверки наличия, grain и допустимых значений |
| `superset/bootstrap_superset.py` | Создание database, datasets, charts и dashboard по API |
| `bootstrap.sh` | Применение SQL и запуск скрипта Superset |
| `env.example` | Шаблон переменных; реальные секреты не коммитить |

## Шесть визуализаций

Все средние онлайна взвешены по `samples` и означают среднее по снимкам
отдельных игр, а не суммарный онлайн категории. SCD2-версии игры объединяются
по `appid`. Время отзывов и активности — UTC.

| № | Представление | Зерно | График |
|---|---|---|---|
| 1 | `marts.bi_top15_games_by_avg_players` | `appid` | Топ-15 игр по среднему онлайну; в подписи `game_label` есть appid, одноимённые игры не схлопываются |
| 2 | `marts.bi_activity_dow_hour_utc` | `(day_of_week_num, hour_of_day)` | Тепловая карта день недели × час UTC |
| 3 | `marts.bi_reviews_daily` | `activity_date` | Столбцы отзывов + линия доли положительных |
| 4 | `marts.bi_top_genres_by_reviews` | `genre_id` | Топ-10 жанров по отзывам |
| 5 | `marts.bi_genre_sentiment` | `genre_id` | Состав положительных и отрицательных отзывов по жанрам |
| 6 | `marts.bi_publisher_reviews_vs_online` | `publisher_id` | Издатели: отзывы против среднего онлайна, пузырьковая диаграмма |

Представления только читают `marts`: `mart_game_daily_metrics`,
`mart_activity_hourly`, `mart_genre_daily_metrics`,
`mart_publisher_daily_metrics`. Категорийные метрики неаддитивны: сумма по
жанрам или издателям не является общим итогом.

## Запуск

Предварительно: инфраструктура ЛР3 поднята, витрины `marts.*` заполнены DAG
`build_marts_dag`, в локальном env заданы `LR3_SUPERSET_ADMIN_USER`,
`LR3_SUPERSET_ADMIN_PASSWORD` и `LR3_SUPERSET_READER_PASSWORD`.

Полный BI-этап:

```bash
bash lab3/bi/bootstrap.sh
```

Только SQL (без Superset):

```bash
bash lab3/bi/bootstrap.sh --sql-only
```

Скрипт:

1. проверяет наличие четырёх витрин;
2. применяет `00_create_bi_views.sql` одной транзакцией;
3. создаёт/обновляет `superset_reader`;
4. копирует `bootstrap_superset.py` в контейнер `lr3-superset`;
5. создаёт подключение `steam-marts`, datasets, шесть charts и dashboard.

Проверка представлений:

```powershell
Get-Content -Raw -Encoding UTF8 lab3/bi/sql/20_verify_bi_views.sql |
  docker compose exec -T postgres psql -U airflow -d airflow -X -v ON_ERROR_STOP=1
```

Ручное применение представлений:

```powershell
Get-Content -Raw -Encoding UTF8 lab3/bi/sql/00_create_bi_views.sql |
  docker compose exec -T postgres psql -U airflow -d airflow -X -q -1 -v ON_ERROR_STOP=1
```

## Роль чтения

`superset_reader` получает только:

- `CONNECT` к `airflow`;
- `USAGE` на схему `marts`;
- `SELECT` на шесть `marts.bi_*` и ничего больше;
- `default_transaction_read_only = on`.

Нет доступа к `steam` и `dds`, нет `SELECT` на сырые таблицы `marts`, нет
`INSERT/UPDATE/DELETE/TRUNCATE`, нет `CREATE`. Старые широкие grants роли на
`marts/dds` отзываются. Если добавите новое BI-представление, выдайте на него
`SELECT` явно: default privileges для `superset_reader` намеренно не задаются.
Членства `superset_reader` в других ролях скрипт не аудитирует; при необходимости
проверьте `pg_auth_members` вручную.
Пароль читается psql из переменной `SUPERSET_READER_PASSWORD` через `\getenv`
и не хранится в SQL-файле. Скрипт Superset собирает URI
`postgresql+psycopg2://superset_reader:...@postgres:5432/airflow`; готовый URI
можно передать через `SUPERSET_MARTS_URI`, не записывая его в файл.

Оговорка: `ALTER ROLE ... PASSWORD` может попасть в серверный лог PostgreSQL
при включённом `log_statement=all`; в файлах репозитория пароля нет.

## Что именно делает скрипт Superset

`superset/bootstrap_superset.py` использует REST API Superset 4.1.2:

- `POST /api/v1/security/login`;
- database `steam-marts` с read-only URI;
- datasets на шесть `marts.bi_*`;
- charts: `echarts_timeseries_bar`, `heatmap`, `mixed_timeseries`, `bubble`;
- dashboard `Steam Analytics (ЛР3)` с layout 3×2.

Повторный запуск идемпотентен по именам: существующие объекты находятся через
`GET ...?q=...` и переиспользуются. Обновлять параметры существующих charts
можно флагом `SUPERSET_UPDATE_EXISTING=1`.

## Ограничения и честные оговорки

- Docker daemon, сеть и проектные сервисы не использовались. SQL **не
  исполнялся** на проектной базе: он прогнан только на временном локальном
  PostgreSQL 15 с mock-данными. Скрипт Superset не исполнялся; проверены
  синтаксис, JSON/YAML, имена и статическая согласованность.
- Нативный импорт ZIP-экспорта Superset не готовился и не проверялся.
  Используется API-скрипт; он тоже не проверен запуском.
- При изменении набора колонок в `marts.bi_*` представления пересоздаются,
  а Superset может хранить старые колонки dataset. В этом случае синхронизируйте
  dataset в UI или удалите его и запустите скрипт повторно.
- Поля REST API и типы графиков в Superset 4.1.2 могут отличаться. Наибольший
  риск: `mixed_timeseries` (график 3) и `bubble` (график 6). Если API вернёт
  ошибку, скрипт завершится ненулевым кодом и не будет выдавать частичный
  результат за успешный.
- Ручной fallback: создать датасеты на те же `marts.bi_*` в UI Superset и
  выбрать графики по таблице выше; для графика 3 при отсутствии
  `mixed_timeseries` использовать две метрики на двойной оси или два графика,
  для графика 6 — Scatter/Bubble с `x=review_count`, `y=avg_players_weighted`,
  `size=samples`.
- Фактические значения, период данных и подтверждение гипотез появятся только
  после реального запуска. В отчёте они не заявляются.
- Секреты не хранятся в файлах: используются `LR3_SUPERSET_ADMIN_PASSWORD`,
  `LR3_SUPERSET_READER_PASSWORD` и `SUPERSET_MARTS_URI` из окружения.

## Что проверено локально

- `bash -n` для `bootstrap.sh`, `python -m py_compile` для
  `bootstrap_superset.py`.
- SQL-представления и роли `superset_reader`/`om_reader` применены на временном
  локальном PostgreSQL 15 с mock-таблицами `dds/marts`: представления
  создались, проверочный SQL не нашёл нарушений, `superset_reader` получил
  `USAGE` на `marts` и `SELECT` только на шесть `marts.bi_*`, без доступа к `steam` и `dds`.
- `docker compose config` (без запуска Docker daemon) подтвердил имена
  сервисов `lr3-superset`, `postgres` и профиль `lr3`.
- Сам Superset, его REST API и дашборд не запускались; создание charts и
  dashboard остаётся непроверенным до реального запуска.
