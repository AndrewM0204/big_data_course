# BI-слой ЛР3 (Superset)

Каталог содержит BI-артефакты: SQL-представления, безопасную роль чтения
и скрипт создания объектов Superset. Витрины `marts.*`, DDS и
инфраструктура находятся в других каталогах и здесь не изменяются.

## Фактические имена инфраструктуры

Из базового `docker-compose.yaml`:

- PostgreSQL: сервис `postgres`, БД `airflow`, схемы `dds` и `marts`.
- Superset: сервис `superset`, UI `http://127.0.0.1:8088`, образ 4.1.2,
  healthcheck на `/health`.
- Метаданные Superset: отдельная БД `lab3_superset` в том же PostgreSQL,
  поэтому BI не пишет в метаданные Airflow.
- Администратор Superset задаётся `LR3_SUPERSET_ADMIN_USER` и
  `LR3_SUPERSET_ADMIN_PASSWORD` в `.env` (алиасы `SUPERSET_*` поддерживаются).
- Пароль роли чтения задаётся `LR3_SUPERSET_READER_PASSWORD` в `.env` и
  совпадает со значением, которое получает сервис `postgres`.

## Состав

| Путь | Назначение |
|---|---|
| `sql/00_create_bi_views.sql` | Восемь представлений `marts.bi_*` |
| `sql/10_create_superset_reader.sql` | Роль `superset_reader`: read-only marts и безопасный DDS |
| `sql/20_verify_bi_views.sql` | Проверки наличия, полей, зерна и значений |
| `superset/bootstrap_superset.py` | Создание database, datasets, charts и dashboard по API |
| `bootstrap.sh` | Применение SQL и запуск скрипта Superset |
| `env.example` | Шаблон переменных; реальные секреты не коммитить |

## Восемь визуализаций и шесть типов

Все средние онлайна взвешены по `samples` и означают среднее по снимкам
отдельных игр, а не суммарный онлайн категории. SCD2-версии игры
объединяются по `appid`. Время отзывов и активности — UTC.

| № | Представление | Тип Superset | Зерно / смысл |
|---|---|---|---|
| 1 | `marts.bi_top15_games_by_avg_players` | `echarts_timeseries_bar` | `appid`; топ-15 игр по среднему онлайну, в подписи есть appid |
| 2 | `marts.bi_activity_dow_hour_utc` | `heatmap` | `(day_of_week, hour_of_day)`; используются `day_of_week`, `day_name` |
| 3 | `marts.bi_weekend_hourly` | `echarts_timeseries_bar` | `(is_weekend, hour_of_day)`; используются `is_weekend` |
| 4 | `marts.bi_reviews_daily` | `mixed_timeseries` | `activity_date`; столбцы отзывов + линия доли положительных |
| 5 | `marts.bi_top_genres_by_reviews` | `pie` | `genre_id`; доля топ-10 жанров в сумме отзывов |
| 6 | `marts.bi_genre_sentiment` | `echarts_timeseries_bar` | `genre_id`; положительные и отрицательные отзывы |
| 7 | `marts.bi_publisher_reviews_vs_online` | `bubble` | `publisher_id`; отзывы против среднего онлайна |
| 8 | `marts.bi_dq_latest` | `table` | `check_name`; последние DQ-проверки из `marts.mart_dq_results` |

Различных типов — шесть: bar, heatmap, mixed_timeseries, pie, bubble,
table. Это больше требуемых пяти; два столбчатых графика решают разные
задачи (топ игр и будни/выходные), а не дублируют друг друга.

Аналитические оговорки:

- Онлайн собирается раз в 3 часа, поэтому на тепловой карте и графике
  будни/выходные есть только фактически наблюдённые часы (обычно
  0, 3, 6, ..., 21 UTC). Отсутствующие часы не досчитываются.
- Для разреза по дню недели нужна история не менее семи суток; при
  меньшем периоде часть дней и выходных может отсутствовать.
- Одна игра может входить в несколько жанров. Сумма отзывов по жанрам
  не является числом уникальных отзывов; это указано в описании пирога.
- Доля положительных — `positive_reviews / review_count`; при нуле
  отзывов значение `NULL`. Оценка отзыва текущая, а не восстановленная
  на историческую дату.
- `max_daily_games_count` в категорийных вью — максимум игр за один день,
  не union за весь период. Для графика он не используется.

## Почему в чартах используется MAX

BI-вью уже агрегированы до зерна графика: одна строка на `appid`, на
`(day_of_week, hour_of_day)`, на `genre_id`, на `publisher_id`, на дату
или на `check_name`. Поэтому `MAX(поле)` в Superset — это передача
единственного значения, а не повторная агрегация и не завышение метрики.
Если бы чарт строился на сырой витрине с несколькими строками на
категорию, `MAX` был бы неверен; здесь это не так.

## Запуск

Предварительно: инфраструктура поднята, витрины `marts.*` заполнены DAG
`build_marts_dag`, в `.env` заданы `LR3_SUPERSET_ADMIN_USER`,
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

1. проверяет пять таблиц `marts.*`, включая `marts.mart_dq_results`;
2. проверяет поля `day_of_week`, `day_name`, `is_weekend` в
   `marts.mart_activity_hourly`;
3. применяет `00_create_bi_views.sql` одной транзакцией;
4. создаёт/обновляет `superset_reader`;
5. копирует `bootstrap_superset.py` в контейнер `superset`;
6. создаёт подключение `steam-marts`, восемь datasets, восемь charts и
   dashboard.

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
- `USAGE` на схемы `marts` и `dds`;
- `SELECT` на пять таблиц `marts` и восемь `marts.bi_*`;
- `SELECT` на безопасные аналитические таблицы DDS: `dim_game`,
  `dim_genre`, `dim_developer`, `dim_publisher`, три bridge-таблицы,
  `fact_player_activity`, `dq_results`;
- `default_transaction_read_only = on`;
- `search_path = marts, dds, pg_catalog`.

Нет доступа к `steam`, `dds.dim_review` (текст отзывов), `dds.dim_user`
(`steamid`) и `dds.fact_review`. Сырые `marts` — агрегаты без PII; DDS с
персональными данными закрыт. Нет `INSERT/UPDATE/DELETE/TRUNCATE`, нет
`CREATE`. Старые широкие grants роли на `marts/dds/steam` отзываются, а
default privileges на новые таблицы намеренно не выдаются: при добавлении
объекта выдайте `SELECT` явно. Членства `superset_reader` в других ролях
скрипт не аудитирует; при необходимости проверьте `pg_auth_members` вручную.
Пароль читается psql из переменной `SUPERSET_READER_PASSWORD` через
`\getenv` и не хранится в SQL-файле. Имя роли зафиксировано:
`SUPERSET_READER_USER` принимает только `superset_reader`, иное значение
bootstrap и Superset-скрипт отклоняют, потому что SQL и grants используют
это имя жёстко. Скрипт Superset собирает URI
`postgresql+psycopg2://superset_reader:...@postgres:5432/airflow`; готовый
URI можно передать через `SUPERSET_MARTS_URI`, не записывая его в файл.

Оговорка: `ALTER ROLE ... PASSWORD` может попасть в серверный лог
PostgreSQL при включённом `log_statement=all`; в файлах репозитория
пароля нет.

## Что именно делает скрипт Superset

`superset/bootstrap_superset.py` использует REST API Superset 4.1.2:

- `POST /api/v1/security/login`;
- database `steam-marts` с read-only URI;
- datasets на восемь `marts.bi_*`;
- charts: `echarts_timeseries_bar` ×3, `heatmap`, `mixed_timeseries`,
  `pie`, `bubble`, `table`;
- dashboard `Steam Analytics (ЛР3)` с layout 2×4.

Повторный запуск идемпотентен по именам: существующие объекты находятся
через `GET ...?q=...` и переиспользуются. Обновлять параметры
существующих charts можно флагом `SUPERSET_UPDATE_EXISTING=1`. Для
существующих датасетов вызывается попытка refresh колонок; если API её не
поддерживает, выводится предупреждение.

## Ограничения и честные оговорки

- Docker daemon, сеть и проектные сервисы не использовались. SQL **не
  исполнялся** на проектной базе: проверены синтаксис, имена и
  статическая согласованность. Скрипт Superset **не исполнялся**;
  проверены синтаксис Python и JSON.
- Нативный импорт ZIP-экспорта Superset не готовился и не проверялся.
  Используется API-скрипт; он тоже не проверен запуском.
- Поля REST API и типы графиков в Superset 4.1.2 могут отличаться.
  Наибольший риск: `mixed_timeseries` (график 4) и `bubble` (график 7),
  а также `pie` с adhoc-метрикой. При ошибке API скрипт завершается
  ненулевым кодом и не выдаёт частичный результат за успешный.
- Ручной fallback: создать датасеты на те же `marts.bi_*` в UI Superset и
  выбрать графики по таблице выше; для графика 4 при отсутствии
  `mixed_timeseries` использовать две метрики на двойной оси или два
  графика, для графика 7 — Scatter/Bubble с `x=review_count`,
  `y=avg_players_weighted`, `size=samples`.
- Фактические значения, период данных и подтверждение гипотез появятся
  только после реального запуска. В отчёте они не заявляются.
- Секреты не хранятся в файлах: используются `LR3_SUPERSET_ADMIN_PASSWORD`,
  `LR3_SUPERSET_READER_PASSWORD` и `SUPERSET_MARTS_URI` из окружения.

## Что проверено статически

- `bash -n` для `bootstrap.sh`, `python -m py_compile` для
  `bootstrap_superset.py`.
- SQL-представления проверены на баланс скобок и ссылки на поля витрин;
  проверочный SQL сверяет `day_of_week`/`day_name`/`is_weekend`,
  `mart_dq_results`, диапазоны и уникальность зерна. На проектной БД SQL
  не исполнялся.
- `docker compose config --services` подтвердил сервисы `postgres` и
  `superset` в базовом compose.
- Сам Superset, его REST API и дашборд не запускались; создание charts и
  dashboard остаётся непроверенным до реального запуска.
