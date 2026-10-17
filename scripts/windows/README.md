# Инструкция для Windows: запуск проекта по шагам

Скрипты лежат в `scripts\windows\`. Все команды выполняются в **PowerShell** из корня проекта
(`big_data_course`). Запускайте скрипты так: `.\scripts\windows\01_setup_env.ps1`.

---

## Часть 0. Что нужно установить заранее

| Что | Зачем | Как проверить |
|---|---|---|
| **Docker Desktop** | Все сервисы (Airflow, базы) работают в контейнерах | `docker --version` |
| **Git** (опционально) | Чтобы скачать проект | `git --version` |
| **Python 3.10+** (опционально) | Нужен только для чтения результатов вне Docker | `python --version` |

Python и библиотеки на компьютере для запуска **не нужны**: всё выполняется внутри контейнеров.

### Установка Docker Desktop

1. Скачайте с https://www.docker.com/products/docker-desktop/ и установите.
2. Включите **WSL 2** (Docker Desktop подскажет, если он не включён). В PowerShell от администратора:
   `wsl --install` и перезагрузите компьютер.
3. Запустите Docker Desktop, дождитесь статуса **«Engine running»** (зелёный значок внизу).
4. Минимум: **6 ГБ оперативной памяти для Docker** (Settings → Resources), диск — 20 ГБ свободно.

### Разрешить запуск скриптов PowerShell (один раз)

Откройте PowerShell и выполните:

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

Подтвердите `Y`. Без этого Windows блокирует `.ps1`-файлы.

### Ключ Steam API (нужен только для каталога игр)

Получите ключ на https://steamcommunity.com/dev/apikey (нужен аккаунт Steam). Домен можно
указать любой, например `localhost`. Ключ понадобится на шаге 1.

---

## Порядок шагов

| № | Скрипт | Что делает | Ожидаемый результат | Время |
|---|---|---|---|---|
| 0 | `00_check_prerequisites.ps1` | Проверяет Docker, память, диск, порты | Все строки `[OK]` | 1 мин |
| 1 | `01_setup_env.ps1` | Создаёт `.env`, генерирует ключ шифрования, спрашивает ключ Steam | Файл `.env` создан, сообщение «.env is ready» | 2–10 мин (первый раз скачивается образ Airflow) |
| 2 | `02_start_stack.ps1` | Запускает все контейнеры и ждёт готовности | Строки `[READY]` для всех сервисов | 5–15 мин (первый раз скачиваются образы, несколько ГБ) |
| 3 | `03_check_connections.ps1` | Проверяет подключения к базам | Все строки `[OK]` | 1 мин |
| 4 | `04_run_catalog.ps1` | Загружает каталог игр Steam в PostgreSQL | Таблица `steam.ods_games` наполняется (100 игр за запуск) | 3–5 мин |
| 5 | `05_show_results.ps1` | Показывает, что лежит в базах | Числа строк в каждой базе | 10 сек |
| 6 | `06_run_player_stats.ps1` | Собирает текущий онлайн игроков в ClickHouse | Строки в `steam.player_stats` | 2–3 мин |
| 7 | `07_run_reviews.ps1` | Собирает отзывы топ-игр в MongoDB | Документы в `steam.ods_reviews` | 10–30 мин |
| 8 | `08_run_benchmark_smoke.ps1` | Быстрая проверка бенчмарка (1000 записей) | Сообщение об успехе, CSV в `data\processed\steam_benchmark\` | 1–2 мин |
| 9 | `09_run_benchmark_full.ps1` | Полный бенчмарк (10k / 50k / 100k записей) | `benchmark_summary.csv` с медианами | 1 час и более |
| 10 | `10_stop_stack.ps1` | Останавливает стек | Сообщение «Stack stopped» | 30 сек |

### Шаг 0. Проверка готовности

```powershell
.\scripts\windows\00_check_prerequisites.ps1
```

**Ожидается:** строки `[OK]` для Docker и Compose. Строки `[WARN]` по памяти, диску или порту не блокируют запуск.
Если `[FAIL]` — выполните подсказку из строки и запустите скрипт снова.

### Шаг 1. Файл настроек `.env`

```powershell
.\scripts\windows\01_setup_env.ps1
```

Скрипт спросит ключ Steam. Вставьте его (символы не отображаются) и нажмите Enter.
Можно нажать Enter без ключа: тогда каталог игр не заработает, остальные DAG — да.

**Ожидается:** `.env is ready`. Файл `.env` не попадает в git (указан в `.gitignore`).

### Шаг 2. Запуск контейнеров

```powershell
.\scripts\windows\02_start_stack.ps1
```

**Ожидается:** `[READY]` для `postgres`, `redis`, `clickhouse`, `mongo`, `airflow-apiserver`, `airflow-scheduler`, `airflow-worker`.
В конце: адрес интерфейса **http://localhost:8080**, логин `airflow`, пароль `airflow`.

Откройте интерфейс в браузере: это удобно для просмотра DAG и логов (вкладка **Grid** → клик по задаче → **Logs**).

### Шаг 3. Проверка подключений

```powershell
.\scripts\windows\03_check_connections.ps1
```

**Ожидается:** пять строк `[OK]`. Подключения задаются в `docker-compose.yaml` и создавать их вручную не нужно.

### Шаг 4. Каталог игр

```powershell
.\scripts\windows\04_run_catalog.ps1
```

Параметры по умолчанию: 100 игр за запуск, максимум 10 000. Для теста можно уменьшить:

```powershell
.\scripts\windows\04_run_catalog.ps1 -BatchSize 20
```

**Ожидается:** в конце сообщение `Done`. Если появилась ошибка `STEAM_API_KEY does not found`, ключ не сохранён:
перезапустите `01_setup_env.ps1`, затем `02_start_stack.ps1`.

> Каталог грузится по одной партии за запуск. Чтобы набрать больше игр, повторите шаг с другой датой:
> `.\scripts\windows\04_run_catalog.ps1 -Date 2026-10-18`.

### Шаг 5. Просмотр результатов

```powershell
.\scripts\windows\05_show_results.ps1
```

**Ожидается:** в `steam.v_catalog_stats` число `total_games` больше 0; в `failed_appids` небольшое число записей
(часть игр Steam не отдаёт данные, это нормально).

### Шаг 6. Онлайн игроков (ClickHouse)

```powershell
.\scripts\windows\06_run_player_stats.ps1
```

**Ожидается:** `SUCCESS`. Если задача `SKIPPED` с текстом «steam.ods_games is empty» — сначала выполните шаг 4.

### Шаг 7. Отзывы (MongoDB)

```powershell
.\scripts\windows\07_run_reviews.ps1
```

**Важно:** этот DAG ждёт завершения каталога за **ту же дату**. Если шаг 4 выполнен для даты `2026-10-17`,
запускайте шаг 7 с той же датой (по умолчанию — сегодня, поэтому менять ничего не нужно, если шаг 4 тоже был сегодня).

Для ускорения теста можно уменьшить объём:

```powershell
.\scripts\windows\07_run_reviews.ps1 -TopN 5 -MaxPages 1
```

**Ожидается:** `SUCCESS`, затем в `05_show_results.ps1` число документов в MongoDB больше 0.

> Если при запуске шаг 7 зависает в состоянии ожидания каталога, значит, каталог за эту дату не выполнялся.
> Выполните шаг 4 с той же датой или запустите `07_run_reviews.ps1` с датой, за которую каталог уже есть.

### Шаг 8. Быстрая проверка бенчмарка

```powershell
.\scripts\windows\08_run_benchmark_smoke.ps1
```

**Ожидается:** `SUCCESS`. Если ошибка — исправьте её до полного запуска: полный прогон длится час и больше.

### Шаг 9. Полный бенчмарк

```powershell
.\scripts\windows\09_run_benchmark_full.ps1
```

Закройте другие тяжёлые программы: бенчмарк сравнивает скорость записи, и посторонняя нагрузка исказит замеры.

**Ожидается:** `SUCCESS` и два файла в `data\processed\steam_benchmark\<дата>\`:
- `benchmark_summary.csv` — медианы по каждому хранилищу и объёму;
- `benchmark_runs.csv` — все прогоны.

Откройте `benchmark_summary.csv` в Excel: это исходные данные для графиков в отчёте.

### Шаг 10. Остановка

```powershell
.\scripts\windows\10_stop_stack.ps1
```

Данные сохраняются. Чтобы запустить снова, выполните шаг 2.

Полная очистка (удаляет **все** базы, нужно только для сброса с нуля):

```powershell
.\scripts\windows\10_stop_stack.ps1 -WipeData
```

---

## Частые проблемы

| Симптом | Что делать |
|---|---|
| `docker : The term 'docker' is not recognized` | Docker Desktop не установлен или не запущен. Запустите его и откройте новое окно PowerShell |
| `cannot be loaded because running scripts is disabled` | Выполните команду из раздела «Разрешить запуск скриптов» |
| `[WARN] Port 8080 is already in use` | Закройте программу, занимающую порт, либо остановите стек (`10_stop_stack.ps1`) |
| Шаг 2 не дожидается `[READY]` за 10 минут | Смотрите логи: `docker compose logs --tail 50 airflow-apiserver` |
| `Failed to generate FERNET_KEY` | Docker Desktop не запущен или не скачался образ: проверьте интернет и повторите шаг 1 |
| Шаг 4: `HTTP 429` или много `failed` | Steam ограничивает частоту запросов; подождите 10–15 минут и повторите |
| Нужно начать с нуля | `10_stop_stack.ps1 -WipeData`, затем шаги 1–2 заново |

## Что проверяет каждый шаг (для защиты)

- **Шаги 4 и 6** — загрузка каталога игр и онлайна (ETL: извлечение из API, преобразование, загрузка в базу).
- **Шаг 7** — отзывы в «документную» базу MongoDB.
- **Шаги 8–9** — сравнение трёх хранилищ по скорости записи, размеру и скорости запроса.
