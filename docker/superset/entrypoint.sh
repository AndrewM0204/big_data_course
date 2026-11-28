#!/bin/bash
# Запуск Superset: миграции метаданных, администратор и gunicorn.
# Пароли и секреты читаются только из env и не выводятся в лог.
set -euo pipefail

for name in SUPERSET_SECRET_KEY SUPERSET_ADMIN_USERNAME \
    SUPERSET_ADMIN_EMAIL SUPERSET_ADMIN_PASSWORD; do
    if [[ -z "${!name:-}" ]]; then
        echo "ОШИБКА: не задана переменная $name" >&2
        exit 1
    fi
done

python /app/pythonpath/init_superset.py

exec gunicorn \
    --bind 0.0.0.0:8088 \
    --workers "${SUPERSET_WORKERS:-2}" \
    --timeout "${SUPERSET_TIMEOUT:-120}" \
    "superset.app:create_app()"
