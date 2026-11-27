#!/bin/bash
set -euo pipefail

superset db upgrade

superset fab create-admin \
    --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
    --firstname "${SUPERSET_ADMIN_FIRSTNAME:-Superset}" \
    --lastname "${SUPERSET_ADMIN_LASTNAME:-Admin}" \
    --email "${SUPERSET_ADMIN_EMAIL:-admin@example.com}" \
    --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true

superset init

exec gunicorn \
    --bind 0.0.0.0:8088 \
    --workers 2 \
    --timeout 120 \
    "superset.app:create_app()"
