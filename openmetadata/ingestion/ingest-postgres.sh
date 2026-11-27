#!/bin/bash
set -euo pipefail

RECIPE_PATH="${1:-/ingestion/postgres.yaml}"
export RECIPE_PATH
OM_SERVER_URL="${OM_SERVER_URL:-http://openmetadata-server:8585/api}"
OM_ADMIN_EMAIL="${OM_ADMIN_EMAIL:-admin@open-metadata.org}"
OM_ADMIN_PASSWORD="${OM_ADMIN_PASSWORD:-admin}"

python - <<'PY'
import base64
import json
import os
import urllib.request

server = os.environ.get("OM_SERVER_URL", "http://openmetadata-server:8585/api").rstrip("/")
email = os.environ.get("OM_ADMIN_EMAIL", "admin@open-metadata.org")
password = os.environ.get("OM_ADMIN_PASSWORD", "admin")

payload = json.dumps({
    "email": email,
    "password": base64.b64encode(password.encode()).decode(),
}).encode("utf-8")

request = urllib.request.Request(
    f"{server}/v1/users/login",
    data=payload,
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=60) as response:
    body = json.loads(response.read().decode("utf-8"))

token = body["accessToken"]
template = open(os.environ["RECIPE_PATH"], encoding="utf-8").read()
recipe = template.replace("__OM_JWT_TOKEN__", token)
with open("/tmp/ingestion_recipe.yaml", "w", encoding="utf-8") as f:
    f.write(recipe)

print("OpenMetadata ingestion recipe prepared.")
PY

metadata ingest -c /tmp/ingestion_recipe.yaml
