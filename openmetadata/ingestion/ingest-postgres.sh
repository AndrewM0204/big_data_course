#!/usr/bin/env bash
# PostgreSQL ingestion в OpenMetadata без хранения секретов в рецепте.
# Запускается внутри контейнера openmetadata/ingestion.
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE="${1:-$SCRIPT_DIR/postgres_recipe.yaml.tmpl}"
PYTHON_BIN="${PYTHON_BIN:-python}"

[[ -r "$TEMPLATE" ]] || {
    printf '[om] ОШИБКА: не найден шаблон %s\n' "$TEMPLATE" >&2
    exit 1
}

export OM_SERVER_URL="${OM_SERVER_URL:-http://openmetadata-server:8585/api}"
export OM_POSTGRES_SERVICE="${OM_POSTGRES_SERVICE:-steam-postgres}"
export OM_POSTGRES_HOST="${OM_POSTGRES_HOST:-postgres}"
export OM_POSTGRES_PORT="${OM_POSTGRES_PORT:-5432}"
export OM_POSTGRES_DB="${OM_POSTGRES_DB:-airflow}"

for key in OM_ADMIN_EMAIL OM_ADMIN_PASSWORD OM_POSTGRES_USER OM_POSTGRES_PASSWORD; do
    if [[ -z "${!key:-}" ]]; then
        printf '[om] ОШИБКА: не задан %s (значение не выводится).\n' "$key" >&2
        exit 1
    fi
done

"$PYTHON_BIN" - "$TEMPLATE" <<'PY'
import base64
import json
import os
import subprocess
import sys
import tempfile
import urllib.request

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

server = os.environ["OM_SERVER_URL"].rstrip("/")
email = os.environ["OM_ADMIN_EMAIL"]
password = os.environ["OM_ADMIN_PASSWORD"]
postgres_password = os.environ["OM_POSTGRES_PASSWORD"]

secrets = [password, postgres_password]


def redact(text):
    text = str(text)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return text


def login():
    payload = json.dumps(
        {
            "email": email,
            "password": base64.b64encode(password.encode("utf-8")).decode("ascii"),
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        server + "/v1/users/login",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        raise SystemExit("[om] Ошибка входа в OpenMetadata: " + redact(exc))
    token = body.get("accessToken") if isinstance(body, dict) else None
    if not token:
        raise SystemExit("[om] OpenMetadata не вернул accessToken.")
    secrets.append(token)
    return token


def prepare_recipe(token):
    with open(os.environ["TEMPLATE_PATH"], encoding="utf-8") as handle:
        recipe = handle.read()

    def escape(value):
        # JSON-строка является корректным YAML double-quoted scalar.
        return json.dumps(str(value), ensure_ascii=False)[1:-1]

    replacements = {
        "__OM_POSTGRES_SERVICE__": escape(os.environ["OM_POSTGRES_SERVICE"]),
        "__OM_POSTGRES_USER__": escape(os.environ["OM_POSTGRES_USER"]),
        "__OM_POSTGRES_PASSWORD__": escape(postgres_password),
        "__OM_POSTGRES_HOST__": escape(os.environ["OM_POSTGRES_HOST"]),
        "__OM_POSTGRES_PORT__": escape(os.environ["OM_POSTGRES_PORT"]),
        "__OM_POSTGRES_DB__": escape(os.environ["OM_POSTGRES_DB"]),
        "__OM_SERVER_URL__": escape(server),
        "__OM_JWT_TOKEN__": escape(token),
    }
    for key, value in replacements.items():
        recipe = recipe.replace(key, value)

    handle, path = tempfile.mkstemp(prefix="om_postgres_", suffix=".yaml", dir="/tmp")
    os.close(handle)
    os.chmod(path, 0o600)
    with open(path, "w", encoding="utf-8") as output:
        output.write(recipe)
    return path


def run_ingest(path):
    process = subprocess.Popen(
        ["metadata", "ingest", "-c", path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert process.stdout is not None
    for line in process.stdout:
        sys.stdout.write(redact(line))
    return process.wait()


def main():
    token = login()
    os.environ["TEMPLATE_PATH"] = sys.argv[1]
    recipe_path = prepare_recipe(token)
    try:
        print("[om] Рецепт подготовлен; запускаю metadata ingest.")
        code = run_ingest(recipe_path)
    finally:
        try:
            os.unlink(recipe_path)
        except OSError:
            pass
    if code != 0:
        raise SystemExit("[om] metadata ingest завершился с кодом {}".format(code))
    print("[om] PostgreSQL ingestion завершён.")


if __name__ == "__main__":
    main()
PY
