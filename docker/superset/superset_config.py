import os

from sqlalchemy.engine import URL


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Не задана обязательная переменная {name}")
    return value


# Секрет обязателен: небезопасные значения по умолчанию не используются.
SECRET_KEY = required("SUPERSET_SECRET_KEY")

# URI можно передать целиком или собрать из частей: при спецсимволах
# в пароле сборка через URL.create экранирует их корректно.
_database_uri = os.environ.get("SUPERSET_SQLALCHEMY_DATABASE_URI")
if _database_uri:
    SQLALCHEMY_DATABASE_URI = _database_uri
else:
    SQLALCHEMY_DATABASE_URI = URL.create(
        "postgresql+psycopg2",
        username=required("SUPERSET_DB_USER"),
        password=required("SUPERSET_DB_PASSWORD"),
        host=os.environ.get("SUPERSET_DB_HOST", "postgres"),
        port=int(os.environ.get("SUPERSET_DB_PORT", "5432")),
        database=required("SUPERSET_DB_NAME"),
    ).render_as_string(hide_password=False)

LOAD_EXAMPLES = False
SQLALCHEMY_TRACK_MODIFICATIONS = False
SQLALCHEMY_ENGINE_OPTIONS = {"pool_pre_ping": True, "pool_recycle": 300}
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"
