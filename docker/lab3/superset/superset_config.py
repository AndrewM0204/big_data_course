import os
from sqlalchemy.engine import URL


SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = URL.create(
    "postgresql+psycopg2",
    username=os.environ["LR3_SUPERSET_DB_USER"],
    password=os.environ["LR3_SUPERSET_DB_PASSWORD"],
    host="lr3-superset-db",
    port=5432,
    database=os.environ["LR3_SUPERSET_DB_NAME"],
).render_as_string(hide_password=False)
