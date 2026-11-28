"""Инициализация Superset: миграции, роли и администратор.

Скрипт идемпотентен: существующий администратор не пересоздаётся.
Пароль берётся из env и не печатается.
"""

import os
import subprocess


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Не задана обязательная переменная {name}")
    return value


subprocess.run(["superset", "db", "upgrade"], check=True)
subprocess.run(["superset", "init"], check=True)

from superset import create_app  # noqa: E402  (после миграций)

app = create_app()
with app.app_context():
    security_manager = app.appbuilder.sm
    username = required("SUPERSET_ADMIN_USERNAME")
    if security_manager.find_user(username=username) is None:
        role = security_manager.find_role("Admin")
        if role is None:
            raise RuntimeError("Роль Admin не найдена после superset init")
        admin = security_manager.add_user(
            username=username,
            first_name=os.environ.get("SUPERSET_ADMIN_FIRSTNAME", "Superset"),
            last_name=os.environ.get("SUPERSET_ADMIN_LASTNAME", "Admin"),
            email=required("SUPERSET_ADMIN_EMAIL"),
            role=role,
            password=required("SUPERSET_ADMIN_PASSWORD"),
        )
        if not admin:
            raise RuntimeError("Не удалось создать администратора Superset")
        print(f"Создан администратор Superset: {username}")
    else:
        print(f"Администратор Superset уже существует: {username}")
