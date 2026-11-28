import os
import subprocess

from superset import create_app


subprocess.run(["superset", "db", "upgrade"], check=True)
subprocess.run(["superset", "init"], check=True)

app = create_app()
with app.app_context():
    username = os.environ["LR3_SUPERSET_ADMIN_USER"]
    manager = app.appbuilder.sm
    if not manager.find_user(username=username):
        admin = manager.add_user(
            username=username,
            first_name="Lab",
            last_name="Admin",
            email=os.environ["LR3_SUPERSET_ADMIN_EMAIL"],
            role=manager.find_role("Admin"),
            password=os.environ["LR3_SUPERSET_ADMIN_PASSWORD"],
        )
        if not admin:
            raise RuntimeError("Не удалось создать администратора Superset")
