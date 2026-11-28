FROM apache/superset:4.1.2

USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.10
COPY superset_config.py /app/pythonpath/superset_config.py
COPY init_superset.py /app/lab3/init_superset.py
USER superset
