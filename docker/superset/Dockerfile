ARG SUPERSET_IMAGE=apache/superset:4.1.2
FROM ${SUPERSET_IMAGE}

USER root
RUN pip install --no-cache-dir psycopg2-binary
RUN mkdir -p /app/superset_home /app/pythonpath && chown -R superset:superset /app/superset_home
COPY superset_config.py /app/pythonpath/superset_config.py
COPY entrypoint.sh /entrypoint-superset.sh
RUN chmod +x /entrypoint-superset.sh

USER superset
ENTRYPOINT ["/entrypoint-superset.sh"]
