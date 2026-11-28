ARG SUPERSET_IMAGE=apache/superset:4.1.2
FROM ${SUPERSET_IMAGE}

USER root
RUN pip install --no-cache-dir "psycopg2-binary==2.9.10"
RUN mkdir -p /app/superset_home /app/pythonpath \
    && chown -R superset:superset /app/superset_home /app/pythonpath
COPY superset_config.py /app/pythonpath/superset_config.py
COPY init_superset.py /app/pythonpath/init_superset.py
COPY entrypoint.sh /entrypoint-superset.sh
RUN chmod 0755 /entrypoint-superset.sh \
    && chown superset:superset /entrypoint-superset.sh /app/pythonpath/*.py

USER superset
ENTRYPOINT ["/entrypoint-superset.sh"]
