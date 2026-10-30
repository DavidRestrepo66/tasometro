# Airflow image with the ingestion package and an isolated dbt.
# dbt lives in its own virtualenv: its pins would otherwise fight Airflow's.
FROM ghcr.io/astral-sh/uv:0.13.6 AS uv

FROM apache/airflow:3.3.2-python3.12

COPY --from=uv /uv /usr/local/bin/uv

USER root
RUN mkdir -p /opt/dbt-venv /opt/tasometro && chown -R airflow:0 /opt/dbt-venv /opt/tasometro
USER airflow

# dbt, pinned by dbt/uv.lock. Build inputs are mounted, so they leave no layer behind.
RUN --mount=type=bind,source=dbt,target=/tmp/dbt \
    uv export --project /tmp/dbt --frozen --no-dev --no-emit-project --no-hashes \
        -o /opt/dbt-venv/requirements.txt \
    && uv venv /opt/dbt-venv --python /usr/local/bin/python --allow-existing \
    && uv pip install --no-cache --python /opt/dbt-venv/bin/python \
        -r /opt/dbt-venv/requirements.txt

# Ingestion package in the Airflow environment; Airflow itself stays pinned.
COPY --chown=airflow:0 ingestion /opt/tasometro/ingestion
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" /opt/tasometro/ingestion
