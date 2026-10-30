"""Weekly SFC lending-rate pipeline.

Each run owns one cut (the Friday two weeks before the run) and re-extracts
the seven cuts before it, since banks revise recent weeks. The flow is:
plan the (source, cut) pairs, discover the entities of each cut, extract one
mapped task per (source, cut, entity) into bronze, load bronze into the
warehouse raw schema, refresh the entity catalog and run ``dbt build``.

The Airflow run id is the bronze run id, so a retried task overwrites only
the files of its own run.
"""

import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.sdk import Connection, Variable, dag, get_current_context, task

BRONZE_ROOT = Path(os.environ.get("TASOMETRO_BRONZE_ROOT", "/opt/tasometro/data/bronze"))
DBT_DIR = os.environ.get("TASOMETRO_DBT_DIR", "/opt/tasometro/dbt")
DBT_BIN = os.environ.get("TASOMETRO_DBT_BIN", "/opt/dbt-venv/bin/dbt")

Unit = dict[str, str]


def _client() -> Any:
    from tasometro_ingestion.client import SocrataClient

    return SocrataClient(app_token=Variable.get("socrata_app_token", default="") or None)


def _warehouse_dsn() -> str:
    return Connection.get("warehouse").get_uri()


@dag(
    dag_id="tasometro_semanal",
    schedule="0 6 * * 5",
    start_date=pendulum.datetime(2022, 7, 15, tz="America/Bogota"),
    catchup=True,
    max_active_runs=1,
    max_active_tasks=4,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
    tags=["tasometro", "sfc"],
    doc_md=__doc__,
)
def tasometro_semanal() -> None:
    # A missing run cut means the SFC has not published it yet: wait, do not fail fast.
    @task(retries=8, retry_delay=timedelta(hours=6), retry_exponential_backoff=False)
    def plan() -> list[Unit]:
        from tasometro_ingestion.plan import plan_cuts, run_cut

        logical = get_current_context()["logical_date"].date()
        with _client() as client:
            cuts = plan_cuts(client, run_cut(logical))
        return [{"source": c.source, "fecha_corte": c.fecha_corte.isoformat()} for c in cuts]

    @task
    def discover(cut: Unit) -> list[Unit]:
        from tasometro_ingestion.ingest import discover_entities

        with _client() as client:
            keys = discover_entities(client, cut["source"], date.fromisoformat(cut["fecha_corte"]))
        return [
            {**cut, "tipo_entidad": k.tipo_entidad, "codigo_entidad": k.codigo_entidad}
            for k in keys
        ]

    @task
    def flatten(groups: list[list[Unit]]) -> list[Unit]:
        return [unit for group in groups for unit in group]

    @task(
        map_index_template=(
            "{{ task.op_kwargs['unit']['source'] }} {{ task.op_kwargs['unit']['fecha_corte'] }}"
            " {{ task.op_kwargs['unit']['tipo_entidad'] }}-"
            "{{ task.op_kwargs['unit']['codigo_entidad'] }}"
        )
    )
    def extract(unit: Unit) -> dict[str, int]:
        from tasometro_ingestion.ingest import ingest_cut
        from tasometro_ingestion.soql import EntityKey

        entity = EntityKey(unit["tipo_entidad"], unit["codigo_entidad"])
        with _client() as client:
            report = ingest_cut(
                client,
                BRONZE_ROOT,
                unit["source"],
                date.fromisoformat(unit["fecha_corte"]),
                get_current_context()["run_id"],
                [entity],
            )
        if report.failed:
            raise RuntimeError(f"extraction failed for {unit}")
        return {"rate_rows": report.rate_rows, "zero_rate_rows": report.zero_rate_rows}

    @task
    def load(cuts: list[Unit]) -> dict[str, int]:
        from tasometro_ingestion.warehouse import load_bronze

        totals = {"loaded": 0, "skipped": 0, "rows": 0}
        for cut in cuts:
            report = load_bronze(
                _warehouse_dsn(), BRONZE_ROOT, cut["source"], date.fromisoformat(cut["fecha_corte"])
            )
            totals["loaded"] += report.loaded
            totals["skipped"] += report.skipped
            totals["rows"] += report.rows
        return totals

    @task
    def catalog() -> int:
        from tasometro_ingestion.warehouse import load_catalog

        with _client() as client:
            return load_catalog(_warehouse_dsn(), client)

    @task.bash(
        env={"DBT_TARGET_PATH": "/tmp/dbt/target", "DBT_LOG_PATH": "/tmp/dbt/logs"},
        append_env=True,
    )
    def dbt_build() -> str:
        return f"{DBT_BIN} build --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"

    cuts = plan()
    extracted = extract.expand(unit=flatten(discover.expand(cut=cuts)))
    loaded = load(cuts)
    extracted >> loaded >> catalog() >> dbt_build()


tasometro_semanal()
