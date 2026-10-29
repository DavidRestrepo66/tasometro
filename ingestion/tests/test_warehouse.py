import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path

import psycopg
import pytest

from tasometro_ingestion.bronze import BatchKind, BronzeBatch, write_batch
from tasometro_ingestion.cli import main
from tasometro_ingestion.soql import RATE_COLUMNS, ZERO_RATE_COLUMNS, EntityKey
from tasometro_ingestion.warehouse import CATALOG_COLUMNS, LoadError, load_bronze, load_catalog

CUT = date(2026, 10, 16)
ENTITY = EntityKey("1", "7")


def rate_row(i: int) -> dict[str, str]:
    row = {column: f"{column}-{i}" for column in RATE_COLUMNS}
    row.update(tipo_entidad="1", codigo_entidad="7")
    return row


def write(root: Path, run_id: str, rows: int, kind: BatchKind = BatchKind.RATES,
          entity: EntityKey = ENTITY, cut: date = CUT) -> None:  # fmt: skip
    columns = RATE_COLUMNS if kind is BatchKind.RATES else ZERO_RATE_COLUMNS
    data = [{c: f"{c}-{i}" for c in columns} for i in range(rows)]
    write_batch(root, BronzeBatch("qzsc-9esp", cut, run_id, entity, kind, {"$q": "x"}, data))


def query(dsn: str, sql: str) -> list[tuple]:
    with psycopg.connect(dsn) as conn:
        return conn.execute(sql).fetchall()


pytestmark = pytest.mark.postgres


def test_load_copies_rows_with_batch_identity(tmp_path: Path, warehouse_dsn: str) -> None:
    write(tmp_path, "run-a", 3)
    write(tmp_path, "run-a", 1, kind=BatchKind.ZERO_RATES)

    report = load_bronze(warehouse_dsn, tmp_path)

    assert (report.loaded, report.skipped, report.rows) == (2, 0, 4)
    rows = query(
        warehouse_dsn,
        "select _source, _fecha_corte, _run_id, _tipo_entidad, _codigo_entidad, sum_montos"
        " from raw.sfc_tasas order by sum_montos",
    )
    assert rows == [("qzsc-9esp", CUT, "run-a", "1", "7", f"sum_montos-{i}") for i in range(3)]
    assert query(warehouse_dsn, "select count(*) from raw.sfc_tasas_cero") == [(1,)]
    lotes = query(warehouse_dsn, "select kind, row_count, run_id from raw.sfc_lotes order by kind")
    assert lotes == [("rates", 3, "run-a"), ("zero_rates", 1, "run-a")]


def test_reloading_unchanged_bronze_skips_every_batch(tmp_path: Path, warehouse_dsn: str) -> None:
    write(tmp_path, "run-a", 3)
    load_bronze(warehouse_dsn, tmp_path)

    report = load_bronze(warehouse_dsn, tmp_path)

    assert (report.loaded, report.skipped, report.rows) == (0, 1, 0)
    assert query(warehouse_dsn, "select count(*) from raw.sfc_tasas") == [(3,)]


def test_retried_run_replaces_only_its_own_batch(tmp_path: Path, warehouse_dsn: str) -> None:
    write(tmp_path, "run-a", 3)
    write(tmp_path, "run-b", 2)
    load_bronze(warehouse_dsn, tmp_path)

    write(tmp_path, "run-a", 1)  # an Airflow retry rewrites run-a
    report = load_bronze(warehouse_dsn, tmp_path)

    assert (report.loaded, report.skipped) == (1, 1)
    counts = query(
        warehouse_dsn, "select _run_id, count(*) from raw.sfc_tasas group by 1 order by 1"
    )
    assert counts == [("run-a", 1), ("run-b", 2)]
    assert query(warehouse_dsn, "select count(*) from raw.sfc_lotes") == [(2,)]


def test_filters_restrict_loaded_cuts(tmp_path: Path, warehouse_dsn: str) -> None:
    write(tmp_path, "run-a", 1)
    write(tmp_path, "run-a", 1, cut=date(2026, 10, 9))

    report = load_bronze(warehouse_dsn, tmp_path, fecha_corte=CUT)

    assert report.loaded == 1
    assert query(warehouse_dsn, "select distinct _fecha_corte from raw.sfc_tasas") == [(CUT,)]


def test_corrupt_batch_fails_and_loads_nothing_for_it(tmp_path: Path, warehouse_dsn: str) -> None:
    write(tmp_path, "run-a", 2)
    manifest = next(tmp_path.rglob("*.manifest.json"))
    data = json.loads(manifest.read_text())
    data["rows_sha256"] = "0" * 64
    manifest.write_text(json.dumps(data))

    with pytest.raises(LoadError, match="hash mismatch"):
        load_bronze(warehouse_dsn, tmp_path)

    assert query(warehouse_dsn, "select count(*) from raw.sfc_tasas") == [(0,)]
    assert query(warehouse_dsn, "select count(*) from raw.sfc_lotes") == [(0,)]


def test_batch_without_manifest_is_ignored(tmp_path: Path, warehouse_dsn: str) -> None:
    write(tmp_path, "run-a", 2)
    next(tmp_path.rglob("*.manifest.json")).unlink()

    report = load_bronze(warehouse_dsn, tmp_path)

    assert (report.loaded, report.skipped, report.rows) == (0, 0, 0)


class CatalogClient:
    def __init__(self, rows: list[dict[str, str]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, dict[str, str]]] = []

    def __enter__(self) -> "CatalogClient":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def fetch_all(self, dataset: str, query: Mapping[str, str]) -> list[dict[str, str]]:
        self.calls.append((dataset, dict(query)))
        return self.rows


def entity(tipo: str, cod: str) -> dict[str, str]:
    return {"tipo_entidad": tipo, "cod_entidad": cod, "razon_social": f"BANCO {cod}",
            "numeroidentificacion": f"86000{cod}"}  # fmt: skip


def test_catalog_replaces_entities_and_requests_no_personal_data(warehouse_dsn: str) -> None:
    load_catalog(warehouse_dsn, CatalogClient([entity("1", "1"), entity("1", "2")]))
    client = CatalogClient([entity("1", "1")])

    count = load_catalog(warehouse_dsn, client)

    assert count == 1
    assert query(warehouse_dsn, "select tipo_entidad, cod_entidad from raw.sfc_entidades") == [
        ("1", "1")
    ]
    dataset, params = client.calls[0]
    assert dataset == "sr9n-792w"
    assert params["$select"] == ",".join(CATALOG_COLUMNS)
    assert "representante_legal" not in params["$select"]


def test_cli_load_prints_summary(
    tmp_path: Path, warehouse_dsn: str, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    write(tmp_path, "run-a", 2)
    monkeypatch.setenv("WAREHOUSE_DSN", warehouse_dsn)

    code = main(["load", "--bronze-root", str(tmp_path)])

    assert code == 0
    assert json.loads(capsys.readouterr().out) == {"loaded": 1, "skipped": 0, "rows": 2}


def test_cli_load_reports_corrupt_batch_on_stderr(
    tmp_path: Path, warehouse_dsn: str, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:  # fmt: skip
    write(tmp_path, "run-a", 2)
    next(tmp_path.rglob("*.parquet")).unlink()
    monkeypatch.setenv("WAREHOUSE_DSN", warehouse_dsn)

    code = main(["load", "--bronze-root", str(tmp_path)])

    captured = capsys.readouterr()
    assert code == 1
    assert captured.out == ""
    assert "missing data file" in captured.err


def test_cli_catalog_loads_entities(
    warehouse_dsn: str, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("WAREHOUSE_DSN", warehouse_dsn)
    monkeypatch.setattr(
        "tasometro_ingestion.cli.make_client", lambda token: CatalogClient([entity("1", "1")])
    )

    code = main(["catalog"])

    assert code == 0
    assert json.loads(capsys.readouterr().out) == {"entities": 1}


@pytest.mark.parametrize("command", [["load", "--bronze-root", "x"], ["catalog"]])
def test_cli_requires_warehouse_dsn(
    command: list[str], capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("WAREHOUSE_DSN", raising=False)

    with pytest.raises(SystemExit) as exc:
        main(command)

    assert exc.value.code == 2
    assert "WAREHOUSE_DSN is not set" in capsys.readouterr().err
