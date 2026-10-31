import json
import re
from dataclasses import replace
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from tasometro_ingestion.bronze import BatchKind, BronzeBatch, write_batch
from tasometro_ingestion.soql import RATE_COLUMNS, ZERO_RATE_COLUMNS, EntityKey

CUT = date(2026, 10, 16)
ENTITY = EntityKey(tipo_entidad="1", codigo_entidad="7")
RUN_ID = "scheduled__2026-10-16T00:00:00+00:00"
QUERY = {"$select": "a", "$where": "b"}


def rate_row(**overrides: str) -> dict[str, str]:
    row = {column: f"v-{column}" for column in RATE_COLUMNS}
    row.update(overrides)
    return row


def make_batch(rows: list[dict[str, str]], run_id: str = RUN_ID) -> BronzeBatch:
    return BronzeBatch(
        source="qzsc-9esp",
        fecha_corte=CUT,
        run_id=run_id,
        entity=ENTITY,
        kind=BatchKind.RATES,
        query=QUERY,
        rows=rows,
    )


def test_batch_is_written_under_source_cut_and_run_partitions(tmp_path: Path) -> None:
    written = write_batch(tmp_path, make_batch([rate_row()]))

    expected_dir = tmp_path / "source=qzsc-9esp" / "fecha_corte=2026-10-16" / f"run_id={RUN_ID}"
    assert written.data_path == expected_dir / "rates_1-7.parquet"
    assert written.manifest_path == expected_dir / "rates_1-7.manifest.json"
    assert sorted(p.name for p in expected_dir.iterdir()) == [
        "rates_1-7.manifest.json",
        "rates_1-7.parquet",
    ]


def test_parquet_keeps_raw_strings_in_the_fixed_column_order(tmp_path: Path) -> None:
    written = write_batch(tmp_path, make_batch([rate_row(sum_montos="5014075321")]))

    frame = pl.read_parquet(written.data_path)
    assert tuple(frame.columns) == RATE_COLUMNS
    assert set(frame.schema.values()) == {pl.String}
    assert frame.row(0, named=True)["sum_montos"] == "5014075321"


def test_keys_omitted_by_socrata_become_nulls(tmp_path: Path) -> None:
    row = rate_row()
    del row["sexo"]

    frame = pl.read_parquet(write_batch(tmp_path, make_batch([row])).data_path)

    assert frame["sexo"].to_list() == [None]


def test_unexpected_column_is_schema_drift_and_writes_nothing(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unexpected columns: \\['nueva_columna'\\]"):
        write_batch(tmp_path, make_batch([rate_row(nueva_columna="x")]))

    assert list(tmp_path.rglob("*")) == []


def test_manifest_records_rows_hash_query_and_identity(tmp_path: Path) -> None:
    written = write_batch(tmp_path, make_batch([rate_row(), rate_row(sexo="Femenino")]))

    manifest = json.loads(written.manifest_path.read_text())
    assert manifest["source"] == "qzsc-9esp"
    assert manifest["fecha_corte"] == "2026-10-16"
    assert manifest["run_id"] == RUN_ID
    assert manifest["tipo_entidad"] == "1"
    assert manifest["codigo_entidad"] == "7"
    assert manifest["kind"] == "rates"
    assert manifest["row_count"] == 2
    assert manifest["soql"] == QUERY
    assert manifest["data_file"] == "rates_1-7.parquet"
    assert len(manifest["rows_sha256"]) == 64
    assert manifest["extracted_at"].endswith("+00:00")
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}\+00:00", manifest["extracted_at"])


def test_rows_hash_ignores_row_order_but_not_values(tmp_path: Path) -> None:
    a, b = rate_row(sexo="Femenino"), rate_row(sexo="Masculino")

    first = write_batch(tmp_path / "1", make_batch([a, b])).rows_sha256
    swapped = write_batch(tmp_path / "2", make_batch([b, a])).rows_sha256
    changed = write_batch(tmp_path / "3", make_batch([a, rate_row(sexo="Otro")])).rows_sha256

    assert first == swapped
    assert first != changed


def test_retry_of_the_same_run_overwrites_only_its_own_file(tmp_path: Path) -> None:
    other = write_batch(tmp_path, make_batch([rate_row()], run_id="manual__1"))
    other_bytes = other.data_path.read_bytes()

    write_batch(tmp_path, make_batch([rate_row()]))
    retried = write_batch(tmp_path, make_batch([rate_row(), rate_row(sexo="F")]))

    assert pl.read_parquet(retried.data_path).height == 2
    assert other.data_path.read_bytes() == other_bytes
    assert not [p for p in tmp_path.rglob("*") if p.name.startswith(".")]


def test_empty_batch_writes_schema_only_file(tmp_path: Path) -> None:
    written = write_batch(tmp_path, make_batch([]))

    frame = pl.read_parquet(written.data_path)
    assert frame.height == 0
    assert tuple(frame.columns) == RATE_COLUMNS
    assert json.loads(written.manifest_path.read_text())["row_count"] == 0


def test_zero_rate_batches_use_their_own_schema_and_file_name(tmp_path: Path) -> None:
    batch = replace(make_batch([{c: "x" for c in ZERO_RATE_COLUMNS}]), kind=BatchKind.ZERO_RATES)

    written = write_batch(tmp_path, batch)

    assert written.data_path.name == "zero_rates_1-7.parquet"
    assert tuple(pl.read_parquet(written.data_path).columns) == ZERO_RATE_COLUMNS


@pytest.mark.parametrize("run_id", ["", ".", "..", "a/b", "a\\b", "run id"])
def test_unsafe_run_ids_are_rejected(tmp_path: Path, run_id: str) -> None:
    with pytest.raises(ValueError, match="run_id"):
        write_batch(tmp_path, make_batch([rate_row()], run_id=run_id))
    assert list(tmp_path.rglob("*")) == []


def test_failed_retry_keeps_the_previous_batch_intact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = write_batch(tmp_path, make_batch([rate_row()]))

    def fail(self: pl.DataFrame, *args: object, **kwargs: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", fail)
    with pytest.raises(OSError, match="disk full"):
        write_batch(tmp_path, make_batch([rate_row(), rate_row()]))

    assert first.manifest_path.exists()
    assert pl.read_parquet(first.data_path).height == 1
    assert not [p for p in tmp_path.rglob("*") if p.name.startswith(".")]
