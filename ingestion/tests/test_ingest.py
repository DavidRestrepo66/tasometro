import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from tasometro_ingestion.cli import main
from tasometro_ingestion.ingest import discover_entities, ingest_cut
from tasometro_ingestion.soql import RATE_COLUMNS, ZERO_RATE_COLUMNS, EntityKey

CUT = date(2026, 10, 16)
RUN_ID = "manual__test"


class FakeClient:
    """Answers SoQL queries from canned rows and records every call."""

    def __init__(self, entities: list[tuple[str, str]], fail_on: str | None = None) -> None:
        self.entities = entities
        self.fail_on = fail_on
        self.calls: list[tuple[str, dict[str, str]]] = []

    def __enter__(self) -> "FakeClient":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def fetch_all(self, dataset: str, query: Mapping[str, str]) -> list[dict[str, str]]:
        self.calls.append((dataset, dict(query)))
        where = query["$where"]
        if self.fail_on and self.fail_on in where:
            raise RuntimeError("boom")
        if "tasa_efectiva_promedio = 0" in where:
            return [dict.fromkeys(ZERO_RATE_COLUMNS, "0")]
        if "tasa_efectiva_promedio > 0" in where:
            return [dict.fromkeys(RATE_COLUMNS, "1"), dict.fromkeys(RATE_COLUMNS, "2")]
        return [{"tipo_entidad": t, "codigo_entidad": c} for t, c in self.entities]


def test_discover_entities_returns_sorted_numeric_keys() -> None:
    client = FakeClient([("1", "7"), ("1", "1"), ("22", "1"), ("1", "13")])

    assert discover_entities(client, "qzsc-9esp", CUT) == [
        EntityKey("1", "1"),
        EntityKey("1", "7"),
        EntityKey("1", "13"),
        EntityKey("22", "1"),
    ]


def test_ingest_cut_writes_rates_and_zero_rates_per_entity(tmp_path: Path) -> None:
    client = FakeClient([("1", "1"), ("1", "7")])

    report = ingest_cut(client, tmp_path, "qzsc-9esp", CUT, RUN_ID)

    run_dir = tmp_path / "source=qzsc-9esp" / "fecha_corte=2026-10-16" / f"run_id={RUN_ID}"
    assert sorted(p.name for p in run_dir.glob("*.parquet")) == [
        "rates_1-1.parquet",
        "rates_1-7.parquet",
        "zero_rates_1-1.parquet",
        "zero_rates_1-7.parquet",
    ]
    assert report.entities == 2
    assert report.rate_rows == 4
    assert report.zero_rate_rows == 2
    assert report.failed == []


def test_explicit_entities_skip_discovery(tmp_path: Path) -> None:
    client = FakeClient([])

    report = ingest_cut(client, tmp_path, "qzsc-9esp", CUT, RUN_ID, entities=[EntityKey("1", "7")])

    assert report.entities == 1
    assert all("codigo_entidad = '7'" in query["$where"] for _, query in client.calls)
    assert len(client.calls) == 2


def test_failed_entity_is_reported_and_other_entities_still_land(tmp_path: Path) -> None:
    client = FakeClient([("1", "1"), ("1", "7")], fail_on="codigo_entidad = '7'")

    report = ingest_cut(client, tmp_path, "qzsc-9esp", CUT, RUN_ID)

    assert report.failed == [EntityKey("1", "7")]
    assert report.entities == 1
    names = sorted(p.name for p in tmp_path.rglob("*.parquet"))
    assert names == ["rates_1-1.parquet", "zero_rates_1-1.parquet"]


def test_rates_are_written_only_after_both_queries_succeed(tmp_path: Path) -> None:
    client = FakeClient([("1", "7")], fail_on="tasa_efectiva_promedio = 0")

    report = ingest_cut(client, tmp_path, "qzsc-9esp", CUT, RUN_ID)

    assert report.failed == [EntityKey("1", "7")]
    assert list(tmp_path.rglob("*")) == []


def test_cli_ingest_writes_bronze_and_prints_json_summary(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    client = FakeClient([("1", "1")])
    monkeypatch.setattr("tasometro_ingestion.cli.make_client", lambda token: client)

    code = main(
        [
            "ingest",
            "--source", "qzsc-9esp",
            "--fecha-corte", "2026-10-16",
            "--run-id", RUN_ID,
            "--bronze-root", str(tmp_path),
        ]
    )  # fmt: skip

    assert code == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary == {
        "source": "qzsc-9esp",
        "fecha_corte": "2026-10-16",
        "run_id": RUN_ID,
        "entities": 1,
        "rate_rows": 2,
        "zero_rate_rows": 1,
        "failed": [],
    }
    assert pl.read_parquet(next(tmp_path.rglob("rates_1-1.parquet"))).height == 2


def test_cli_exits_one_when_any_entity_fails(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    client = FakeClient([("1", "1"), ("1", "7")], fail_on="codigo_entidad = '7'")
    monkeypatch.setattr("tasometro_ingestion.cli.make_client", lambda token: client)

    code = main(
        ["ingest", "--source", "qzsc-9esp", "--fecha-corte", "2026-10-16",
         "--run-id", RUN_ID, "--bronze-root", str(tmp_path)]
    )  # fmt: skip

    assert code == 1
    assert json.loads(capsys.readouterr().out)["failed"] == ["1-7"]


def test_cli_passes_explicit_entities_and_app_token_from_env(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    client = FakeClient([])
    tokens: list[str | None] = []

    def factory(token: str | None) -> FakeClient:
        tokens.append(token)
        return client

    monkeypatch.setattr("tasometro_ingestion.cli.make_client", factory)
    monkeypatch.setenv("SOCRATA_APP_TOKEN", "tok")

    code = main(
        ["ingest", "--source", "w9zh-vetq", "--fecha-corte", "2026-07-03",
         "--run-id", RUN_ID, "--bronze-root", str(tmp_path), "--entity", "1-7"]
    )  # fmt: skip

    assert code == 0
    assert tokens == ["tok"]
    assert json.loads(capsys.readouterr().out)["entities"] == 1
    assert {dataset for dataset, _ in client.calls} == {"w9zh-vetq"}


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--source", "abcd-1234"], "invalid choice"),
        (["--fecha-corte", "2026-13-01"], "invalid date"),
        (["--entity", "1;7"], "expected <tipo>-<codigo>"),
        (["--run-id", "../x"], "unsafe run_id"),
    ],
)
def test_cli_rejects_invalid_arguments(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], args: list[str], message: str
) -> None:
    base = {
        "--source": "qzsc-9esp",
        "--fecha-corte": "2026-10-16",
        "--run-id": RUN_ID,
        "--bronze-root": str(tmp_path),
    }
    argv = ["ingest"]
    for flag, value in base.items():
        if flag not in args:
            argv += [flag, value]
    argv += args

    with pytest.raises(SystemExit) as exc:
        main(argv)

    assert exc.value.code == 2
    assert message in capsys.readouterr().err
    assert list(tmp_path.rglob("*")) == []


def test_write_failure_is_reported_per_entity_and_later_entities_still_land(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tasometro_ingestion import ingest

    real_write = ingest.write_batch

    def flaky(root: Path, batch: object) -> object:
        if batch.entity == EntityKey("1", "1"):  # type: ignore[attr-defined]
            raise OSError("disk full")
        return real_write(root, batch)  # type: ignore[arg-type]

    monkeypatch.setattr(ingest, "write_batch", flaky)
    client = FakeClient([("1", "1"), ("1", "7")])

    report = ingest_cut(client, tmp_path, "qzsc-9esp", CUT, RUN_ID)

    assert report.failed == [EntityKey("1", "1")]
    assert report.entities == 1
    names = sorted(p.name for p in tmp_path.rglob("*.manifest.json"))
    assert names == ["rates_1-7.manifest.json", "zero_rates_1-7.manifest.json"]


def test_schema_drift_is_reported_per_entity_and_later_entities_still_land(
    tmp_path: Path,
) -> None:
    class DriftingClient(FakeClient):
        def fetch_all(self, dataset: str, query: Mapping[str, str]) -> list[dict[str, str]]:
            rows = super().fetch_all(dataset, query)
            if (
                "codigo_entidad = '1'" in query["$where"]
                and "tasa_efectiva_promedio" in query["$where"]
            ):
                return [{**row, "columna_nueva": "x"} for row in rows]
            return rows

    client = DriftingClient([("1", "1"), ("1", "7")])

    report = ingest_cut(client, tmp_path, "qzsc-9esp", CUT, RUN_ID)

    assert report.failed == [EntityKey("1", "1")]
    assert report.entities == 1
    names = sorted(p.name for p in tmp_path.rglob("*.manifest.json"))
    assert names == ["rates_1-7.manifest.json", "zero_rates_1-7.manifest.json"]


def test_schema_drift_in_one_batch_publishes_no_batch_of_the_entity(tmp_path: Path) -> None:
    class ZeroRateDriftClient(FakeClient):
        def fetch_all(self, dataset: str, query: Mapping[str, str]) -> list[dict[str, str]]:
            rows = super().fetch_all(dataset, query)
            where = query["$where"]
            if "codigo_entidad = '1'" in where and "tasa_efectiva_promedio = 0" in where:
                return [{**row, "columna_nueva": "x"} for row in rows]
            return rows

    client = ZeroRateDriftClient([("1", "1"), ("1", "7")])

    report = ingest_cut(client, tmp_path, "qzsc-9esp", CUT, RUN_ID)

    assert report.failed == [EntityKey("1", "1")]
    assert report.entities == 1
    names = sorted(p.name for p in tmp_path.rglob("*") if p.is_file())
    assert names == [
        "rates_1-7.manifest.json",
        "rates_1-7.parquet",
        "zero_rates_1-7.manifest.json",
        "zero_rates_1-7.parquet",
    ]
