from datetime import date

import pytest

from tasometro_ingestion.soql import (
    GROUP_COLUMNS,
    EntityKey,
    cut_exists_query,
    entities_query,
    rates_query,
    soql_timestamp,
    zero_rate_query,
)

CUT = date(2026, 10, 16)
ENTITY = EntityKey(tipo_entidad="1", codigo_entidad="7")


def test_soql_timestamp_formats_floating_timestamp_literal() -> None:
    assert soql_timestamp(CUT) == "'2026-10-16T00:00:00.000'"


def test_rates_query_groups_by_every_design_dimension() -> None:
    query = rates_query(CUT, ENTITY)

    assert query["$group"] == ",".join(GROUP_COLUMNS)
    assert GROUP_COLUMNS == (
        "fecha_corte",
        "tipo_entidad",
        "codigo_entidad",
        "nombre_entidad",
        "tipo_de_persona",
        "sexo",
        "tama_o_de_empresa",
        "tipo_de_cr_dito",
        "producto_de_cr_dito",
        "plazo_de_cr_dito",
        "tipo_de_tasa",
    )


def test_rates_query_selects_additive_measures_and_rate_spread() -> None:
    select = rates_query(CUT, ENTITY)["$select"]

    for measure in (
        "sum(montos_desembolsados) AS sum_montos",
        "sum(numero_de_creditos) AS sum_creditos",
        "sum(tasa_efectiva_promedio * montos_desembolsados) AS sum_tasa_x_monto",
        "avg(tasa_efectiva_promedio) AS avg_tasa_simple",
        "min(tasa_efectiva_promedio) AS min_tasa",
        "max(tasa_efectiva_promedio) AS max_tasa",
        "count(*) AS n_registros",
    ):
        assert measure in select
    assert select.startswith(",".join(GROUP_COLUMNS) + ",")


def test_rates_query_filters_cut_entity_positive_rate_and_known_rate_type() -> None:
    where = rates_query(CUT, ENTITY)["$where"]

    assert where == (
        "fecha_corte = '2026-10-16T00:00:00.000'"
        " AND tipo_entidad = '1'"
        " AND codigo_entidad = '7'"
        " AND tasa_efectiva_promedio > 0"
        " AND tipo_de_tasa != 'N/A'"
    )


def test_rates_query_orders_by_grain_for_stable_pagination() -> None:
    assert rates_query(CUT, ENTITY)["$order"] == ",".join(GROUP_COLUMNS)


def test_zero_rate_query_counts_only_zero_rate_rows_on_the_same_grain() -> None:
    query = zero_rate_query(CUT, ENTITY)

    assert query["$select"] == ",".join(GROUP_COLUMNS) + ",count(*) AS n_tasa_cero"
    assert query["$group"] == ",".join(GROUP_COLUMNS)
    assert query["$order"] == ",".join(GROUP_COLUMNS)
    assert query["$where"].endswith(" AND tasa_efectiva_promedio = 0")
    assert "codigo_entidad = '7'" in query["$where"]


def test_entities_query_lists_entities_reporting_in_a_cut_without_aggregates() -> None:
    # Verified live: adding count(*) or $order to this whole-cut query times out.
    assert entities_query(CUT) == {
        "$select": "tipo_entidad,codigo_entidad",
        "$where": "fecha_corte = '2026-10-16T00:00:00.000'",
        "$group": "tipo_entidad,codigo_entidad",
    }


def test_cut_exists_query_probes_a_cut_with_a_bare_filter() -> None:
    assert cut_exists_query(CUT) == {
        "$select": "fecha_corte",
        "$where": "fecha_corte = '2026-10-16T00:00:00.000'",
    }


@pytest.mark.parametrize(
    ("tipo_entidad", "codigo_entidad"),
    [("1' OR '1'='1", "7"), ("1", "7; DROP"), ("", "7"), ("1", " 7")],
)
def test_entity_key_rejects_non_numeric_codes(tipo_entidad: str, codigo_entidad: str) -> None:
    with pytest.raises(ValueError, match="must be a non-empty string of digits"):
        EntityKey(tipo_entidad=tipo_entidad, codigo_entidad=codigo_entidad)
