"""Pure builders for the SoQL queries sent to the SFC lending-rate datasets.

Aggregation happens at the source: one query per (cut, entity) returns a few
hundred grouped rows instead of ~900k raw rows per cut. Measures are kept
additive (sums and counts) so the weighted average rate can be recomputed
correctly at any coarser grain downstream.
"""

import re
from dataclasses import dataclass
from datetime import date

GROUP_COLUMNS: tuple[str, ...] = (
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

RATE_MEASURES: tuple[str, ...] = (
    "sum(montos_desembolsados) AS sum_montos",
    "sum(numero_de_creditos) AS sum_creditos",
    "sum(tasa_efectiva_promedio * montos_desembolsados) AS sum_tasa_x_monto",
    "avg(tasa_efectiva_promedio) AS avg_tasa_simple",
    "min(tasa_efectiva_promedio) AS min_tasa",
    "max(tasa_efectiva_promedio) AS max_tasa",
    "count(*) AS n_registros",
)

_DIGITS = re.compile(r"\d+")

SoqlParams = dict[str, str]


@dataclass(frozen=True, slots=True)
class EntityKey:
    """Composite key of a supervised entity: (tipo_entidad, codigo_entidad)."""

    tipo_entidad: str
    codigo_entidad: str

    def __post_init__(self) -> None:
        # Values are interpolated into SoQL literals, so only digits are accepted.
        for field in ("tipo_entidad", "codigo_entidad"):
            if not _DIGITS.fullmatch(getattr(self, field)):
                raise ValueError(f"{field} must be a non-empty string of digits")


def soql_timestamp(day: date) -> str:
    """Return a SoQL floating-timestamp literal for midnight of ``day``."""
    return f"'{day.isoformat()}T00:00:00.000'"


def _entity_cut_filter(fecha_corte: date, entity: EntityKey) -> str:
    return (
        f"fecha_corte = {soql_timestamp(fecha_corte)}"
        f" AND tipo_entidad = '{entity.tipo_entidad}'"
        f" AND codigo_entidad = '{entity.codigo_entidad}'"
    )


def _grouped(select: str, where: str, group_columns: tuple[str, ...]) -> SoqlParams:
    grain = ",".join(group_columns)
    return {"$select": select, "$where": where, "$group": grain, "$order": grain}


def rates_query(fecha_corte: date, entity: EntityKey) -> SoqlParams:
    """Aggregated rates for one entity in one cut.

    Zero rates are placeholders, not rates, so they are excluded from both the
    weighted numerator and its denominator. Rows without a rate type are dropped.
    """
    where = (
        _entity_cut_filter(fecha_corte, entity)
        + " AND tasa_efectiva_promedio > 0"
        + " AND tipo_de_tasa != 'N/A'"
    )
    select = ",".join((*GROUP_COLUMNS, *RATE_MEASURES))
    return _grouped(select, where, GROUP_COLUMNS)


def zero_rate_query(fecha_corte: date, entity: EntityKey) -> SoqlParams:
    """Count of zero-rate rows on the same grain, used to monitor their share."""
    where = _entity_cut_filter(fecha_corte, entity) + " AND tasa_efectiva_promedio = 0"
    select = ",".join(GROUP_COLUMNS) + ",count(*) AS n_tasa_cero"
    return _grouped(select, where, GROUP_COLUMNS)


def entities_query(fecha_corte: date) -> SoqlParams:
    """Entities that reported rates in a cut; drives one rates query per entity.

    This is the only whole-cut query and it is slow (~50 s). Aggregates or an
    ``$order`` push it past the server timeout, so it selects bare group keys.
    """
    keys = ",".join(("tipo_entidad", "codigo_entidad"))
    return {
        "$select": keys,
        "$where": f"fecha_corte = {soql_timestamp(fecha_corte)}",
        "$group": keys,
    }
