"""Decide which (source, cut) pairs one weekly run extracts.

A run owns one cut: the Friday two weeks before the run, since the SFC
publishes each cut about twelve days late. Banks revise recent cuts, so the
run also re-extracts the seven cuts before it, but only from the current
dataset (qzsc-9esp). The history dataset (w9zh-vetq) is read only for the
run's own cut, which happens once per cut during the catchup backfill.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Protocol

from tasometro_ingestion.soql import cut_exists_query

HISTORY_SOURCE = "w9zh-vetq"
CURRENT_SOURCE = "qzsc-9esp"
PUBLICATION_LAG = timedelta(days=14)
REVISION_WEEKS = 8
_FRIDAY = 4


class CutProbe(Protocol):
    def fetch_first(self, dataset: str, query: Mapping[str, str]) -> dict[str, Any] | None: ...


class CutNotPublishedError(RuntimeError):
    """The run's own cut is in neither dataset yet; retrying later may succeed."""


@dataclass(frozen=True, slots=True)
class PlannedCut:
    source: str
    fecha_corte: date


def run_cut(logical_date: date) -> date:
    """Cut owned by a run: the last Friday on or before ``logical_date`` - 14 days."""
    day = logical_date - PUBLICATION_LAG
    return day - timedelta(days=(day.weekday() - _FRIDAY) % 7)


def candidate_cuts(cut: date, weeks: int = REVISION_WEEKS) -> list[date]:
    """``cut`` and the weekly cuts before it, newest first."""
    if weeks < 1:
        raise ValueError("weeks must be positive")
    return [cut - timedelta(weeks=i) for i in range(weeks)]


def plan_cuts(client: CutProbe, cut: date, weeks: int = REVISION_WEEKS) -> list[PlannedCut]:
    """Pairs to extract for the run owning ``cut``, newest first."""
    plan: list[PlannedCut] = []
    for candidate in candidate_cuts(cut, weeks):
        if _published(client, CURRENT_SOURCE, candidate):
            plan.append(PlannedCut(CURRENT_SOURCE, candidate))
        elif candidate == cut:
            if not _published(client, HISTORY_SOURCE, candidate):
                raise CutNotPublishedError(f"cut {cut.isoformat()} is not published yet")
            plan.append(PlannedCut(HISTORY_SOURCE, candidate))
    return plan


def _published(client: CutProbe, source: str, fecha_corte: date) -> bool:
    return client.fetch_first(source, cut_exists_query(fecha_corte)) is not None
