import itertools
from collections.abc import Mapping
from datetime import date

import pytest

from tasometro_ingestion.plan import (
    CutNotPublishedError,
    PlannedCut,
    candidate_cuts,
    plan_cuts,
    run_cut,
)

HISTORY = "w9zh-vetq"
CURRENT = "qzsc-9esp"


class ProbeClient:
    """Answers existence probes from a set of published (source, cut) pairs."""

    def __init__(self, published: set[tuple[str, date]]) -> None:
        self.published = published
        self.probes: list[tuple[str, str]] = []

    def fetch_first(self, dataset: str, query: Mapping[str, str]) -> dict[str, str] | None:
        self.probes.append((dataset, query["$where"]))
        day = date.fromisoformat(query["$where"].split("'")[1][:10])
        return {"fecha_corte": day.isoformat()} if (dataset, day) in self.published else None


@pytest.mark.parametrize(
    ("logical", "expected"),
    [
        (date(2026, 10, 30), date(2026, 10, 16)),  # scheduled Friday run
        (date(2026, 11, 2), date(2026, 10, 16)),  # manual Monday run: last Friday cut
        (date(2026, 11, 5), date(2026, 10, 16)),  # Thursday: still before the next cut
        (date(2026, 11, 6), date(2026, 10, 23)),
    ],
)
def test_run_cut_is_the_friday_two_weeks_before_the_run(logical: date, expected: date) -> None:
    assert run_cut(logical) == expected


def test_candidate_cuts_cover_eight_weeks_newest_first() -> None:
    cuts = candidate_cuts(date(2026, 10, 16))

    assert len(cuts) == 8
    assert cuts[0] == date(2026, 10, 16)
    assert cuts[-1] == date(2026, 8, 28)
    assert all((a - b).days == 7 for a, b in itertools.pairwise(cuts))


def test_candidate_cuts_rejects_a_non_positive_window() -> None:
    with pytest.raises(ValueError, match="weeks must be positive"):
        candidate_cuts(date(2026, 10, 16), weeks=0)


def test_plan_cuts_reextracts_every_recent_cut_from_the_current_source() -> None:
    cut = date(2026, 10, 16)
    client = ProbeClient({(CURRENT, c) for c in candidate_cuts(cut)})

    assert plan_cuts(client, cut) == [PlannedCut(CURRENT, c) for c in candidate_cuts(cut)]
    assert {source for source, _ in client.probes} == {CURRENT}


def test_plan_cuts_takes_the_run_cut_from_history_when_current_lacks_it() -> None:
    cut = date(2022, 8, 5)
    client = ProbeClient({(HISTORY, cut)})

    assert plan_cuts(client, cut, weeks=3) == [PlannedCut(HISTORY, cut)]
    # Older cuts missing from the current source are skipped, never probed in history:
    # history is loaded once, by the run that owns each cut.
    assert client.probes.count((HISTORY, "fecha_corte = '2022-08-05T00:00:00.000'")) == 1
    assert [s for s, _ in client.probes].count(HISTORY) == 1


def test_plan_cuts_mixes_sources_at_the_boundary_between_datasets() -> None:
    cut = date(2024, 1, 19)
    client = ProbeClient({(CURRENT, cut), (CURRENT, date(2024, 1, 12))})

    assert plan_cuts(client, cut, weeks=3) == [
        PlannedCut(CURRENT, date(2024, 1, 19)),
        PlannedCut(CURRENT, date(2024, 1, 12)),
    ]


def test_plan_cuts_fails_when_the_run_cut_is_not_published_yet() -> None:
    cut = date(2026, 10, 23)
    client = ProbeClient({(CURRENT, date(2026, 10, 16))})

    with pytest.raises(CutNotPublishedError, match="2026-10-23 is not published"):
        plan_cuts(client, cut)
