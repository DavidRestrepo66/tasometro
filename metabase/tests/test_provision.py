import json
import sys
from pathlib import Path

import httpx
import pytest
import respx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import provision

BASE = "http://metabase.test"
ENV = {
    "METABASE_URL": BASE,
    "METABASE_ADMIN_PASSWORD": "a-long-unique-password-1",
    "POSTGRES_USER": "taso",
    "POSTGRES_PASSWORD": "secret",
}


@pytest.fixture
def settings() -> provision.Settings:
    return provision.Settings.from_env(ENV)


def _bodies(route: respx.Route) -> list[dict]:
    return [json.loads(call.request.content) for call in route.calls]


def test_settings_require_admin_password_and_postgres_credentials():
    with pytest.raises(SystemExit, match="METABASE_ADMIN_PASSWORD"):
        provision.Settings.from_env({"POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p"})


def test_settings_default_to_the_compose_network_for_the_warehouse(settings):
    assert (settings.host, settings.port, settings.dbname) == ("postgres", 5432, "warehouse")
    assert settings.admin_email == "admin@tasometro.local"


def test_each_card_declares_only_the_tags_its_sql_uses():
    for spec in provision.CARDS:
        used = {name for name in provision.FILTERS if "{{" + name + "}}" in spec.sql}
        assert used == set(spec.tags), spec.name


def test_dashcard_mappings_target_the_template_tags_of_their_card():
    dashcards = provision.build_dashcards([10, 11, 12, 13])
    assert [d["card_id"] for d in dashcards] == [10, 11, 12, 13]
    for dashcard, spec in zip(dashcards, provision.CARDS, strict=True):
        mapped = {m["parameter_id"] for m in dashcard["parameter_mappings"]}
        assert mapped == set(spec.tags)
        assert all(m["target"][1][0] == "template-tag" for m in dashcard["parameter_mappings"])


@respx.mock
def test_first_run_sets_up_metabase_registers_the_warehouse_and_builds_the_dashboard(settings):
    respx.get(f"{BASE}/api/session/properties").respond(
        json={"has-user-setup": False, "setup-token": "tok"}
    )
    setup = respx.post(f"{BASE}/api/setup").respond(json={"id": "session-1"})
    respx.get(f"{BASE}/api/database").respond(json={"data": []})
    database = respx.post(f"{BASE}/api/database").respond(json={"id": 7})
    respx.get(f"{BASE}/api/dashboard").respond(json=[])
    respx.get(f"{BASE}/api/card").respond(json=[])
    cards = respx.post(f"{BASE}/api/card").mock(
        side_effect=[httpx.Response(200, json={"id": 100 + i}) for i in range(4)]
    )
    respx.post(f"{BASE}/api/dashboard").respond(json={"id": 3})
    put = respx.put(f"{BASE}/api/dashboard/3").respond(json={"id": 3})

    url = provision.provision(settings)

    assert url == f"{BASE}/dashboard/3"
    assert _bodies(setup)[0]["user"]["email"] == "admin@tasometro.local"
    assert _bodies(database)[0]["details"]["dbname"] == "warehouse"
    assert [b["dataset_query"]["database"] for b in _bodies(cards)] == [7, 7, 7, 7]
    assert [d["card_id"] for d in _bodies(put)[0]["dashcards"]] == [100, 101, 102, 103]
    assert {p["id"] for p in _bodies(put)[0]["parameters"]} == set(provision.FILTERS)


def _existing_cards(database_id: int = 7) -> list[dict]:
    return [
        {"id": 100 + i, "name": spec.name, "dataset_query": {"database": database_id}}
        for i, spec in enumerate(provision.CARDS)
    ]


def _complete_dashboard() -> dict:
    return {
        "id": 3,
        "name": provision.DASHBOARD_NAME,
        "dashcards": provision.build_dashcards([100 + i for i in range(4)]),
        "parameters": provision.dashboard_parameters(),
    }


def _database_listing(**overrides) -> dict:
    database = {
        "id": 7,
        "name": provision.DATABASE_NAME,
        "engine": "postgres",
        "details": {"host": "postgres", "port": 5432, "dbname": "warehouse", "user": "taso"},
    }
    database.update(overrides)
    return {"data": [database]}


def _mock_existing_environment() -> None:
    respx.get(f"{BASE}/api/session/properties").respond(
        json={"has-user-setup": True, "setup-token": "stale"}
    )
    respx.post(f"{BASE}/api/session").respond(json={"id": "session-2"})
    respx.get(f"{BASE}/api/database").respond(json=_database_listing())
    respx.get(f"{BASE}/api/dashboard").respond(json=[{"id": 3, "name": provision.DASHBOARD_NAME}])


@respx.mock
def test_second_run_logs_in_and_creates_nothing(settings):
    _mock_existing_environment()
    respx.get(f"{BASE}/api/card").respond(json=_existing_cards())
    respx.get(f"{BASE}/api/dashboard/3").respond(json=_complete_dashboard())
    creations = [
        respx.post(f"{BASE}/api/setup"),
        respx.post(f"{BASE}/api/database"),
        respx.post(f"{BASE}/api/card"),
        respx.post(f"{BASE}/api/dashboard"),
        respx.put(f"{BASE}/api/dashboard/3"),
        respx.put(f"{BASE}/api/database/7"),
    ]

    url = provision.provision(settings)

    assert url == f"{BASE}/dashboard/3"
    assert all(not route.called for route in creations)


@respx.mock
def test_rerun_after_a_failed_put_completes_the_existing_dashboard(settings):
    _mock_existing_environment()
    respx.get(f"{BASE}/api/card").respond(json=_existing_cards())
    respx.get(f"{BASE}/api/dashboard/3").respond(
        json={"id": 3, "name": provision.DASHBOARD_NAME, "dashcards": [], "parameters": []}
    )
    put = respx.put(f"{BASE}/api/dashboard/3").respond(json={"id": 3})
    created = [respx.post(f"{BASE}/api/card"), respx.post(f"{BASE}/api/dashboard")]

    url = provision.provision(settings)

    assert url == f"{BASE}/dashboard/3"
    body = _bodies(put)[0]
    assert [d["card_id"] for d in body["dashcards"]] == [100, 101, 102, 103]
    assert {p["id"] for p in body["parameters"]} == set(provision.FILTERS)
    assert all(not route.called for route in created)


@respx.mock
def test_a_card_with_the_same_name_on_another_database_is_not_reused(settings):
    _mock_existing_environment()
    foreign = _existing_cards(database_id=99)
    respx.get(f"{BASE}/api/card").respond(json=foreign)
    respx.get(f"{BASE}/api/dashboard/3").respond(
        json={"id": 3, "name": provision.DASHBOARD_NAME, "dashcards": [], "parameters": []}
    )
    cards = respx.post(f"{BASE}/api/card").mock(
        side_effect=[httpx.Response(200, json={"id": 200 + i}) for i in range(4)]
    )
    put = respx.put(f"{BASE}/api/dashboard/3").respond(json={"id": 3})

    provision.provision(settings)

    assert [b["dataset_query"]["database"] for b in _bodies(cards)] == [7, 7, 7, 7]
    assert [d["card_id"] for d in _bodies(put)[0]["dashcards"]] == [200, 201, 202, 203]


@respx.mock
def test_a_rejected_login_stops_before_touching_anything(settings):
    respx.get(f"{BASE}/api/session/properties").respond(
        json={"has-user-setup": True, "setup-token": "stale"}
    )
    respx.post(f"{BASE}/api/session").respond(status_code=401, json={"errors": "bad"})
    database = respx.post(f"{BASE}/api/database")

    with pytest.raises(httpx.HTTPStatusError):
        provision.provision(settings)

    assert not database.called


@respx.mock
def test_a_dashboard_with_missing_parameter_mappings_is_completed(settings):
    _mock_existing_environment()
    respx.get(f"{BASE}/api/card").respond(json=_existing_cards())
    broken = _complete_dashboard()
    broken["dashcards"][0]["parameter_mappings"] = []
    respx.get(f"{BASE}/api/dashboard/3").respond(json=broken)
    put = respx.put(f"{BASE}/api/dashboard/3").respond(json={"id": 3})

    provision.provision(settings)

    mappings = _bodies(put)[0]["dashcards"][0]["parameter_mappings"]
    assert {m["parameter_id"] for m in mappings} == set(provision.CARDS[0].tags)


@respx.mock
def test_a_database_with_the_same_name_but_other_details_is_reconciled(settings):
    respx.get(f"{BASE}/api/session/properties").respond(json={"has-user-setup": True})
    respx.post(f"{BASE}/api/session").respond(json={"id": "session-2"})
    wrong = _database_listing(
        details={"host": "elsewhere", "port": 5432, "dbname": "x", "user": "u"}
    )
    respx.get(f"{BASE}/api/database").respond(json=wrong)
    put = respx.put(f"{BASE}/api/database/7").respond(json={"id": 7})
    created = respx.post(f"{BASE}/api/database")
    respx.get(f"{BASE}/api/dashboard").respond(json=[{"id": 3, "name": provision.DASHBOARD_NAME}])
    respx.get(f"{BASE}/api/card").respond(json=_existing_cards())
    respx.get(f"{BASE}/api/dashboard/3").respond(json=_complete_dashboard())

    provision.provision(settings)

    details = _bodies(put)[0]["details"]
    assert (details["host"], details["dbname"], details["user"]) == (
        "postgres",
        "warehouse",
        "taso",
    )
    assert not created.called


@respx.mock
def test_a_dashcard_without_card_id_does_not_crash_and_is_reconciled(settings):
    _mock_existing_environment()
    respx.get(f"{BASE}/api/card").respond(json=_existing_cards())
    broken = _complete_dashboard()
    broken["dashcards"][0]["card_id"] = None
    respx.get(f"{BASE}/api/dashboard/3").respond(json=broken)
    put = respx.put(f"{BASE}/api/dashboard/3").respond(json={"id": 3})

    provision.provision(settings)

    assert [d["card_id"] for d in _bodies(put)[0]["dashcards"]] == [100, 101, 102, 103]


@respx.mock
def test_refresh_flag_pushes_the_warehouse_password_even_when_details_match():
    flagged = provision.Settings.from_env({**ENV, "METABASE_REFRESH_DATABASE": "1"})
    _mock_existing_environment()
    put = respx.put(f"{BASE}/api/database/7").respond(json={"id": 7})
    respx.get(f"{BASE}/api/card").respond(json=_existing_cards())
    respx.get(f"{BASE}/api/dashboard/3").respond(json=_complete_dashboard())

    provision.provision(flagged)

    assert _bodies(put)[0]["details"]["password"] == "secret"
