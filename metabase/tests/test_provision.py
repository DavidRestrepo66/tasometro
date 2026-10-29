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


@respx.mock
def test_second_run_logs_in_and_creates_nothing(settings):
    respx.get(f"{BASE}/api/session/properties").respond(
        json={"has-user-setup": True, "setup-token": "stale"}
    )
    login = respx.post(f"{BASE}/api/session").respond(json={"id": "session-2"})
    respx.get(f"{BASE}/api/database").respond(
        json={"data": [{"id": 7, "name": provision.DATABASE_NAME}]}
    )
    respx.get(f"{BASE}/api/dashboard").respond(json=[{"id": 3, "name": provision.DASHBOARD_NAME}])
    creations = [
        respx.post(f"{BASE}/api/setup"),
        respx.post(f"{BASE}/api/database"),
        respx.post(f"{BASE}/api/card"),
        respx.post(f"{BASE}/api/dashboard"),
        respx.put(f"{BASE}/api/dashboard/3"),
    ]

    url = provision.provision(settings)

    assert url == f"{BASE}/dashboard/3"
    assert _bodies(login)[0]["username"] == "admin@tasometro.local"
    assert all(not route.called for route in creations)


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
