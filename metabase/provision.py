"""Provision a local Metabase over the tasometro warehouse through its API.

Idempotent: the first run completes the Metabase setup (admin user), registers
the warehouse connection, creates the four dashboard questions and the
dashboard; later runs find them by name and change nothing.

Usage (with .env exported)::

    uv run --project ingestion python metabase/provision.py
"""

from __future__ import annotations

import os
import sys
from collections import Counter
from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass
from typing import Any

import httpx

DATABASE_NAME = "tasometro warehouse"
DASHBOARD_NAME = "Tasometro - tasas de crédito vs TPM"

# Dashboard filters: id -> (label, Metabase parameter type, template-tag type).
FILTERS: dict[str, tuple[str, str, str]] = {
    "tipo_credito": ("Tipo de crédito", "string/=", "text"),
    "plazo_banda": ("Plazo", "string/=", "text"),
    "desde": ("Desde", "date/single", "date"),
    "entidad": ("Entidad", "string/=", "text"),
}

_OPTIONAL = {
    "tipo_credito": "[[and {a}.tipo_credito = {{{{tipo_credito}}}}]]",
    "plazo_banda": "[[and {a}.plazo_banda = {{{{plazo_banda}}}}]]",
    "desde": "[[and {a}.fecha_corte >= {{{{desde}}}}]]",
    "entidad": "[[and e.nombre_entidad = {{{{entidad}}}}]]",
}


def _where(alias: str, *filters: str) -> str:
    return "\n  ".join(_OPTIONAL[name].format(a=alias) for name in filters)


@dataclass(frozen=True)
class CardSpec:
    name: str
    sql: str
    tags: tuple[str, ...]
    display: str
    visualization: dict[str, Any]


CARDS: tuple[CardSpec, ...] = (
    CardSpec(
        name="Tasa ponderada vs TPM",
        sql=f"""select m.fecha_corte,
  sum(m.sum_tasa_x_monto) / nullif(sum(m.sum_montos), 0) as tasa_ponderada,
  max(m.tpm_semana) as tpm
from gold.mart_tasas_semanales as m
inner join gold.dim_entidad as e on e.entidad_key = m.entidad_key
where true
  {_where("m", "tipo_credito", "plazo_banda", "desde", "entidad")}
group by 1
order by 1""",
        tags=("tipo_credito", "plazo_banda", "desde", "entidad"),
        display="line",
        visualization={
            "graph.dimensions": ["fecha_corte"],
            "graph.metrics": ["tasa_ponderada", "tpm"],
        },
    ),
    CardSpec(
        name="Spread de la última semana por entidad y producto",
        sql=f"""select e.nombre_entidad, m.producto_credito,
  round(sum(m.sum_tasa_x_monto) / nullif(sum(m.sum_montos), 0) - max(m.tpm_semana), 2) as spread,
  max(m.semana_con_cambio_tpm::int) = 1 as semana_con_cambio_tpm
from gold.mart_tasas_semanales as m
inner join gold.dim_entidad as e on e.entidad_key = m.entidad_key
where m.fecha_corte = (select max(fecha_corte) from gold.mart_tasas_semanales)
  {_where("m", "tipo_credito", "plazo_banda", "entidad")}
group by 1, 2
order by spread desc
limit 50""",
        tags=("tipo_credito", "plazo_banda", "entidad"),
        display="table",
        visualization={},
    ),
    CardSpec(
        name="Brecha por sexo (mujeres menos hombres, misma entidad y producto)",
        sql=f"""select b.fecha_corte,
  sum(b.brecha_femenino_menos_masculino * (b.creditos_femenino + b.creditos_masculino))
    / nullif(sum(b.creditos_femenino + b.creditos_masculino), 0) as brecha
from gold.mart_brecha_sexo as b
where true
  {_where("b", "tipo_credito", "plazo_banda", "desde")}
group by 1
order by 1""",
        tags=("tipo_credito", "plazo_banda", "desde"),
        display="line",
        visualization={"graph.dimensions": ["fecha_corte"], "graph.metrics": ["brecha"]},
    ),
    CardSpec(
        name="Tasa por tamaño de empresa (personas jurídicas)",
        sql=f"""select f.fecha_corte, s.tamano_empresa,
  sum(f.sum_tasa_x_monto) / nullif(sum(f.sum_montos), 0) as tasa_ponderada
from gold.fact_tasas as f
inner join gold.dim_segmento as s on s.segmento_key = f.segmento_key
inner join gold.dim_producto as p on p.producto_key = f.producto_key
where p.en_spread
  and s.tipo_de_persona = 'Jurídica'
  {_where("p", "tipo_credito", "plazo_banda")}
  [[and f.fecha_corte >= {{{{desde}}}}]]
group by 1, 2
order by 1, 2""",
        tags=("tipo_credito", "plazo_banda", "desde"),
        display="line",
        visualization={
            "graph.dimensions": ["fecha_corte", "tamano_empresa"],
            "graph.metrics": ["tasa_ponderada"],
        },
    ),
)

# (row, col, width, height) on Metabase's 24-column grid, one per card.
LAYOUT = ((0, 0, 12, 8), (0, 12, 12, 8), (8, 0, 12, 8), (8, 12, 12, 8))


@dataclass(frozen=True)
class Settings:
    url: str
    admin_email: str
    admin_password: str
    host: str
    port: int
    dbname: str
    user: str
    password: str
    # The API never returns the password, so a changed one is only pushed on request.
    refresh_database: bool = False

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Settings:
        missing = [
            k
            for k in ("METABASE_ADMIN_PASSWORD", "POSTGRES_USER", "POSTGRES_PASSWORD")
            if not env.get(k)
        ]
        if missing:
            raise SystemExit(f"missing environment variables: {', '.join(missing)}")
        port = env.get("METABASE_HOST_PORT", "3000")
        return cls(
            url=env.get("METABASE_URL", f"http://localhost:{port}"),
            admin_email=env.get("METABASE_ADMIN_EMAIL", "admin@tasometro.local"),
            admin_password=env["METABASE_ADMIN_PASSWORD"],
            # The connection is opened by Metabase, so it uses the compose network address.
            host=env.get("METABASE_WAREHOUSE_HOST", "postgres"),
            port=int(env.get("METABASE_WAREHOUSE_PORT", "5432")),
            dbname=env.get("METABASE_WAREHOUSE_DB", "warehouse"),
            user=env["POSTGRES_USER"],
            password=env["POSTGRES_PASSWORD"],
            refresh_database=env.get("METABASE_REFRESH_DATABASE", "") in {"1", "true", "yes"},
        )


def build_card(spec: CardSpec, database_id: int) -> dict[str, Any]:
    tags = {
        name: {
            "id": name,
            "name": name,
            "display-name": FILTERS[name][0],
            "type": FILTERS[name][2],
        }
        for name in spec.tags
    }
    return {
        "name": spec.name,
        "display": spec.display,
        "visualization_settings": spec.visualization,
        "dataset_query": {
            "type": "native",
            "database": database_id,
            "native": {"query": spec.sql, "template-tags": tags},
        },
    }


def build_dashcards(card_ids: list[int]) -> list[dict[str, Any]]:
    dashcards = []
    for index, (spec, card_id) in enumerate(zip(CARDS, card_ids, strict=True)):
        row, col, width, height = LAYOUT[index]
        dashcards.append(
            {
                "id": -(index + 1),
                "card_id": card_id,
                "row": row,
                "col": col,
                "size_x": width,
                "size_y": height,
                "parameter_mappings": [
                    {
                        "parameter_id": name,
                        "card_id": card_id,
                        "target": ["variable", ["template-tag", name]],
                    }
                    for name in spec.tags
                ],
            }
        )
    return dashcards


def dashboard_parameters() -> list[dict[str, str]]:
    return [
        {"id": name, "name": label, "slug": name, "type": kind}
        for name, (label, kind, _) in FILTERS.items()
    ]


def open_session(settings: Settings) -> httpx.Client:
    client = httpx.Client(base_url=settings.url, timeout=60)
    properties = client.get("/api/session/properties").raise_for_status().json()
    # The setup token stays in the response after setup; only has-user-setup tells them apart.
    token = None if properties.get("has-user-setup") else properties.get("setup-token")
    if token:
        response = client.post(
            "/api/setup",
            json={
                "token": token,
                "user": {
                    "first_name": "Tasometro",
                    "last_name": "Admin",
                    "email": settings.admin_email,
                    "password": settings.admin_password,
                },
                "prefs": {"site_name": "Tasometro", "allow_tracking": False},
            },
        )
    else:
        response = client.post(
            "/api/session",
            json={"username": settings.admin_email, "password": settings.admin_password},
        )
    client.headers["X-Metabase-Session"] = response.raise_for_status().json()["id"]
    return client


def _database_payload(settings: Settings) -> dict[str, Any]:
    return {
        "name": DATABASE_NAME,
        "engine": "postgres",
        "is_full_sync": True,
        "details": {
            "host": settings.host,
            "port": settings.port,
            "dbname": settings.dbname,
            "user": settings.user,
            "password": settings.password,
            "ssl": False,
        },
    }


def _database_matches(database: Mapping[str, Any], payload: Mapping[str, Any]) -> bool:
    # The password is never returned by the API, so it is not compared.
    keys = ("host", "port", "dbname", "user")
    current = database.get("details") or {}
    return database.get("engine") == payload["engine"] and all(
        current.get(key) == payload["details"][key] for key in keys
    )


def ensure_database(client: httpx.Client, settings: Settings) -> int:
    payload = _database_payload(settings)
    listing = client.get("/api/database").raise_for_status().json()
    for database in listing["data"]:
        if database["name"] == DATABASE_NAME:
            if settings.refresh_database or not _database_matches(database, payload):
                client.put(f"/api/database/{database['id']}", json=payload).raise_for_status()
            return database["id"]
    created = client.post("/api/database", json=payload)
    return created.raise_for_status().json()["id"]


def ensure_dashboard(client: httpx.Client, database_id: int) -> int:
    card_ids = _ensure_cards(client, database_id)
    existing = client.get("/api/dashboard").raise_for_status().json()
    dashboard_id = next((d["id"] for d in existing if d["name"] == DASHBOARD_NAME), None)
    if dashboard_id is not None:
        # A previous run may have created the dashboard and failed before filling it.
        current = client.get(f"/api/dashboard/{dashboard_id}").raise_for_status().json()
        if _is_complete(current, card_ids):
            return dashboard_id
    else:
        created = client.post(
            "/api/dashboard", json={"name": DASHBOARD_NAME, "parameters": dashboard_parameters()}
        )
        dashboard_id = created.raise_for_status().json()["id"]
    client.put(
        f"/api/dashboard/{dashboard_id}",
        json={"dashcards": build_dashcards(card_ids), "parameters": dashboard_parameters()},
    ).raise_for_status()
    return dashboard_id


def _ensure_cards(client: httpx.Client, database_id: int) -> list[int]:
    # Reuse a card by name only when it queries the requested database.
    reusable = {
        card["name"]: card["id"]
        for card in client.get("/api/card").raise_for_status().json()
        if (card.get("dataset_query") or {}).get("database") == database_id
    }
    card_ids = []
    for spec in CARDS:
        if spec.name in reusable:
            card_ids.append(reusable[spec.name])
            continue
        created = client.post("/api/card", json=build_card(spec, database_id))
        card_ids.append(created.raise_for_status().json()["id"])
    return card_ids


def _mappings(dashcards: list[dict[str, Any]]) -> dict[Any, list[str]]:
    return {
        dashcard.get("card_id"): sorted(
            repr((m.get("parameter_id"), m.get("target")))
            for m in dashcard.get("parameter_mappings") or []
        )
        for dashcard in dashcards
    }


def _is_complete(dashboard: dict[str, Any], card_ids: list[int]) -> bool:
    dashcards = dashboard.get("dashcards") or []
    placed = [dashcard.get("card_id") for dashcard in dashcards]
    parameters = {parameter.get("id") for parameter in dashboard.get("parameters") or []}
    return (
        Counter(placed) == Counter(card_ids)
        and parameters == set(FILTERS)
        and _mappings(dashcards) == _mappings(build_dashcards(card_ids))
    )


def provision(settings: Settings) -> str:
    with closing(open_session(settings)) as client:
        database_id = ensure_database(client, settings)
        dashboard_id = ensure_dashboard(client, database_id)
    return f"{settings.url}/dashboard/{dashboard_id}"


def main() -> None:
    print(provision(Settings.from_env(os.environ)))


if __name__ == "__main__":
    sys.exit(main())
