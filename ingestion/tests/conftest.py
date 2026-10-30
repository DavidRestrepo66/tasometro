import os
import uuid
from collections.abc import Iterator

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

ADMIN_DSN_ENV = "TASOMETRO_TEST_DSN"


@pytest.fixture(scope="session")
def _test_database() -> Iterator[str]:
    """A throwaway database on the Postgres named by TASOMETRO_TEST_DSN."""
    admin_dsn = os.environ.get(ADMIN_DSN_ENV)
    if not admin_dsn:
        pytest.skip(f"{ADMIN_DSN_ENV} is not set; Postgres tests need a server")
    name = f"tasometro_test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{name}"')
    try:
        yield make_conninfo(admin_dsn, dbname=name)
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{name}" WITH (FORCE)')


@pytest.fixture
def warehouse_dsn(_test_database: str) -> str:
    """DSN of an empty warehouse: the raw schema is dropped before each test."""
    assert conninfo_to_dict(_test_database)["dbname"].startswith("tasometro_test_")
    with psycopg.connect(_test_database, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS raw CASCADE")
    return _test_database
