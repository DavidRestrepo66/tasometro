# tasometro-ingestion

Python package that extracts lending rates published by the Superintendencia
Financiera de Colombia on [datos.gov.co](https://www.datos.gov.co) and writes
them to the bronze layer as Parquet.

```bash
uv sync
uv run ruff check . && uv run ruff format --check .
uv run pytest            # offline tests
uv run pytest -m live    # tests against the real API
```
