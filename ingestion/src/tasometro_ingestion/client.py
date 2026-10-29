"""HTTP client for the Socrata (SODA 2.x) API behind datos.gov.co.

The API aborts slow aggregations and throttles bursts, so every page request
is retried with exponential backoff on timeouts, network errors, 429 and 5xx.
Other 4xx responses are query errors: retrying them cannot succeed, so they
fail fast with the message returned by Socrata.
"""

import re
from collections.abc import Mapping
from types import TracebackType
from typing import Any, Self

import httpx
from tenacity import (
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

DEFAULT_BASE_URL = "https://www.datos.gov.co"
_DATASET_ID = re.compile(r"[a-z0-9]{4}-[a-z0-9]{4}")
_RESERVED = frozenset({"$limit", "$offset"})

Row = dict[str, Any]


class SocrataError(RuntimeError):
    """Non-retryable error returned by the Socrata API."""


class _RetryableStatus(Exception):
    def __init__(self, response: httpx.Response) -> None:
        super().__init__(f"HTTP {response.status_code}")
        self.response = response


def _is_retryable(exc: BaseException) -> bool:
    return isinstance(exc, httpx.TransportError | _RetryableStatus)


class SocrataClient:
    """Paginated, retrying reader of Socrata resources.

    Use as a context manager so the underlying connection pool is closed.
    """

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        app_token: str | None = None,
        *,
        page_size: int = 50_000,
        timeout_seconds: float = 180.0,
        max_attempts: int = 5,
        backoff_seconds: float = 2.0,
    ) -> None:
        if page_size < 1 or max_attempts < 1:
            raise ValueError("page_size and max_attempts must be positive")
        headers = {"Accept": "application/json"}
        if app_token:
            headers["X-App-Token"] = app_token
        self._http = httpx.Client(
            base_url=base_url,
            headers=headers,
            timeout=httpx.Timeout(timeout_seconds, connect=10.0),
        )
        self._page_size = page_size
        self._max_attempts = max_attempts
        self._backoff_seconds = backoff_seconds

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    def fetch_all(self, dataset: str, query: Mapping[str, str]) -> list[Row]:
        """Return every row of ``query`` against ``dataset``, following pages.

        The query must carry a deterministic ``$order`` (or be a grouped query
        returning a single page) for pagination to be stable.
        """
        self._check(dataset, query)
        rows: list[Row] = []
        offset = 0
        while True:
            params = {**query, "$limit": str(self._page_size), "$offset": str(offset)}
            page = self._get_page(f"/resource/{dataset}.json", params)
            rows.extend(page)
            if len(page) < self._page_size:
                return rows
            offset += self._page_size

    def fetch_first(self, dataset: str, query: Mapping[str, str]) -> Row | None:
        """Return the first row of ``query``, or ``None`` when nothing matches."""
        self._check(dataset, query)
        page = self._get_page(f"/resource/{dataset}.json", {**query, "$limit": "1"})
        return page[0] if page else None

    def _check(self, dataset: str, query: Mapping[str, str]) -> None:
        if not _DATASET_ID.fullmatch(dataset):
            raise ValueError(f"invalid Socrata dataset id: {dataset!r}")
        if _RESERVED & query.keys():
            raise ValueError("$limit/$offset are managed by the client")

    def _get_page(self, path: str, params: Mapping[str, str]) -> list[Row]:
        retrying = Retrying(
            retry=retry_if_exception(_is_retryable),
            stop=stop_after_attempt(self._max_attempts),
            wait=wait_exponential(multiplier=self._backoff_seconds, max=60),
            reraise=True,
        )
        try:
            response = retrying(self._request, path, params)
        except _RetryableStatus as exc:
            raise SocrataError(_describe(exc.response)) from exc

        payload = response.json()
        if not isinstance(payload, list):
            raise SocrataError(f"expected a JSON array, got {type(payload).__name__}")
        return payload

    def _request(self, path: str, params: Mapping[str, str]) -> httpx.Response:
        response = self._http.get(path, params=params)
        if response.status_code == 429 or response.status_code >= 500:
            raise _RetryableStatus(response)
        if response.is_error:
            raise SocrataError(_describe(response))
        return response


def _describe(response: httpx.Response) -> str:
    try:
        body = response.json()
        message = body.get("message", body) if isinstance(body, dict) else body
    except ValueError:
        message = response.text[:300]
    return f"HTTP {response.status_code}: {message}"
