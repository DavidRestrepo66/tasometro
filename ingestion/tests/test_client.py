import httpx
import pytest
import respx

from tasometro_ingestion.client import SocrataClient, SocrataError

BASE = "https://example.test"
URL = f"{BASE}/resource/abcd-1234.json"
QUERY = {"$select": "a", "$where": "a > 0"}


def make_client(**overrides: object) -> SocrataClient:
    options: dict[str, object] = {
        "base_url": BASE,
        "page_size": 2,
        "max_attempts": 3,
        "backoff_seconds": 0,
    }
    options.update(overrides)
    return SocrataClient(**options)  # type: ignore[arg-type]


@respx.mock
def test_fetch_all_paginates_until_a_short_page() -> None:
    route = respx.get(URL).mock(
        side_effect=[
            httpx.Response(200, json=[{"a": "1"}, {"a": "2"}]),
            httpx.Response(200, json=[{"a": "3"}]),
        ]
    )

    with make_client() as client:
        rows = client.fetch_all("abcd-1234", QUERY)

    assert rows == [{"a": "1"}, {"a": "2"}, {"a": "3"}]
    offsets = [call.request.url.params["$offset"] for call in route.calls]
    limits = {call.request.url.params["$limit"] for call in route.calls}
    assert offsets == ["0", "2"]
    assert limits == {"2"}
    assert all(call.request.url.params["$where"] == "a > 0" for call in route.calls)


@respx.mock
def test_fetch_all_stops_on_an_empty_page_after_a_full_one() -> None:
    route = respx.get(URL).mock(
        side_effect=[
            httpx.Response(200, json=[{"a": "1"}, {"a": "2"}]),
            httpx.Response(200, json=[]),
        ]
    )

    with make_client() as client:
        assert client.fetch_all("abcd-1234", QUERY) == [{"a": "1"}, {"a": "2"}]
    assert route.call_count == 2


@respx.mock
def test_app_token_is_sent_as_header_when_configured() -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(200, json=[]))

    with make_client(app_token="secret-token") as client:
        client.fetch_all("abcd-1234", QUERY)

    assert route.calls.last.request.headers["X-App-Token"] == "secret-token"


@respx.mock
def test_no_app_token_header_without_token() -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(200, json=[]))

    with make_client() as client:
        client.fetch_all("abcd-1234", QUERY)

    assert "X-App-Token" not in route.calls.last.request.headers


@pytest.mark.parametrize(
    "failure",
    [
        httpx.ReadTimeout("slow"),
        httpx.ConnectError("down"),
        httpx.Response(429),
        httpx.Response(500),
        httpx.Response(503),
    ],
    ids=["read-timeout", "connect-error", "429", "500", "503"],
)
@respx.mock
def test_transient_failures_are_retried(failure: object) -> None:
    route = respx.get(URL).mock(side_effect=[failure, httpx.Response(200, json=[{"a": "1"}])])

    with make_client() as client:
        assert client.fetch_all("abcd-1234", QUERY) == [{"a": "1"}]
    assert route.call_count == 2


@respx.mock
def test_retries_are_bounded_and_last_error_is_raised() -> None:
    route = respx.get(URL).mock(side_effect=httpx.ReadTimeout("slow"))

    with make_client(max_attempts=3) as client, pytest.raises(httpx.ReadTimeout):
        client.fetch_all("abcd-1234", QUERY)
    assert route.call_count == 3


@respx.mock
def test_exhausted_server_errors_raise_socrata_error() -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(503, text="busy"))

    with make_client(max_attempts=2) as client, pytest.raises(SocrataError, match="HTTP 503"):
        client.fetch_all("abcd-1234", QUERY)
    assert route.call_count == 2


@respx.mock
def test_query_errors_fail_fast_with_socrata_message() -> None:
    body = {"errorCode": "query.soql.no-such-column", "message": "No such column: x"}
    route = respx.get(URL).mock(return_value=httpx.Response(400, json=body))

    with make_client() as client, pytest.raises(SocrataError, match=r"400.*No such column: x"):
        client.fetch_all("abcd-1234", QUERY)
    assert route.call_count == 1


@respx.mock
def test_non_list_payload_is_rejected() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json={"error": True}))

    with make_client() as client, pytest.raises(SocrataError, match="expected a JSON array"):
        client.fetch_all("abcd-1234", QUERY)


def test_caller_cannot_override_pagination_parameters() -> None:
    with make_client() as client, pytest.raises(ValueError, match=r"\$limit/\$offset"):
        client.fetch_all("abcd-1234", {**QUERY, "$limit": "10"})


@pytest.mark.parametrize("dataset", ["../etc", "abcd1234", "ABCD-1234", ""])
def test_dataset_id_must_be_a_socrata_four_by_four(dataset: str) -> None:
    with make_client() as client, pytest.raises(ValueError, match="dataset id"):
        client.fetch_all(dataset, QUERY)


@respx.mock
def test_fetch_first_requests_one_row_and_returns_it() -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(200, json=[{"a": "1"}]))

    with make_client() as client:
        row = client.fetch_first("abcd-1234", QUERY)

    assert row == {"a": "1"}
    params = route.calls.last.request.url.params
    assert params["$limit"] == "1"
    assert "$offset" not in params
    assert params["$where"] == "a > 0"


@respx.mock
def test_fetch_first_returns_none_when_nothing_matches() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json=[]))

    with make_client() as client:
        assert client.fetch_first("abcd-1234", QUERY) is None


def test_fetch_first_rejects_client_managed_parameters() -> None:
    with make_client() as client, pytest.raises(ValueError, match="managed by the client"):
        client.fetch_first("abcd-1234", {**QUERY, "$limit": "5"})


def test_page_size_above_the_soda2_maximum_is_rejected() -> None:
    with pytest.raises(ValueError, match="50000"):
        make_client(page_size=50_001)


def test_page_size_at_the_soda2_maximum_is_accepted() -> None:
    make_client(page_size=50_000).close()


def test_app_token_requires_an_https_base_url() -> None:
    with pytest.raises(ValueError, match="https"):
        make_client(base_url="http://example.test", app_token="secret-token")


def test_plain_http_is_allowed_without_a_token() -> None:
    make_client(base_url="http://example.test").close()


@respx.mock
def test_redirect_responses_raise_socrata_error() -> None:
    route = respx.get(URL).mock(
        return_value=httpx.Response(302, headers={"Location": "https://other.test/x"})
    )

    with make_client() as client, pytest.raises(SocrataError, match="302"):
        client.fetch_all("abcd-1234", QUERY)
    assert route.call_count == 1
