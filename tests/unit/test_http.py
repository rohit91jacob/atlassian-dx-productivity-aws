from __future__ import annotations

import json

import pytest

from dx_productivity.http import (
    BudgetExhausted,
    HttpClient,
    HttpError,
    RateLimited,
    Request,
    Response,
)


class Scripted:
    """Transport that replays a list of responses (or exceptions) in order."""

    def __init__(self, *items):
        self.items = list(items)
        self.calls: list[Request] = []

    def send(self, request: Request, timeout: float) -> Response:
        self.calls.append(request)
        item = self.items.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def ok(body=None, **headers) -> Response:
    return Response(200, headers, json.dumps(body if body is not None else {"ok": True}).encode())


def client(transport, **kw) -> tuple[HttpClient, list[float]]:
    sleeps: list[float] = []
    return HttpClient(transport, {"User-Agent": "test"}, sleep=sleeps.append, **kw), sleeps


def test_retries_5xx_honouring_retry_after() -> None:
    t = Scripted(Response(503, {"Retry-After": "7"}, b"busy"), ok({"n": 1}))
    c, sleeps = client(t)
    body, _, _ = c.get_json("https://example.test/x", {"a": 1})
    assert body == {"n": 1}
    assert sleeps == [7.0]
    assert t.calls[0].full_url == "https://example.test/x?a=1"


def test_network_errors_and_truncated_bodies_are_retried() -> None:
    t = Scripted(ConnectionError("IncompleteRead"), TimeoutError(), ok())
    c, sleeps = client(t, retries=3)
    c.get_json("https://example.test/")
    assert len(sleeps) == 2
    assert c.requests_made == 3


def test_gives_up_after_retries() -> None:
    t = Scripted(*[Response(502, {}, b"bad gateway")] * 3)
    c, _ = client(t, retries=2)
    with pytest.raises(HttpError, match="502"):
        c.get_json("https://example.test/")


def test_client_errors_are_not_retried() -> None:
    c, sleeps = client(Scripted(Response(404, {}, b"missing")))
    with pytest.raises(HttpError):
        c.get_json("https://example.test/")
    assert sleeps == []


def test_primary_rate_limit_stops_immediately() -> None:
    t = Scripted(
        Response(403, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1791548145"}, b"{}")
    )
    c, sleeps = client(t)
    with pytest.raises(RateLimited) as exc:
        c.get_json("https://api.github.test/")
    assert exc.value.reset_at == 1791548145
    assert sleeps == []


def test_secondary_rate_limit_is_retried() -> None:
    t = Scripted(
        Response(403, {"Retry-After": "2"}, b"You have exceeded a secondary rate limit"), ok()
    )
    c, sleeps = client(t)
    c.get_json("https://api.github.test/")
    assert sleeps == [2.0]


def test_request_budget_is_hard() -> None:
    c, _ = client(Scripted(ok(), ok(), ok()), max_requests=2)
    c.get_json("https://example.test/1")
    c.get_json("https://example.test/2")
    assert c.remaining_budget == 0
    with pytest.raises(BudgetExhausted):
        c.get_json("https://example.test/3")


def test_min_interval_paces_requests() -> None:
    now = [100.0]
    sleeps: list[float] = []
    c = HttpClient(
        Scripted(ok(), ok()), {}, min_interval_s=1.0, sleep=sleeps.append, clock=lambda: now[0]
    )
    c.get_json("https://example.test/1")
    now[0] += 0.25
    c.get_json("https://example.test/2")
    assert sleeps == [pytest.approx(0.75)]
