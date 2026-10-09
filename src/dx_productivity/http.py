"""A small, dependency-free HTTP client for polite API extraction.

Standard library only (``urllib``) so the extractor Lambda zip needs no third-party wheels.
Features: retries with exponential backoff on 429/5xx/network errors, ``Retry-After``, a
per-client minimum interval between requests, a hard request budget, GitHub primary
rate-limit awareness, and a pluggable transport so tests replay recorded API data offline.
"""

from __future__ import annotations

import http.client
import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from dx_productivity.logs import get_logger

log = get_logger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class Request:
    method: str
    url: str
    params: tuple[tuple[str, str], ...] = ()
    headers: Mapping[str, str] = field(default_factory=dict)

    @property
    def full_url(self) -> str:
        if not self.params:
            return self.url
        return f"{self.url}?{urllib.parse.urlencode(self.params)}"

    def param(self, name: str, default: str | None = None) -> str | None:
        for key, value in self.params:
            if key == name:
                return value
        return default


@dataclass(frozen=True)
class Response:
    status: int
    headers: Mapping[str, str]
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body.decode("utf-8"))

    def header(self, name: str) -> str | None:
        lowered = name.lower()
        for key, value in self.headers.items():
            if key.lower() == lowered:
                return value
        return None


class Transport(Protocol):
    def send(self, request: Request, timeout: float) -> Response: ...


class UrllibTransport:
    """Real network transport."""

    def send(self, request: Request, timeout: float) -> Response:
        req = urllib.request.Request(  # noqa: S310 - URLs come from validated settings (https)
            request.full_url, method=request.method, headers=dict(request.headers)
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
                return Response(resp.status, dict(resp.headers.items()), resp.read())
        except urllib.error.HTTPError as err:
            return Response(err.code, dict(err.headers.items()) if err.headers else {}, err.read())
        except urllib.error.URLError:
            raise
        except (http.client.HTTPException, OSError) as exc:
            # A connection dropped mid-body (IncompleteRead, RemoteDisconnected, reset): retryable.
            raise ConnectionError(f"{type(exc).__name__}: {exc}") from exc


class HttpError(RuntimeError):
    def __init__(self, request: Request, response: Response):
        snippet = response.body[:300].decode("utf-8", "replace")
        super().__init__(
            f"{request.method} {request.full_url} -> HTTP {response.status}: {snippet}"
        )
        self.status = response.status


class BudgetExhausted(RuntimeError):  # noqa: N818 - a control-flow signal, not an error
    """The run's request budget is spent; callers stop cleanly and keep their watermark."""


class RateLimited(RuntimeError):  # noqa: N818
    """The API's primary rate limit is exhausted until ``reset_at`` (epoch seconds)."""

    def __init__(self, reset_at: float | None):
        super().__init__(f"rate limit exhausted until {reset_at}")
        self.reset_at = reset_at


@dataclass
class HttpClient:
    transport: Transport
    base_headers: Mapping[str, str]
    timeout_s: float = 60.0
    retries: int = 5
    min_interval_s: float = 0.0
    max_requests: int | None = None
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic
    backoff_base_s: float = 1.0
    max_backoff_s: float = 120.0
    requests_made: int = 0
    _last_request_at: float | None = None

    @property
    def remaining_budget(self) -> int | None:
        return None if self.max_requests is None else self.max_requests - self.requests_made

    def get_json(
        self,
        url: str,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> tuple[Any, Response, Request]:
        request = Request(
            "GET",
            url,
            tuple((k, str(v)) for k, v in (params or {}).items()),
            {**self.base_headers, **(headers or {})},
        )
        response = self.send(request)
        return response.json(), response, request

    def send(self, request: Request) -> Response:
        attempt = 0
        while True:
            if self.max_requests is not None and self.requests_made >= self.max_requests:
                raise BudgetExhausted(f"request budget of {self.max_requests} spent")
            self._pace()
            self.requests_made += 1
            try:
                response = self.transport.send(request, self.timeout_s)
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                if attempt >= self.retries:
                    raise
                delay = self._backoff(attempt)
                log.warning(
                    "network error, retrying",
                    extra={"url": request.url, "error": str(exc), "delay_s": round(delay, 2)},
                )
                self.sleep(delay)
                attempt += 1
                continue

            if response.status < 400:
                return response
            if self._is_primary_rate_limit(response):
                reset = response.header("x-ratelimit-reset")
                raise RateLimited(float(reset) if reset else None)
            if response.status in RETRYABLE_STATUS or self._is_secondary_rate_limit(response):
                if attempt >= self.retries:
                    raise HttpError(request, response)
                delay = self._retry_after(response) or self._backoff(attempt)
                log.warning(
                    "retryable HTTP status",
                    extra={
                        "url": request.url,
                        "status": response.status,
                        "delay_s": round(delay, 2),
                    },
                )
                self.sleep(delay)
                attempt += 1
                continue
            raise HttpError(request, response)

    # ------------------------------------------------------------------ helpers
    def _pace(self) -> None:
        if self.min_interval_s <= 0:
            return
        now = self.clock()
        if self._last_request_at is not None:
            wait = self.min_interval_s - (now - self._last_request_at)
            if wait > 0:
                self.sleep(wait)
                now += wait
        self._last_request_at = now

    def _backoff(self, attempt: int) -> float:
        # Full jitter keeps concurrent extractors from retrying in lock-step.
        ceiling = min(self.max_backoff_s, self.backoff_base_s * 2**attempt)
        return random.uniform(ceiling / 2, ceiling)  # noqa: S311 - jitter, not crypto

    @staticmethod
    def _retry_after(response: Response) -> float | None:
        value = response.header("retry-after")
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            return None

    @staticmethod
    def _is_primary_rate_limit(response: Response) -> bool:
        return response.status in {403, 429} and response.header("x-ratelimit-remaining") == "0"

    @staticmethod
    def _is_secondary_rate_limit(response: Response) -> bool:
        if response.status != 403:
            return False
        return b"secondary rate limit" in response.body.lower() or bool(
            response.header("retry-after")
        )
