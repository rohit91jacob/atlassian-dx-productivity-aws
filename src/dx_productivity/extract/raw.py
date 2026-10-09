"""Raw-zone landing: every API response is stored as received, gzip JSON, never mutated.

Object layout (Hive-style partitions so Athena/Glue can read the raw zone directly)::

    raw/<source>/<entity>/scope=<project-or-repo>/dt=<YYYY-MM-DD>/<run_id>-<seq>.json.gz

Each object is an *envelope* holding one or more pages, together with the request that produced
each page. Envelopes are deterministic (sorted keys, gzip mtime 0), so landing the same pages
twice produces byte-identical objects.
"""

from __future__ import annotations

import gzip
import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from dx_productivity.http import Request, Response
from dx_productivity.storage.objects import ObjectStore

SCHEMA = "dx.raw.v1"
_KEPT_HEADERS = ("etag", "last-modified", "x-ratelimit-remaining", "x-ratelimit-resource", "link")


def iso(ts: datetime) -> str:
    return ts.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def new_run_id(now: datetime) -> str:
    return f"{now.astimezone(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def page_record(
    request: Request, response: Response, body: Any, fetched_at: datetime, **extra: Any
) -> dict:
    headers = {k: v for k in _KEPT_HEADERS if (v := response.header(k)) is not None}
    record = {
        "request": {"url": request.url, "params": dict(request.params)},
        "status": response.status,
        "headers": headers,
        "fetched_at": iso(fetched_at),
        "body": body,
    }
    record.update(extra)
    return record


def encode_envelope(envelope: dict[str, Any]) -> bytes:
    data = json.dumps(envelope, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return gzip.compress(data.encode("utf-8"), compresslevel=6, mtime=0)


def decode_envelope(data: bytes) -> dict[str, Any]:
    return json.loads(gzip.decompress(data).decode("utf-8"))


@dataclass
class RawWriter:
    store: ObjectStore
    prefix: str
    run_id: str
    now: datetime
    landed: list[str] = field(default_factory=list)
    _seq: int = 0

    def land(
        self,
        source: str,
        entity: str,
        scope: str,
        pages: list[dict],
        meta: dict[str, Any] | None = None,
    ) -> str:
        """Write one envelope and return its key."""
        self._seq += 1
        key = (
            f"{self.prefix}/{source}/{entity}/scope={scope}/dt={self.now.astimezone(UTC):%Y-%m-%d}/"
            f"{self.run_id}-{self._seq:05d}.json.gz"
        )
        envelope = {
            "schema": SCHEMA,
            "source": source,
            "entity": entity,
            "scope": scope,
            "run_id": self.run_id,
            "landed_at": iso(self.now),
            "meta": meta or {},
            "pages": pages,
        }
        self.store.put(key, encode_envelope(envelope), content_type="application/json")
        self.landed.append(key)
        return key
