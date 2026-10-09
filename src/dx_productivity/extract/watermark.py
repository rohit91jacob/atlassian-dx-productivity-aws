"""Watermarks with optimistic locking (DynamoDB conditional writes / SQLite equivalent)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from dx_productivity.extract.raw import iso
from dx_productivity.storage.state import StateStore


def parse_ts(value: str) -> datetime:
    """Parse GitHub (``...Z``) and Jira (``...+0000`` / ``.000+0000``) timestamps to aware UTC."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    # Jira: 2026-10-01T09:15:00.000+0000 -> +00:00
    if len(text) >= 5 and text[-5] in "+-" and text[-3] != ":":
        text = f"{text[:-2]}:{text[-2:]}"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


@dataclass
class Watermark:
    """A single stream's high-water mark. ``advance`` never moves it backwards."""

    state: StateStore
    pk: str
    default: datetime
    value: datetime | None = None
    version: int | None = None
    doc: dict[str, Any] | None = None

    def load(self) -> datetime:
        item = self.state.get(self.pk)
        if item is None:
            self.value, self.version, self.doc = None, None, {}
            return self.default
        self.version = item.version
        self.doc = dict(item.doc)
        self.value = parse_ts(item.doc["watermark"])
        return self.value

    @property
    def current(self) -> datetime:
        return self.value or self.default

    def start_with_overlap(self, overlap: timedelta) -> datetime:
        return self.current - overlap if self.value is not None else self.default

    def advance(self, new_value: datetime, run_id: str, **extra: Any) -> bool:
        if self.value is not None and new_value <= self.value:
            return False
        doc = {**(self.doc or {}), "watermark": iso(new_value), "run_id": run_id, **extra}
        item = self.state.put(self.pk, doc, expected_version=self.version)
        self.value, self.version, self.doc = new_value, item.version, doc
        return True
