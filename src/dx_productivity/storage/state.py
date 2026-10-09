"""Pipeline state: watermarks, processed-object markers and run records.

DynamoDB in AWS (single table, ``pk`` string hash key, optimistic locking on ``version``) and a
SQLite stand-in locally with the same conditional-write semantics, so two concurrent runs can
never both "win" a watermark update.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol


class ConflictError(RuntimeError):
    """A conditional write lost the race (another run updated the item first)."""


@dataclass(frozen=True)
class StateItem:
    pk: str
    version: int
    doc: dict[str, Any]
    updated_at: str


class StateStore(Protocol):
    def get(self, pk: str) -> StateItem | None: ...

    def put(self, pk: str, doc: dict[str, Any], expected_version: int | None) -> StateItem: ...

    def processed(self, pks: Iterable[str]) -> set[str]: ...

    def mark_processed(self, pks: Iterable[str], doc: dict[str, Any]) -> None: ...

    def scan(self, prefix: str) -> list[StateItem]: ...

    def delete_prefix(self, prefix: str) -> int: ...


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def watermark_key(source: str, scope: str, entity: str) -> str:
    return f"watermark#{source}#{scope}#{entity}"


def processed_key(raw_key: str) -> str:
    return f"processed#{raw_key}"


class SqliteStateStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as con:
            con.execute(
                "CREATE TABLE IF NOT EXISTS state ("
                " pk TEXT PRIMARY KEY, version INTEGER NOT NULL, doc TEXT NOT NULL,"
                " updated_at TEXT NOT NULL)"
            )

    def _connect(self) -> sqlite3.Connection:
        # Autocommit mode; multi-statement writes open explicit transactions.
        return sqlite3.connect(self.path, timeout=30, isolation_level=None)

    def get(self, pk: str) -> StateItem | None:
        with closing(self._connect()) as con:
            row = con.execute(
                "SELECT pk, version, doc, updated_at FROM state WHERE pk = ?", (pk,)
            ).fetchone()
        return StateItem(row[0], row[1], json.loads(row[2]), row[3]) if row else None

    def put(self, pk: str, doc: dict[str, Any], expected_version: int | None) -> StateItem:
        item = StateItem(pk, (expected_version or 0) + 1, doc, _now())
        payload = json.dumps(doc, sort_keys=True, default=str)
        with closing(self._connect()) as con:
            if expected_version is None:
                cur = con.execute(
                    "INSERT OR IGNORE INTO state VALUES (?, ?, ?, ?)",
                    (pk, item.version, payload, item.updated_at),
                )
            else:
                cur = con.execute(
                    "UPDATE state SET version = ?, doc = ?, updated_at = ?"
                    " WHERE pk = ? AND version = ?",
                    (item.version, payload, item.updated_at, pk, expected_version),
                )
            if cur.rowcount != 1:
                raise ConflictError(
                    f"conditional write on {pk!r} lost (expected version {expected_version})"
                )
        return item

    def processed(self, pks: Iterable[str]) -> set[str]:
        wanted = list(pks)
        found: set[str] = set()
        with closing(self._connect()) as con:
            for i in range(0, len(wanted), 500):
                chunk = wanted[i : i + 500]
                marks = ",".join("?" * len(chunk))
                rows = con.execute(f"SELECT pk FROM state WHERE pk IN ({marks})", chunk)
                found.update(r[0] for r in rows)
        return found

    def mark_processed(self, pks: Iterable[str], doc: dict[str, Any]) -> None:
        payload = json.dumps(doc, sort_keys=True, default=str)
        now = _now()
        with closing(self._connect()) as con:
            con.execute("BEGIN")
            con.executemany(
                "INSERT OR REPLACE INTO state VALUES (?, 1, ?, ?)",
                [(pk, payload, now) for pk in pks],
            )
            con.execute("COMMIT")

    def scan(self, prefix: str) -> list[StateItem]:
        with closing(self._connect()) as con:
            rows = con.execute(
                "SELECT pk, version, doc, updated_at FROM state WHERE substr(pk, 1, ?) = ? ORDER BY pk",
                (len(prefix), prefix),
            ).fetchall()
        return [StateItem(r[0], r[1], json.loads(r[2]), r[3]) for r in rows]

    def delete_prefix(self, prefix: str) -> int:
        with closing(self._connect()) as con:
            cur = con.execute("DELETE FROM state WHERE substr(pk, 1, ?) = ?", (len(prefix), prefix))
        return cur.rowcount


class DynamoStateStore:
    """Single-table DynamoDB store (hash key ``pk``). Scans are only used by operator commands."""

    def __init__(self, table: str, client: Any | None = None, region: str | None = None):
        import boto3

        self.table = table
        self.client = client or boto3.client("dynamodb", region_name=region)

    @staticmethod
    def _item(raw: dict[str, Any]) -> StateItem:
        return StateItem(
            raw["pk"]["S"],
            int(raw["version"]["N"]),
            json.loads(raw["doc"]["S"]),
            raw["updated_at"]["S"],
        )

    def get(self, pk: str) -> StateItem | None:
        resp = self.client.get_item(
            TableName=self.table, Key={"pk": {"S": pk}}, ConsistentRead=True
        )
        return self._item(resp["Item"]) if "Item" in resp else None

    def put(self, pk: str, doc: dict[str, Any], expected_version: int | None) -> StateItem:
        from botocore.exceptions import ClientError

        item = StateItem(pk, (expected_version or 0) + 1, doc, _now())
        kwargs: dict[str, Any] = {
            "TableName": self.table,
            "Item": {
                "pk": {"S": pk},
                "version": {"N": str(item.version)},
                "doc": {"S": json.dumps(doc, sort_keys=True, default=str)},
                "updated_at": {"S": item.updated_at},
            },
        }
        if expected_version is None:
            kwargs["ConditionExpression"] = "attribute_not_exists(pk)"
        else:
            kwargs["ConditionExpression"] = "version = :v"
            kwargs["ExpressionAttributeValues"] = {":v": {"N": str(expected_version)}}
        try:
            self.client.put_item(**kwargs)
        except ClientError as err:
            if err.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                raise ConflictError(f"conditional write on {pk!r} lost") from err
            raise
        return item

    def processed(self, pks: Iterable[str]) -> set[str]:
        wanted = list(dict.fromkeys(pks))
        found: set[str] = set()
        for i in range(0, len(wanted), 100):
            request: dict[str, Any] = {
                self.table: {
                    "Keys": [{"pk": {"S": pk}} for pk in wanted[i : i + 100]],
                    "ProjectionExpression": "pk",
                }
            }
            while request:
                resp = self.client.batch_get_item(RequestItems=request)
                found.update(r["pk"]["S"] for r in resp["Responses"].get(self.table, []))
                request = resp.get("UnprocessedKeys") or {}
        return found

    def mark_processed(self, pks: Iterable[str], doc: dict[str, Any]) -> None:
        payload = json.dumps(doc, sort_keys=True, default=str)
        now = _now()
        items = [
            {
                "PutRequest": {
                    "Item": {
                        "pk": {"S": pk},
                        "version": {"N": "1"},
                        "doc": {"S": payload},
                        "updated_at": {"S": now},
                    }
                }
            }
            for pk in pks
        ]
        for i in range(0, len(items), 25):
            request: dict[str, Any] = {self.table: items[i : i + 25]}
            while request:
                resp = self.client.batch_write_item(RequestItems=request)
                request = resp.get("UnprocessedItems") or {}

    def scan(self, prefix: str) -> list[StateItem]:
        paginator = self.client.get_paginator("scan")
        items: list[StateItem] = []
        for page in paginator.paginate(
            TableName=self.table,
            FilterExpression="begins_with(pk, :p)",
            ExpressionAttributeValues={":p": {"S": prefix}},
        ):
            items.extend(self._item(raw) for raw in page.get("Items", []))
        return sorted(items, key=lambda item: item.pk)

    def delete_prefix(self, prefix: str) -> int:
        items = self.scan(prefix)
        for item in items:
            self.client.delete_item(TableName=self.table, Key={"pk": {"S": item.pk}})
        return len(items)
