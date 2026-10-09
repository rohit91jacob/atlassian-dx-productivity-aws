"""Object storage for the raw zone: S3 in AWS, a filesystem stand-in locally.

Both implementations expose the same tiny interface, so extractors and the curate step never
know which one they are writing to. The S3 implementation is exercised against moto in the unit
tests and against LocalStack in CI.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Protocol


class ObjectStore(Protocol):
    bucket: str

    def put(self, key: str, data: bytes, content_type: str = "application/json") -> None: ...

    def get(self, key: str) -> bytes: ...

    def list(self, prefix: str) -> list[str]: ...

    def exists(self, key: str) -> bool: ...

    def uri(self, key: str) -> str: ...


class LocalObjectStore:
    """``<root>/<bucket>/<key>`` on disk. Writes are atomic (temp file + rename), like S3 PUTs."""

    def __init__(self, root: Path, bucket: str):
        self.root = Path(root)
        self.bucket = bucket

    def _path(self, key: str) -> Path:
        if key.startswith("/") or ".." in key.split("/"):
            raise ValueError(f"invalid object key {key!r}")
        return self.root / self.bucket / key

    def put(self, key: str, data: bytes, content_type: str = "application/json") -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
        tmp.write_bytes(data)
        tmp.replace(path)

    def get(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def list(self, prefix: str) -> list[str]:
        base = self.root / self.bucket
        if not base.exists():
            return []
        keys = [
            p.relative_to(base).as_posix()
            for p in base.rglob("*")
            if p.is_file() and not p.name.startswith(".")
        ]
        return sorted(k for k in keys if k.startswith(prefix))

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def uri(self, key: str) -> str:
        return self._path(key).resolve().as_uri()


class S3ObjectStore:
    """Amazon S3 via boto3. Encryption is enforced by the bucket's default SSE-KMS (see infra/)."""

    def __init__(self, bucket: str, client: Any | None = None, region: str | None = None):
        import boto3

        self.bucket = bucket
        self.client = client or boto3.client("s3", region_name=region)

    def put(self, key: str, data: bytes, content_type: str = "application/json") -> None:
        extra: dict[str, str] = {"ContentType": content_type}
        if key.endswith(".gz"):
            extra["ContentEncoding"] = "gzip"
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, **extra)

    def get(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def list(self, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return sorted(keys)

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as err:
            if err.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"
