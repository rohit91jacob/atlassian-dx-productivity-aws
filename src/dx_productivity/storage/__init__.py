"""Storage backends: raw object store + state store, local stand-ins or AWS."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dx_productivity.storage.objects import LocalObjectStore, ObjectStore, S3ObjectStore
from dx_productivity.storage.state import DynamoStateStore, SqliteStateStore, StateStore

if TYPE_CHECKING:
    from dx_productivity.config import Settings

__all__ = ["ObjectStore", "StateStore", "build_object_store", "build_state_store"]


def build_object_store(settings: Settings) -> ObjectStore:
    if settings.backend == "aws":
        return S3ObjectStore(settings.raw_bucket, region=settings.aws_region)
    return LocalObjectStore(settings.lake_dir, settings.raw_bucket)


def build_state_store(settings: Settings) -> StateStore:
    if settings.backend == "aws":
        return DynamoStateStore(settings.state_table, region=settings.aws_region)
    return SqliteStateStore(settings.state_db)
