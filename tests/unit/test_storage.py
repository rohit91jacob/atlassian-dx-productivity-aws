"""The local stand-ins and the AWS implementations (under moto) honour the same contract."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import boto3
import pytest
from moto import mock_aws

from dx_productivity.extract.watermark import Watermark, parse_ts
from dx_productivity.storage.objects import LocalObjectStore, S3ObjectStore
from dx_productivity.storage.state import ConflictError, DynamoStateStore, SqliteStateStore


@pytest.fixture(params=["local", "s3"])
def object_store(request, tmp_path) -> Iterator:
    if request.param == "local":
        yield LocalObjectStore(tmp_path, "dx-raw")
        return
    with mock_aws():
        s3 = boto3.client("s3", region_name="eu-west-1")
        s3.create_bucket(
            Bucket="dx-raw", CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
        )
        yield S3ObjectStore("dx-raw", client=s3)


@pytest.fixture(params=["sqlite", "dynamodb"])
def state_store(request, tmp_path) -> Iterator:
    if request.param == "sqlite":
        yield SqliteStateStore(tmp_path / "state.sqlite")
        return
    with mock_aws():
        ddb = boto3.client("dynamodb", region_name="eu-west-1")
        ddb.create_table(
            TableName="dx-state",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield DynamoStateStore("dx-state", client=ddb)


def test_object_store_round_trip(object_store) -> None:
    object_store.put("raw/jira/issues/scope=KAFKA/dt=2026-10-09/a.json.gz", b"one")
    object_store.put("raw/jira/issues/scope=KAFKA/dt=2026-10-09/b.json.gz", b"two")
    object_store.put("ops/dq_results/x.json", b"{}")
    assert object_store.list("raw/") == [
        "raw/jira/issues/scope=KAFKA/dt=2026-10-09/a.json.gz",
        "raw/jira/issues/scope=KAFKA/dt=2026-10-09/b.json.gz",
    ]
    assert object_store.get("raw/jira/issues/scope=KAFKA/dt=2026-10-09/b.json.gz") == b"two"
    assert object_store.exists("ops/dq_results/x.json")
    assert not object_store.exists("ops/missing.json")


def test_local_store_rejects_path_traversal(tmp_path) -> None:
    with pytest.raises(ValueError):
        LocalObjectStore(tmp_path, "b").put("../escape", b"x")


def test_conditional_writes(state_store) -> None:
    first = state_store.put(
        "watermark#jira#KAFKA#issues", {"watermark": "a"}, expected_version=None
    )
    assert first.version == 1
    with pytest.raises(ConflictError):  # someone else already created it
        state_store.put("watermark#jira#KAFKA#issues", {"watermark": "b"}, expected_version=None)
    second = state_store.put("watermark#jira#KAFKA#issues", {"watermark": "b"}, expected_version=1)
    assert second.version == 2
    with pytest.raises(ConflictError):  # stale writer
        state_store.put("watermark#jira#KAFKA#issues", {"watermark": "c"}, expected_version=1)
    assert state_store.get("watermark#jira#KAFKA#issues").doc == {"watermark": "b"}


def test_processed_markers_and_scan(state_store) -> None:
    keys = [f"processed#raw/{i:03d}" for i in range(130)]  # > one DynamoDB batch
    state_store.mark_processed(keys[:120], {"run_id": "r1"})
    assert state_store.processed(keys) == set(keys[:120])
    state_store.put("watermark#x", {"watermark": "2026-01-01T00:00:00Z"}, None)
    assert [i.pk for i in state_store.scan("watermark#")] == ["watermark#x"]
    assert state_store.delete_prefix("processed#") == 120
    assert state_store.processed(keys) == set()


def test_watermark_never_moves_backwards(state_store) -> None:
    default = datetime(2026, 7, 1, tzinfo=UTC)
    wm = Watermark(state_store, "watermark#t", default)
    assert wm.load() == default
    assert wm.start_with_overlap(timedelta(hours=1)) == default  # no overlap before the first run
    assert wm.advance(datetime(2026, 10, 1, tzinfo=UTC), "r1")
    assert not wm.advance(datetime(2026, 9, 1, tzinfo=UTC), "r2")
    again = Watermark(state_store, "watermark#t", default)
    assert again.load() == datetime(2026, 10, 1, tzinfo=UTC)
    assert again.start_with_overlap(timedelta(hours=1)) == datetime(2026, 9, 30, 23, tzinfo=UTC)


def test_two_runs_racing_on_a_watermark(state_store) -> None:
    default = datetime(2026, 7, 1, tzinfo=UTC)
    a = Watermark(state_store, "watermark#race", default)
    b = Watermark(state_store, "watermark#race", default)
    a.load()
    b.load()
    a.advance(datetime(2026, 10, 1, tzinfo=UTC), "a")
    with pytest.raises(ConflictError):
        b.advance(datetime(2026, 10, 2, tzinfo=UTC), "b")


@pytest.mark.parametrize(
    ("raw", "iso"),
    [
        ("2026-10-08T14:04:49.000+0000", "2026-10-08T14:04:49+00:00"),
        ("2026-10-08T16:04:49.000+0200", "2026-10-08T14:04:49+00:00"),
        ("2026-10-08T14:04:49Z", "2026-10-08T14:04:49+00:00"),
    ],
)
def test_parse_ts_handles_jira_and_github_formats(raw: str, iso: str) -> None:
    assert parse_ts(raw).isoformat() == iso
