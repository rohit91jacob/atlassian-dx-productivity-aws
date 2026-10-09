"""The AWS code paths: Lambda handlers, S3 raw zone, DynamoDB watermarks, Iceberg on S3.

* Locally (and in the CI ``unit`` job) the AWS APIs are emulated by moto: in-process for the
  boto3-only tests, and as a moto *server* for the Iceberg-on-S3 test, because pyarrow's S3
  filesystem talks HTTP directly and cannot be patched in-process.
* In the CI ``localstack`` job the same tests run against LocalStack: set
  ``LOCALSTACK_ENDPOINT=http://localhost:4566`` (and ``LOCALSTACK_ICEBERG_CATALOG=sql``, because the
  Glue Data Catalog is not part of LocalStack's free tier).
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from dx_productivity.lambdas import handlers
from tests.conftest import AS_OF, FIXTURES

pytestmark = pytest.mark.aws
REGION = "eu-west-1"


def create_resources(endpoint: str | None = None) -> None:
    kw = {"region_name": REGION, **({"endpoint_url": endpoint} if endpoint else {})}
    s3 = boto3.client("s3", **kw)
    for bucket in ("dx-raw", "dx-curated"):
        s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": REGION})
    boto3.client("dynamodb", **kw).create_table(
        TableName="dx-state",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )


def lambda_env(monkeypatch, tmp_path: Path, **extra: str) -> None:
    values = {
        "DX_BACKEND": "aws",
        "DX_RAW_BUCKET": "dx-raw",
        "DX_STATE_TABLE": "dx-state",
        "AWS_REGION": REGION,
        "DX_SOURCE": "fixtures",
        "DX_FIXTURES_DIR": str(FIXTURES),
        "DX_AS_OF": AS_OF.isoformat(),
        "DX_DATA_DIR": str(tmp_path / "lambda-tmp"),
        "DX_LOG_FORMAT": "text",
        "DX_LOG_LEVEL": "WARNING",
        **extra,
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)


class FakeContext:
    def __init__(self, remaining_ms: int):
        self.remaining_ms = remaining_ms

    def get_remaining_time_in_millis(self) -> int:
        return self.remaining_ms


def test_handlers_in_process(monkeypatch, tmp_path) -> None:
    with mock_aws():
        create_resources()
        lambda_env(monkeypatch, tmp_path)
        jira = handlers.extract_handler({"sources": ["jira"]}, FakeContext(900_000))
        gh = handlers.extract_handler({"sources": ["github"]}, FakeContext(900_000))
        assert jira["raw_objects"] > 0 and gh["raw_objects"] > 0

        keys = boto3.client("s3", region_name=REGION).list_objects_v2(
            Bucket="dx-raw", Prefix="raw/"
        )
        assert keys["KeyCount"] == jira["raw_objects"] + gh["raw_objects"]
        item = boto3.client("dynamodb", region_name=REGION).get_item(
            TableName="dx-state", Key={"pk": {"S": "watermark#jira#KAFKA#issues"}}
        )
        assert "Item" in item

        first = handlers.curate_handler({}, None)
        assert first["raw_objects_new"] == keys["KeyCount"]
        assert first["tables"]["jira_issues"] > 0
        again = handlers.curate_handler({}, None)
        assert again["raw_objects_new"] == 0 and again["written"] == []


def test_extract_handler_stops_before_the_lambda_timeout(monkeypatch, tmp_path) -> None:
    with mock_aws():
        create_resources()
        lambda_env(monkeypatch, tmp_path)
        summary = handlers.extract_handler({"sources": ["jira"]}, FakeContext(30_000))
        assert {s["stopped"] for s in summary["jira"]} == {"deadline"}


def test_unknown_source_is_rejected(monkeypatch, tmp_path) -> None:
    lambda_env(monkeypatch, tmp_path)
    with pytest.raises(ValueError, match="unknown sources"):
        handlers.extract_handler({"sources": ["confluence"]}, None)


def test_github_token_comes_from_secrets_manager(monkeypatch, tmp_path) -> None:
    with mock_aws():
        arn = boto3.client("secretsmanager", region_name=REGION).create_secret(
            Name="dx/github-token", SecretString="ghp_fromsecret"
        )["ARN"]
        lambda_env(monkeypatch, tmp_path, DX_GITHUB_TOKEN_SECRET_ARN=arn)
        handlers._github_token_from_secret.cache_clear()
        assert handlers._settings().github_token == "ghp_fromsecret"


def test_extractor_lambda_needs_no_third_party_wheels() -> None:
    """The extract Lambda ships as a zip of this package only: importing it must not pull in
    pyarrow, pyiceberg, duckdb or dbt."""
    code = (
        "import sys; import dx_productivity.lambdas.handlers as h; import dx_productivity.extract.runner;"
        "heavy = [m for m in ('pyarrow', 'pyiceberg', 'duckdb', 'dbt') if m in sys.modules];"
        "print(','.join(heavy)); sys.exit(1 if heavy else 0)"
    )
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stdout + proc.stderr


# ----------------------------------------------------------------------------- server mode
@pytest.fixture
def aws_endpoint(monkeypatch) -> Iterator[tuple[str, str]]:
    external = os.environ.get("LOCALSTACK_ENDPOINT")
    catalog = os.environ.get("LOCALSTACK_ICEBERG_CATALOG", "glue")
    if external:
        yield external, catalog
        return
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    try:
        yield f"http://{host}:{port}", catalog
    finally:
        server.stop()


def test_full_aws_mode_iceberg_on_s3(aws_endpoint, monkeypatch, tmp_path) -> None:
    endpoint, catalog = aws_endpoint
    monkeypatch.setenv("AWS_ENDPOINT_URL", endpoint)
    create_resources(endpoint)
    extra = {"DX_ICEBERG_CATALOG": catalog, "DX_ICEBERG_WAREHOUSE": "s3://dx-curated/warehouse"}
    if catalog == "sql":
        extra["DX_ICEBERG_CATALOG_URI"] = f"sqlite:///{(tmp_path / 'catalog.sqlite').as_posix()}"
    lambda_env(monkeypatch, tmp_path, **extra)

    handlers.extract_handler({"sources": ["jira", "github"]}, None)
    result = handlers.curate_handler({}, None)
    assert result["tables"]["gh_pull_requests"] > 0

    s3 = boto3.client("s3", region_name=REGION, endpoint_url=endpoint)
    data_files = [
        o["Key"]
        for o in s3.list_objects_v2(Bucket="dx-curated", Prefix="warehouse/").get("Contents", [])
    ]
    assert any(k.endswith(".parquet") for k in data_files)
    assert any(k.endswith(".metadata.json") for k in data_files)
    if catalog == "glue":
        tables = boto3.client("glue", region_name=REGION, endpoint_url=endpoint).get_tables(
            DatabaseName="curated"
        )
        assert {t["Name"] for t in tables["TableList"]} >= {"jira_issues", "gh_pull_requests"}
    assert handlers.curate_handler({}, None)["written"] == []
