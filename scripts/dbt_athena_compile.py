"""Compile the dbt project for the Athena target without an AWS account.

dbt-athena opens a session (STS GetCallerIdentity, Glue lookups) even for ``dbt compile``. This
script points it at a throwaway moto server instead of AWS, so CI proves that every model renders
valid Athena/Trino SQL through the real adapter macros, with no credentials and no cloud calls.
The compiled SQL lands in dbt/target/compiled/ (uploaded as a CI artifact).
"""

from __future__ import annotations

import os
import sys

import boto3
from moto.server import ThreadedMotoServer

from dx_productivity.cli import main as dx

REGION = "eu-west-1"


def run() -> int:
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    try:
        os.environ.update(
            {
                "AWS_ENDPOINT_URL": endpoint,
                "AWS_ACCESS_KEY_ID": "moto",
                "AWS_SECRET_ACCESS_KEY": "moto",
                "AWS_REGION": REGION,
                "AWS_DEFAULT_REGION": REGION,
                "DX_ATHENA_STAGING_DIR": "s3://dx-athena-results/staging/",
                "DX_ATHENA_DATA_DIR": "s3://dx-curated/dbt/",
            }
        )
        s3 = boto3.client("s3", region_name=REGION, endpoint_url=endpoint)
        for bucket in ("dx-athena-results", "dx-curated"):
            s3.create_bucket(
                Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": REGION}
            )
        boto3.client("athena", region_name=REGION, endpoint_url=endpoint).create_work_group(
            Name="dx-analytics", Configuration={}
        )
        boto3.client("glue", region_name=REGION, endpoint_url=endpoint).create_database(
            DatabaseInput={"Name": "dx_curated"}
        )
        return dx(["transform", "--target", "athena", "--command", "compile", *sys.argv[1:]])
    finally:
        server.stop()


if __name__ == "__main__":
    raise SystemExit(run())
