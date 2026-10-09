"""Apache Iceberg curated zone via pyiceberg (no JVM).

* Local / CI: ``SqlCatalog`` (SQLite) over a filesystem warehouse, or an S3 warehouse on
  LocalStack/moto.
* AWS: ``GlueCatalog`` over the curated S3 bucket, which Athena (and dbt-athena) query directly.

Writes are copy-on-write table overwrites inside one Iceberg transaction, together with a
content hash stored as a table property. An unchanged table is never rewritten, so reruns
create no new snapshots.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table

from dx_productivity.config import Settings
from dx_productivity.curate.schemas import TABLES, TableSpec

HASH_PROPERTY = "dx.content-sha256"
RUN_PROPERTY = "dx.run-id"


def catalog_properties(settings: Settings) -> dict[str, str]:
    warehouse = settings.warehouse_uri
    props: dict[str, str] = {"warehouse": warehouse}
    if settings.iceberg_catalog == "glue":
        props.update(
            {"type": "glue", "glue.region": settings.aws_region, "s3.region": settings.aws_region}
        )
    else:
        props.update({"type": "sql", "uri": settings.catalog_uri})
    if warehouse.startswith("file:"):
        # pyarrow's local filesystem mis-parses Windows file:// URIs; fsspec handles both OSes.
        props["py-io-impl"] = "pyiceberg.io.fsspec.FsspecFileIO"
        Path(settings.data_dir / "curated").mkdir(parents=True, exist_ok=True)
    elif settings.catalog_uri.startswith("sqlite:///"):
        Path(settings.catalog_uri.removeprefix("sqlite:///")).parent.mkdir(
            parents=True, exist_ok=True
        )
    endpoint = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL")
    if endpoint and warehouse.startswith("s3:"):
        props.update({"s3.endpoint": endpoint, "s3.region": settings.aws_region})
        if os.environ.get("AWS_ACCESS_KEY_ID"):
            props["s3.access-key-id"] = os.environ["AWS_ACCESS_KEY_ID"]
            props["s3.secret-access-key"] = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    return props


def load_catalog(settings: Settings) -> Catalog:
    from pyiceberg.catalog import load_catalog as _load

    catalog = _load("dx", **catalog_properties(settings))
    catalog.create_namespace_if_not_exists(settings.iceberg_namespace)
    return catalog


def ensure_table(catalog: Catalog, namespace: str, spec: TableSpec) -> Table:
    identifier = f"{namespace}.{spec.name}"
    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        return catalog.create_table(
            identifier,
            schema=spec.schema,
            properties={"format-version": "2", "comment": spec.doc},
        )
    current = table.schema().as_arrow()
    if set(current.names) != set(spec.schema.names):
        with table.update_schema() as update:
            update.union_by_name(spec.schema)
        table = catalog.load_table(identifier)
    return table


def read_table(table: Table, spec: TableSpec) -> pa.Table:
    data = table.scan().to_arrow()
    return (
        data.select(spec.schema.names).cast(spec.schema)
        if data.num_rows
        else spec.schema.empty_table()
    )


def content_hash(data: pa.Table) -> str:
    """Hash of the business content (lineage column excluded), independent of row order."""
    cols = [c for c in data.column_names if c != "_raw_key"]
    digest = hashlib.sha256()
    digest.update(",".join(cols).encode())
    rows = sorted(repr(tuple(r[c] for c in cols)) for r in data.select(cols).to_pylist())
    for row in rows:
        digest.update(row.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def publish(table: Table, data: pa.Table, run_id: str) -> dict[str, Any]:
    digest = content_hash(data)
    if table.properties.get(HASH_PROPERTY) == digest:
        return {"written": False, "rows": data.num_rows, "sha256": digest}
    with table.transaction() as tx:
        tx.overwrite(data)
        tx.set_properties({HASH_PROPERTY: digest, RUN_PROPERTY: run_id})
    return {"written": True, "rows": data.num_rows, "sha256": digest}


def all_specs() -> list[TableSpec]:
    return list(TABLES.values())
