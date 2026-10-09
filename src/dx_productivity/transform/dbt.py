"""Run the dbt project programmatically (dbt-core's ``dbtRunner``).

``dx transform`` points the DuckDB profile at this run's Iceberg catalog and warehouse through
``DX_*`` environment variables and passes the run's as-of instant as a dbt var, so ages, WIP and
"is this release old enough to judge" are reproducible for a given run.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC
from pathlib import Path

from dx_productivity.config import Settings
from dx_productivity.logs import get_logger

log = get_logger(__name__)

DBT_DIR = Path(__file__).resolve().parents[3] / "dbt"


class DbtError(RuntimeError):
    pass


class DbtTestError(DbtError):
    """dbt ran, but data tests failed: a data-quality gate (CLI exit code 3)."""


def load_curated_snapshot(settings: Settings) -> dict[str, int]:
    """Copy the current Iceberg snapshot of every curated table into DuckDB ``curated.*``.

    One connection, one transaction: dbt then sees a consistent cut of the curated zone. The
    snapshot ids are recorded in ``curated._snapshots`` for lineage.
    """
    import duckdb

    from dx_productivity.curate.iceberg import ensure_table, load_catalog, read_table
    from dx_productivity.curate.schemas import TABLES

    catalog = load_catalog(settings)
    settings.warehouse_db.parent.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    con = duckdb.connect(str(settings.warehouse_db))
    try:
        con.execute("BEGIN")
        con.execute("CREATE SCHEMA IF NOT EXISTS curated")
        con.execute(
            "CREATE OR REPLACE TABLE curated._snapshots (table_name VARCHAR, snapshot_id BIGINT,"
            " rows BIGINT, loaded_at TIMESTAMP)"
        )
        for spec in TABLES.values():
            table = ensure_table(catalog, settings.iceberg_namespace, spec)
            data = read_table(table, spec)
            con.execute(f"CREATE OR REPLACE TABLE curated.{spec.name} AS SELECT * FROM data")
            snapshot = table.current_snapshot()
            con.execute(
                "INSERT INTO curated._snapshots VALUES (?, ?, ?, now()::TIMESTAMP)",
                [spec.name, snapshot.snapshot_id if snapshot else None, data.num_rows],
            )
            counts[spec.name] = data.num_rows
        con.execute("COMMIT")
    finally:
        con.close()
    return counts


def dbt_env(settings: Settings) -> dict[str, str]:
    settings.warehouse_db.parent.mkdir(parents=True, exist_ok=True)
    return {
        "DX_DUCKDB_PATH": str(settings.warehouse_db.resolve()),
        "DX_ICEBERG_CATALOG_URI": settings.catalog_uri,
        "DX_ICEBERG_WAREHOUSE": settings.warehouse_uri,
    }


@contextmanager
def _environ(values: dict[str, str]) -> Iterator[None]:
    previous = {k: os.environ.get(k) for k in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def run_dbt(
    settings: Settings,
    command: str = "build",
    *,
    target: str | None = None,
    extra_args: list[str] | None = None,
    project_dir: Path = DBT_DIR,
) -> None:
    from dbt.cli.main import dbtRunner

    as_of = settings.now().astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
    args = [
        *({"docs": ["docs", "generate"]}.get(command, [command])),
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(project_dir),
        "--vars",
        json.dumps({"as_of": as_of}),
    ]
    if target:
        args += ["--target", target]
    args += extra_args or []
    with _environ(dbt_env(settings)):
        log.info("dbt", extra={"dbt_args": args})
        result = dbtRunner().invoke(args)
    if result.success:
        return
    statuses = [
        str(getattr(r, "status", "")) for r in getattr(result.result, "results", None) or []
    ]
    failed_tests = sum(1 for s in statuses if s.endswith("fail"))
    errors = sum(1 for s in statuses if s.endswith("error"))
    if failed_tests and not errors and result.exception is None:
        raise DbtTestError(f"dbt {command}: {failed_tests} data test(s) failed")
    raise DbtError(f"dbt {command} failed: {result.exception or f'{errors} node error(s)'}")
