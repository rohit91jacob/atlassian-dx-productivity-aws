"""Raw -> curated (Iceberg): incremental, idempotent, gated by data-quality checks.

1. List raw objects and drop the ones already processed (markers in the state store).
2. Validate every new envelope (raw gate). Any invalid envelope aborts the step before anything
   is written; the bad object stays in the raw zone for inspection.
3. Normalise to rows, then merge with the current table content: for each *parent* key the
   newest ``_version`` wins (ties keep the existing row), and a winning parent replaces all of its
   children. The merge runs in DuckDB over Arrow tables.
4. Run the curated DQ checks on the merged tables (curated gate).
5. Publish every changed table (Iceberg copy-on-write overwrite + content hash), then mark the
   raw objects processed. Publishing is per table; marking happens last, so a crash in between
   simply re-merges the same objects on the next run, which is a no-op by construction.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import duckdb
import pyarrow as pa

from dx_productivity.config import Settings
from dx_productivity.curate import iceberg
from dx_productivity.curate.normalize import PARENTS, Normalizer
from dx_productivity.curate.schemas import TABLES, TableSpec
from dx_productivity.extract.raw import decode_envelope
from dx_productivity.logs import get_logger
from dx_productivity.quality import checks as dq
from dx_productivity.storage import ObjectStore, StateStore
from dx_productivity.storage.state import processed_key

log = get_logger(__name__)


@dataclass
class CurateResult:
    run_id: str
    raw_objects_new: int = 0
    raw_objects_total: int = 0
    tables: dict[str, dict[str, Any]] = field(default_factory=dict)
    dq: list[dq.CheckResult] = field(default_factory=list)
    seconds: float = 0.0


class RawGateError(dq.DataQualityError):
    pass


def _arrow(spec: TableSpec, rows: list[dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=spec.schema) if rows else spec.schema.empty_table()


def merge(
    con: duckdb.DuckDBPyConnection,
    spec: TableSpec,
    existing: pa.Table,
    incoming: pa.Table,
    parents: list[dict[str, Any]] | None = None,
) -> pa.Table:
    """Latest-version-wins merge at the parent grain. Pure function of its inputs."""
    parent = ", ".join(spec.parent_key)
    pk = ", ".join(spec.primary_key)
    marker_schema = pa.schema(
        [spec.schema.field(c) for c in spec.parent_key]
        + [spec.schema.field("_version"), spec.schema.field("_raw_key")]
    )
    markers = pa.Table.from_pylist(parents or [], schema=marker_schema)
    con.register("existing_t", existing)
    con.register("incoming_t", incoming)
    con.register("markers_t", markers)
    try:
        sql = f"""
        WITH candidates AS (
            SELECT DISTINCT {parent}, _version, 0 AS _src, _raw_key FROM existing_t
            UNION ALL SELECT DISTINCT {parent}, _version, 1, _raw_key FROM incoming_t
            UNION ALL SELECT DISTINCT {parent}, _version, 1, _raw_key FROM markers_t
        ),
        winners AS (
            SELECT {parent}, _version, _src, _raw_key FROM candidates
            QUALIFY row_number() OVER (
                PARTITION BY {parent} ORDER BY _version DESC, _src ASC, _raw_key ASC) = 1
        ),
        rows_all AS (
            SELECT *, 0 AS _src FROM existing_t
            UNION ALL BY NAME SELECT *, 1 AS _src FROM incoming_t
        )
        SELECT r.* EXCLUDE (_src) FROM rows_all r
        JOIN winners w USING ({parent}, _version, _src, _raw_key)
        QUALIFY row_number() OVER (PARTITION BY {pk} ORDER BY r._raw_key) = 1
        ORDER BY {pk}
        """
        result = con.execute(sql).arrow()
        if isinstance(result, pa.RecordBatchReader):
            result = result.read_all()
    finally:
        for name in ("existing_t", "incoming_t", "markers_t"):
            con.unregister(name)
    return result.select(spec.schema.names).cast(spec.schema)


def run_curate(
    settings: Settings,
    store: ObjectStore,
    state: StateStore,
    run_id: str,
    now: datetime,
    *,
    full_refresh: bool = False,
) -> CurateResult:
    started = time.monotonic()
    result = CurateResult(run_id)
    keys = [k for k in store.list(f"{settings.raw_prefix}/") if k.endswith(".json.gz")]
    result.raw_objects_total = len(keys)
    done = set() if full_refresh else state.processed(processed_key(k) for k in keys)
    new_keys = [k for k in keys if processed_key(k) not in done]
    result.raw_objects_new = len(new_keys)

    # ---- raw gate + normalise
    normalizer = Normalizer(settings.pseudonym_salt, settings.jira_projects)
    rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    raw_failures: list[dq.CheckResult] = []
    for key in new_keys:
        envelope = decode_envelope(store.get(key))
        problems = dq.validate_envelope(envelope)
        if problems:
            raw_failures.append(
                dq.CheckResult(
                    "raw_envelope_valid",
                    "raw",
                    "error",
                    False,
                    len(problems),
                    [f"{key}: {p}" for p in problems[:5]],
                    "raw envelope shape",
                )
            )
            continue
        for table, table_rows in normalizer.envelope(envelope, key).items():
            rows[table].extend(table_rows)
    raw_results = raw_failures or [
        dq.CheckResult("raw_envelope_valid", "raw", "error", True, 0, [], "raw envelope shape")
    ]
    dq.record(store, raw_results, run_id, "raw", now)
    if raw_failures:
        result.dq = raw_results
        raise RawGateError(raw_failures)

    # ---- merge
    catalog = iceberg.load_catalog(settings)
    con = duckdb.connect()
    merged: dict[str, pa.Table] = {}
    handles = {}
    for spec in TABLES.values():
        table = iceberg.ensure_table(catalog, settings.iceberg_namespace, spec)
        handles[spec.name] = table
        existing = spec.schema.empty_table() if full_refresh else iceberg.read_table(table, spec)
        incoming = _arrow(spec, rows.get(spec.name, []))
        parents = rows.get(PARENTS + spec.name)
        if incoming.num_rows == 0 and not parents:
            merged[spec.name] = existing
            continue
        merged[spec.name] = merge(con, spec, existing, incoming, parents)

    # ---- curated gate
    for name, data in merged.items():
        con.register(name, data)
    result.dq = dq.run_checks(con, dq.curated_checks(now))
    dq.record(store, result.dq, run_id, "curated", now)
    dq.gate(result.dq, settings.dq_fail_on_error)

    # ---- publish, then mark processed
    for name, data in merged.items():
        result.tables[name] = iceberg.publish(handles[name], data, run_id)
    if new_keys:
        state.mark_processed((processed_key(k) for k in new_keys), {"run_id": run_id})
    result.seconds = round(time.monotonic() - started, 1)
    log.info(
        "curate finished",
        extra={
            "raw_new": result.raw_objects_new,
            "written": sorted(n for n, t in result.tables.items() if t["written"]),
            "seconds": result.seconds,
        },
    )
    return result
