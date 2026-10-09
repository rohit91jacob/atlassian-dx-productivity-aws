"""``dx`` command line: extraction, curation, dbt, data quality, reporting and operations.

Exit codes: 0 ok, 1 failure, 2 usage or configuration error, 3 data-quality failure,
4 verification mismatch.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from dx_productivity import __version__
from dx_productivity.config import ConfigError, Settings
from dx_productivity.logs import configure_logging, get_logger

log = get_logger("dx_productivity.cli")

EXIT_OK, EXIT_FAILURE, EXIT_USAGE, EXIT_DQ, EXIT_MISMATCH = 0, 1, 2, 3, 4


def _print(obj: object) -> None:
    print(json.dumps(obj, indent=2, default=str))


def _sources(value: str) -> tuple[str, ...]:
    from dx_productivity.extract.runner import SOURCES

    chosen = tuple(s.strip() for s in value.split(",") if s.strip())
    bad = [s for s in chosen if s not in SOURCES]
    if bad or not chosen:
        raise argparse.ArgumentTypeError(f"sources must be a subset of {','.join(SOURCES)}")
    return chosen


# --------------------------------------------------------------------------- steps
def step_extract(settings: Settings, sources: tuple[str, ...]) -> dict:
    from dx_productivity.extract.runner import run_extract

    return run_extract(settings, sources)


def step_curate(settings: Settings, full_refresh: bool) -> dict:
    from dx_productivity.curate.pipeline import run_curate
    from dx_productivity.extract.raw import new_run_id
    from dx_productivity.extract.runner import effective_settings
    from dx_productivity.storage import build_object_store, build_state_store

    settings = effective_settings(settings)
    now = settings.now()
    result = run_curate(
        settings,
        build_object_store(settings),
        build_state_store(settings),
        new_run_id(now),
        now,
        full_refresh=full_refresh,
    )
    failed = [f"{r.severity}:{r.name} ({r.violations})" for r in result.dq if not r.passed]
    return {
        "raw_objects_new": result.raw_objects_new,
        "raw_objects_total": result.raw_objects_total,
        "tables": {
            k: {"rows": v["rows"], "written": v["written"]} for k, v in result.tables.items()
        },
        "dq_not_passed": failed,
        "seconds": result.seconds,
    }


def step_transform(
    settings: Settings, target: str | None, command: str, select: str | None
) -> dict:
    from dx_productivity.extract.runner import effective_settings
    from dx_productivity.transform.dbt import load_curated_snapshot, run_dbt

    settings = effective_settings(settings)
    loaded = {}
    if target != "athena":
        loaded = load_curated_snapshot(settings)
    extra = ["--select", select] if select else []
    run_dbt(settings, command, target=target, extra_args=extra)
    return {"curated_rows_loaded": loaded, "dbt": command, "target": target or "default"}


def step_dq_marts(settings: Settings) -> list:
    import duckdb

    from dx_productivity.extract.raw import new_run_id
    from dx_productivity.extract.runner import effective_settings
    from dx_productivity.quality import checks as dq
    from dx_productivity.storage import build_object_store

    settings = effective_settings(settings)
    # Same config as dbt-duckdb's in-process connection (DuckDB refuses mixed configs).
    con = duckdb.connect(str(settings.warehouse_db))
    try:
        results = dq.run_checks(con, dq.MART_CHECKS)
    finally:
        con.close()
    now = settings.now()
    dq.record(build_object_store(settings), results, new_run_id(now), "marts", now)
    dq.gate(results, settings.dq_fail_on_error)
    return results


def step_dq_curated(settings: Settings) -> list:
    import duckdb

    from dx_productivity.curate.iceberg import ensure_table, load_catalog, read_table
    from dx_productivity.curate.schemas import TABLES
    from dx_productivity.extract.runner import effective_settings
    from dx_productivity.quality import checks as dq

    settings = effective_settings(settings)
    catalog = load_catalog(settings)
    con = duckdb.connect()
    for spec in TABLES.values():
        con.register(
            spec.name, read_table(ensure_table(catalog, settings.iceberg_namespace, spec), spec)
        )
    results = dq.run_checks(con, dq.curated_checks(settings.now()))
    dq.gate(results, settings.dq_fail_on_error)
    return results


# --------------------------------------------------------------------------- commands
def cmd_extract(args: argparse.Namespace, settings: Settings) -> int:
    _print(step_extract(settings, args.sources))
    return EXIT_OK


def cmd_curate(args: argparse.Namespace, settings: Settings) -> int:
    _print(step_curate(settings, args.full_refresh))
    return EXIT_OK


def cmd_transform(args: argparse.Namespace, settings: Settings) -> int:
    _print(step_transform(settings, args.target, args.command, args.select))
    return EXIT_OK


def cmd_dq(args: argparse.Namespace, settings: Settings) -> int:
    from dx_productivity.quality.checks import DataQualityError

    results = []
    error: DataQualityError | None = None
    for layer, fn in (("curated", step_dq_curated), ("marts", step_dq_marts)):
        if args.layer not in (layer, "all"):
            continue
        try:
            results += fn(settings)
        except DataQualityError as exc:
            results += exc.failures
            error = exc
    for r in results:
        print(
            f"{'PASS' if r.passed else 'FAIL'}  {r.severity:<5}  {r.layer}.{r.name}  ({r.violations})"
        )
    if error:
        raise error
    return EXIT_OK


def cmd_run(args: argparse.Namespace, settings: Settings) -> int:
    summary: dict = {}
    if not args.skip_extract:
        summary["extract"] = step_extract(settings, args.sources)
    summary["curate"] = step_curate(settings, args.full_refresh)
    summary["transform"] = step_transform(settings, None, "build", None)
    summary["dq_marts"] = [
        f"{r.severity}:{r.name}" for r in step_dq_marts(settings) if not r.passed
    ]
    _print(summary)
    return EXIT_OK


def cmd_report(args: argparse.Namespace, settings: Settings) -> int:
    from dx_productivity.extract.runner import effective_settings
    from dx_productivity.report.site import build_site

    summary = build_site(effective_settings(settings), Path(args.out))
    _print({"out": args.out, **summary["headline"]})
    return EXIT_OK


def cmd_state(args: argparse.Namespace, settings: Settings) -> int:
    from dx_productivity.storage import build_state_store

    state = build_state_store(settings)
    items = state.scan(args.prefix)
    if args.reset:
        if not args.yes:
            print(f"would delete {len(items)} state items with prefix {args.prefix!r}; add --yes")
            return EXIT_USAGE
        print(json.dumps({"deleted": state.delete_prefix(args.prefix)}))
        return EXIT_OK
    for item in items:
        print(
            f"{item.pk}  v{item.version}  {item.updated_at}  {json.dumps(item.doc, sort_keys=True)}"
        )
    return EXIT_OK


def cmd_sql(args: argparse.Namespace, settings: Settings) -> int:
    import duckdb

    from dx_productivity.extract.runner import effective_settings

    con = duckdb.connect(str(effective_settings(settings).warehouse_db))
    try:
        if args.list:
            con.sql(
                "SELECT table_schema, table_name FROM information_schema.tables"
                " WHERE table_schema IN ('curated', 'marts') ORDER BY 1, 2"
            ).show(max_rows=200)
        else:
            con.sql(args.query).show(max_rows=args.max_rows, max_width=220)
    finally:
        con.close()
    return EXIT_OK


def cmd_verify(args: argparse.Namespace, settings: Settings) -> int:
    from dx_productivity.verify.published import run_verification

    report = run_verification(settings, Path(args.figures), offline=args.offline)
    _print(report["summary"])
    if args.out:
        Path(args.out).write_text(report["markdown"], encoding="utf-8")
    return EXIT_MISMATCH if report["summary"]["mismatches"] and args.strict else EXIT_OK


# --------------------------------------------------------------------------- parser
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dx", description=__doc__.splitlines()[0])
    parser.add_argument("--version", action="version", version=f"dx {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    def with_sources(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--sources",
            type=_sources,
            default=("jira", "github"),
            help="comma-separated subset of jira,github (default: both)",
        )

    p = sub.add_parser("extract", help="incremental API extraction -> raw zone")
    with_sources(p)
    p.set_defaults(fn=cmd_extract)

    p = sub.add_parser("curate", help="raw -> curated Iceberg tables (raw + curated DQ gates)")
    p.add_argument("--full-refresh", action="store_true", help="rebuild from every raw object")
    p.set_defaults(fn=cmd_curate)

    p = sub.add_parser("transform", help="snapshot curated -> DuckDB, then dbt")
    p.add_argument("--target", choices=["dev", "ci", "athena"], default=None)
    p.add_argument(
        "--command",
        default="build",
        choices=["build", "run", "test", "compile", "parse", "seed", "docs"],
    )
    p.add_argument("--select", default=None)
    p.set_defaults(fn=cmd_transform)

    p = sub.add_parser("dq", help="run data-quality checks")
    p.add_argument("--layer", choices=["curated", "marts", "all"], default="all")
    p.set_defaults(fn=cmd_dq)

    p = sub.add_parser("run", help="extract -> curate -> dbt build -> mart DQ")
    with_sources(p)
    p.add_argument("--skip-extract", action="store_true")
    p.add_argument("--full-refresh", action="store_true")
    p.set_defaults(fn=cmd_run)

    p = sub.add_parser("report", help="write the static results site")
    p.add_argument("--out", default="site")
    p.set_defaults(fn=cmd_report)

    p = sub.add_parser("state", help="show (or reset) watermarks and markers")
    p.add_argument("--prefix", default="watermark#")
    p.add_argument("--reset", action="store_true")
    p.add_argument("--yes", action="store_true")
    p.set_defaults(fn=cmd_state)

    p = sub.add_parser("sql", help="query the DuckDB warehouse")
    p.add_argument("query", nargs="?", default="SELECT 1")
    p.add_argument("--list", action="store_true")
    p.add_argument("--max-rows", type=int, default=50)
    p.set_defaults(fn=cmd_sql)

    p = sub.add_parser("verify", help="reconcile outputs with published figures")
    p.add_argument("--figures", default="verification/published_figures.json")
    p.add_argument("--out", default=None, help="write a markdown report here")
    p.add_argument("--offline", action="store_true", help="skip live Jira/GitHub count queries")
    p.add_argument("--strict", action="store_true", help="exit 4 on any mismatch")
    p.set_defaults(fn=cmd_verify)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        settings = Settings.from_env()
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    configure_logging(settings.log_level, settings.log_format)

    from dx_productivity.quality.checks import DataQualityError
    from dx_productivity.transform.dbt import DbtError, DbtTestError

    try:
        return args.fn(args, settings)
    except DataQualityError as exc:
        log.error("data-quality gate failed", extra={"failures": [f.name for f in exc.failures]})
        print(f"DATA QUALITY FAILURE: {exc}", file=sys.stderr)
        return EXIT_DQ
    except DbtTestError as exc:
        print(f"DATA QUALITY FAILURE: {exc}", file=sys.stderr)
        return EXIT_DQ
    except DbtError as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_FAILURE
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except KeyboardInterrupt:
        return EXIT_FAILURE
    except Exception:
        log.exception("command failed")
        return EXIT_FAILURE


if __name__ == "__main__":
    raise SystemExit(main())
