"""AWS Lambda entry points (deployed by infra/, invoked by EventBridge Scheduler).

* ``extract_handler``: zip-packaged Lambda (standard library + the boto3 the runtime ships).
  Event ``{"sources": ["jira"]}`` or ``{"sources": ["github"]}``; one schedule per source, so
  each source gets its own invocation, time budget and watermarks. Both schedules fire together
  and the function has a reserved concurrency of 1, so the two invocations run one after the
  other. Stops cleanly ~60 s before the Lambda timeout, keeping every watermark it has
  checkpointed.
* ``curate_handler``: container-image Lambda (pyarrow + pyiceberg). Merges new raw objects into
  the Glue-catalogued Iceberg tables behind the raw and curated DQ gates.

Configuration is the same ``DX_*`` environment as the CLI. The GitHub token is read once per
container from Secrets Manager (``DX_GITHUB_TOKEN_SECRET_ARN``) and never logged.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

from dx_productivity.config import Settings
from dx_productivity.logs import configure_logging, get_logger

log = get_logger(__name__)
SAFETY_MARGIN_MS = 60_000


@lru_cache(maxsize=1)
def _github_token_from_secret(secret_arn: str) -> str | None:
    import boto3

    value = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn).get("SecretString")
    return (value or "").strip() or None


def _settings() -> Settings:
    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
    secret_arn = os.environ.get("DX_GITHUB_TOKEN_SECRET_ARN", "").strip()
    if secret_arn and not settings.github_token:
        settings = settings.with_overrides(github_token=_github_token_from_secret(secret_arn))
    return settings


def _deadline(context: Any):
    if context is None or not hasattr(context, "get_remaining_time_in_millis"):
        return lambda: False
    return lambda: context.get_remaining_time_in_millis() < SAFETY_MARGIN_MS


def extract_handler(event: dict[str, Any] | None, context: Any) -> dict[str, Any]:
    from dx_productivity.extract.runner import SOURCES, run_extract

    settings = _settings()
    sources = tuple((event or {}).get("sources") or SOURCES)
    unknown = [s for s in sources if s not in SOURCES]
    if unknown:
        raise ValueError(f"unknown sources {unknown}; expected a subset of {SOURCES}")
    summary = run_extract(settings, sources, should_stop=_deadline(context))
    log.info(
        "extract complete",
        extra={"run_id": summary["run_id"], "raw_objects": summary["raw_objects"]},
    )
    return summary


def curate_handler(event: dict[str, Any] | None, context: Any) -> dict[str, Any]:
    from dx_productivity.curate.pipeline import run_curate
    from dx_productivity.extract.raw import new_run_id
    from dx_productivity.extract.runner import effective_settings
    from dx_productivity.storage import build_object_store, build_state_store

    settings = effective_settings(_settings())
    now = settings.now()
    result = run_curate(
        settings,
        build_object_store(settings),
        build_state_store(settings),
        new_run_id(now),
        now,
        full_refresh=bool((event or {}).get("full_refresh")),
    )
    return {
        "run_id": result.run_id,
        "raw_objects_new": result.raw_objects_new,
        "tables": {k: v["rows"] for k, v in result.tables.items()},
        "written": sorted(k for k, v in result.tables.items() if v["written"]),
        "dq_not_passed": [r.name for r in result.dq if not r.passed],
    }
