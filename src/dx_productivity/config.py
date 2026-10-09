"""Environment-driven settings (12-factor). Every knob is a ``DX_*`` variable; see .env.example.

The same settings object configures the CLI, the Lambda handlers and the tests, so a Lambda in
AWS and a laptop run differ only in environment variables (``DX_BACKEND``, bucket/table names,
catalog type).
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path


class ConfigError(ValueError):
    """Raised when an environment variable holds an invalid value."""


_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}
_PROJECT_KEY = re.compile(r"^[A-Z][A-Z0-9_]+$")
_REPO = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


class _Env:
    def __init__(self, env: Mapping[str, str]):
        self._env = env

    def text(self, name: str, default: str) -> str:
        value = self._env.get(name, "").strip()
        return value or default

    def optional(self, name: str) -> str | None:
        value = self._env.get(name, "").strip()
        return value or None

    def flag(self, name: str, default: bool) -> bool:
        raw = self._env.get(name, "").strip().lower()
        if not raw:
            return default
        if raw in _TRUE:
            return True
        if raw in _FALSE:
            return False
        raise ConfigError(f"{name} must be a boolean (true/false), got {raw!r}")

    def integer(self, name: str, default: int, minimum: int | None = None) -> int:
        raw = self._env.get(name, "").strip()
        try:
            value = int(raw) if raw else default
        except ValueError as exc:
            raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc
        if minimum is not None and value < minimum:
            raise ConfigError(f"{name} must be >= {minimum}, got {value}")
        return value

    def number(self, name: str, default: float, minimum: float | None = None) -> float:
        raw = self._env.get(name, "").strip()
        try:
            value = float(raw) if raw else default
        except ValueError as exc:
            raise ConfigError(f"{name} must be a number, got {raw!r}") from exc
        if minimum is not None and value < minimum:
            raise ConfigError(f"{name} must be >= {minimum}, got {value}")
        return value

    def choice(self, name: str, default: str, choices: set[str]) -> str:
        value = self.text(name, default)
        if value not in choices:
            raise ConfigError(f"{name} must be one of {sorted(choices)}, got {value!r}")
        return value


@dataclass(frozen=True)
class RepoConfig:
    """A GitHub repository, its default branch and the Jira project its PR titles reference."""

    full_name: str
    default_branch: str
    jira_project: str

    @property
    def slug(self) -> str:
        """Filesystem/S3-safe name, e.g. ``apache__kafka``."""
        return self.full_name.replace("/", "__")


def parse_repos(spec: str) -> tuple[RepoConfig, ...]:
    """Parse ``owner/repo:branch:PROJECT,...``."""
    repos: list[RepoConfig] = []
    for item in filter(None, (part.strip() for part in spec.split(","))):
        parts = item.split(":")
        if len(parts) != 3 or not _REPO.match(parts[0]) or not _PROJECT_KEY.match(parts[2]):
            raise ConfigError(
                f"DX_GITHUB_REPOS entries must look like owner/repo:branch:PROJECT, got {item!r}"
            )
        repos.append(RepoConfig(parts[0], parts[1], parts[2]))
    if not repos:
        raise ConfigError("DX_GITHUB_REPOS must list at least one repository")
    return tuple(repos)


def parse_projects(spec: str) -> tuple[str, ...]:
    projects = tuple(p.strip().upper() for p in spec.split(",") if p.strip())
    bad = [p for p in projects if not _PROJECT_KEY.match(p)]
    if bad or not projects:
        raise ConfigError(f"DX_JIRA_PROJECTS must be comma-separated Jira keys, got {spec!r}")
    return projects


def parse_as_of(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ConfigError(f"DX_AS_OF must be an ISO-8601 timestamp, got {raw!r}") from exc
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


DEFAULT_REPOS = "apache/kafka:trunk:KAFKA,apache/spark:master:SPARK,apache/flink:master:FLINK"
USER_AGENT = "atlassian-dx-productivity-aws/0.1 (+https://github.com/rohit91jacob/atlassian-dx-productivity-aws)"


@dataclass(frozen=True)
class Settings:
    # ------------------------------------------------------------------ storage / emulation
    data_dir: Path = Path("data")
    # local = filesystem stand-in for S3 + SQLite stand-in for DynamoDB;
    # aws = boto3 (real AWS, moto in tests, LocalStack in CI via AWS_ENDPOINT_URL)
    backend: str = "local"
    raw_bucket: str = "dx-raw"
    raw_prefix: str = "raw"
    state_table: str = "dx-state"
    aws_region: str = "eu-west-1"
    # sql = pyiceberg SqlCatalog (SQLite) on a local/S3 warehouse; glue = AWS Glue Data Catalog
    iceberg_catalog: str = "sql"
    iceberg_warehouse: str | None = None
    iceberg_catalog_uri: str | None = None
    iceberg_namespace: str = "curated"
    duckdb_path: Path | None = None

    # ------------------------------------------------------------------ sources
    source: str = "live"  # live | fixtures
    fixtures_dir: Path = Path("tests/fixtures/api")
    jira_base_url: str = "https://issues.apache.org/jira"
    jira_projects: tuple[str, ...] = ("KAFKA", "SPARK", "FLINK")
    jira_page_size: int = 50
    jira_min_interval_s: float = 1.0
    jira_max_pages: int = 400
    github_api_url: str = "https://api.github.com"
    github_repos: tuple[RepoConfig, ...] = field(default_factory=lambda: parse_repos(DEFAULT_REPOS))
    github_token: str | None = field(default=None, repr=False)
    github_max_requests: int = 800
    github_min_interval_s: float = 0.25
    jira_lookback_days: int = 90
    github_lookback_days: int = 90
    overlap_minutes: int = 60
    as_of: datetime | None = None

    # ------------------------------------------------------------------ http
    http_timeout_s: float = 60.0
    http_retries: int = 5
    user_agent: str = USER_AGENT

    # ------------------------------------------------------------------ privacy / quality / logs
    pseudonym_salt: str = field(default="dx-productivity-public-salt", repr=False)
    dq_fail_on_error: bool = True
    log_level: str = "INFO"
    log_format: str = "json"

    # ------------------------------------------------------------------ derived paths
    @property
    def lake_dir(self) -> Path:
        return self.data_dir / "lake"

    @property
    def state_db(self) -> Path:
        return self.data_dir / "state" / "state.sqlite"

    @property
    def warehouse_uri(self) -> str:
        if self.iceberg_warehouse:
            return self.iceberg_warehouse
        return (self.data_dir / "curated" / "warehouse").resolve().as_uri()

    @property
    def catalog_uri(self) -> str:
        if self.iceberg_catalog_uri:
            return self.iceberg_catalog_uri
        return f"sqlite:///{(self.data_dir / 'curated' / 'catalog.sqlite').resolve().as_posix()}"

    @property
    def warehouse_db(self) -> Path:
        return self.duckdb_path or self.data_dir / "warehouse" / "dx.duckdb"

    @property
    def ops_dir(self) -> Path:
        return self.data_dir / "ops"

    def now(self) -> datetime:
        return self.as_of or datetime.now(UTC)

    def with_overrides(self, **changes: object) -> Settings:
        return replace(self, **changes)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        e = _Env(os.environ if env is None else env)
        duckdb_path = e.optional("DX_DUCKDB_PATH")
        return cls(
            data_dir=Path(e.text("DX_DATA_DIR", "data")),
            backend=e.choice("DX_BACKEND", "local", {"local", "aws"}),
            raw_bucket=e.text("DX_RAW_BUCKET", "dx-raw"),
            raw_prefix=e.text("DX_RAW_PREFIX", "raw").strip("/"),
            state_table=e.text("DX_STATE_TABLE", "dx-state"),
            aws_region=e.text("AWS_REGION", e.text("AWS_DEFAULT_REGION", "eu-west-1")),
            iceberg_catalog=e.choice("DX_ICEBERG_CATALOG", "sql", {"sql", "glue"}),
            iceberg_warehouse=e.optional("DX_ICEBERG_WAREHOUSE"),
            iceberg_catalog_uri=e.optional("DX_ICEBERG_CATALOG_URI"),
            iceberg_namespace=e.text("DX_ICEBERG_NAMESPACE", "curated"),
            duckdb_path=Path(duckdb_path) if duckdb_path else None,
            source=e.choice("DX_SOURCE", "live", {"live", "fixtures"}),
            fixtures_dir=Path(e.text("DX_FIXTURES_DIR", "tests/fixtures/api")),
            jira_base_url=e.text("DX_JIRA_BASE_URL", "https://issues.apache.org/jira").rstrip("/"),
            jira_projects=parse_projects(e.text("DX_JIRA_PROJECTS", "KAFKA,SPARK,FLINK")),
            jira_page_size=e.integer("DX_JIRA_PAGE_SIZE", 50, minimum=1),
            jira_min_interval_s=e.number("DX_JIRA_MIN_INTERVAL_SECONDS", 1.0, minimum=0),
            jira_max_pages=e.integer("DX_JIRA_MAX_PAGES", 400, minimum=1),
            github_api_url=e.text("DX_GITHUB_API_URL", "https://api.github.com").rstrip("/"),
            github_repos=parse_repos(e.text("DX_GITHUB_REPOS", DEFAULT_REPOS)),
            github_token=e.optional("GITHUB_TOKEN"),
            github_max_requests=e.integer("DX_GITHUB_MAX_REQUESTS", 800, minimum=1),
            github_min_interval_s=e.number("DX_GITHUB_MIN_INTERVAL_SECONDS", 0.25, minimum=0),
            jira_lookback_days=e.integer(
                "DX_JIRA_LOOKBACK_DAYS", e.integer("DX_LOOKBACK_DAYS", 90, 1), minimum=1
            ),
            github_lookback_days=e.integer(
                "DX_GITHUB_LOOKBACK_DAYS", e.integer("DX_LOOKBACK_DAYS", 90, 1), minimum=1
            ),
            overlap_minutes=e.integer("DX_OVERLAP_MINUTES", 60, minimum=0),
            as_of=parse_as_of(e.optional("DX_AS_OF")),
            http_timeout_s=e.number("DX_HTTP_TIMEOUT_SECONDS", 60.0, minimum=1),
            http_retries=e.integer("DX_HTTP_RETRIES", 5, minimum=0),
            user_agent=e.text("DX_HTTP_USER_AGENT", USER_AGENT),
            pseudonym_salt=e.text("DX_PSEUDONYM_SALT", "dx-productivity-public-salt"),
            dq_fail_on_error=e.flag("DX_DQ_FAIL_ON_ERROR", True),
            log_level=e.choice(
                "DX_LOG_LEVEL", "INFO", {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
            ),
            log_format=e.choice("DX_LOG_FORMAT", "json", {"json", "text"}),
        )
