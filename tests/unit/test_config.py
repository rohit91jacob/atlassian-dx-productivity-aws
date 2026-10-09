from __future__ import annotations

from datetime import UTC, datetime

import pytest

from dx_productivity.config import ConfigError, Settings, parse_as_of, parse_projects, parse_repos


def test_defaults_cover_three_apache_projects() -> None:
    s = Settings.from_env({})
    assert s.jira_projects == ("KAFKA", "SPARK", "FLINK")
    assert [r.full_name for r in s.github_repos] == ["apache/kafka", "apache/spark", "apache/flink"]
    assert s.github_repos[0].default_branch == "trunk"
    assert s.backend == "local"
    assert s.github_token is None


def test_repo_spec_parsing_and_slug() -> None:
    (repo,) = parse_repos("apache/kafka:trunk:KAFKA")
    assert repo.slug == "apache__kafka"
    assert repo.jira_project == "KAFKA"


@pytest.mark.parametrize(
    "spec", ["apache/kafka", "apache/kafka:trunk:kafka", "nope:trunk:KAFKA", ""]
)
def test_bad_repo_specs_are_rejected(spec: str) -> None:
    with pytest.raises(ConfigError):
        parse_repos(spec)


def test_projects_are_uppercased_and_validated() -> None:
    assert parse_projects("kafka, spark") == ("KAFKA", "SPARK")
    with pytest.raises(ConfigError):
        parse_projects("KAFKA,bad key")


def test_as_of_is_utc() -> None:
    assert parse_as_of("2026-10-09T12:00:00") == datetime(2026, 10, 9, 12, tzinfo=UTC)
    assert parse_as_of("2026-10-09T14:00:00+02:00") == datetime(2026, 10, 9, 12, tzinfo=UTC)
    with pytest.raises(ConfigError):
        parse_as_of("yesterday")


def test_invalid_values_fail_fast() -> None:
    with pytest.raises(ConfigError):
        Settings.from_env({"DX_BACKEND": "gcp"})
    with pytest.raises(ConfigError):
        Settings.from_env({"DX_GITHUB_MAX_REQUESTS": "0"})


def test_lookback_can_be_set_per_source() -> None:
    s = Settings.from_env({"DX_LOOKBACK_DAYS": "30", "DX_GITHUB_LOOKBACK_DAYS": "3"})
    assert (s.jira_lookback_days, s.github_lookback_days) == (30, 3)


def test_token_is_not_in_repr() -> None:
    s = Settings.from_env({"GITHUB_TOKEN": "ghp_secretvalue"})
    assert s.github_token == "ghp_secretvalue"
    assert "ghp_secretvalue" not in repr(s)
