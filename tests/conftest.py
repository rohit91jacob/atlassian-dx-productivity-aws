from __future__ import annotations

import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dx_productivity.config import Settings

FIXTURES = Path(__file__).parent / "fixtures" / "api"
AS_OF = datetime(2026, 10, 9, 16, 25, 16, tzinfo=UTC)  # manifest.json recorded_at


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests never see the developer's DX_* / AWS settings or tokens."""
    for key in list(os.environ):
        if key.startswith(("DX_", "AWS_")) or key in {"GITHUB_TOKEN", "DBT_TARGET"}:
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")


@pytest.fixture
def fixtures_copy(tmp_path: Path) -> Path:
    """A writable copy of the recorded API fixtures (for tests that simulate new activity)."""
    dest = tmp_path / "api"
    shutil.copytree(FIXTURES, dest)
    return dest


@pytest.fixture
def fixture_settings(tmp_path: Path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        source="fixtures",
        fixtures_dir=FIXTURES,
        as_of=AS_OF,
        log_format="text",
    )
