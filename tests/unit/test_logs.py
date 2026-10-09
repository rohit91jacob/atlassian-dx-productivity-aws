"""Structured-log context must never collide with LogRecord's own attributes.

``logging`` raises ``KeyError("Attempt to overwrite 'args' in LogRecord")`` for such keys, but
only when the record is actually created, i.e. at INFO level and above. The test suite runs with
WARNING, so a collision in an ``log.info(..., extra=...)`` call would otherwise go unnoticed.
"""

from __future__ import annotations

import ast
import logging
from pathlib import Path

from dx_productivity.logs import _RESERVED, TextFormatter

SRC = Path(__file__).parents[2] / "src" / "dx_productivity"


def _extra_keys() -> list[tuple[str, int, str]]:
    found = []
    for path in SRC.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            for kw in node.keywords:
                if kw.arg == "extra" and isinstance(kw.value, ast.Dict):
                    for key in kw.value.keys:
                        if isinstance(key, ast.Constant) and isinstance(key.value, str):
                            found.append((path.name, node.lineno, key.value))
    return found


def test_log_extra_keys_do_not_overwrite_logrecord_attributes() -> None:
    keys = _extra_keys()
    assert keys, "expected structured log calls in the source tree"
    clashes = [k for k in keys if k[2] in _RESERVED]
    assert clashes == []


def test_info_record_with_extra_is_formatted() -> None:
    record = logging.getLogger("dx.test").makeRecord(
        "dx.test", logging.INFO, __file__, 1, "dbt", (), None, extra={"dbt_args": ["build"]}
    )
    assert '"dbt_args": ["build"]' in TextFormatter().format(record)
