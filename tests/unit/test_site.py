from __future__ import annotations

import re

from dx_productivity.report.site import line_chart, render


def test_line_chart_breaks_the_line_at_suppressed_points_and_shows_n() -> None:
    svg = line_chart(
        "p50",
        {
            "apache/kafka": [
                ("2026-09-07", 10.0, 9),
                ("2026-09-14", None, 2),
                ("2026-09-21", 30.0, 7),
            ]
        },
        unit="hours",
    )
    # The suppressed week is a gap: two isolated points, no line drawn across the gap.
    assert svg.count("<circle") == 2
    assert "<polyline" not in svg
    assert "apache/kafka 2026-09-07: 10.0 hours (n = 9)" in svg
    assert "2026-09-14:" not in svg
    # The x axis still spans every week, suppressed or not.
    assert "2026-09-21" in svg
    # The n strip under the axis prints every week's sample size, the suppressed week's too.
    assert re.findall(r"<text class=n [^>]*>([\d,]+)</text>", svg) == ["9", "2", "7"]
    assert 'viewBox="0 0 520 218"' in svg  # 200 + one 14 px row + 4 px padding


def test_line_chart_draws_one_line_per_unbroken_run() -> None:
    points = [("w1", 1.0), ("w2", 2.0), ("w3", None), ("w4", 4.0), ("w5", 5.0)]
    svg = line_chart("t", {"apache/flink": points}, unit="h")
    assert svg.count("<polyline") == 2
    assert "(n =" not in svg  # two-element points carry no sample size
    assert "class=n" not in svg and 'viewBox="0 0 520 200"' in svg


def test_line_chart_with_nothing_to_plot() -> None:
    assert "no data yet" in line_chart("t", {"a": [("w1", None, 1)]}, unit="h")


def _week(repo: str, week: str, merged: int, reviewed: int, approved: int, fetched: int) -> dict:
    return {
        "repo": repo,
        "week": week,
        "prs_merged": merged,
        "p50_hours_to_merge": 10.0 + merged,
        "p90_hours_to_merge": 50.0 + merged,
        "prs_with_review_data": fetched,
        "prs_reviewed": reviewed,
        "p50_hours_to_first_review": 2.0,
        "prs_approved_before_merge": approved,
        "p50_hours_approval_to_merge": 1.0,
        "avg_review_rounds": 1.5,
        "sample_threshold": 5,
        "is_small_merge_sample": merged < 5,
        "is_small_review_sample": reviewed < 5,
    }


def test_weekly_cycle_time_flags_each_figure_by_its_own_sample_size() -> None:
    keys = (
        "coverage",
        "gh_coverage",
        "dora_12m",
        "dora_summary",
        "lead_times",
        "pr_lead_times",
        "restore",
        "merge",
        "flow_weekly",
        "aging",
        "wip_now",
        "reviewers",
        "reviewer_share",
        "linkage",
        "ci",
        "releases_recent",
        "snapshots",
        "dq",
    )
    empty: dict[str, list] = {k: [] for k in keys}
    weeks = [
        _week("apache/kafka", "2026-09-07", merged=9, reviewed=8, approved=3, fetched=9),
        _week("apache/kafka", "2026-09-14", merged=2, reviewed=2, approved=2, fetched=2),
    ]
    page = render({**empty, "pr_weekly": weeks}, generated="g", as_of="a", mode="live")
    table = page[page.index("Weekly PR cycle time") :]
    flagged = re.findall(r"<span class=low>([^<]+)<abbr class=flag title=\"([^\"]+)\"", table)
    # Week 1: only approval->merge is a small sample (3 approved PRs). Week 2: every figure.
    assert flagged[0] == ("1.0", "small sample: n = 3, below 5")
    assert [value for value, _ in flagged[1:]] == ["12.0", "52.0", "2.0", "1.0", "1.50"]
    # The chart plots the 9-PR week only; the 2-PR week shows up in the n strip, not as a mark.
    chart = page[page.index("PR time to merge, weekly median") : page.index("Weekly PR cycle time")]
    assert chart.count("<circle") == 1 and "(n = 9)" in chart
    assert re.findall(r"<text class=n [^>]*>(\d+)</text>", chart) == ["9", "2"]
