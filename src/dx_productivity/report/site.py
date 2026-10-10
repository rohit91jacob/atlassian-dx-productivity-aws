"""Static results site built from the dbt marts (``dx report --out site``).

Plain HTML, one stylesheet and inline SVG charts; no JavaScript and no external assets, so it
can be served from GitHub Pages or opened from disk. Every chart has a table view next to it and
native SVG ``<title>`` tooltips on each mark.

Files written:
* ``index.html``: DORA keys per project, PR cycle time, Jira flow, reviewer load, PR->Jira
  linkage, CI health, data coverage, data-quality status and the metric definitions.
* ``summary.json``: machine-readable headline numbers and freshness metadata.
"""

from __future__ import annotations

import html
import json
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from dx_productivity.config import Settings

REPO_URL = "https://github.com/rohit91jacob/atlassian-dx-productivity-aws"
JIRA_URL = "https://issues.apache.org/jira/"
Row = dict[str, Any]

# Categorical slots 1-3 of the validated reference palette (light / dark); fixed per project so
# a project keeps its colour on every chart.
PROJECT_COLOURS = {"KAFKA": 1, "SPARK": 2, "FLINK": 3}

STYLE = """
:root { color-scheme: light dark; --surface: #fcfcfb; --ink: #0b0b0b; --ink-2: #52514e;
  --ink-3: #7a7974; --line: #e3e2de; --accent: #2a78d6;
  --s1: #2a78d6; --s2: #eb6834; --s3: #1baf7a; --s4: #eda100; }
@media (prefers-color-scheme: dark) {
  :root { --surface: #1a1a19; --ink: #ffffff; --ink-2: #c3c2b7; --ink-3: #9a998f; --line: #3a3a37;
    --accent: #3987e5; --s1: #3987e5; --s2: #d95926; --s3: #199e70; --s4: #c98500; }
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--surface); color: var(--ink);
  font: 15px/1.5 system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; }
header, main, footer { max-width: 1120px; margin: 0 auto; padding: 0 1rem; }
header { padding-top: 1.5rem; }
h1 { font-size: 1.7rem; margin: .2rem 0; }
h2 { font-size: 1.25rem; margin-top: 2.2rem; border-bottom: 1px solid var(--line); padding-bottom: .3rem; }
h3 { font-size: 1.02rem; margin: 1.2rem 0 .4rem; }
a { color: var(--accent); }
.meta, .note, caption { color: var(--ink-2); font-size: .88rem; }
.lede { font-size: 1.02rem; max-width: 70ch; }
.badge { display: inline-block; border: 1px solid var(--line); border-radius: 4px; padding: 0 .4rem;
  font-size: .8rem; color: var(--ink-2); }
.tiles { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: .75rem; }
.tile { border: 1px solid var(--line); border-radius: 6px; padding: .7rem .8rem; }
.tile .k { color: var(--ink-2); font-size: .8rem; }
.tile .v { font-size: 1.45rem; font-weight: 600; font-variant-numeric: tabular-nums; }
.tile .s { color: var(--ink-3); font-size: .78rem; }
.swatch { display: inline-block; width: .7rem; height: .7rem; border-radius: 2px; margin-right: .35rem;
  vertical-align: baseline; }
.table-wrap { overflow-x: auto; margin: .4rem 0 1rem; }
table { border-collapse: collapse; font-size: .86rem; min-width: 50%; }
caption { text-align: left; padding: .2rem 0 .4rem; }
th, td { padding: .28rem .6rem; border-bottom: 1px solid var(--line); text-align: left; white-space: nowrap; }
th { font-weight: 600; color: var(--ink-2); }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
.empty { color: var(--ink-3); font-style: italic; }
.low { color: var(--ink-3); }
abbr.flag { text-decoration: none; cursor: help; }
figure { margin: .6rem 0 1rem; max-width: 760px; }
figcaption { color: var(--ink-2); font-size: .85rem; margin-bottom: .3rem; }
svg text { fill: var(--ink-2); font-size: 11px; }
svg text.n { font-size: 10px; font-variant-numeric: tabular-nums; }
svg .grid { stroke: var(--line); stroke-width: 1; }
svg .axis { stroke: var(--ink-3); stroke-width: 1; }
.legend { font-size: .82rem; color: var(--ink-2); margin: .2rem 0; }
.legend span { margin-right: 1rem; }
.pass { color: var(--ink); } .fail { font-weight: 600; }
dl dt { font-weight: 600; margin-top: .6rem; } dl dd { margin: 0 0 0 1rem; color: var(--ink-2); }
footer { border-top: 1px solid var(--line); margin-top: 3rem; padding: 1rem; color: var(--ink-2);
  font-size: .85rem; }
"""


# --------------------------------------------------------------------------- formatting
def _esc(value: object) -> str:
    return "&ndash;" if value is None else html.escape(str(value))


def _num(value: object, places: int = 1) -> str:
    if value is None:
        return "&ndash;"
    return f"{float(value):,.{places}f}"


def _int(value: object) -> str:
    return "&ndash;" if value is None else f"{int(value):,}"


def _pct(value: object) -> str:
    return "&ndash;" if value is None else f"{float(value) * 100:.0f}%"


def _swatch(project: str | None) -> str:
    slot = PROJECT_COLOURS.get(project or "", 4)
    return f'<span class=swatch style="background:var(--s{slot})" aria-hidden=true></span>'


@dataclass(frozen=True)
class Column:
    header: str
    render: Callable[[Row], str]
    numeric: bool = False


def _table(caption: str, columns: Sequence[Column], rows: Iterable[Row], empty: str) -> str:
    rows = list(rows)
    if not rows:
        return f'<p class="empty">{html.escape(empty)}</p>'
    head = "".join(
        f"<th scope=col{' class=num' if c.numeric else ''}>{html.escape(c.header)}</th>"
        for c in columns
    )
    body = "".join(
        "<tr>"
        + "".join(f"<td{' class=num' if c.numeric else ''}>{c.render(r)}</td>" for c in columns)
        + "</tr>"
        for r in rows
    )
    return (
        f"<div class=table-wrap><table><caption>{html.escape(caption)}</caption>"
        f"<thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"
    )


def _col(
    header: str, key: str, fmt: Callable[[object], str] = _esc, numeric: bool = False
) -> Column:
    return Column(header, lambda r: fmt(r.get(key)), numeric)


# --------------------------------------------------------------------------- charts
Point = tuple[str, float | None] | tuple[str, float | None, int | None]


def line_chart(
    title: str,
    series: dict[str, list[Point]],
    *,
    unit: str,
    width: int = 520,
    height: int = 200,
) -> str:
    """Multi-series line chart (one per project/repo) with a shared x axis of ISO dates.

    A point is ``(x, value)`` or ``(x, value, n)``. ``n`` is the sample size behind the value: it
    is printed in a strip under the x axis (one row per series, including points not plotted)
    and in the mark's tooltip. A ``None`` value is a gap: the line is broken there rather than
    drawn across it.
    """
    xs = sorted({p[0] for points in series.values() for p in points})
    values = [p[1] for points in series.values() for p in points if p[1] is not None]
    if not xs or not values:
        return f'<p class="empty">{html.escape(title)}: no data yet.</p>'
    sizes = {
        name: {p[0]: p[2] for p in points if len(p) > 2 and p[2] is not None}
        for name, points in series.items()
    }
    strip = [name for name in series if sizes[name]]
    left, right, top, bottom = 44, 12, 10, 26
    plot_w, plot_h = width - left - right, height - top - bottom
    vmax = max(values) * 1.1 or 1.0
    step = plot_w / max(1, len(xs) - 1)
    x_of = {x: left + i * step for i, x in enumerate(xs)}
    total_h = height + (14 * len(strip) + 4 if strip else 0)

    def y_of(v: float) -> float:
        return top + plot_h - (v / vmax) * plot_h

    parts = [
        f'<svg viewBox="0 0 {width} {total_h}" width="100%" role=img aria-label="{html.escape(title)}">'
    ]
    for row, name in enumerate(strip):
        y = height + 14 * (row + 1)
        slot = PROJECT_COLOURS.get(name.split("/")[-1].upper(), 4)
        # Swatch and "n" sit at the far left so they clear the first week's value, which is
        # centred on the y axis.
        parts.append(
            f'<rect x=2 y={y - 8} width=8 height=8 rx=1 fill="var(--s{slot})"/>'
            f"<text x=13 y={y} text-anchor=start><title>{html.escape(name)}: sample size</title>n</text>"
        )
        parts.extend(
            f'<text class=n x="{x_of[x]:.1f}" y={y} text-anchor=middle>{int(n):,}</text>'
            for x, n in sizes[name].items()
        )
    for frac in (0, 0.5, 1):
        y = top + plot_h - frac * plot_h
        parts.append(f'<line class=grid x1={left} x2={width - right} y1="{y:.1f}" y2="{y:.1f}"/>')
        parts.append(
            f'<text x={left - 6} y="{y + 4:.1f}" text-anchor=end>{vmax * frac:,.0f}</text>'
        )
    for i, x in enumerate(xs):
        if i in (0, len(xs) - 1) or (len(xs) > 6 and i == len(xs) // 2):
            anchor = "start" if i == 0 else "end" if i == len(xs) - 1 else "middle"
            parts.append(
                f'<text x="{x_of[x]:.1f}" y={height - 8} text-anchor={anchor}>{html.escape(x)}</text>'
            )
    for name, points in series.items():
        slot = PROJECT_COLOURS.get(name.split("/")[-1].upper(), 4)
        segments: list[list[str]] = [[]]
        for x, v, *_ in points:
            if v is None:
                if segments[-1]:
                    segments.append([])
                continue
            segments[-1].append(f"{x_of[x]:.1f},{y_of(v):.1f}")
        for segment in segments:
            if len(segment) > 1:
                parts.append(
                    f'<polyline fill=none stroke="var(--s{slot})" stroke-width=2 stroke-linejoin=round points="{" ".join(segment)}"/>'
                )
        for x, v, *rest in points:
            if v is None:
                continue
            n = f" (n = {int(rest[0]):,})" if rest and rest[0] is not None else ""
            parts.append(
                f'<circle cx="{x_of[x]:.1f}" cy="{y_of(v):.1f}" r=4 fill="var(--s{slot})" stroke="var(--surface)" stroke-width=2>'
                f"<title>{html.escape(name)} {html.escape(x)}: {v:,.1f} {html.escape(unit)}{n}</title></circle>"
            )
    parts.append("</svg>")
    legend = (
        "<p class=legend>"
        + "".join(
            f"<span>{_swatch(name.split('/')[-1].upper())}{html.escape(name)}</span>"
            for name in series
        )
        + "</p>"
    )
    return f"<figure><figcaption>{html.escape(title)}</figcaption>{legend}{''.join(parts)}</figure>"


# --------------------------------------------------------------------------- data
class Warehouse:
    def __init__(self, path: Path):
        self.con = duckdb.connect(str(path))

    def rows(self, sql: str, params: Sequence[Any] = ()) -> list[Row]:
        cur = self.con.execute(sql, list(params))
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r, strict=True)) for r in cur.fetchall()]

    def one(self, sql: str, params: Sequence[Any] = ()) -> Row:
        rows = self.rows(sql, params)
        return rows[0] if rows else {}

    def close(self) -> None:
        self.con.close()


def _iso(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M UTC")
    return str(value)


def _dq_status(settings: Settings) -> list[Row]:
    """Latest recorded result per layer from the ops zone."""
    from dx_productivity.storage import build_object_store

    store = build_object_store(settings)
    keys = store.list("ops/dq_results/")
    latest: dict[str, str] = {}
    for key in keys:
        layer = key.rsplit("-", 1)[-1].removesuffix(".json")
        latest[layer] = key  # keys sort by date then run id, so the last one wins
    rows: list[Row] = []
    for layer in ("raw", "curated", "marts"):
        if layer not in latest:
            continue
        payload = json.loads(store.get(latest[layer]))
        results = payload["results"]
        rows.append(
            {
                "layer": layer,
                "checked_at": payload["checked_at"][:16].replace("T", " "),
                "checks": len(results),
                "passed": sum(1 for r in results if r["passed"]),
                "warnings": [
                    r["name"] for r in results if not r["passed"] and r["severity"] == "warn"
                ],
                "errors": [
                    r["name"] for r in results if not r["passed"] and r["severity"] == "error"
                ],
            }
        )
    return rows


def collect(settings: Settings) -> dict[str, Any]:
    wh = Warehouse(settings.warehouse_db)
    try:
        as_of = settings.now()
        months_from = f"{as_of.year - 1:04d}-{as_of.month:02d}-01"
        data: dict[str, Any] = {
            "coverage": wh.rows(
                "SELECT c.project_key, c.covered_from, c.covered_to,"
                " (SELECT count(*) FROM marts.fct_jira_issues i WHERE i.project_key = c.project_key) AS issues"
                " FROM intermediate.int_jira_coverage c ORDER BY 1"
            ),
            "gh_coverage": wh.rows(
                "SELECT repo, count(*) AS prs, min(f.created_at) AS first_created,"
                " min(c.covered_from) AS covered_from, max(p.updated_at) AS last_updated,"
                " sum(CASE WHEN has_review_data THEN 1 ELSE 0 END) AS with_reviews,"
                " sum(CASE WHEN is_merged THEN 1 ELSE 0 END) AS merged,"
                " sum(CASE WHEN merged_in_coverage THEN 1 ELSE 0 END) AS merged_in_coverage"
                " FROM marts.fct_pull_requests f JOIN staging.stg_github__pull_requests p USING (pr_key, repo)"
                " JOIN intermediate.int_github_coverage c USING (repo)"
                " GROUP BY 1 ORDER BY 1"
            ),
            "dora_12m": wh.rows(
                "SELECT * FROM marts.mart_dora_monthly WHERE month >= CAST(? AS DATE) ORDER BY project_key, month",
                [months_from],
            ),
            "dora_summary": wh.rows(
                "SELECT project_key,"
                " sum(deployments) AS deployments_12m, sum(core_releases) AS core_12m,"
                " sum(feature_releases) AS feature_12m, sum(patch_releases) AS patch_12m,"
                " sum(evaluable_releases) AS evaluable, sum(failed_releases) AS failed"
                " FROM marts.mart_dora_monthly WHERE month >= CAST(? AS DATE) GROUP BY 1 ORDER BY 1",
                [months_from],
            ),
            "lead_times": wh.rows(
                "SELECT i.project_key, count(*) AS n,"
                " quantile_cont(i.lead_time_to_release_days, 0.5) AS p50_days"
                " FROM marts.fct_jira_issues i JOIN intermediate.int_jira_coverage c USING (project_key)"
                " WHERE i.lead_time_to_release_days IS NOT NULL AND i.first_fix_release_date >= c.covered_from_date"
                " GROUP BY 1 ORDER BY 1"
            ),
            "pr_lead_times": wh.rows(
                "SELECT jira_project AS project_key, count(*) AS n,"
                " quantile_cont(lead_time_for_change_days, 0.5) AS p50_days"
                " FROM marts.fct_pull_requests WHERE lead_time_for_change_days IS NOT NULL GROUP BY 1 ORDER BY 1"
            ),
            "restore": wh.rows(
                "SELECT i.project_key, count(*) AS n, quantile_cont(i.lead_time_hours, 0.5) AS p50_hours"
                " FROM marts.fct_jira_issues i JOIN intermediate.int_jira_coverage c USING (project_key)"
                " WHERE i.is_bug AND i.is_severe AND i.is_fixed AND i.lead_time_hours IS NOT NULL"
                " AND i.resolved_at >= c.covered_from GROUP BY 1 ORDER BY 1"
            ),
            "merge": wh.rows(
                "SELECT repo, jira_project AS project_key, count(*) AS n,"
                " quantile_cont(hours_to_merge, 0.5) AS p50_hours, quantile_cont(hours_to_merge, 0.9) AS p90_hours,"
                " sum(CASE WHEN merge_source = 'commit_reference' THEN 1 ELSE 0 END) AS by_commit_ref,"
                " sum(CASE WHEN has_review_data THEN 1 ELSE 0 END) AS with_reviews,"
                " quantile_cont(hours_to_first_review, 0.5) AS p50_first_review_hours,"
                " avg(review_rounds) AS avg_rounds,"
                " avg(CASE WHEN has_review_data THEN CASE WHEN hours_to_first_review <= 24 THEN 1.0 ELSE 0.0 END END) AS within_24h"
                " FROM marts.fct_pull_requests WHERE is_merged AND merged_in_coverage AND NOT author_is_bot"
                " GROUP BY 1, 2 ORDER BY 1"
            ),
            "pr_weekly": wh.rows(
                "SELECT * FROM marts.mart_pr_cycle_time_weekly ORDER BY repo, week"
            ),
            "flow_weekly": wh.rows(
                "SELECT * FROM marts.mart_issue_flow_weekly ORDER BY project_key, week"
            ),
            "aging": wh.rows(
                "SELECT project_key, age_bucket, bucket_order, sum(issues) AS issues"
                " FROM marts.mart_issue_aging GROUP BY ALL ORDER BY project_key, bucket_order"
            ),
            "wip_now": wh.rows(
                "SELECT project_key, flow_state, count(*) AS issues, quantile_cont(wip_age_days, 0.5) AS p50_wip_age_days"
                " FROM marts.fct_jira_issues WHERE flow_state IN ('in_progress', 'in_review') GROUP BY 1, 2 ORDER BY 1, 2"
            ),
            "reviewers": wh.rows(
                "SELECT repo, count(DISTINCT reviewer_id) AS reviewers, sum(reviews) AS reviews,"
                " max(reviews) AS max_weekly_reviews FROM marts.mart_reviewer_load_weekly GROUP BY 1 ORDER BY 1"
            ),
            "reviewer_share": wh.rows(
                "WITH t AS (SELECT repo, reviewer_id, sum(reviews) AS r FROM marts.mart_reviewer_load_weekly GROUP BY 1, 2),"
                " ranked AS (SELECT *, row_number() OVER (PARTITION BY repo ORDER BY r DESC, reviewer_id) AS k FROM t)"
                " SELECT repo, sum(r) AS reviews, max(r) * 1.0 / sum(r) AS top_share,"
                " sum(CASE WHEN k <= 3 THEN r ELSE 0 END) * 1.0 / sum(r) AS top3_share,"
                " quantile_cont(r, 0.5) AS p50_per_reviewer FROM ranked GROUP BY 1 ORDER BY 1"
            ),
            "linkage": wh.rows(
                "SELECT repo, sum(prs_merged) AS merged, sum(prs_linked) AS linked,"
                " sum(prs_linked_resolvable) AS resolvable, sum(prs_released) AS released"
                " FROM marts.mart_pr_jira_linkage GROUP BY 1 ORDER BY 1"
            ),
            "ci": wh.rows(
                "SELECT repo, count(*) AS prs, sum(check_runs) AS runs,"
                " avg(CASE WHEN ci_failed THEN 1.0 ELSE 0.0 END) AS failed_share,"
                " quantile_cont(ci_wall_minutes, 0.5) AS p50_minutes"
                " FROM marts.fct_pull_requests WHERE check_runs > 0 GROUP BY 1 ORDER BY 1"
            ),
            "releases_recent": wh.rows(
                "SELECT project_key, version_name, release_type, release_date, days_since_previous_release,"
                " issues_fixed_in_data, escaped_bugs, escaped_severe_bugs_in_window, is_evaluable, is_failed_proxy"
                " FROM marts.fct_releases ORDER BY release_date DESC, project_key LIMIT 15"
            ),
            "snapshots": wh.rows("SELECT * FROM curated._snapshots ORDER BY table_name"),
        }
    finally:
        wh.close()
    data["dq"] = _dq_status(settings)
    return data


# --------------------------------------------------------------------------- page
DEFINITIONS = [
    (
        "Deployment frequency",
        "Released Jira versions (a project's releases) per month. Open-source projects ship versioned releases, not continuous deployments; a release is the deployable unit here.",
    ),
    (
        "Lead time for changes",
        "Two proxies. Jira-side: issue reported to the end of the release day of its first released fix version, for releases inside the extracted window. PR-side: PR opened to the release that shipped it, via the Jira key in the PR title and that issue's fix version (needs PRs old enough to have shipped).",
    ),
    (
        "Merge lead time",
        "PR opened to merged. 'Merged' is GitHub's merged_at, or, when a project closes PRs from a merge script (Apache Spark), the default-branch commit that names the PR (\"Closes #123\"). Only merges inside the GitHub coverage window count: a PR merged earlier is in the data only if it was touched again later.",
    ),
    (
        "Change failure rate (proxy)",
        "Share of evaluable releases with at least one Blocker/Critical Bug that lists the release as an affected version and was reported within 30 days. Evaluable = the 30-day window has passed and lies inside the extracted history.",
    ),
    (
        "Time to restore (proxy)",
        "Median time from report to resolution of Blocker/Critical Bugs resolved as Fixed.",
    ),
    (
        "PR cycle-time breakdown",
        "Created to first human review, first review to first approval, approval to merge. Bots and the author's own reviews are excluded. A review round is a distinct PR revision (head commit) that received a review.",
    ),
    (
        "Issue flow",
        "Lead time = created to resolved; cycle time = first move to In Progress / Patch Available to resolved. WIP at week end is reconstructed from changelog transitions. Throughput = issues resolved per week.",
    ),
    (
        "Reviewer load",
        "Reviews per pseudonymised reviewer. People are never named: identities are HMAC pseudonyms.",
    ),
]


def _page(body: str, *, generated: str, as_of: str, mode: str) -> str:
    return (
        "<!doctype html><html lang=en><head><meta charset=utf-8>"
        '<meta name=viewport content="width=device-width, initial-scale=1">'
        "<title>Developer experience &amp; delivery: Apache Kafka, Spark, Flink</title>"
        '<link rel=stylesheet href="style.css"></head><body>'
        '<header><p class=meta><a href="' + REPO_URL + '">atlassian-dx-productivity-aws</a></p>'
        "<h1>Developer experience &amp; delivery analytics</h1>"
        f"<p class=meta>Apache Kafka, Spark and Flink · Jira + GitHub · evaluated as of "
        f"<time>{html.escape(as_of)}</time> · generated <time>{html.escape(generated)}</time> · "
        f"<span class=badge>source: {html.escape(mode)}</span></p></header>"
        f"<main>{body}</main>"
        '<footer><p>Issue data from the <a href="'
        + JIRA_URL
        + '">Apache Software Foundation Jira</a>; '
        "pull requests, reviews, commits and check runs from the GitHub REST API for the apache/kafka, "
        "apache/spark and apache/flink repositories. Contributions to ASF projects are licensed under the "
        "Apache License 2.0; see DATA_LICENSE.md. People are pseudonymised. Not affiliated with the ASF, "
        "Atlassian or GitHub. "
        f'Built by <a href="{REPO_URL}">atlassian-dx-productivity-aws</a>.</p></footer></body></html>'
    )


def _tile(label: str, value: str, sub: str, project: str | None = None) -> str:
    return (
        f"<div class=tile><div class=k>{_swatch(project) if project else ''}{html.escape(label)}</div>"
        f"<div class=v>{value}</div><div class=s>{sub}</div></div>"
    )


def render(data: dict[str, Any], *, generated: str, as_of: str, mode: str) -> str:
    by = lambda rows, key="project_key": {r[key]: r for r in rows}  # noqa: E731
    dora, lt, prlt, restore = (
        by(data["dora_summary"]),
        by(data["lead_times"]),
        by(data["pr_lead_times"]),
        by(data["restore"]),
    )
    merge = by(data["merge"], "project_key")
    projects = sorted({r["project_key"] for r in data["coverage"]})

    tiles = []
    for p in projects:
        d, merge_p = dora.get(p, {}), merge.get(p, {})
        cfr = None if not d.get("evaluable") else d["failed"] / d["evaluable"]
        tiles.append(
            "<h3>"
            + _swatch(p)
            + html.escape(p)
            + "</h3><div class=tiles>"
            + _tile(
                "Releases, last 12 months",
                _int(d.get("deployments_12m")),
                f"core {_int(d.get('core_12m'))} ({_int(d.get('feature_12m'))} feature, "
                f"{_int(d.get('patch_12m'))} patch); the rest are sub-projects",
            )
            + _tile(
                "Lead time to release (Jira), p50",
                f"{_num(lt.get(p, {}).get('p50_days'))} d",
                f"n = {_int(lt.get(p, {}).get('n'))} issues in releases inside the window",
            )
            + _tile(
                "PR merge lead time, p50",
                f"{_num(merge_p.get('p50_hours'))} h",
                f"n = {_int(merge_p.get('n'))} merged PRs · PR&rarr;release p50 "
                f"{_num(prlt.get(p, {}).get('p50_days'))} d (n = {_int(prlt.get(p, {}).get('n'))})",
            )
            + _tile(
                "Change failure rate (proxy)",
                _pct(cfr),
                f"{_int(d.get('failed'))} of {_int(d.get('evaluable'))} evaluable releases",
            )
            + _tile(
                "Time to restore (proxy), p50",
                f"{_num(restore.get(p, {}).get('p50_hours'))} h",
                f"n = {_int(restore.get(p, {}).get('n'))} Blocker/Critical bugs fixed",
            )
            + "</div>"
        )

    def series(rows: list[Row], key: str, value: str, xfmt: Callable[[Any], str] = str) -> dict:
        out: dict[str, list] = {}
        for r in rows:
            out.setdefault(r[key], []).append((xfmt(r["week"]), r[value]))
        return out

    complete_weeks = [r for r in data["flow_weekly"] if not r["is_partial_week"]]
    flow_chart = line_chart(
        "Issues resolved per week (throughput), complete weeks",
        series(complete_weeks, "project_key", "throughput"),
        unit="issues",
    )
    wip_chart = line_chart(
        "Work in progress at week end (In Progress + Patch Available)",
        series(complete_weeks, "project_key", "wip_end_of_week"),
        unit="issues",
    )
    min_n = max((int(r["sample_threshold"]) for r in data["pr_weekly"]), default=0)
    merge_points: dict[str, list[Point]] = {}
    for r in data["pr_weekly"]:
        value = None if r["is_small_merge_sample"] else r["p50_hours_to_merge"]
        merge_points.setdefault(r["repo"], []).append((str(r["week"]), value, r["prs_merged"]))
    merge_title = "PR time to merge, weekly median (hours)"
    if min_n:
        merge_title += f". n = merged PRs in the week; a week with n below {min_n} is not plotted"
    merge_chart = line_chart(merge_title, merge_points, unit="hours")

    def sampled(key: str, n_key: str, fmt: Callable[[object], str] = _num) -> Callable[[Row], str]:
        """A weekly figure, marked when its own sample size is below the threshold."""

        def cell(r: Row) -> str:
            if r.get(key) is None or int(r[n_key]) >= int(r["sample_threshold"]):
                return fmt(r.get(key))
            tip = f"small sample: n = {int(r[n_key])}, below {int(r['sample_threshold'])}"
            return f'<span class=low>{fmt(r[key])}<abbr class=flag title="{tip}">*</abbr></span>'

        return cell

    dora_cols = [
        Column("Project", lambda r: _swatch(r["project_key"]) + _esc(r["project_key"])),
        _col("Month", "month"),
        _col("Releases", "deployments", _int, True),
        _col("Core", "core_releases", _int, True),
        _col("PRs merged", "prs_merged", _int, True),
        _col("Merge lead p50 (h)", "merge_lead_time_p50_hours", _num, True),
        _col("Issues shipped", "issues_shipped", _int, True),
        _col("Issue→release p50 (d)", "issue_lead_time_to_release_p50_days", _num, True),
        _col("Evaluable", "evaluable_releases", _int, True),
        _col("Failed (proxy)", "failed_releases", _int, True),
        _col("CFR (proxy)", "change_failure_rate_proxy", _pct, True),
        _col("Severe bugs fixed", "severe_bugs_fixed", _int, True),
        _col("Restore p50 (h)", "time_to_restore_p50_hours", _num, True),
    ]
    merge_cols = [
        Column("Repository", lambda r: _swatch(r["project_key"]) + _esc(r["repo"])),
        _col("Merged PRs", "n", _int, True),
        _col("via commit ref", "by_commit_ref", _int, True),
        _col("Time to merge p50 (h)", "p50_hours", _num, True),
        _col("p90 (h)", "p90_hours", _num, True),
        _col("With review data", "with_reviews", _int, True),
        _col("First review p50 (h)", "p50_first_review_hours", _num, True),
        _col("First review ≤ 24 h", "within_24h", _pct, True),
        _col("Avg review rounds", "avg_rounds", lambda v: _num(v, 2), True),
    ]
    flow_cols = [
        Column("Project", lambda r: _swatch(r["project_key"]) + _esc(r["project_key"])),
        _col("Week", "week"),
        _col("Created", "arrivals", _int, True),
        _col("Resolved", "throughput", _int, True),
        _col("Lead p50 (d)", "p50_lead_time_days", _num, True),
        _col("Lead p85 (d)", "p85_lead_time_days", _num, True),
        _col("Cycle p50 (d)", "p50_cycle_time_days", _num, True),
        _col("Cycle p85 (d)", "p85_cycle_time_days", _num, True),
        _col("WIP at end", "wip_end_of_week", _int, True),
        Column("Partial", lambda r: "yes" if r["is_partial_week"] else ""),
    ]
    aging_projects: dict[str, dict[str, int]] = {}
    buckets = ["0-7d", "7-30d", "30-90d", "90-365d", "365d+"]
    for r in data["aging"]:
        aging_projects.setdefault(r["project_key"], {})[r["age_bucket"]] = int(r["issues"])
    aging_rows = [{"project_key": p, **v} for p, v in aging_projects.items()]
    aging_cols = [
        Column("Project", lambda r: _swatch(r["project_key"]) + _esc(r["project_key"]))
    ] + [Column(b, lambda r, b=b: _int(r.get(b, 0)), True) for b in buckets]
    dq_cols = [
        _col("Layer", "layer"),
        _col("Checked", "checked_at"),
        _col("Checks", "checks", _int, True),
        _col("Passed", "passed", _int, True),
        Column("Warnings", lambda r: _esc(", ".join(r["warnings"]) or "none")),
        Column(
            "Errors",
            lambda r: (
                f"<span class=fail>{_esc(', '.join(r['errors']))}</span>" if r["errors"] else "none"
            ),
        ),
    ]

    body = (
        "<p class=lede>Delivery and developer-experience metrics for three Apache projects, computed from "
        "their public Jira and GitHub activity. DORA keys are <strong>proxies</strong> adapted to "
        "open-source release engineering; every definition is at the bottom of the page. Nothing on this "
        "page is simulated.</p>"
        "<h2>DORA keys (proxies)</h2>"
        + "".join(tiles)
        + _table(
            "DORA keys per project and month, last 12 months",
            dora_cols,
            data["dora_12m"],
            "No months yet.",
        )
        + "<h2>Pull-request cycle time</h2>"
        + _table(
            "PRs merged inside the GitHub coverage window (bot-authored PRs excluded)",
            merge_cols,
            data["merge"],
            "No merged PRs yet.",
        )
        + merge_chart
        + _table(
            "Weekly PR cycle time. Each (n) column is the sample size of the figures to its"
            f" right; * marks a figure from fewer than {min_n} PRs, a small sample",
            [
                Column("Repository", lambda r: _esc(r["repo"])),
                _col("Week", "week"),
                _col("Merged (n)", "prs_merged", _int, True),
                Column("Merge p50 (h)", sampled("p50_hours_to_merge", "prs_merged"), True),
                Column("Merge p90 (h)", sampled("p90_hours_to_merge", "prs_merged"), True),
                _col("Reviewed (n)", "prs_reviewed", _int, True),
                Column(
                    "First review p50 (h)",
                    sampled("p50_hours_to_first_review", "prs_reviewed"),
                    True,
                ),
                _col("Approved (n)", "prs_approved_before_merge", _int, True),
                Column(
                    "Approval→merge p50 (h)",
                    sampled("p50_hours_approval_to_merge", "prs_approved_before_merge"),
                    True,
                ),
                _col("Reviews fetched (n)", "prs_with_review_data", _int, True),
                Column(
                    "Avg rounds",
                    sampled("avg_review_rounds", "prs_with_review_data", lambda v: _num(v, 2)),
                    True,
                ),
            ],
            data["pr_weekly"],
            "No merged PRs yet.",
        )
        + "<h2>Jira issue flow</h2>"
        + flow_chart
        + wip_chart
        + _table(
            "Weekly flow (weeks inside the extracted window)",
            flow_cols,
            data["flow_weekly"],
            "No weeks yet.",
        )
        + _table(
            "Open issues by age (work in progress aged from when work started)",
            aging_cols,
            aging_rows,
            "No open issues.",
        )
        + _table(
            "Work in progress now",
            [
                Column("Project", lambda r: _swatch(r["project_key"]) + _esc(r["project_key"])),
                _col("State", "flow_state"),
                _col("Issues", "issues", _int, True),
                _col("WIP age p50 (d)", "p50_wip_age_days", _num, True),
            ],
            data["wip_now"],
            "No WIP.",
        )
        + "<h2>Reviewer load</h2>"
        + _table(
            "Review concentration over the extracted window (pseudonymised reviewers)",
            [
                _col("Repository", "repo"),
                _col("Reviews", "reviews", _int, True),
                _col("Reviews per reviewer p50", "p50_per_reviewer", _num, True),
                _col("Busiest reviewer share", "top_share", _pct, True),
                _col("Top-3 share", "top3_share", _pct, True),
            ],
            data["reviewer_share"],
            "No reviews fetched yet.",
        )
        + "<h2>PR &harr; Jira linkage</h2>"
        + _table(
            "Merged PRs whose title names a Jira issue",
            [
                _col("Repository", "repo"),
                _col("Merged", "merged", _int, True),
                _col("Linked", "linked", _int, True),
                Column(
                    "Linked share",
                    lambda r: _pct(r["linked"] / r["merged"] if r["merged"] else None),
                    True,
                ),
                _col("Issue found in Jira data", "resolvable", _int, True),
                _col("Already released", "released", _int, True),
            ],
            data["linkage"],
            "No merged PRs yet.",
        )
        + "<h2>CI health</h2>"
        + _table(
            "Check runs on closed PRs' final head commit",
            [
                _col("Repository", "repo"),
                _col("PRs", "prs", _int, True),
                _col("Check runs", "runs", _int, True),
                _col("PRs with a failed run", "failed_share", _pct, True),
                _col("CI wall time p50 (min)", "p50_minutes", _num, True),
            ],
            data["ci"],
            "No check runs fetched yet.",
        )
        + "<p class=note>Apache Flink's main CI reports through an external system rather than GitHub check runs: only the few Flink PRs counted in the PRs column carry GitHub Actions runs, so its row is not comparable with Kafka's and Spark's.</p>"
        + "<h2>Recent releases</h2>"
        + _table(
            "Latest released Jira versions",
            [
                Column("Project", lambda r: _swatch(r["project_key"]) + _esc(r["project_key"])),
                _col("Version", "version_name"),
                _col("Type", "release_type"),
                _col("Released", "release_date"),
                _col("Days since previous", "days_since_previous_release", _int, True),
                _col("Fixed issues in data", "issues_fixed_in_data", _int, True),
                _col("Escaped bugs", "escaped_bugs", _int, True),
                Column(
                    "Failed (proxy)",
                    lambda r: (
                        "&ndash;"
                        if r["is_failed_proxy"] is None
                        else ("yes" if r["is_failed_proxy"] else "no")
                    ),
                ),
            ],
            data["releases_recent"],
            "No releases.",
        )
        + "<h2>Data coverage and quality</h2>"
        + _table(
            "Jira coverage (issues updated inside the extraction window)",
            [
                Column("Project", lambda r: _swatch(r["project_key"]) + _esc(r["project_key"])),
                _col("Issues", "issues", _int, True),
                Column("Covered from", lambda r: _esc(_iso(r["covered_from"]))),
                Column("Latest update", lambda r: _esc(_iso(r["covered_to"]))),
            ],
            data["coverage"],
            "No Jira data.",
        )
        + _table(
            "GitHub coverage (PRs updated inside the extraction window; merge metrics count only merges from 'Covered from' on)",
            [
                _col("Repository", "repo"),
                _col("PRs", "prs", _int, True),
                _col("Merged (any time)", "merged", _int, True),
                _col("Merged inside coverage", "merged_in_coverage", _int, True),
                _col("Reviews fetched", "with_reviews", _int, True),
                Column("Covered from", lambda r: _esc(_iso(r["covered_from"]))),
                Column("Latest PR update", lambda r: _esc(_iso(r["last_updated"]))),
            ],
            data["gh_coverage"],
            "No GitHub data.",
        )
        + _table(
            "Latest data-quality run per layer", dq_cols, data["dq"], "No DQ results recorded."
        )
        + "<h2>Definitions</h2><dl>"
        + "".join(f"<dt>{html.escape(k)}</dt><dd>{html.escape(v)}</dd>" for k, v in DEFINITIONS)
        + "</dl>"
    )
    return _page(body, generated=generated, as_of=as_of, mode=mode)


def build_site(settings: Settings, out: Path, *, now: datetime | None = None) -> dict[str, Any]:
    data = collect(settings)
    generated = (now or datetime.now(UTC)).strftime("%Y-%m-%dT%H:%M:%SZ")
    as_of = settings.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    mode = "recorded fixtures (CI)" if settings.source == "fixtures" else "live APIs"
    out.mkdir(parents=True, exist_ok=True)
    (out / "style.css").write_text(STYLE.strip() + "\n", encoding="utf-8")
    (out / ".nojekyll").write_text("", encoding="utf-8")
    (out / "index.html").write_text(
        render(data, generated=generated, as_of=as_of, mode=mode), encoding="utf-8"
    )

    headline = {
        "as_of": as_of,
        "generated_at": generated,
        "source": settings.source,
        "jira_issues": sum(int(r["issues"]) for r in data["coverage"]),
        "pull_requests": sum(int(r["prs"]) for r in data["gh_coverage"]),
        "projects": {
            r["project_key"]: {
                "releases_12m": int(r["deployments_12m"] or 0),
                "evaluable_releases": int(r["evaluable"] or 0),
                "failed_releases_proxy": int(r["failed"] or 0),
            }
            for r in data["dora_summary"]
        },
    }
    summary = {
        "headline": headline,
        "merge": data["merge"],
        "lead_times": data["lead_times"],
        "restore": data["restore"],
        "coverage": data["coverage"],
        "dq": data["dq"],
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    return summary
