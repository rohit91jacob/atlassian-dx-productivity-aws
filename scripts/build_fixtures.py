"""Build the committed test fixtures from a real raw-zone download.

Usage::

    DX_DATA_DIR=~/projects/_data/atlassian-dx-productivity-aws/real uv run dx extract
    uv run python scripts/build_fixtures.py --raw ~/projects/_data/.../real/lake/dx-raw/raw \
        --out tests/fixtures/api

What it does, so the fixtures are small, coherent and safe to redistribute:

* selects a coherent subset per project/repo: PRs whose reviews were fetched, merged PRs, the
  Jira issues those PRs name, severe bugs with affected versions, issues with status history
  and issues fixed in recent releases, plus every Jira version (the release history);
* trims every record to the fields the pipeline reads (no avatars, URLs, bodies or comments);
* replaces every login/username with a stable pseudonym (``user-<8 hex>``) and drops display
  names, e-mail addresses and free-text commit bodies (only the subject line and the
  "Closes #123" style reference survive, with the contributor's login pseudonymised).

The values that remain (keys, titles, states, timestamps, versions) are unmodified real data.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

SALT = b"fixture-pseudonyms"
PER_PROJECT_ISSUES = 45
PER_REPO_PRS = 40
JIRA_KEY = re.compile(r"\b(KAFKA|SPARK|FLINK)-\d+\b")


def pseudo(login: str | None) -> str | None:
    if not login:
        return None
    return "user-" + hashlib.sha256(SALT + login.lower().encode()).hexdigest()[:8]


def envelopes(raw: Path):
    for path in sorted(raw.rglob("*.json.gz")):
        yield json.loads(gzip.decompress(path.read_bytes()))


def latest(records: dict, key: str, ts: str, item: dict) -> None:
    if key not in records or item[ts] >= records[key][ts]:
        records[key] = item


# ---------------------------------------------------------------- trimming
def trim_user(user: dict | None, field: str = "name") -> dict | None:
    if not user:
        return None
    out = {field: pseudo(user.get(field))}
    if "type" in user:
        out["type"] = user["type"]
    return out


def trim_issue(issue: dict) -> dict:
    f = issue["fields"]
    histories = []
    for h in (issue.get("changelog") or {}).get("histories", []):
        items = [
            {"field": "status", "fromString": i.get("fromString"), "toString": i.get("toString")}
            for i in h.get("items", [])
            if i.get("field") == "status"
        ]
        if items:
            histories.append(
                {
                    "id": h["id"],
                    "created": h["created"],
                    "author": trim_user(h.get("author")),
                    "items": items,
                }
            )

    def versions(vs: list | None) -> list:
        return [
            {"id": v["id"], "name": v.get("name"), "released": v.get("released")} for v in vs or []
        ]

    def named(obj: dict | None) -> dict | None:
        return {"name": obj["name"]} if obj else None

    status = f.get("status") or {}
    return {
        "id": issue.get("id"),
        "key": issue["key"],
        "fields": {
            "summary": f.get("summary"),
            "issuetype": {
                "name": (f.get("issuetype") or {}).get("name"),
                "subtask": (f.get("issuetype") or {}).get("subtask"),
            },
            "status": {
                "name": status.get("name"),
                "statusCategory": {"key": (status.get("statusCategory") or {}).get("key")},
            },
            "priority": named(f.get("priority")),
            "resolution": named(f.get("resolution")),
            "created": f.get("created"),
            "updated": f.get("updated"),
            "resolutiondate": f.get("resolutiondate"),
            "fixVersions": versions(f.get("fixVersions")),
            "versions": versions(f.get("versions")),
            "components": [{"name": c["name"]} for c in f.get("components") or []],
            "labels": f.get("labels") or [],
            "assignee": trim_user(f.get("assignee")),
            "reporter": trim_user(f.get("reporter")),
            "project": {"key": (f.get("project") or {}).get("key")},
        },
        "changelog": {"total": len(histories), "histories": histories},
    }


def trim_version(v: dict) -> dict:
    keep = ("id", "name", "released", "archived", "releaseDate", "startDate")
    return {k: v[k] for k in keep if k in v}


def trim_pr(pr: dict) -> dict:
    return {
        "number": pr["number"],
        "title": pr.get("title"),
        "state": pr.get("state"),
        "draft": pr.get("draft"),
        "user": trim_user(pr.get("user"), "login"),
        "author_association": pr.get("author_association"),
        "created_at": pr.get("created_at"),
        "updated_at": pr.get("updated_at"),
        "closed_at": pr.get("closed_at"),
        "merged_at": pr.get("merged_at"),
        "merge_commit_sha": pr.get("merge_commit_sha"),
        "head": {"sha": (pr.get("head") or {}).get("sha")},
        "base": {"ref": (pr.get("base") or {}).get("ref")},
        "labels": [{"name": label["name"]} for label in pr.get("labels") or []],
        "requested_reviewers": [
            {"login": pseudo(r.get("login"))} for r in pr.get("requested_reviewers") or []
        ],
    }


def trim_review(r: dict) -> dict:
    return {
        "id": r["id"],
        "user": trim_user(r.get("user"), "login"),
        "author_association": r.get("author_association"),
        "state": r.get("state"),
        "submitted_at": r.get("submitted_at"),
        "commit_id": r.get("commit_id"),
    }


def trim_check_runs(body: dict) -> dict:
    return {
        "total_count": body.get("total_count"),
        "check_runs": [
            {
                "id": c["id"],
                "name": c.get("name"),
                "app": {"slug": (c.get("app") or {}).get("slug")},
                "status": c.get("status"),
                "conclusion": c.get("conclusion"),
                "started_at": c.get("started_at"),
                "completed_at": c.get("completed_at"),
            }
            for c in body.get("check_runs") or []
        ],
    }


_REF_LINE = re.compile(
    r"^(Closes #\d+ from )(\S+?)/(\S+)$|^(This closes #\d+\.?)$", re.MULTILINE | re.IGNORECASE
)


def trim_commit(c: dict) -> dict:
    message = c["commit"].get("message") or ""
    lines = [message.splitlines()[0] if message else ""]
    for m in _REF_LINE.finditer(message):
        lines.append(f"{m.group(1)}{pseudo(m.group(2))}/{m.group(3)}" if m.group(1) else m.group(4))
    return {
        "sha": c["sha"],
        "commit": {
            "message": "\n\n".join(lines),
            "author": {"date": c["commit"]["author"]["date"]},
            "committer": {"date": c["commit"]["committer"]["date"]},
        },
        "author": trim_user(c.get("author"), "login"),
        "committer": trim_user(c.get("committer"), "login"),
    }


# ---------------------------------------------------------------- selection
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    issues: dict[str, dict] = {}
    versions: dict[str, list] = {}
    pulls: dict[str, dict[int, dict]] = defaultdict(dict)
    reviews: dict[str, dict[int, list]] = defaultdict(dict)
    checks: dict[str, dict[str, dict]] = defaultdict(dict)
    commits: dict[str, dict[str, dict]] = defaultdict(dict)
    recorded_at = ""
    for env in envelopes(args.raw):
        recorded_at = max(recorded_at, env["landed_at"])
        kind = (env["source"], env["entity"])
        for page in env["pages"]:
            body = page["body"]
            if kind == ("jira", "issues"):
                for i in body["issues"]:
                    if (
                        i["key"] not in issues
                        or i["fields"]["updated"] >= issues[i["key"]]["fields"]["updated"]
                    ):
                        issues[i["key"]] = i
            elif kind == ("jira", "versions"):
                versions[env["scope"]] = body
            elif kind == ("github", "pulls"):
                for pr in body:
                    latest(pulls[env["meta"]["repo"]], pr["number"], "updated_at", pr)
            elif kind == ("github", "pr_details"):
                repo = env["meta"]["repo"]
                if page["kind"] == "reviews":
                    reviews[repo].setdefault(page["pr_number"], []).extend(body)
                else:
                    checks[repo][page["head_sha"]] = body
            elif kind == ("github", "commits"):
                for c in body:
                    commits[env["meta"]["repo"]][c["sha"]] = c

    out = args.out
    (out / "jira").mkdir(parents=True, exist_ok=True)
    wanted_keys: set[str] = set()
    for repo, prs in pulls.items():
        slug = repo.replace("/", "__")
        enriched = sorted(n for n in reviews[repo])
        merged = sorted(
            (n for n, p in prs.items() if p.get("merged_at") or p.get("state") == "closed"),
            reverse=True,
        )
        open_ = sorted((n for n, p in prs.items() if p.get("state") == "open"), reverse=True)
        chosen: list[int] = []
        for n in enriched + merged[:20] + open_[:8]:
            if n not in chosen and len(chosen) < PER_REPO_PRS:
                chosen.append(n)
        # Commits that close a chosen PR, plus a few that do not (direct pushes).
        chosen_set = set(chosen)
        repo_commits = []
        for c in sorted(
            commits[repo].values(), key=lambda c: c["commit"]["committer"]["date"], reverse=True
        ):
            refs = {int(x) for x in re.findall(r"#(\d+)", c["commit"].get("message") or "")}
            if refs & chosen_set or len(repo_commits) < 5:
                repo_commits.append(trim_commit(c))
        heads = {prs[n]["head"]["sha"] for n in chosen}
        (out / "github" / slug).mkdir(parents=True, exist_ok=True)
        dump(out / "github" / slug / "pulls.json", [trim_pr(prs[n]) for n in chosen])
        dump(
            out / "github" / slug / "reviews.json",
            {
                str(n): [trim_review(r) for r in reviews[repo][n]]
                for n in chosen
                if n in reviews[repo]
            },
        )
        dump(
            out / "github" / slug / "check_runs.json",
            {sha: trim_check_runs(b) for sha, b in checks[repo].items() if sha in heads},
        )
        dump(out / "github" / slug / "commits.json", repo_commits)
        dump(out / "github" / slug / "releases.json", [])
        for n in chosen:
            wanted_keys.update(m.group(0) for m in JIRA_KEY.finditer(prs[n].get("title") or ""))

    by_project: dict[str, list[dict]] = defaultdict(list)
    for i in issues.values():
        by_project[i["key"].split("-")[0]].append(i)
    for project, items in by_project.items():
        items.sort(key=lambda i: i["fields"]["updated"], reverse=True)
        picked: dict[str, dict] = {i["key"]: i for i in items if i["key"] in wanted_keys}

        def take(pred, limit, items=items, picked=picked):
            for i in items:
                if len(picked) >= limit:
                    return
                if i["key"] not in picked and pred(i):
                    picked[i["key"]] = i

        def f(i: dict) -> dict:
            return i["fields"]

        take(
            lambda i: (
                f(i)["issuetype"]["name"] == "Bug"
                and (f(i).get("priority") or {}).get("name") in ("Blocker", "Critical")
                and f(i).get("versions")
            ),
            len(picked) + 6,
        )
        take(
            lambda i: (
                any(v.get("released") for v in f(i).get("fixVersions") or [])
                and f(i).get("resolution")
            ),
            len(picked) + 10,
        )
        take(
            lambda i: any(
                it.get("field") == "status"
                for h in (i.get("changelog") or {}).get("histories", [])
                for it in h["items"]
            ),
            len(picked) + 10,
        )
        take(lambda i: True, max(PER_PROJECT_ISSUES, len(picked)))
        dump(out / "jira" / f"issues_{project}.json", [trim_issue(i) for i in picked.values()])
    for project, vs in versions.items():
        dump(out / "jira" / f"versions_{project}.json", [trim_version(v) for v in vs])

    dump(
        out / "manifest.json",
        {
            "recorded_at": recorded_at,
            "description": "Real Apache Jira + GitHub API records, trimmed and pseudonymised by scripts/build_fixtures.py",
            "licence": "Apache License 2.0 (contributions to ASF projects); see DATA_LICENSE.md",
            "sources": [
                "https://issues.apache.org/jira/rest/api/2",
                "https://api.github.com/repos/apache/{kafka,spark,flink}",
            ],
        },
    )
    for path in sorted(out.rglob("*.json")):
        print(f"{path.stat().st_size / 1024:7.1f} KB  {path.relative_to(out)}")


def dump(path: Path, obj: Any) -> None:
    path.write_text(
        json.dumps(obj, indent=1, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
