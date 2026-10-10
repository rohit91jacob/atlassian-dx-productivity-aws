# Verification against published figures

`dx verify` compares this pipeline's outputs with figures the projects and platforms publish
independently, fetching those figures live (sources in
[`verification/published_figures.json`](../verification/published_figures.json)). The run
below was made on **10 October 2026 at 14:26 UTC**, right after an incremental refresh (14:20 to
14:25 UTC) of the warehouse built from the real download described in the README (Jira: 90-day
window, 5,504 issues; GitHub: 30-day window, authenticated, 2,260 PRs, with reviews fetched for
every PR and check runs for every closed one). Mismatches are reported as found; none were tuned
away.

Summary: **107 matches, 14 mismatches, 1 missing, 1 skipped** across five checks.

| Check | What it compares | Result |
|---|---|---|
| 1a. `release_dates_jira` | `fct_releases.release_date` vs the live Jira versions API | 44 of 44 match |
| 1b. `release_dates_published` | the same Jira dates vs the projects' announcement pages | 30 match, 14 differ, 1 missing |
| 2. `release_notes` | pipeline fix-version membership vs official Kafka release notes | 7 of 7 comparable releases match (47 of 47 issues); 1 skipped, nothing comparable |
| 3. `jira_throughput` | weekly resolved issues vs Jira's server-side count | 12 of 12 project-weeks match |
| 4. `github_merged` | daily merged PRs vs GitHub search | 14 of 14 repository-days match |

Every check of pipeline output (1a, 2, 3, 4) matches. The 14 mismatches and the 1 missing
release are all in 1b, a comparison of two *sources* that record different events; they are
explained one by one below and do not go away by changing the pipeline.

## How each comparison is scoped

Each check compares a pipeline output with an independent figure for the same population. Where
a published figure describes a different event or population, the check is split or scoped until
both sides measure the same thing:

* **Release dates (1a, 1b).** The pipeline reports Jira's `releaseDate`, the day a release
  manager marks the version released in Jira. The projects' pages record the announcement, a
  different step of an ASF release. 1a tests the pipeline like-for-like against the Jira versions
  API; 1b compares the same dates with the announcement pages and reports every difference as a
  mismatch, with the evidence for each.
* **Release contents (2).** Release notes are generated once, at release time, while Jira fix
  versions keep changing. On 10 October 2026 Jira listed 8 issues with fix version Kafka 4.1.0
  that its notes do not, each tagged on or after the release day (Jira changelog), and 1 issue in
  the notes no longer carried it; for 4.3.0 the totals agreed (226 and 226) although 2 issues in
  the notes had lost the fix version since and 2 others that the notes do not list carried it.
  Jira's live count against the notes measures those later edits, not the pipeline, so check 2
  compares the pipeline's own fix-version membership on the release-note issues the pipeline can
  see (section 2).
* **GitHub merges (4).** PRs merged before the extracted GitHub window are in the data only if
  something touched them later, so a day is compared only when it lies wholly inside the window.

## 1a. Release dates: pipeline vs Jira (like-for-like)

`marts.fct_releases.release_date` against `releaseDate` from
`/rest/api/2/project/{key}/versions`, fetched live, for every core (x.y.z) version released in
Jira since 1 January 2025.

| Project | Versions | Same date |
|---|---|---|
| Kafka | 13 | 13 |
| Spark | 14 | 14 |
| Flink | 17 | 17 |
| **Total** | **44** | **44** |

This tests extraction (versions snapshot), curation and dbt end to end: no release is lost,
added or re-dated.

## 1b. Release dates: Jira vs the official release pages (source agreement)

The same Jira dates against the [Kafka downloads page](https://kafka.apache.org/community/downloads/),
the [Spark news page](https://spark.apache.org/news/index.html) and the
[Flink downloads page](https://flink.apache.org/downloads/), for core releases since 1 January
2025.

| Project | Versions compared | Same date | Different date | Missing in Jira |
|---|---|---|---|---|
| Kafka | 13 | 12 | 1 | 0 |
| Spark | 15 | 13 | 1 | 1 |
| Flink | 17 | 5 | 12 | 0 |
| **Total** | **45** | **30** | **14** | **1** |

Each difference, with the evidence for what each source records. "Blog post" is the date in
the file name of the release announcement in apache/flink-web (`docs/content/posts/`). "Archive"
is the last-modified time of the release's directory on archive.apache.org; it is only
indicative, because later uploads change it and the archive rewrote many directories on
1 May 2026 (those are marked "rewritten").

| Version | Jira (pipeline) | Page | Difference | Blog post | Archive |
|---|---|---|---|---|---|
| Kafka 4.1.1 | 2025-11-14 | 2025-11-12 | +2 days | | rewritten; its files (release notes, artifacts) 2025-11-12 20:52 |
| Spark 3.5.7 | 2025-09-23 | 2025-09-24 | -1 day | | 2025-09-23 19:56 |
| Flink 1.19.2 | 2025-02-11 | 2025-02-12 | -1 day | 2025-02-12 | 2025-02-11 15:26 |
| Flink 1.20.1 | 2025-02-11 | 2025-02-12 | -1 day | 2025-02-12 | 2025-02-11 15:28 |
| Flink 1.20.3 | 2025-09-25 | 2025-10-09 | -14 days | 2025-10-09 | rewritten |
| Flink 1.20.4 | 2026-05-04 | 2026-04-22 | +12 days | 2026-04-22 | 2026-05-04 16:32 (a later upload) |
| Flink 1.20.5 | 2026-06-11 | 2026-06-03 | +8 days | 2026-06-08 | 2026-06-11 11:57 |
| Flink 2.0.0 | 2025-03-18 | 2025-03-24 | -6 days | 2025-03-24 | 2025-03-18 09:35 |
| Flink 2.0.1 | 2025-10-30 | 2025-11-10 | -11 days | 2025-11-10 | rewritten |
| Flink 2.1.1 | 2025-10-30 | 2025-11-10 | -11 days | 2025-11-10 | rewritten |
| Flink 2.0.2 | 2026-05-09 | 2026-05-11 | -2 days | 2026-05-11 | 2026-05-09 18:28 |
| Flink 2.1.3 | 2026-06-11 | 2026-06-14 | -3 days | 2026-06-14 | 2026-06-11 11:57 |
| Flink 2.2.1 | 2026-05-12 | 2026-05-15 | -3 days | 2026-05-15 | 2026-05-11 16:30 |
| Flink 2.3.0 | 2026-06-22 | 2026-06-25 | -3 days | 2026-06-25 | 2026-06-22 17:14 |
| Spark 3.5.9 | not released in Jira | 2026-07-16 | missing | | 2026-07-16 09:00 |

Reading:

* **Flink's page dates are announcement dates.** They come from `release_archive.yml` in
  apache/flink-web and equal the blog post date for 11 of the 12 (1.20.5 is the exception: page
  3 June, post 8 June). Flink's Jira dates fall on the archive directory's date in 7 of the 8
  cases where that date is usable, i.e. around when the artifacts were published, before the
  announcement; for 1.20.4 and 1.20.5 the release manager marked Jira after announcing. Which
  step of the release the Jira click follows varies by release manager; it is not recorded
  anywhere, so the gap cannot be corrected per release.
* **Kafka and Spark mark the Jira version on the announcement day** for 26 of their 28
  releases. Kafka 4.1.1 was marked two days after its announcement and archive upload; Spark
  3.5.7 on the day its artifacts reached the archive, one day before the announcement.
* **Spark 3.5.9 is announced but not released in Jira.** For a missing version the check looks
  the version up in Jira live: 3.5.9 is `released: false`, although the release was announced
  on 16 July 2026 and is on the ASF archive. `fct_releases` contains released versions only.
* **Consequence for deployment frequency** (`mart_dora_monthly`): monthly counts for Kafka and
  Spark are right except that Spark's July 2026 is one short (3.5.9) until the Jira version is
  marked released. For Flink a release near a month boundary can land in the neighbouring month:
  2.0.1 and 2.1.1 count in October 2025, the announcement was 10 November; 1.20.4 counts in May
  2026, announced 22 April. The pipeline reports Jira's date and documents this, rather than
  mixing sources.

## 2. Release contents: pipeline vs official release notes

The issues the pipeline holds with a given fix version (`staging.stg_jira__issue_versions`)
against the `KAFKA-*` keys in the official `RELEASE_NOTES.html` shipped with each release on the
[ASF archive](https://archive.apache.org/dist/kafka/).

Like-for-like scope: the pipeline only holds issues updated since its first Jira watermark, so
the comparable published population is the release-note issues whose *current* Jira `updated`
lies inside the extracted window (12 July 04:16 to 10 October 11:01 UTC for Kafka, rounded
inwards to Jira's minute precision) and that still carry the fix version. An issue last touched
inside the window was extracted in its current state, so every one of them must be in the
pipeline with that fix version. Jira is used only to find that scope (one JQL query per
release); the figure compared is the pipeline's.

| Release | Notes list | Comparable (in window, still tagged) | In the pipeline | Status | Pipeline-only (tagged after the notes) |
|---|---|---|---|---|---|
| Kafka 4.1.0 | 473 | 5 | 5 | match | 1 |
| Kafka 4.1.1 | 18 | 0 | 0 | skipped: nothing comparable | 0 |
| Kafka 4.1.2 | 30 | 1 | 1 | match | 0 |
| Kafka 4.2.0 | 359 | 4 | 4 | match | 2 |
| Kafka 4.2.1 | 27 | 2 | 2 | match | 0 |
| Kafka 4.2.2 | 33 | 24 | 24 | match | 0 |
| Kafka 4.3.0 | 226 | 9 | 9 | match | 2 |
| Kafka 4.3.1 | 11 | 2 | 2 | match | 1 |
| **Total** | **1,177** | **47** | **47** | | **6** |

Reading: no comparable issue is missing from the pipeline. The comparable share is small for
old releases (an issue fixed for 4.1.0 is rarely touched a year later) and large for the
release made inside the window (24 of 33 for 4.2.2, released 29 September 2026). Issues the
pipeline holds but the notes do not list are Jira-side edits after the notes were generated
(see "How each comparison is scoped"); they are outside the comparison by construction.

## 3. Jira throughput: our weekly counts vs Jira's own count

`marts.mart_issue_flow_weekly.throughput` (issues resolved in the ISO week and currently in a
Done status category) against Jira's server-side `total` for
`project = X AND statusCategory = Done AND resolved >= <Monday> AND resolved < <next Monday>`,
for the last four complete weeks.

| Project | Week of 2026-09-07 | Week of 2026-09-14 | Week of 2026-09-21 | Week of 2026-09-28 |
|---|---|---|---|---|
| Kafka | 33 / 33 | 17 / 17 | 30 / 30 | 22 / 22 |
| Spark | 143 / 143 | 135 / 135 | 134 / 134 | 128 / 128 |
| Flink | 50 / 50 | 84 / 84 | 57 / 57 | 91 / 91 |

(ours / Jira) 12 of 12 match. This is the end-to-end check of the incremental Jira extractor
(keyset paging on `updated`, overlap, de-duplication, latest-version merge) across a 90-day
backfill and three incremental runs: no issue was lost or double-counted.

## 4. GitHub merges: our daily counts vs GitHub search

PRs merged per UTC day according to `marts.fct_pull_requests` (`merge_source = 'github_merge'`)
against GitHub search's `total_count` for
`repo:<repo> is:pr is:merged merged:<day>T00:00:00Z..<day>T23:59:59Z`, for each of the last 7
complete days. A day is compared only when it lies wholly inside the repository's extracted
GitHub window (`intermediate.int_github_coverage`, here from 10 September 2026); other days are
reported as skipped (none were in this run). GitHub search is paced under its limits (30
requests/minute with a token, 10 without), and a rate-limited request is reported as skipped
rather than failing the run.

| Repository | 03 Oct | 04 Oct | 05 Oct | 06 Oct | 07 Oct | 08 Oct | 09 Oct |
|---|---|---|---|---|---|---|---|
| apache/kafka | 3 / 3 | 5 / 5 | 2 / 2 | 11 / 11 | 8 / 8 | 6 / 6 | 11 / 11 |
| apache/flink | 2 / 2 | 1 / 1 | 8 / 8 | 9 / 9 | 14 / 14 | 6 / 6 | 9 / 9 |

(ours / GitHub) 14 of 14 match, 95 merged PRs in all.

Apache Spark is not in this check: its PRs are closed by a merge script and GitHub never marks
them merged, so GitHub search has no comparable figure. Our Spark merges come from
default-branch commits that say `Closes #<n>`; all 384 Spark PRs merged inside the window were
identified that way (`merge_source = 'commit_reference'`).

## Reproduce

```bash
export GITHUB_TOKEN=...              # read-only, public repositories; without it, see below
DX_DATA_DIR=<dir> DX_JIRA_LOOKBACK_DAYS=90 DX_GITHUB_LOOKBACK_DAYS=30 DX_GITHUB_MAX_REQUESTS=4500 \
  uv run dx run                      # first run: about 4 minutes of Jira, about 1 hour of GitHub
DX_DATA_DIR=<dir> uv run dx verify --out verification-run.md   # live, about 70 s
DX_DATA_DIR=<dir> uv run dx verify --strict                    # exit code 4 on any mismatch
```

Without a token, run the same commands without the three overrides (the `.env.example`
defaults: 60 days of Jira, 3 days of GitHub, a 50-request GitHub budget); check 4 then compares
only the days inside that short GitHub window and paces GitHub search under the unauthenticated
limit. `refresh.yml` runs `dx verify` daily with the Actions token and publishes the result as
`verification.md` next to the results site. CI runs `dx verify --offline` (no network), and the
end-to-end test runs checks 1a, 3 and 4 against figures counted directly from the recorded API
responses.
