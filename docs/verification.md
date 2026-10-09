# Verification against published figures

`dx verify` compares this pipeline's outputs with figures the projects and platforms publish
independently, fetching those figures live (sources in
[`verification/published_figures.json`](../verification/published_figures.json)). The run
below was made on **9 October 2026 at 17:44 UTC** against the warehouse built from the real
download described in the README (Jira: 60 days, 4,229 issues; GitHub: a 3-day window,
714 PRs). Mismatches are reported as found; none were tuned away.

Summary: **39 matches, 16 mismatches, 1 missing** across four checks. The PR and issue *counts*
reconcile exactly; the mismatches are differences between the *sources themselves* (Jira
release dates vs announcement dates, release notes frozen at release time).

## 1. Release dates: Jira versions vs official release pages

`marts.fct_releases.release_date` (from each project's Jira versions, which drive deployment
frequency) against the [Kafka downloads page](https://kafka.apache.org/community/downloads/),
the [Spark news page](https://spark.apache.org/news/index.html) and the
[Flink downloads page](https://flink.apache.org/downloads/), for core releases since
1 January 2025.

| Project | Versions compared | Same date | Different date | Missing in Jira |
|---|---|---|---|---|
| Kafka | 13 | 12 | 1 | 0 |
| Spark | 15 | 13 | 1 | 1 |
| Flink | 17 | 5 | 12 | 0 |
| **Total** | **45** | **30** | **14** | **1** |

The differences:

| Version | Jira | Published | Difference |
|---|---|---|---|
| Kafka 4.1.1 | 2025-11-14 | 2025-11-12 | +2 days |
| Spark 3.5.7 | 2025-09-23 | 2025-09-24 | -1 day |
| Spark 3.5.9 | not released in Jira | 2026-07-16 | missing |
| Flink 1.19.2, 1.20.1 | 2025-02-11 | 2025-02-12 | -1 day |
| Flink 1.20.3 | 2025-09-25 | 2025-10-09 | -14 days |
| Flink 1.20.4 | 2026-05-04 | 2026-04-22 | +12 days |
| Flink 1.20.5 | 2026-06-11 | 2026-06-03 | +8 days |
| Flink 2.0.0 | 2025-03-18 | 2025-03-24 | -6 days |
| Flink 2.0.1, 2.1.1 | 2025-10-30 | 2025-11-10 | -11 days |
| Flink 2.0.2 | 2026-05-09 | 2026-05-11 | -2 days |
| Flink 2.1.3 | 2026-06-11 | 2026-06-14 | -3 days |
| Flink 2.2.1 | 2026-05-12 | 2026-05-15 | -3 days |
| Flink 2.3.0 | 2026-06-22 | 2026-06-25 | -3 days |

Reading: Kafka and Spark mark the Jira version released on the announcement day (26 of 28
agree). Flink's Jira dates are mostly earlier than the downloads page, by 1 to 14 days, and in
three patch releases later. A likely cause (not verified) is that the Jira version is marked
released at a different step of the release process (for example when the vote passes) than the
announcement the downloads page records. Spark 3.5.9 is announced but its Jira version was not
marked released when this ran, so it is absent from deployment frequency. **Consequence:**
monthly deployment counts are right for Kafka and Spark; for Flink a release near a month
boundary can land in the neighbouring month (2.0.1 and 2.1.1 count in October 2025, the
announcement was 10 November). The pipeline reports Jira's date and documents this, rather than
mixing sources.

## 2. Issues per release: Jira fix versions vs official release notes

Number of distinct `KAFKA-*` keys in the official `RELEASE_NOTES.html` shipped with each
release on the [ASF archive](https://archive.apache.org/dist/kafka/), against Jira's count of
issues with that fix version (live JQL total through the pipeline's HTTP client).

| Release | Jira now | Release notes | Status |
|---|---|---|---|
| Kafka 4.1.0 | 480 | 473 | mismatch: +7 in Jira |
| Kafka 4.2.0 | 364 | 359 | mismatch: +5 in Jira |
| Kafka 4.3.0 | 226 | 226 | match |

Reading: release notes are generated once, at release time; issues tagged with the fix version
afterwards (typically tasks closed late, or back-filled tags) appear only in Jira. The gap is
1-2% and grows with a release's age, as expected. The latest of the three matches exactly.

## 3. Jira throughput: our weekly counts vs Jira's own count

`marts.mart_issue_flow_weekly.throughput` (issues resolved in the ISO week and currently in a
Done status category) against Jira's server-side `total` for
`project = X AND statusCategory = Done AND resolved >= <Monday> AND resolved < <next Monday>`,
for the last two complete weeks.

| Project | Week of 2026-09-21 (ours / Jira) | Week of 2026-09-28 (ours / Jira) |
|---|---|---|
| Kafka | 30 / 30 | 22 / 22 |
| Spark | 134 / 134 | 128 / 128 |
| Flink | 57 / 57 | 91 / 91 |

6 of 6 match. This is the end-to-end check of the incremental Jira extractor (keyset paging on
`updated`, overlap, de-duplication, latest-version merge): no issue was lost or double-counted.

## 4. GitHub merges: our daily count vs GitHub search

PRs merged on 8 October 2026 (UTC) according to `marts.fct_pull_requests`
(`merge_source = 'github_merge'`) against GitHub search's `total_count` for
`repo:<repo> is:pr is:merged merged:2026-10-08T00:00:00Z..2026-10-08T23:59:59Z`.

| Repository | Ours | GitHub search | Status |
|---|---|---|---|
| apache/kafka | 6 | 6 | match |
| apache/flink | 6 | 6 | match |

Apache Spark is not in this check: its PRs are closed by a merge script and GitHub never marks
them merged, so GitHub search has no comparable figure. Our Spark merges come from
default-branch commits that say `Closes #<n>`; all 35 merged Spark PRs in the window were
identified that way (`merge_source = 'commit_reference'`).

## Reproduce

```bash
DX_DATA_DIR=<your data dir> uv run dx run          # live extraction and build
uv run dx verify --out verification-run.md          # live comparison, prints a summary
uv run dx verify --strict                           # exit code 4 on any mismatch
```

`refresh.yml` runs `dx verify` daily and publishes the result as `verification.md` next to the
results site. CI runs `dx verify --offline` (no network) as part of the end-to-end test.
