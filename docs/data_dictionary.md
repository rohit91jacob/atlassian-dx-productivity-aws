# Data dictionary

All timestamps are UTC (`timestamp` without zone, microsecond precision). People are never
stored by name: every `*_id` that refers to a person (`author_id`, `reviewer_id`,
`assignee_id`, ...) is an HMAC pseudonym `u_<12 hex>` (see `src/dx_productivity/pseudonym.py`).
Weeks are ISO weeks starting Monday (`date_trunc('week', ...)`); months are calendar months.

## Raw zone (S3 / local stand-in)

```
raw/<source>/<entity>/scope=<project|owner__repo>/dt=<YYYY-MM-DD>/<run_id>-<seq>.json.gz
```

Each object is a gzip JSON *envelope* (`schema = dx.raw.v1`) with `source`, `entity`, `scope`,
`run_id`, `landed_at`, `meta` and `pages[]`. Every page holds the `request` (URL and params),
HTTP `status`, selected response `headers`, `fetched_at` and the response `body` exactly as the
API returned it. Envelopes are immutable and deterministic (sorted keys, gzip mtime 0).

| source / entity | One object per | Body |
|---|---|---|
| `jira/issues` | search page (50 issues, changelog expanded) | `/rest/api/2/search` response |
| `jira/versions` | project per run (snapshot) | `/rest/api/2/project/{key}/versions` |
| `github/pulls` | list page (100 PRs) | `/repos/{repo}/pulls?state=all&sort=updated` |
| `github/pr_details` | bundle of up to 25 PRs | `/pulls/{n}/reviews` pages (`kind=reviews`) and `/commits/{sha}/check-runs` (`kind=check_runs`), each tagged with `pr_number` |
| `github/commits` | repo per run | `/commits?sha=<default branch>&since=...` pages |
| `github/releases` | repo per run (snapshot) | `/releases` |

`ops/dq_results/dt=<date>/<run_id>-<layer>.json` holds every data-quality result (raw, curated,
marts) with violation counts and samples.

## State (DynamoDB / SQLite stand-in)

Single table, hash key `pk`, attributes `version` (optimistic-lock counter), `doc` (JSON) and
`updated_at`.

| `pk` pattern | `doc` |
|---|---|
| `watermark#jira#<PROJECT>#issues` | `watermark` (max `updated` landed), `run_id`, `last_raw_key` |
| `watermark#github#<owner__repo>#pulls` | `watermark` = `updated_at` of the last PR whose reviews/check runs are landed |
| `watermark#github#<owner__repo>#commits` | `watermark` = newest committer date landed |
| `processed#<raw object key>` | `run_id` of the curate run that merged the object |

## Curated zone (Apache Iceberg, namespace `curated`)

Every table also has `_version` (ISO string ordering versions of the same parent: the source's
`updated` timestamp, or the fetch time for snapshots and child sets) and `_raw_key` (lineage to
the raw object). The *parent* is the grain at which a newer record replaces older ones.

| Table | Primary key / parent | Columns |
|---|---|---|
| `jira_issues` | `issue_key` | `issue_id`, `project_key`, `summary`, `issue_type`, `is_subtask`, `status`, `status_category` (new / indeterminate / done), `priority`, `resolution`, `created_at`, `updated_at`, `resolved_at`, `reporter_id`, `assignee_id`, `components`, `labels` (comma-joined, sorted), `changelog_total`, `changelog_returned` |
| `jira_issue_versions` | (`issue_key`, `relation`, `version_id`) / `issue_key` | `project_key`, `relation` (`fix` = fixVersions, `affects` = versions), `version_name` |
| `jira_status_transitions` | (`issue_key`, `history_id`, `item_seq`) / `issue_key` | `project_key`, `changed_at`, `author_id`, `from_status`, `to_status` |
| `jira_versions` | `version_id` | `project_key`, `version_name`, `released`, `archived`, `release_date`, `start_date` |
| `gh_pull_requests` | `pr_key` (`owner/repo#number`) | `repo`, `number`, `title`, `state` (open / closed), `is_draft`, `author_id`, `author_is_bot`, `author_association`, `created_at`, `updated_at`, `closed_at`, `merged_at` (GitHub's; null for Spark), `merge_commit_sha`, `head_sha`, `base_ref`, `labels`, `requested_reviewers` |
| `gh_reviews` | `review_id` / `pr_key` | `repo`, `number`, `reviewer_id`, `reviewer_is_bot`, `author_association`, `state`, `submitted_at`, `commit_id` (PR head reviewed) |
| `gh_check_runs` | `check_run_id` / `pr_key` | `repo`, `head_sha`, `name`, `app_slug`, `status`, `conclusion`, `started_at`, `completed_at`, `total_count` (runs on the commit; only the first 100 are fetched) |
| `gh_pr_fetches` | (`pr_key`, `kind`) | `repo`, `number`, `fetched_at`, `pr_updated_at`; tells "no reviews" apart from "reviews not fetched yet" |
| `gh_commits` | `sha` | `repo`, `branch`, `committed_at`, `authored_at`, `author_id`, `committer_id`, `message_title`, `referenced_pr`, `reference_style` (`merge_commit`, `closes`, `this_closes`, `squash_title`) |
| `gh_releases` | `release_id` | `repo`, `tag_name`, `name`, `is_prerelease`, `is_draft`, `created_at`, `published_at` |
| `gh_pr_jira_links` | (`pr_key`, `jira_key`) / `pr_key` | `repo`, `number`, `jira_project`, `position` (0 = first key in the title) |

## dbt models

### Staging (views, schema `staging`)

One per curated table (`stg_jira__issues`, `stg_jira__status_transitions`,
`stg_jira__issue_versions`, `stg_jira__versions`, `stg_github__pull_requests`,
`stg_github__reviews`, `stg_github__check_runs`, `stg_github__commits`,
`stg_github__pr_jira_links`, `stg_github__pr_fetches`, `stg_github__releases`). They add:

* `flow_state` (todo / in_progress / in_review / done) from the `jira_status_map` seed, falling
  back to the status category; `is_bug`, `is_severe` (Blocker or Critical), `is_done`, `is_fixed`;
* parsed semantic versions (`major`, `minor`, `patch`) and `release_type` (major / minor / patch,
  or `subproject` for names such as `kafka-5.0.0` or `kubernetes-operator-1.16.0`);
* `jira_project` for each PR (seed `repo_project_map`); `duration_minutes` and `is_failure` for
  check runs; PENDING (unsubmitted) reviews removed.

### Intermediate (tables, schema `intermediate`)

| Model | Grain | Meaning |
|---|---|---|
| `int_pr_merges` | PR | `merged_at`, `merge_source` (`github_merge` or `commit_reference`), `landed_sha` |
| `int_pr_review_stats` | PR | Human reviews only (no bots, no self-review): `review_count`, `reviewer_count`, `review_rounds` (distinct reviewed head commits), `changes_requested_count`, `approval_count`, `first_review_at`, `first_approval_at` |
| `int_pr_ci_stats` | PR | `check_runs`, `failed_check_runs`, `successful_check_runs`, CI start/end |
| `int_pr_releases` | merged, linked PR | `released_in_version`, `released_on`: earliest released fix version of the PR's first Jira key that shipped on/after the merge date |
| `int_issue_flow` | issue | `work_started_at` (first move to In Progress / Patch Available), `reopen_count`, `lead_time_hours`, `cycle_time_hours` |
| `int_jira_coverage` | project | `covered_from` (earliest `updated` extracted), `covered_to`: the window inside which the data is complete |

### Marts (tables, schema `marts`)

| Model | Grain | Key columns |
|---|---|---|
| `fct_pull_requests` | PR | `merged_at`, `merge_source`, `is_merged`, `has_review_data`, `review_count`, `reviewer_count`, `review_rounds`, `changes_requested_count`, `hours_to_first_review`, `hours_review_to_approval`, `hours_approval_to_merge`, `hours_to_merge`, `check_runs`, `failed_check_runs`, `ci_failed`, `ci_wall_minutes`, `jira_key`, `linked_jira_keys`, `is_linked`, `jira_issue_in_data`, `released_in_version`, `released_on`, `lead_time_for_change_days` |
| `fct_jira_issues` | issue | `flow_state`, `is_bug`, `is_severe`, `is_done`, `is_fixed`, `work_started_at`, `lead_time_days`, `cycle_time_days`, `age_days` (open issues), `wip_age_days` (in progress), `first_fix_release_date`, `lead_time_to_release_days`, `reopen_count`, `is_changelog_truncated` |
| `fct_releases` | released Jira version | `release_type`, `release_date`, `previous_release_date`, `days_since_previous_release`, `issues_fixed_in_data`, `escaped_bugs`, `escaped_severe_bugs`, `escaped_severe_bugs_in_window`, `is_evaluable`, `is_failed_proxy` |
| `mart_dora_monthly` | project x month | `deployments`, `core_releases`, `feature_releases`, `patch_releases`, `prs_merged`, `merge_lead_time_p50_hours`, `prs_shipped`, `lead_time_for_changes_p50_days`, `issues_shipped`, `issue_lead_time_to_release_p50_days`, `evaluable_releases`, `failed_releases`, `change_failure_rate_proxy`, `severe_bugs_fixed`, `time_to_restore_p50_hours` |
| `mart_pr_cycle_time_weekly` | repo x merge week | `prs_merged`, `p50/p90_hours_to_merge`, `prs_with_review_data`, `p50/p90_hours_to_first_review`, `p50_hours_review_to_approval`, `p50_hours_approval_to_merge`, `avg_review_rounds`, `avg_reviewers`, `share_first_review_within_24h`, `share_merged_without_review`, `prs_merged_by_commit_reference` |
| `mart_issue_flow_weekly` | project x week | `arrivals`, `bug_arrivals`, `throughput`, `bug_throughput`, `p50/p85_lead_time_days`, `p50/p85_cycle_time_days`, `resolved_with_cycle_time`, `wip_end_of_week`, `is_partial_week` |
| `mart_issue_aging` | project x flow state x age bucket | `issues`, `p50_age_days` (buckets 0-7d, 7-30d, 30-90d, 90-365d, 365d+) |
| `mart_reviewer_load_weekly` | repo x week x reviewer | `reviews`, `prs_reviewed`, `approvals`, `changes_requested` |
| `mart_reviewer_load_summary` | repo x week | `active_reviewers`, `reviews`, `p50_reviews_per_reviewer`, `max_reviews_per_reviewer`, `top_reviewer_share`, `top3_reviewer_share` |
| `mart_pr_jira_linkage` | repo x merge week | `prs_merged`, `prs_linked`, `prs_linked_resolvable`, `prs_released`, `linked_share`, `resolvable_share` |
| `mart_ci_health_weekly` | repo x close week | `prs_with_checks`, `check_runs`, `failed_check_runs`, `share_prs_ci_failed`, `p50/p90_ci_wall_minutes` |

## Metric definitions and proxies

| Metric | Definition here | Why it is a proxy |
|---|---|---|
| Deployment frequency | Released Jira versions per month (`core_releases` excludes sub-project versions) | OSS projects ship versioned releases to mirrors, not deployments to production |
| Lead time for changes (Jira) | Issue created -> end of the release day of its first released fix version, for releases inside the extracted window | Starts at "reported", not at first commit; release dates are day-granular |
| Lead time for changes (PR) | PR opened -> release that shipped it (title key -> fix version) | Needs a Jira key in the title and PRs old enough to have shipped |
| Merge lead time | PR opened -> merged (GitHub merge or default-branch commit naming the PR) | Merge, not production |
| Change failure rate | Evaluable releases with a Blocker/Critical Bug that affects them reported within 30 days, divided by evaluable releases | Bugs are a lagging, partial signal of failure; reporters set "affects version" inconsistently |
| Time to restore | Median report -> resolution of Blocker/Critical Bugs resolved Fixed | Measures fixing in the repo, not restoring a running service |
| Time to first review | PR created -> first human, non-author review | GitHub review objects only; Spark/Flink reviewers also review in comments |
| Review rounds | Distinct PR revisions (head commits) that received a review | Proxy for back-and-forth; force-pushes and review comments without a review object are not seen |
| Cycle time | First move to In Progress / Patch Available -> resolved | Many issues (Spark especially) are resolved straight from Open |
| WIP | Issues whose last transition before the week end went to In Progress / Patch Available | Only issues updated inside the extraction window are known |
