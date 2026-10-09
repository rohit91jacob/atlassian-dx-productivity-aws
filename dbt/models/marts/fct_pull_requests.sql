-- One row per pull request with its cycle-time breakdown, review, CI and Jira/release linkage.
--   hours_to_first_review    created -> first human review
--   hours_review_to_approval first human review -> first approval
--   hours_approval_to_merge  first approval -> merge
--   hours_to_merge           created -> merge (GitHub merge or commit reference)
--   lead_time_for_change_days created -> release date of the Jira fix version that shipped it
-- Review metrics are null when the PR's reviews have not been fetched yet (has_review_data).
-- merged_in_coverage: merged at or after the repository's extraction coverage start
-- (int_github_coverage). Merge-based marts count only these; earlier merges are a biased sample.
with prs as (
    select * from {{ ref('stg_github__pull_requests') }}
),

merges as (
    select * from {{ ref('int_pr_merges') }}
),

reviews as (
    select * from {{ ref('int_pr_review_stats') }}
),

ci as (
    select * from {{ ref('int_pr_ci_stats') }}
),

fetches as (
    select
        pr_key,
        max(case when kind = 'reviews' then 1 else 0 end) = 1 as has_review_data,
        max(case when kind = 'check_runs' then 1 else 0 end) = 1 as has_ci_data
    from {{ ref('stg_github__pr_fetches') }}
    group by pr_key
),

links as (
    select
        pr_key,
        count(*) as linked_jira_keys,
        min(case when position = 0 then jira_key end) as jira_key
    from {{ ref('stg_github__pr_jira_links') }}
    group by pr_key
),

issues as (
    select issue_key, issue_type, priority from {{ ref('stg_jira__issues') }}
),

releases as (
    select * from {{ ref('int_pr_releases') }}
),

coverage as (
    select * from {{ ref('int_github_coverage') }}
),

joined as (
    select
        p.pr_key,
        p.repo,
        p.jira_project,
        p.number,
        p.author_id,
        p.author_is_bot,
        p.is_draft,
        p.state,
        p.base_ref,
        p.created_at,
        p.closed_at,
        m.merged_at,
        m.merge_source,
        m.merged_at is not null as is_merged,
        coalesce(m.merged_at >= cov.covered_from, false) as merged_in_coverage,
        coalesce(f.has_review_data, false) as has_review_data,
        coalesce(f.has_ci_data, false) as has_ci_data,
        r.first_review_at,
        r.first_approval_at,
        case when f.has_review_data then coalesce(r.review_count, 0) end as review_count,
        case when f.has_review_data then coalesce(r.reviewer_count, 0) end as reviewer_count,
        case when f.has_review_data then coalesce(r.review_rounds, 0) end as review_rounds,
        case when f.has_review_data then coalesce(r.changes_requested_count, 0) end
            as changes_requested_count,
        case when f.has_review_data then coalesce(r.approval_count, 0) end as approval_count,
        c.check_runs,
        c.failed_check_runs,
        case when c.check_runs > 0 then c.failed_check_runs > 0 end as ci_failed,
        case
            when c.ci_completed_at >= c.ci_started_at
                then {{ hours_between('c.ci_started_at', 'c.ci_completed_at') }} * 60.0
        end as ci_wall_minutes,
        coalesce(l.linked_jira_keys, 0) as linked_jira_keys,
        l.jira_key,
        i.issue_key is not null as jira_issue_in_data,
        i.issue_type as jira_issue_type,
        rel.released_in_version,
        rel.released_on
    from prs as p
    left join merges as m on p.pr_key = m.pr_key
    left join reviews as r on p.pr_key = r.pr_key
    left join ci as c on p.pr_key = c.pr_key
    left join fetches as f on p.pr_key = f.pr_key
    left join links as l on p.pr_key = l.pr_key
    left join issues as i on l.jira_key = i.issue_key
    left join releases as rel on p.pr_key = rel.pr_key
    left join coverage as cov on p.repo = cov.repo
)

select
    *,
    case
        when first_review_at is not null
            then {{ hours_between('created_at', 'first_review_at') }}
    end as hours_to_first_review,
    case
        when first_approval_at >= first_review_at
            then {{ hours_between('first_review_at', 'first_approval_at') }}
    end as hours_review_to_approval,
    case
        when merged_at >= first_approval_at
            then {{ hours_between('first_approval_at', 'merged_at') }}
    end as hours_approval_to_merge,
    case
        when merged_at is not null then {{ hours_between('created_at', 'merged_at') }}
    end as hours_to_merge,
    case
        when released_on is not null
            -- Jira release dates are day-granular: the release instant is taken as the end of
            -- that UTC day, so a PR merged and released on the same day has a lead time >= 0.
            then {{ days_between('created_at', dbt.dateadd('day', 1, 'cast(released_on as timestamp)')) }}
    end as lead_time_for_change_days,
    linked_jira_keys > 0 as is_linked
from joined
