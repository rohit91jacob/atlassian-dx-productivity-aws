-- PR cycle-time breakdown per repository and ISO week of merge. Bot-authored PRs are excluded.
-- Every percentile's sample size (n) is a column: prs_merged for the merge timings, prs_reviewed
-- (PRs with a human review) for the first-review timings, prs_approved for review -> approval
-- and prs_approved_before_merge for approval -> merge. prs_with_review_data (PRs whose reviews
-- were fetched) is the base of the review averages and shares. A figure whose n is below
-- var('min_weekly_sample') is a small sample: is_small_merge_sample / is_small_review_sample.
with prs as (
    select
        *,
        cast({{ dbt.date_trunc('week', 'merged_at') }} as date) as merge_week
    from {{ ref('fct_pull_requests') }}
    where is_merged and merged_in_coverage and not author_is_bot
)

select
    repo,
    jira_project,
    merge_week as week,
    count(*) as prs_merged,
    {{ percentile('hours_to_merge', 0.5) }} as p50_hours_to_merge,
    {{ percentile('hours_to_merge', 0.9) }} as p90_hours_to_merge,
    {{ count_when('has_review_data') }} as prs_with_review_data,
    count(hours_to_first_review) as prs_reviewed,
    {{ percentile('hours_to_first_review', 0.5) }} as p50_hours_to_first_review,
    {{ percentile('hours_to_first_review', 0.9) }} as p90_hours_to_first_review,
    count(hours_review_to_approval) as prs_approved,
    {{ percentile('hours_review_to_approval', 0.5) }} as p50_hours_review_to_approval,
    count(hours_approval_to_merge) as prs_approved_before_merge,
    {{ percentile('hours_approval_to_merge', 0.5) }} as p50_hours_approval_to_merge,
    avg(cast(review_rounds as double)) as avg_review_rounds,
    avg(cast(reviewer_count as double)) as avg_reviewers,
    {{ safe_ratio(
        'sum(case when has_review_data and hours_to_first_review <= 24 then 1 else 0 end)',
        'sum(case when has_review_data then 1 else 0 end)') }} as share_first_review_within_24h,
    {{ safe_ratio(
        'sum(case when has_review_data and review_count = 0 then 1 else 0 end)',
        'sum(case when has_review_data then 1 else 0 end)') }} as share_merged_without_review,
    {{ count_when("merge_source = 'commit_reference'") }} as prs_merged_by_commit_reference,
    {{ var('min_weekly_sample') }} as sample_threshold,
    count(*) < {{ var('min_weekly_sample') }} as is_small_merge_sample,
    count(hours_to_first_review) < {{ var('min_weekly_sample') }} as is_small_review_sample
from prs
group by 1, 2, 3
