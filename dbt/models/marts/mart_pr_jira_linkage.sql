-- How well merged PRs trace back to Jira, per repository and ISO week of merge:
--   linked_share      merged PRs whose title names a Jira key of the project (KAFKA-1234, ...)
--   resolvable_share  linked PRs whose (first) key is an issue present in the curated Jira data
with prs as (
    select
        *,
        cast({{ dbt.date_trunc('week', 'merged_at') }} as date) as week
    from {{ ref('fct_pull_requests') }}
    where is_merged and not author_is_bot
)

select
    repo,
    jira_project,
    week,
    count(*) as prs_merged,
    {{ count_when('is_linked') }} as prs_linked,
    {{ count_when('is_linked and jira_issue_in_data') }} as prs_linked_resolvable,
    {{ count_when('released_on is not null') }} as prs_released,
    {{ safe_ratio('sum(case when is_linked then 1 else 0 end)', 'count(*)') }} as linked_share,
    {{ safe_ratio(
        'sum(case when is_linked and jira_issue_in_data then 1 else 0 end)',
        'sum(case when is_linked then 1 else 0 end)') }} as resolvable_share
from prs
group by 1, 2, 3
