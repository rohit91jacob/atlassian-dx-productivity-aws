-- CI health on closed PRs' final head commits, per repository and ISO week the PR was closed.
-- Only repositories whose CI reports GitHub check runs appear (Flink's CI reports elsewhere).
with prs as (
    select
        *,
        cast({{ dbt.date_trunc('week', 'closed_at') }} as date) as week
    from {{ ref('fct_pull_requests') }}
    where check_runs > 0
)

select
    repo,
    week,
    count(*) as prs_with_checks,
    {{ sum_bigint('check_runs') }} as check_runs,
    {{ sum_bigint('failed_check_runs') }} as failed_check_runs,
    {{ safe_ratio('sum(case when ci_failed then 1 else 0 end)', 'count(*)') }} as share_prs_ci_failed,
    {{ percentile('ci_wall_minutes', 0.5) }} as p50_ci_wall_minutes,
    {{ percentile('ci_wall_minutes', 0.9) }} as p90_ci_wall_minutes
from prs
group by 1, 2
