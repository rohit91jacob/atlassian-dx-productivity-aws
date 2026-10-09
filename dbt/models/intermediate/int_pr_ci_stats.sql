-- CI outcome on each closed PR's final head commit (check runs are fetched for closed PRs only).
select
    pr_key,
    count(*) as check_runs,
    {{ count_when('is_failure') }} as failed_check_runs,
    {{ count_when("conclusion = 'success'") }} as successful_check_runs,
    max(total_count) as check_runs_reported,
    min(started_at) as ci_started_at,
    max(completed_at) as ci_completed_at
from {{ ref('stg_github__check_runs') }}
group by pr_key
