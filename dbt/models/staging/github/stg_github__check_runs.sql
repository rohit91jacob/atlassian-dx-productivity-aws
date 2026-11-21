select
    check_run_id,
    pr_key,
    repo,
    head_sha,
    name,
    app_slug,
    status,
    conclusion,
    started_at,
    completed_at,
    -- Unknown (null) when the clock runs backwards or the run spans more than 100,000 minutes (~69 days):
    -- GitHub reports some re-run or long-pending checks against their original start time, and one
    -- such outlier failed the between(0, 100000) test on every refresh from 9 Nov 2026.
    case
        when completed_at >= started_at
            and {{ hours_between('started_at', 'completed_at') }} * 60.0 <= 100000
            then {{ hours_between('started_at', 'completed_at') }} * 60.0
    end as duration_minutes,
    conclusion in ('failure', 'timed_out', 'startup_failure') as is_failure,
    total_count
from {{ source('curated', 'gh_check_runs') }}
