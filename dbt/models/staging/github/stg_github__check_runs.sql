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
    case
        when completed_at >= started_at then {{ hours_between('started_at', 'completed_at') }} * 60.0
    end as duration_minutes,
    conclusion in ('failure', 'timed_out', 'startup_failure') as is_failure,
    total_count
from {{ source('curated', 'gh_check_runs') }}
