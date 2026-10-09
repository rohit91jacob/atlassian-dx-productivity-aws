-- One row per Jira issue: flow state, lead/cycle time, age and first shipped release.
with flow as (
    select * from {{ ref('int_issue_flow') }}
),

fix_release as (
    select
        iv.issue_key,
        min(v.release_date) as first_fix_release_date
    from {{ ref('stg_jira__issue_versions') }} as iv
    inner join {{ ref('stg_jira__versions') }} as v on iv.version_id = v.version_id
    where iv.relation = 'fix'
        and v.released
        and v.release_date is not null
    group by iv.issue_key
)

select
    f.issue_key,
    f.project_key,
    f.issue_type,
    f.is_subtask,
    f.priority,
    f.status,
    f.status_category,
    f.flow_state,
    f.resolution,
    f.is_bug,
    f.is_severe,
    f.is_done,
    f.is_fixed,
    f.created_at,
    f.updated_at,
    f.resolved_at,
    f.work_started_at,
    f.reopen_count,
    f.lead_time_hours,
    f.cycle_time_hours,
    f.lead_time_hours / 24.0 as lead_time_days,
    f.cycle_time_hours / 24.0 as cycle_time_days,
    case
        when not f.is_done then {{ days_between('f.created_at', as_of_ts()) }}
    end as age_days,
    case
        when f.flow_state in ('in_progress', 'in_review') and f.work_started_at is not null
            then {{ days_between('f.work_started_at', as_of_ts()) }}
    end as wip_age_days,
    r.first_fix_release_date,
    -- Jira-side lead time for changes: reported -> end of the release day of the first released
    -- fix version (only for issues fixed in a version released after they were created).
    case
        when f.is_fixed and r.first_fix_release_date >= cast(f.created_at as date)
            then {{ days_between('f.created_at', dbt.dateadd('day', 1, 'cast(r.first_fix_release_date as timestamp)')) }}
    end as lead_time_to_release_days,
    f.assignee_id,
    f.reporter_id,
    f.is_changelog_truncated
from flow as f
left join fix_release as r on f.issue_key = r.issue_key
