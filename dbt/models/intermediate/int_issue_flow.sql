-- Issue-level flow timings.
--   lead time  = created -> resolved
--   cycle time = first move into In Progress or Patch Available (work started) -> resolved
-- Issues resolved straight from Open (common in Spark) have no cycle time.
with issues as (
    select * from {{ ref('stg_jira__issues') }}
),

transitions as (
    select * from {{ ref('stg_jira__status_transitions') }}
),

started as (
    select
        issue_key,
        min(changed_at) as work_started_at
    from transitions
    where to_flow_state in ('in_progress', 'in_review')
    group by issue_key
),

reopened as (
    select
        issue_key,
        count(*) as reopen_count
    from transitions
    where to_status = 'Reopened'
    group by issue_key
)

select
    i.*,
    s.work_started_at,
    coalesce(r.reopen_count, 0) as reopen_count,
    case
        when i.is_done and i.resolved_at is not null
            then {{ hours_between('i.created_at', 'i.resolved_at') }}
    end as lead_time_hours,
    case
        when i.is_done and i.resolved_at is not null and s.work_started_at <= i.resolved_at
            then {{ hours_between('s.work_started_at', 'i.resolved_at') }}
    end as cycle_time_hours
from issues as i
left join started as s on i.issue_key = s.issue_key
left join reopened as r on i.issue_key = r.issue_key
