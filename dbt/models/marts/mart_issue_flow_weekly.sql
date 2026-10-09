-- Jira issue flow per project and ISO week (UTC):
--   arrivals            issues created in the week
--   throughput          issues resolved (status category done) in the week
--   lead/cycle p50/p85  for issues resolved in the week, in days
--   wip_end_of_week     issues in In Progress / Patch Available at the end of the week,
--                       reconstructed from changelog transitions (the state an issue was in
--                       at week end = the target of its last transition before then)
-- Only weeks that start after the project's covered_from date are reported, because earlier
-- weeks would be missing issues that have not been updated since.
with issues as (
    select * from {{ ref('fct_jira_issues') }}
),

transitions as (
    select * from {{ ref('stg_jira__status_transitions') }}
),

coverage as (
    select * from {{ ref('int_jira_coverage') }}
),

weeks as (
    select distinct
        i.project_key,
        cast({{ dbt.date_trunc('week', 'i.created_at') }} as date) as week
    from issues as i
    union
    select distinct
        t.project_key,
        cast({{ dbt.date_trunc('week', 't.changed_at') }} as date) as week
    from transitions as t
),

covered_weeks as (
    select w.project_key, w.week
    from weeks as w
    inner join coverage as c on w.project_key = c.project_key
    where cast(w.week as timestamp) >= c.covered_from
        and cast(w.week as timestamp) <= {{ as_of_ts() }}
),

arrivals as (
    select
        project_key,
        cast({{ dbt.date_trunc('week', 'created_at') }} as date) as week,
        count(*) as arrivals,
        {{ count_when('is_bug') }} as bug_arrivals
    from issues
    group by 1, 2
),

departures as (
    select
        project_key,
        cast({{ dbt.date_trunc('week', 'resolved_at') }} as date) as week,
        count(*) as throughput,
        {{ count_when('is_bug') }} as bug_throughput,
        {{ percentile('lead_time_days', 0.5) }} as p50_lead_time_days,
        {{ percentile('lead_time_days', 0.85) }} as p85_lead_time_days,
        {{ percentile('cycle_time_days', 0.5) }} as p50_cycle_time_days,
        {{ percentile('cycle_time_days', 0.85) }} as p85_cycle_time_days,
        {{ count_when('cycle_time_days is not null') }} as resolved_with_cycle_time
    from issues
    where is_done and resolved_at is not null
    group by 1, 2
),

state_at_week_end as (
    select
        w.project_key,
        w.week,
        t.issue_key,
        t.to_flow_state,
        row_number() over (
            partition by w.project_key, w.week, t.issue_key
            order by t.changed_at desc, t.history_id desc, t.item_seq desc
        ) as rn
    from covered_weeks as w
    inner join transitions as t
        on w.project_key = t.project_key
        and t.changed_at < {{ dbt.dateadd('day', 7, 'cast(w.week as timestamp)') }}
),

wip as (
    select
        project_key,
        week,
        sum(case when to_flow_state in ('in_progress', 'in_review') then 1 else 0 end)
            as wip_end_of_week
    from state_at_week_end
    where rn = 1
    group by 1, 2
)

select
    w.project_key,
    w.week,
    coalesce(a.arrivals, 0) as arrivals,
    coalesce(a.bug_arrivals, 0) as bug_arrivals,
    coalesce(d.throughput, 0) as throughput,
    coalesce(d.bug_throughput, 0) as bug_throughput,
    d.p50_lead_time_days,
    d.p85_lead_time_days,
    d.p50_cycle_time_days,
    d.p85_cycle_time_days,
    coalesce(d.resolved_with_cycle_time, 0) as resolved_with_cycle_time,
    coalesce(wp.wip_end_of_week, 0) as wip_end_of_week,
    {{ dbt.dateadd('day', 7, 'cast(w.week as timestamp)') }} > {{ as_of_ts() }} as is_partial_week
from covered_weeks as w
left join arrivals as a on w.project_key = a.project_key and w.week = a.week
left join departures as d on w.project_key = d.project_key and w.week = d.week
left join wip as wp on w.project_key = wp.project_key and w.week = wp.week
