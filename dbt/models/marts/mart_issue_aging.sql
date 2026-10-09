-- Open (not done) issues as of the run, by flow state and age bucket. For work in progress the
-- age is measured from when work started; for everything else from creation. Covers issues
-- updated inside the extraction window only (long-untouched issues are not extracted).
with open_issues as (
    select
        project_key,
        flow_state,
        coalesce(wip_age_days, age_days) as age_days
    from {{ ref('fct_jira_issues') }}
    where not is_done
),

bucketed as (
    select
        *,
        case
            when age_days < 7 then '0-7d'
            when age_days < 30 then '7-30d'
            when age_days < 90 then '30-90d'
            when age_days < 365 then '90-365d'
            else '365d+'
        end as age_bucket,
        case
            when age_days < 7 then 1
            when age_days < 30 then 2
            when age_days < 90 then 3
            when age_days < 365 then 4
            else 5
        end as bucket_order
    from open_issues
)

select
    project_key,
    flow_state,
    age_bucket,
    bucket_order,
    count(*) as issues,
    {{ percentile('age_days', 0.5) }} as p50_age_days
from bucketed
group by 1, 2, 3, 4
