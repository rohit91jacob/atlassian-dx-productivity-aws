-- One row per released Jira version (= a project release) with the change-failure proxy.
--
-- Change failure (proxy): a release "failed" when at least one Blocker/Critical Bug that lists it
-- as an affected version was reported within cfr_window_days of the release date. A release is
-- only evaluable when (a) the whole window lies inside the extracted Jira history and (b) the
-- window has fully elapsed; otherwise is_failed_proxy is null rather than a guess.
with versions as (
    select * from {{ ref('stg_jira__versions') }}
    where released and release_date is not null
),

coverage as (
    select * from {{ ref('int_jira_coverage') }}
),

issues as (
    select * from {{ ref('stg_jira__issues') }}
),

bridge as (
    select * from {{ ref('stg_jira__issue_versions') }}
),

fixed as (
    select
        version_id,
        count(*) as issues_fixed_in_data
    from bridge
    where relation = 'fix'
    group by version_id
),

escaped as (
    select
        b.version_id,
        count(*) as escaped_bugs,
        {{ count_when('i.is_severe') }} as escaped_severe_bugs,
        cast(sum(
            case
                when i.is_severe
                    and i.created_at < {{ dbt.dateadd('day', var('cfr_window_days'), 'cast(v.release_date as timestamp)') }}
                    then 1
                else 0
            end
        ) as bigint) as escaped_severe_bugs_in_window
    from bridge as b
    inner join issues as i on b.issue_key = i.issue_key
    inner join versions as v on b.version_id = v.version_id
    where b.relation = 'affects'
        and i.is_bug
        and i.created_at >= cast(v.release_date as timestamp)
    group by b.version_id
),

ordered as (
    select
        v.*,
        lag(v.release_date) over (
            partition by v.project_key order by v.release_date, v.version_name
        ) as previous_release_date
    from versions as v
)

select
    o.version_id,
    o.project_key,
    o.version_name,
    o.release_type,
    o.major,
    o.minor,
    o.patch,
    o.release_date,
    o.previous_release_date,
    {{ dbt.datediff('o.previous_release_date', 'o.release_date', 'day') }} as days_since_previous_release,
    coalesce(f.issues_fixed_in_data, 0) as issues_fixed_in_data,
    coalesce(e.escaped_bugs, 0) as escaped_bugs,
    coalesce(e.escaped_severe_bugs, 0) as escaped_severe_bugs,
    coalesce(e.escaped_severe_bugs_in_window, 0) as escaped_severe_bugs_in_window,
    (
        c.covered_from_date is not null
        and o.release_date >= c.covered_from_date
        and {{ dbt.dateadd('day', var('cfr_window_days'), 'cast(o.release_date as timestamp)') }} <= {{ as_of_ts() }}
    ) as is_evaluable,
    case
        when c.covered_from_date is not null
            and o.release_date >= c.covered_from_date
            and {{ dbt.dateadd('day', var('cfr_window_days'), 'cast(o.release_date as timestamp)') }} <= {{ as_of_ts() }}
            then coalesce(e.escaped_severe_bugs_in_window, 0) > 0
    end as is_failed_proxy
from ordered as o
left join fixed as f on o.version_id = f.version_id
left join escaped as e on o.version_id = e.version_id
left join coverage as c on o.project_key = c.project_key
