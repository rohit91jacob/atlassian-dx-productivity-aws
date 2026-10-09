-- DORA "four keys" per Jira project and calendar month (UTC). Every metric is a documented
-- proxy for an open-source project that ships versioned releases rather than deployments:
--
--   deployments                   released Jira versions with a release date in the month
--                                  (core_releases excludes sub-project versions such as connectors)
--   lead_time_for_changes_p50_days PR opened -> release that shipped it (via the Jira key in the
--                                  PR title and that issue's fix version), by release month
--   issue_lead_time_to_release_p50_days  Jira-only variant: issue reported -> first released fix
--                                  version, by release month (needs no GitHub data)
--   merge_lead_time_p50_hours      PR opened -> merged, by merge month (the part of lead time the
--                                  project controls day to day)
--   change_failure_rate_proxy      failed / evaluable releases in the month (see fct_releases)
--   time_to_restore_p50_hours      Blocker/Critical bugs fixed in the month: reported -> resolved
with releases as (
    select * from {{ ref('fct_releases') }}
),

prs as (
    select * from {{ ref('fct_pull_requests') }}
    where not author_is_bot
),

issues as (
    select * from {{ ref('fct_jira_issues') }}
),

release_months as (
    select
        project_key,
        cast({{ dbt.date_trunc('month', 'release_date') }} as date) as month,
        count(*) as deployments,
        {{ count_when("release_type <> 'subproject'") }} as core_releases,
        {{ count_when("release_type in ('major', 'minor')") }} as feature_releases,
        {{ count_when("release_type = 'patch'") }} as patch_releases,
        {{ count_when('is_evaluable') }} as evaluable_releases,
        {{ count_when('is_failed_proxy') }} as failed_releases
    from releases
    group by 1, 2
),

merge_months as (
    select
        jira_project as project_key,
        cast({{ dbt.date_trunc('month', 'merged_at') }} as date) as month,
        count(*) as prs_merged,
        {{ percentile('hours_to_merge', 0.5) }} as merge_lead_time_p50_hours
    from prs
    where is_merged
    group by 1, 2
),

shipped_months as (
    select
        jira_project as project_key,
        cast({{ dbt.date_trunc('month', 'released_on') }} as date) as month,
        count(*) as prs_shipped,
        {{ percentile('lead_time_for_change_days', 0.5) }} as lead_time_for_changes_p50_days
    from prs
    where released_on is not null
    group by 1, 2
),

-- Only releases inside the extracted window: older releases would be represented solely by the
-- few of their issues that happen to have been edited recently (a biased sample).
issue_shipped_months as (
    select
        i.project_key,
        cast({{ dbt.date_trunc('month', 'i.first_fix_release_date') }} as date) as month,
        count(*) as issues_shipped,
        {{ percentile('i.lead_time_to_release_days', 0.5) }} as issue_lead_time_to_release_p50_days
    from issues as i
    inner join {{ ref('int_jira_coverage') }} as c on i.project_key = c.project_key
    where i.lead_time_to_release_days is not null
        and i.first_fix_release_date >= c.covered_from_date
    group by 1, 2
),

restore_months as (
    select
        project_key,
        cast({{ dbt.date_trunc('month', 'resolved_at') }} as date) as month,
        count(*) as severe_bugs_fixed,
        {{ percentile('lead_time_hours', 0.5) }} as time_to_restore_p50_hours
    from issues
    where is_bug and is_severe and is_fixed and lead_time_hours is not null
    group by 1, 2
),

spine as (
    select project_key, month from release_months
    union
    select project_key, month from merge_months
    union
    select project_key, month from shipped_months
    union
    select project_key, month from issue_shipped_months
    union
    select project_key, month from restore_months
)

select
    s.project_key,
    s.month,
    coalesce(r.deployments, 0) as deployments,
    coalesce(r.core_releases, 0) as core_releases,
    coalesce(r.feature_releases, 0) as feature_releases,
    coalesce(r.patch_releases, 0) as patch_releases,
    coalesce(m.prs_merged, 0) as prs_merged,
    m.merge_lead_time_p50_hours,
    coalesce(sh.prs_shipped, 0) as prs_shipped,
    sh.lead_time_for_changes_p50_days,
    coalesce(isd.issues_shipped, 0) as issues_shipped,
    isd.issue_lead_time_to_release_p50_days,
    coalesce(r.evaluable_releases, 0) as evaluable_releases,
    coalesce(r.failed_releases, 0) as failed_releases,
    {{ safe_ratio('coalesce(r.failed_releases, 0)', 'coalesce(r.evaluable_releases, 0)') }}
        as change_failure_rate_proxy,
    coalesce(rs.severe_bugs_fixed, 0) as severe_bugs_fixed,
    rs.time_to_restore_p50_hours
from spine as s
left join release_months as r on s.project_key = r.project_key and s.month = r.month
left join merge_months as m on s.project_key = m.project_key and s.month = m.month
left join shipped_months as sh on s.project_key = sh.project_key and s.month = sh.month
left join issue_shipped_months as isd on s.project_key = isd.project_key and s.month = isd.month
left join restore_months as rs on s.project_key = rs.project_key and s.month = rs.month
