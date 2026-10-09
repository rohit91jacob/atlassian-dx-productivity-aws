-- PR -> Jira issue (first key in the title) -> fix version -> release date. A PR counts as
-- released in the earliest released fix version of that issue that shipped on or after the merge.
with merges as (
    select * from {{ ref('int_pr_merges') }}
    where merged_at is not null
),

links as (
    select * from {{ ref('stg_github__pr_jira_links') }}
    where position = 0
),

fix_versions as (
    select
        iv.issue_key,
        v.version_name,
        v.release_date
    from {{ ref('stg_jira__issue_versions') }} as iv
    inner join {{ ref('stg_jira__versions') }} as v on iv.version_id = v.version_id
    where iv.relation = 'fix'
        and v.released
        and v.release_date is not null
),

candidates as (
    select
        m.pr_key,
        f.version_name,
        f.release_date,
        row_number() over (partition by m.pr_key order by f.release_date, f.version_name) as rn
    from merges as m
    inner join links as l on m.pr_key = l.pr_key
    inner join fix_versions as f on l.jira_key = f.issue_key
    where f.release_date >= cast(m.merged_at as date)
)

select
    pr_key,
    version_name as released_in_version,
    release_date as released_on
from candidates
where rn = 1
