with source as (
    select * from {{ source('curated', 'jira_issues') }}
),

status_map as (
    select * from {{ ref('jira_status_map') }}
)

select
    i.issue_key,
    i.issue_id,
    i.project_key,
    i.summary,
    i.issue_type,
    coalesce(i.is_subtask, false) as is_subtask,
    i.status,
    i.status_category,
    coalesce(
        m.flow_state,
        case i.status_category
            when 'new' then 'todo'
            when 'indeterminate' then 'in_progress'
            when 'done' then 'done'
        end
    ) as flow_state,
    i.priority,
    i.resolution,
    i.created_at,
    i.updated_at,
    i.resolved_at,
    i.reporter_id,
    i.assignee_id,
    i.components,
    i.labels,
    i.issue_type = 'Bug' as is_bug,
    i.priority in {{ sql_list(var('severe_priorities')) }} as is_severe,
    i.status_category = 'done' as is_done,
    i.resolution = 'Fixed' as is_fixed,
    i.changelog_returned < i.changelog_total as is_changelog_truncated
from source as i
left join status_map as m on i.status = m.status
