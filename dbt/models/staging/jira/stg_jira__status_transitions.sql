with source as (
    select * from {{ source('curated', 'jira_status_transitions') }}
),

status_map as (
    select * from {{ ref('jira_status_map') }}
)

select
    t.issue_key,
    t.project_key,
    t.history_id,
    t.item_seq,
    t.changed_at,
    t.author_id,
    t.from_status,
    t.to_status,
    coalesce(f.flow_state, 'other') as from_flow_state,
    coalesce(m.flow_state, 'other') as to_flow_state
from source as t
left join status_map as m on t.to_status = m.status
left join status_map as f on t.from_status = f.status
