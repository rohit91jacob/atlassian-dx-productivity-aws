select
    issue_key,
    project_key,
    relation,
    version_id,
    version_name
from {{ source('curated', 'jira_issue_versions') }}
