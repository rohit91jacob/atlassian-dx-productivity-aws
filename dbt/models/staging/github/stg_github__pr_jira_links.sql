select
    pr_key,
    repo,
    number,
    jira_key,
    jira_project,
    position
from {{ source('curated', 'gh_pr_jira_links') }}
