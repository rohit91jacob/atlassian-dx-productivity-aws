with source as (
    select * from {{ source('curated', 'gh_pull_requests') }}
),

projects as (
    select * from {{ ref('repo_project_map') }}
)

select
    pr.pr_key,
    pr.repo,
    p.jira_project,
    pr.number,
    pr.title,
    pr.state,
    coalesce(pr.is_draft, false) as is_draft,
    pr.author_id,
    coalesce(pr.author_is_bot, false) as author_is_bot,
    pr.author_association,
    pr.created_at,
    pr.updated_at,
    pr.closed_at,
    pr.merged_at as github_merged_at,
    pr.merge_commit_sha,
    pr.head_sha,
    pr.base_ref,
    pr.labels,
    pr.requested_reviewers
from source as pr
left join projects as p on pr.repo = p.repo
