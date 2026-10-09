select
    sha,
    repo,
    branch,
    committed_at,
    authored_at,
    author_id,
    committer_id,
    message_title,
    referenced_pr,
    reference_style
from {{ source('curated', 'gh_commits') }}
