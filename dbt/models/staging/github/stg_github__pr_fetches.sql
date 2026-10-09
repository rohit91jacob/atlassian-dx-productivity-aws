select
    pr_key,
    kind,
    repo,
    number,
    fetched_at,
    pr_updated_at
from {{ source('curated', 'gh_pr_fetches') }}
