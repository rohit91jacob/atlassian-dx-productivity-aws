select
    release_id,
    repo,
    tag_name,
    name,
    coalesce(is_prerelease, false) as is_prerelease,
    coalesce(is_draft, false) as is_draft,
    created_at,
    published_at
from {{ source('curated', 'gh_releases') }}
