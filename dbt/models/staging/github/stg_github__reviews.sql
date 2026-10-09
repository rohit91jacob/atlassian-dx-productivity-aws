-- PENDING reviews are drafts that were never submitted; they are not review activity.
select
    review_id,
    pr_key,
    repo,
    number,
    reviewer_id,
    coalesce(reviewer_is_bot, false) as reviewer_is_bot,
    author_association,
    state,
    submitted_at,
    commit_id
from {{ source('curated', 'gh_reviews') }}
where state <> 'PENDING' and submitted_at is not null
