-- Human review activity per PR. Bot reviews and the PR author's own reviews are excluded.
-- review_rounds = distinct PR revisions (head commits) that received at least one review. A
-- revision pushed in response to feedback and then reviewed again counts as a new round.
with reviews as (
    select
        r.*,
        p.author_id as pr_author_id
    from {{ ref('stg_github__reviews') }} as r
    inner join {{ ref('stg_github__pull_requests') }} as p on r.pr_key = p.pr_key
),

human as (
    select * from reviews
    where not reviewer_is_bot
        and (pr_author_id is null or reviewer_id is null or reviewer_id <> pr_author_id)
)

select
    pr_key,
    count(*) as review_count,
    count(distinct reviewer_id) as reviewer_count,
    count(distinct commit_id) as review_rounds,
    {{ count_when("state = 'CHANGES_REQUESTED'") }} as changes_requested_count,
    {{ count_when("state = 'APPROVED'") }} as approval_count,
    min(submitted_at) as first_review_at,
    min(case when state = 'APPROVED' then submitted_at end) as first_approval_at
from human
group by pr_key
