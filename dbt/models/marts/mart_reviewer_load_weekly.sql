-- Reviews submitted per (pseudonymised) human reviewer, repository and ISO week.
with reviews as (
    select
        r.*,
        p.author_id as pr_author_id
    from {{ ref('stg_github__reviews') }} as r
    inner join {{ ref('stg_github__pull_requests') }} as p on r.pr_key = p.pr_key
    where not r.reviewer_is_bot
        and r.reviewer_id is not null
        and (p.author_id is null or r.reviewer_id <> p.author_id)
)

select
    repo,
    cast({{ dbt.date_trunc('week', 'submitted_at') }} as date) as week,
    reviewer_id,
    count(*) as reviews,
    count(distinct pr_key) as prs_reviewed,
    {{ count_when("state = 'APPROVED'") }} as approvals,
    {{ count_when("state = 'CHANGES_REQUESTED'") }} as changes_requested
from reviews
group by 1, 2, 3
