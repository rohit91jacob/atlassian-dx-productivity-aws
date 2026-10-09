-- How concentrated review work is, per repository and week: active reviewers, reviews per
-- reviewer, and the share of all reviews done by the busiest reviewer and the top three.
with load as (
    select
        *,
        row_number() over (
            partition by repo, week order by reviews desc, reviewer_id
        ) as load_rank
    from {{ ref('mart_reviewer_load_weekly') }}
)

select
    repo,
    week,
    count(*) as active_reviewers,
    {{ sum_bigint('reviews') }} as reviews,
    {{ sum_bigint('prs_reviewed') }} as reviewer_pr_pairs,
    {{ percentile('reviews', 0.5) }} as p50_reviews_per_reviewer,
    max(reviews) as max_reviews_per_reviewer,
    {{ safe_ratio('max(reviews)', 'sum(reviews)') }} as top_reviewer_share,
    {{ safe_ratio('sum(case when load_rank <= 3 then reviews else 0 end)', 'sum(reviews)') }}
        as top3_reviewer_share
from load
group by 1, 2
