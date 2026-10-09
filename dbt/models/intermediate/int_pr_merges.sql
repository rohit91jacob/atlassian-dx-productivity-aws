-- When did each PR land on the default branch, and how do we know?
--   github_merge      GitHub recorded merged_at (merge button, squash or rebase).
--   commit_reference  The PR was closed without a GitHub merge, but a default-branch commit names
--                     it ("Closes #123 from ...", "This closes #123", "... (#123)"). Apache Spark's
--                     merge script lands PRs this way. The commit must come after the PR was
--                     opened and no later than one day after it was closed.
with prs as (
    select * from {{ ref('stg_github__pull_requests') }}
),

refs as (
    select
        c.repo,
        c.referenced_pr as number,
        c.sha,
        c.committed_at,
        c.reference_style,
        row_number() over (
            partition by c.repo, c.referenced_pr order by c.committed_at, c.sha
        ) as rn
    from {{ ref('stg_github__commits') }} as c
    inner join prs as p
        on c.repo = p.repo
        and c.referenced_pr = p.number
    where c.referenced_pr is not null
        and p.state = 'closed'
        and c.committed_at >= p.created_at
        and c.committed_at <= {{ dbt.dateadd('day', 1, 'p.closed_at') }}
)

select
    p.pr_key,
    p.repo,
    p.number,
    case
        when p.github_merged_at is not null then p.github_merged_at
        when r.committed_at is not null then r.committed_at
    end as merged_at,
    case
        when p.github_merged_at is not null then 'github_merge'
        when r.committed_at is not null then 'commit_reference'
    end as merge_source,
    case
        when p.github_merged_at is not null then p.merge_commit_sha
        else r.sha
    end as landed_sha,
    r.reference_style
from prs as p
left join refs as r
    on p.repo = r.repo
    and p.number = r.number
    and r.rn = 1
