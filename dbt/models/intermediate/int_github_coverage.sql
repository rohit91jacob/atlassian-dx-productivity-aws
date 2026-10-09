-- How far back the extracted GitHub pull-request data is complete, per repository. The extractor
-- lists PRs *updated* since its first watermark. A PR merged (or closed) after the earliest update
-- seen was necessarily updated at that moment, so it is present; a PR merged earlier is present
-- only if something touched it later (a biased sample). Merge-based metrics therefore only count
-- merges from covered_from on (fct_pull_requests.merged_in_coverage).
select
    repo,
    min(updated_at) as covered_from,
    max(updated_at) as covered_to
from {{ ref('stg_github__pull_requests') }}
group by repo
