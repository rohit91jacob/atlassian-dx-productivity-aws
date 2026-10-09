-- How far back the extracted Jira data is complete, per project. The extractor pulls issues
-- *updated* since its first watermark, so every issue created after the earliest update seen is
-- present. Metrics that need complete history (change-failure proxy, WIP) only use later periods.
select
    project_key,
    min(updated_at) as covered_from,
    cast(min(updated_at) as date) as covered_from_date,
    max(updated_at) as covered_to
from {{ ref('stg_jira__issues') }}
group by project_key
