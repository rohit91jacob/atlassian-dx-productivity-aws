with source as (
    select * from {{ source('curated', 'jira_versions') }}
),

parsed as (
    select
        version_id,
        project_key,
        version_name,
        released,
        archived,
        release_date,
        start_date,
        {{ semver_part('version_name', 1) }} as major,
        {{ semver_part('version_name', 2) }} as minor,
        {{ semver_part('version_name', 3) }} as patch
    from source
)

select
    *,
    case
        -- Names without a leading x.y version are sub-project releases (connectors, operators,
        -- clients: 'kafka-5.0.0', 'kubernetes-operator-1.16.0', 'connect-swift-0.7.0').
        when major is null then 'subproject'
        when minor = 0 and coalesce(patch, 0) = 0 then 'major'
        when coalesce(patch, 0) = 0 then 'minor'
        else 'patch'
    end as release_type
from parsed
