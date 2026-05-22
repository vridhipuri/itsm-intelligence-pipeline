{{ config(
    materialized='table',
    description='Dimension table isolating unique IT assignment groups'
) }}

with raw_source as (
    select assignment_group, number
    from {{ source('databricks_source', 'stg_incidents') }}
),

aggregated as (
    select
        assignment_group,
        count(distinct number) as lifetime_incidents_handled
    from raw_source
    where assignment_group is not null
    group by assignment_group
)

select
    -- Generates a consistent surrogate key using Databricks hashing
    abs(hash(assignment_group)) as group_key,
    assignment_group,
    lifetime_incidents_handled,
    current_timestamp() as dbt_transformed_at
from aggregated