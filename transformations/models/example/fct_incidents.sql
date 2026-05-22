{{ config(
    materialized='incremental',
    unique_key='incident_number',
    description='Incremental fact table containing core incident performance telemetry'
) }}

with raw_data as (
    select * from {{ source('databricks_source', 'stg_incidents') }}
)

select
    number as incident_number,
    short_description,
    priority,
    state,
    category,
    resolution_hours,
    sla_breach_risk,
    opened_at,
    -- Business Logic: Flags tickets taking longer than a business day (24 hours)
    case 
        when resolution_hours > 24 then 1 
        else 0 
    end as is_sla_breached,
    -- Maps back to dim_groups using the exact same hashing algorithm
    abs(hash(assignment_group)) as group_key
from raw_data

{% if is_incremental() %}
    -- On subsequent daily runs, only pull tickets newer than the max date we already stored
    where opened_at > (select max(opened_at) from {{ this }})
{% endif %}