{{ config(materialized='view') }}

with source_data
 as (
    select * from {{ source('staging', 'avm_data_table_test') }}
)
select
    index,
    cast(date as timestamp) as date,
    indicator_id,
    zip_code,
    region_id,
    value,
    metro,
    metro_state,
    region_type,
    size_rank,
    state,
    county,
    city,
    RUCA1,
    Total_estimated_single_family_owner_occupied,
    Total_estimated_multi_family_owner_occupied,
    YoY___change as YoY_change,
    QoQ___change as QoQ_change,
    MoM___change as MoM_change,
    region,
    chained_dollar_index,
    value_inf_adj,
    AVM_SF,
    AVM_MF,
    AVM_Tot
from source_data