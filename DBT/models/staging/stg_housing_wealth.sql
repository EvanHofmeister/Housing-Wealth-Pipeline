{{ config(materialized='view') }}

with source_data
 as (
    select * from {{ source('staging', 'housing_data_partitioned') }}
)
select
    index,
    cast(date as timestamp) as date,
    indicator_id,
    zip_code,
    region_id,
    value as AVM_value,
    metro,
    metro_state,
    region_type,
    size_rank,
    state,
    county,
    city,
    RUCA1,
    Total_estimated_single_family_owner_occupied as count_of_sf_owner_occupied,
    Total_estimated_multi_family_owner_occupied as count_of_mf_owner_occupied,
    YoY___change as YoY_change_AVM_value,
    QoQ___change as QoQ_change_AVM_value,
    MoM___change as MoM_change_AVM_value,
    region,
    chained_dollar_index,
    value_inf_adj as AVM_value_inf_adj,
    AVM_SF as agg_AVM_sf,
    AVM_MF as agg_AVM_mf,
    AVM_Tot as agg_AVM_tot
from source_data