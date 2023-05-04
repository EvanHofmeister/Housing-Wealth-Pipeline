{{ config(materialized='table') }}

with source_data as (

    SELECT * from {{ source('staging', 'stg_housing_wealth') }}
),
state_region_lookup as (
        SELECT * from {{ ref('state_region_lookup') }}
)

SELECT
sub.*,
sub_pct_change.MoM_change_AVM_sf,
sub_pct_change.QoQ_change_AVM_sf,
sub_pct_change.YoY_change_AVM_sf,
sub_pct_change.MoM_change_AVM_mf,
sub_pct_change.QoQ_change_AVM_mf,
sub_pct_change.YoY_change_AVM_mf,
sub_pct_change.MoM_change_AVM_tot,
sub_pct_change.QoQ_change_AVM_tot,
sub_pct_change.YoY_change_AVM_tot
FROM (
    SELECT
    source_data.*,
    state_region_lookup.region as region_dbt_derived,
    from source_data
    left join state_region_lookup
    on source_data.state = state_region_lookup.state
) sub
left join
(
    SELECT
    source_data.date,
    source_data.zip_code,
    SUM(source_data.agg_AVM_sf) / nullif(LAG(SUM(source_data.agg_AVM_sf), 1) OVER (PARTITION BY zip_code ORDER BY source_data.date),0) - 1 AS MoM_change_AVM_sf,
    SUM(source_data.agg_AVM_sf) / nullif(LAG(SUM(source_data.agg_AVM_sf), 3) OVER (PARTITION BY zip_code ORDER BY source_data.date),0) - 1 AS QoQ_change_AVM_sf,
    SUM(source_data.agg_AVM_sf) / nullif(LAG(SUM(source_data.agg_AVM_sf), 12) OVER (PARTITION BY zip_code ORDER BY source_data.date),0) - 1 AS YoY_change_AVM_sf,

    SUM(source_data.agg_AVM_sf) / nullif(LAG(SUM(source_data.agg_AVM_mf), 1) OVER (PARTITION BY zip_code ORDER BY source_data.date),0) - 1 AS MoM_change_AVM_mf,
    SUM(source_data.agg_AVM_sf) / nullif(LAG(SUM(source_data.agg_AVM_mf), 3) OVER (PARTITION BY zip_code ORDER BY source_data.date),0) - 1 AS QoQ_change_AVM_mf,
    SUM(source_data.agg_AVM_sf) / nullif(LAG(SUM(source_data.agg_AVM_mf), 12) OVER (PARTITION BY zip_code ORDER BY source_data.date),0) - 1 AS YoY_change_AVM_mf,

    SUM(source_data.agg_AVM_sf) / nullif(LAG(SUM(source_data.agg_AVM_tot), 1) OVER (PARTITION BY zip_code ORDER BY source_data.date),0) - 1 AS MoM_change_AVM_tot,
    SUM(source_data.agg_AVM_sf) / nullif(LAG(SUM(source_data.agg_AVM_tot), 3) OVER (PARTITION BY zip_code ORDER BY source_data.date),0) - 1 AS QoQ_change_AVM_tot,
    SUM(source_data.agg_AVM_sf) / nullif(LAG(SUM(source_data.agg_AVM_tot), 12) OVER (PARTITION BY zip_code ORDER BY source_data.date),0) - 1 AS YoY_change_AVM_tot
    from source_data
    group by source_data.date, source_data.zip_code
    order by source_data.date, source_data.zip_code
) sub_pct_change
on sub.date = sub_pct_change.date
and sub.zip_code = sub_pct_change.zip_code
order by sub.date, sub.zip_code