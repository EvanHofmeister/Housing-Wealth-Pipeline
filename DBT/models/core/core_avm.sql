{{ config(materialized='view') }}

with source as (

    SELECT * from {{ source('core', 'stg_data') }};
)
SELECT * from source
Where index NOT NULL;



