with source as (
dbt run-operation generate_base_model --args '{"source_name": "bronze_layer", "table_name": "raw_hris_timesheets_v1"}'
),
staged as (
    select * from source
)
select * from staged