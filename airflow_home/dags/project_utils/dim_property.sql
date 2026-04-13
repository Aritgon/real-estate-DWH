/*
    This task to create a dim property table in the gold layer of this project.

    In airflow worker scheduler, this script should run fast before the fact table creation to follow
    normal data warehouse work ethics.

    example workflow manager : api_fetch >> raw >> silver >> dim_property >> fact_real_estate
*/

create or replace table `{{ params.project_id }}.{{ params.gold_dataset_id }}.dim_property` (
    property_id INT64 not null,
    property_type STRING,
    residential_type STRING,

    primary key (project_id) not enforced
) AS
with cte (
    select
        distinct property_type, residential_type
    from `{{ params.project_id }}.{{ params.silver_dataset_id }}.real_estate_silver`
)
select 
    -- synthetic key as primary key.
    row_number() as (order by property_type, residential_type) as property_id,
    property_type,
    residential_type
from cte;