/*
    This task to create a dim property table in the gold layer of this project.

    In airflow worker scheduler, this script should run first before fact table to follow
    normal data warehouse work ethics.

    example workflow manager : api_fetch >> raw >> silver >> dim_property >> fact_real_estate
*/

create or replace table `{{ params.project_id }}.{{ params.gold_dataset_id }}.dim_property` (
    property_id INT64 not null,
    property_type STRING,
    residential_type STRING,

    -- primary key.
    primary key (property_id) not enforced
);

-- MERGE operations for smoother data ingestions and data quality.
MERGE `{{ params.project_id }}.{{ params.gold_dataset_id }}.dim_property` as P
USING (
    with cte as (
    select
        distinct property_type, residential_type
    from `{{ params.project_id }}.{{ params.silver_dataset_id }}.real_estate_silver`)
select 
    -- synthetic key as primary key.
    FARM_FINGERPRINT(concat(safe_cast(property_type as STRING), safe_cast(residential_type as STRING))),
    property_type,
    residential_type
from cte
) as S
-- joining columns.
ON P.property_type = S.property_type and P.residential_type = S.residential_type

-- Update policy when columns match.
WHEN MATCHED THEN
    UPDATE SET
    P.property_type = S.property_type,
    P.residential_type = S.residential_type

WHEN NOT MATCHED THEN
INSERT (property_id, property_type, residential_type)
values (
    FARM_FINGERPRINT(concat(safe_cast(S.property_type as STRING), safe_cast(S.residential_type as STRING))),
    S.property_type,
    S.residential_type
);

