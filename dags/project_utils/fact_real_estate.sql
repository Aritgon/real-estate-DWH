/*
    Script to create the fact real estate table.
    This script to be executed after the dim_property script.
*/

create or replace table `{{ params.project_id }}.{{ params.gold_dataset_id }}.fact_real_estate` (
  utid INT64, -- synthetic data to recognize each row in fact table.
  serial_number INT64 not null,
  list_year INT64 not null,
  date_recorded DATE not null,
  town STRING,
  address STRING,

  property_id INT64, -- FK to dim_property

  assessed_value FLOAT64,
  sale_amount FLOAT64,

  sales_ratio FLOAT64,

  primary key (utid) not enforced,

  constraint fk_to_dim_property_property_id foreign key (property_id)
  references `{{ params.project_id }}.{{ params.gold_dataset_id }}.dim_property`(property_id) not enforced
);

-- STEP 2 : Implementing MERGE operations in the fact Real Estate table for continuing data ingestion operation.
-- main table from silver layer.
MERGE `{{ params.project_id }}.{{ params.gold_dataset_id }}.fact_real_estate` as T
USING (
select
  FARM_FINGERPRINT(concat(safe_cast(a.serial_number as STRING), safe_cast(a.list_year as STRING), town)) as utid,
  a.serial_number,
  a.list_year,
  a.date_recorded,
  a.town,
  a.address,

  b.property_id,

  a.assessed_value,
  a.sale_amount,
  
  round(safe_divide(a.assessed_value, a.sale_amount), 3) as sales_ratio
from `{{ params.project_id }}.{{ params.silver_dataset_id }}.real_estate_silver` as a
join `{{ params.project_id }}.{{ params.gold_dataset_id }}.dim_property` as b
  on a.property_type = b.property_type and a.residential_type = b.residential_type
) as S

-- join operation on the most important columns.
ON T.serial_number = S.serial_number
AND T.list_year = S.list_year
AND T.date_recorded = S.date_recorded

-- update policy setup if column policy is matching (key columns matches but desc. data is mismatched)
WHEN MATCHED AND (T.town != S.town or T.address != S.address 
or T.assessed_value != S.assessed_value or T.sale_amount != S.sale_amount) THEN
  UPDATE SET
  T.town = S.town,
  T.address = S.address,
  T.assessed_value = S.assessed_value,
  T.sale_amount = S.sale_amount

-- update policy if new data arrives.
WHEN NOT MATCHED THEN
  INSERT (utid, serial_number, list_year, date_recorded, town, address, 
  property_id, assessed_value, sale_amount, sales_ratio)
  VALUES (
    FARM_FINGERPRINT(concat(safe_cast(S.serial_number as STRING), safe_cast(S.list_year as STRING), S.town)),
    S.serial_number,
    S.list_year,
    S.date_recorded,
    S.town,
    S.address,
    S.property_id,
    S.assessed_value,
    S.sale_amount,
    S.sales_ratio
  );