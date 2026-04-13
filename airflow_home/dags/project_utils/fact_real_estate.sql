/*
    Script to create the fact real estate table.

    This script to be executed after the dim_property script.

*/

create or replace table `{{ params.project_id }}.{{ params.gold_dataset_id }}.fact_real_estate` (
  unique_transaction_id INT64, -- synthetic data to recognize each row in fact table.
  serial_number INT64 not null,
  list_year INT64 not null,
  recorded_year DATE not null,
  town STRING,
  address STRING,

  property_id INT64, -- FK to dim_property

  assessed_value FLOAT64,
  sale_amount FLOAT64,

  sales_ratio FLOAT64,

  primary key (unique_transaction_id) not enforced,

  constraint fk_to_dim_property_property_id foreign key (property_id)
  references `{{ params.project_id }}.{{ params.gold_dataset_id }}.dim_property`(property_id) not enforced
)
cluster by town, property_id
as
select
  -- unique ID for each row. (x) removed generate_UUID() because of an coerce issue from google API
  -- GENERATE_UUID() as unique_transaction_id,
  row_number() over() as unique_transaction_id, -- using window function

  a.serial_number,
  a.list_year,
  a.recorded_year,
  a.town,
  a.address,

  b.property_id,

  a.assessed_value,
  a.sale_amount,
  
  round(safe_divide(a.assessed_value, a.sale_amount), 3) as sales_ratio
from `{{ params.project_id }}.{{ params.silver_dataset_id }}.real_estate_silver` as a
join `{{ params.project_id }}.{{ params.gold_dataset_id }}.dim_property` as b
  on trim(a.property_type) = trim(b.property_type)
  and trim(a.residential_type) = trim(b.residential_type)
where
  a.assessed_value between 2000 and 2250000
  and a.sale_amount between 2000 and 3050000
  and safe_divide(a.assessed_value, a.sale_amount) between 0 and 1.4;