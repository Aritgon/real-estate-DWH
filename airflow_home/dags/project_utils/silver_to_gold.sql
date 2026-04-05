/*
  --- Data warehouse (Star Schema) establishment ---

  dim_property -> property_id (PK, synthetic key), property_type, residential_type, master_property_type

  fact_real_estate -> serial_number (PK), list_year, recorded_year, town, address, property_id (FK), assessed_value, sale_amount, sales_ratio, sales_ratio_outlier_flag

*/

create or replace table `{{ params.project_id }}.{{ params.gold_dataset_id }}.dim_property` (
  property_id INT64 not null,
  property_type STRING,
  residential_type STRING,
  master_property_type STRING,

  primary key (property_id) not enforced
)
as
with r_types as (select distinct residential_type from `{{ params.project_id }}.{{ "silver_dataset_id" }}.real_estate_silver`),
p_types as (select distinct property_type from `{{ params.project_id }}.{{ "silver_dataset_id" }}.real_estate_silver`)
select
  row_number() over () as property_id,
  p.property_type,
  r.residential_type,
  case
    when p.property_type = r.residential_type then p.property_type -- any value.
    when p.property_type = 'unspecified' and r.residential_type != 'unspecified' then r.residential_type
    when p.property_type != 'unspecified' and r.residential_type = 'unspecified' then p.property_type
    else concat(p.property_type, ' - ', r.residential_type)
  end as master_property_type
from r_types as r
cross join p_types as p;


/*
  fact_real_estate procedures :- 

    - no more nulls from columns -> list_year, recorded_year, assessed_value, sale_amount and sales_ratio
    - sales_ratio between 0 and 1.4 as 'normal' data and beyond that will be marked as 'sales_ratio_outlier'

    -- for assessed_value and sale_amount (query the data profiling script inside transformation page), we are using percentile limit of 99 as max is becoming an outlier for the data
    - assessed_value between 2000 and 2250000
    - sale_amount between 2000 and 3025000

    -- list_year <= recorded_year (already done in raw layer to silver layer)

*/

-- drop table `project-9b661baf-6bda-4b5e-b47.real_estate_gold.fact_real_estate`;
create or replace table `{{ params.project_id }}.{{ params.gold_dataset_id }}.fact_real_estate` (
  serial_number INT64 not null,
  list_year INT64 not null,
  recorded_year DATE not null,
  town STRING,
  address STRING,

  property_id INT64, -- FK to dim_property

  assessed_value FLOAT64,
  sale_amount FLOAT64,

  sales_ratio FLOAT64,

  primary key (serial_number) not enforced,

  constraint fk_to_dim_property_property_id foreign key (property_id)
  references `{{ params.project_id }}.{{ params.gold_dataset_id }}.dim_property`(property_id) not enforced
)
cluster by town, property_id
as
select
  a.serial_number,
  a.list_year,
  a.recorded_year,
  a.town,
  a.address,

  b.property_id,

  a.assessed_value,
  a.sale_amount,
  
  round(safe_divide(a.assessed_value, a.sale_amount), 3) as sales_ratio
from `{{ params.project_id }}.{{ "silver_dataset_id" }}.real_estate_silver` as a
join `{{ params.project_id }}.{{ params.gold_dataset_id }}.dim_property` as b
  on trim(a.property_type) = trim(b.property_type)
  and trim(a.residential_type) = trim(b.residential_type)
where
  a.assessed_value between 2000 and 2250000
  and a.sale_amount between 2000 and 3050000
  and safe_divide(a.assessed_value, a.sale_amount) between 0 and 1.4;
