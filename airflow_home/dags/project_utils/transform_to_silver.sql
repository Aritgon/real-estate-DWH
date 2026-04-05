-- initiation of silver layer --
/*
  This script will create a separate table at dataset ID 'real_estate_silver' with mentioned columns.

  For this project, we are removing following columns - 
    
    - opmremarks -> doesn't really help in identifying any character related to the data
    - assessorremarks -> Mostly null and same as OPM_remarks
    - nonusecode -> mostly null and doesn't mirror a value related to the data.
    - computed bigquery columns -> 100% null and not needed.

  More data cleaning, to be continued at silver layer.
*/

create or replace table `{{ params.project_id }}.{{ params.silver_dataset_id }}.real_estate_silver` AS
select
  safe_cast(serialnumber as INT64) as serial_number,
  safe_cast(listyear as INT64) as list_year,
  parse_date('%Y-%m-%d', LEFT(daterecorded, 10)) as recorded_year,

  trim(regexp_replace(lower(town), r'[^a-z0-9 ]', "")) as town,
  trim(regexp_replace(lower(address), r"[^a-z0-9 ]", "")) as address,

  safe_cast(assessedvalue as FLOAT64) as assessed_value,
  safe_cast(saleamount as FLOAT64) as sale_amount,
  safe_cast(salesratio as FLOAT64) as sales_ratio,

  lower(ifnull(propertytype, "unspecified")) as property_type,
  lower(ifnull(residentialtype, "unspecified")) as residential_type,

  -- creating a separate timestamp column to check which column got processed at what time.
  current_timestamp() as process_timestmp
  
from `{{ params.project_id }}.{{ params.raw_dataset_id }}.real_estate_raw`

-- filters

where assessedvalue is not null and saleamount is not null
  and safe_cast(assessedvalue as FLOAT64) > 0 and safe_cast(saleamount as FLOAT64) > 0
  -- filtering date.
  and parse_date('%Y-%m-%d', LEFT(daterecorded, 10)) between '2001-01-01' and current_date()
  and safe_cast(listyear as INT64) <= extract(year from (parse_date('%Y-%m-%d', LEFT(daterecorded, 10))));