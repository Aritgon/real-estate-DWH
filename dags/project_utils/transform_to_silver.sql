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

-- Using a more safer approach to create the silver table.
create or replace table `{{ params.project_id }}.{{ params.silver_dataset_id }}.real_estate_silver` (
    serial_number INT64 NOT NULL,
    list_year INT64 NOT NULL,
    date_recorded DATE NOT NULL,

    -- address related columns.
    town STRING,
    address STRING,

    -- numerical columns.
    assessed_value FLOAT64,
    sale_amount FLOAT64,
    sales_ratio FLOAT64,
    
    -- categorical columns.
    property_type STRING,
    residential_type STRING,

    -- row processing timestamp.
    process_timestamp TIMESTAMP
);

-- SCD type 1 implementation for the silver table to make sure we are not losing any data and also updating the existing records with the new data if there is any change in the data.
MERGE `{{ params.project_id }}.{{ params.silver_dataset_id }}.real_estate_silver` as main
USING (
  select
    safe_cast(serialnumber as INT64) as serial_number,
    safe_cast(listyear as INT64) as list_year,
    parse_date('%Y-%m-%d', LEFT(daterecorded, 10)) as date_recorded,

    trim(regexp_replace(lower(town), r'[^a-z0-9 ]', "")) as town,
    trim(regexp_replace(lower(address), r"[^a-z0-9 ]", "")) as address,

    round(safe_cast(assessedvalue as FLOAT64), 2) as assessed_value,
    round(safe_cast(saleamount as FLOAT64), 2) as sale_amount,
    round(safe_cast(salesratio as FLOAT64), 3) as sales_ratio,

    trim(ifnull(propertytype, "unspecified")) as property_type,
    trim(ifnull(residentialtype, "unspecified")) as residential_type,

    current_timestamp() as process_timestamp

  from `{{ params.project_id }}.{{ params.raw_dataset_id }}.{{ params.raw_table_id }}`

  -- putting filters to actually get good and clean data to the silver layer.
  -- putting business logics on both assessed value and sale amount to be between 2000 and 3 million as per the data distribution in the raw layer.
  -- putting filter on sales ratio with the same context.
  where safe_cast(assessedvalue as FLOAT64) is not null and safe_cast(assessedvalue as FLOAT64) between 2000 and 2250000
    and safe_cast(saleamount as FLOAT64) is not null and safe_cast(saleamount as FLOAT64) between 2000 and 3050000
    and safe_cast(salesratio as FLOAT64) is not null and safe_cast(salesratio as FLOAT64) between 0 and 1.4
    and parse_date('%Y-%m-%d', LEFT(daterecorded, 10)) between '2001-01-01' and '2025-01-01'
    and safe_cast(listyear as INT64) <= extract(year from (parse_date('%Y-%m-%d', LEFT(daterecorded, 10))))
) as temp

-- on most common columns.
on main.serial_number = temp.serial_number
  and main.list_year = temp.list_year
  and main.date_recorded = temp.date_recorded

-- when certain columns match but other columns have different values, then update the existing record with the new values.
when matched and (main.town != temp.town 
or main.address != temp.address 
or main.assessed_value != temp.assessed_value 
or main.sale_amount != temp.sale_amount
or main.sales_ratio != temp.sales_ratio 
or main.property_type != temp.property_type 
or main.residential_type != temp.residential_type)

-- then update the existing record with the new values from the temp table.
THEN UPDATE SET
  main.town = temp.town,
  main.address = temp.address,
  main.assessed_value = temp.assessed_value,
  main.sale_amount = temp.sale_amount,
  main.sales_ratio = temp.sales_ratio,
  main.property_type = temp.property_type,
  main.residential_type = main.residential_type

-- when there is no match found, then insert the new record to the silver table.
WHEN NOT MATCHED THEN
INSERT (serial_number, list_year, date_recorded, town, address, 
assessed_value, sale_amount, sales_ratio, property_type, residential_type)
VALUES (
  temp.serial_number,
  temp.list_year,
  temp.date_recorded,
  temp.town,
  temp.address,
  temp.assessed_value,
  temp.sale_amount,
  temp.sales_ratio,
  temp.property_type,
  temp.residential_type
);