WITH dim_bill_to_customer__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_bill_to_customer__rename_column AS(
  SELECT
    bill_to_customer_id AS bill_to_customer_key
    , customer_name AS bill_to_customer_name
  FROM dim_bill_to_customer__source
)

, dim_bill_to_customer__cast_type AS(
  SELECT
    CAST(bill_to_customer_key AS INTEGER) AS bill_to_customer_key
    , CAST(bill_to_customer_name AS STRING) AS bill_to_customer_name
  FROM dim_bill_to_customer__rename_column
) 

, dim_bill_to_customer__add_undefined_record AS(
  SELECT 
    bill_to_customer_key
    , bill_to_customer_name
  FROM dim_bill_to_customer__cast_type

  UNION ALL
  SELECT 
    0 AS bill_to_customer_key
    , 'Undefined' AS bill_to_customer_name

  UNION ALL
  SELECT 
    -1 AS bill_to_customer_key
    , 'Invalid' AS bill_to_customer_name
) 

SELECT 
  bill_to_customer_key
  , bill_to_customer_name
FROM dim_bill_to_customer__add_undefined_record