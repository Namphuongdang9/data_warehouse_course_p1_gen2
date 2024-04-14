WITH fact_sales_order__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, fact_sales_order__rename_column AS(
  SELECT
    order_id AS sales_order_key
    , customer_id as customer_key
    , picked_by_person_id AS picked_by_person_key
    , salesperson_person_id AS salesperson_person_key
    , contact_person_id AS contact_person_key
    , order_date
    , expected_delivery_date
    , customer_purchase_order_number AS number_of_customer_purchase
  FROM fact_sales_order__source
)

, fact_sales_order__cast_type AS(
  SELECT
    CAST(sales_order_key AS INTEGER) AS sales_order_key
    , CAST(customer_key AS INTEGER) AS customer_key
    , CAST(picked_by_person_key AS INTEGER) AS picked_by_person_key
    , CAST(salesperson_person_key AS INTEGER) AS salesperson_person_key
    , CAST(contact_person_key AS INTEGER) AS contact_person_key
    , CAST(order_date AS DATE) AS order_date
    , CAST(expected_delivery_date AS DATE) AS expected_delivery_date
    , CAST(number_of_customer_purchase AS INTEGER) AS number_of_customer_purchase
  FROM fact_sales_order__rename_column
) 

SELECT 
  sales_order_key
  , customer_key
  , COALESCE(picked_by_person_key,0) AS picked_by_person_key
  , COALESCE(salesperson_person_key,0) AS salesperson_person_key
  , COALESCE(contact_person_key,0) AS contact_person_key
  , order_date
  , expected_delivery_date
  , number_of_customer_purchase
FROM fact_sales_order__cast_type