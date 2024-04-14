WITH dim_person__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.application__people`
)
, dim_person__rename_column AS (
  SELECT
    person_id AS person_key
    , full_name
    , preferred_name
    , search_name
    , is_employee AS is_employee_boolean
    , is_salesperson AS is_salesperson_boolean
    , phone_number
    , fax_number
    , email_address
  FROM dim_person__source
)

, dim_person__cast_type AS (
  SELECT
    CAST(person_key AS INTEGER) AS person_key
    , CAST(full_name AS STRING) AS full_name
    , CAST(preferred_name AS STRING) AS preferred_name 
    , CAST(search_name AS STRING) AS search_name 
    , CAST(is_employee_boolean AS BOOLEAN) AS is_employee_boolean 
    , CAST(is_salesperson_boolean AS BOOLEAN) AS is_salesperson_boolean 
    , CAST(phone_number AS STRING) AS phone_number 
    , CAST(fax_number AS STRING) AS fax_number 
    , CAST(email_address AS STRING) AS email_address 
  FROM dim_person__rename_column
)

, dim_person__convert_boolean AS (
  SELECT
    *
    , CASE 
      WHEN is_employee_boolean IS TRUE THEN 'Employee'
      WHEN is_employee_boolean IS FALSE THEN 'Not Employee'
      WHEN is_employee_boolean IS NULL THEN 'Undefined'
      ELSE 'Invalid'
      END AS is_employee
      , CASE 
      WHEN is_salesperson_boolean IS TRUE THEN 'Sales Person'
      WHEN is_salesperson_boolean IS FALSE THEN 'Not Sales Person Hold'
      WHEN is_salesperson_boolean IS NULL THEN 'Undefined'
      ELSE 'Invalid'
      END AS is_salesperson
  FROM dim_person__cast_type
) 

, dim_person__add_undefined_record AS (
  SELECT 
    person_key
    , full_name
    , preferred_name
    , search_name
    , is_employee
    , is_salesperson
    , phone_number
    , fax_number
    , email_address
  FROM dim_person__convert_boolean

  UNION ALL
  SELECT
    0 AS person_key
    , 'Undefined' AS full_name
    , 'Undefined' AS preferred_name
    , 'Undefined' AS search_name
    , 'Undefined' AS is_employee
    , 'Undefined' AS is_salesperson
    , 'Undefined' AS phone_number
    , 'Undefined' AS fax_number
    , 'Undefined' AS email_address
  UNION ALL
  SELECT
    -1 AS person_key
    , 'Error' AS full_name
    , 'Error' AS preferred_name
    , 'Error' AS search_name
    , 'Error' AS is_employee
    , 'Error' AS is_salesperson
    , 'Error' AS phone_number
    , 'Error' AS fax_number
    , 'Error' AS email_address
) 

SELECT
    person_key
    , full_name
    , preferred_name
    , search_name
    , is_employee
    , is_salesperson
    , phone_number
    , fax_number
    , email_address
FROM dim_person__add_undefined_record