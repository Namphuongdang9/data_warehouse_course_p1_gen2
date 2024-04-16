WITH dim_is_undersupply_backorder AS (
  SELECT
    TRUE AS is_backordered_boolean
    , 'Undersupply Backordered' AS is_undersupply_backordered

  UNION ALL 
  SELECT
    FALSE AS is_backordered_boolean
    , 'Not Undersupply Backordered' AS is_undersupply_backordered 
)

SELECT 
  * 
FROM dim_is_undersupply_backorder
CROSS JOIN {{ref('stg_dim_package_type')}} AS dim_package_type
ORDER BY 1,3