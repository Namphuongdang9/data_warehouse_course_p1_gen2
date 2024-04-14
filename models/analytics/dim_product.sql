WITH dim_product__source AS(
  SELECT * 
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

, dim_product__rename_column AS(
  SELECT 
  stock_item_id	AS product_key
  , stock_item_name	AS product_name
  , brand	AS brand_name
  , supplier_id AS supplier_key
  , is_chiller_stock AS is_chiller_stock_boolean
  , color_id AS product_color_key
  , unit_package_id AS unit_package_type_key
  , outer_package_id AS outer_package_type_key
  , size
  , tax_rate
  , unit_price
  , recommended_retail_price
  , typical_weight_per_unit
FROM dim_product__source
)

, dim_product__cast_type AS(
SELECT 
  CAST(product_key AS INTEGER) AS product_key
  , CAST(product_name AS STRING) AS product_name
  , CAST(brand_name AS STRING)	AS brand_name
  , CAST(supplier_key AS INTEGER) AS supplier_key
  , CAST(is_chiller_stock_boolean AS BOOLEAN) AS is_chiller_stock_boolean
  , CAST(product_color_key AS INTEGER) AS product_color_key
  , CAST(unit_package_type_key AS INTEGER) AS unit_package_type_key
  , CAST(outer_package_type_key AS INTEGER) AS outer_package_type_key
  , CAST(size AS STRING) AS size
  , CAST(tax_rate AS FLOAT64) AS tax_rate
  , CAST(unit_price AS FLOAT64) AS unit_price
  , CAST(recommended_retail_price AS FLOAT64) AS recommended_retail_price
  , CAST(typical_weight_per_unit AS FLOAT64) AS typical_weight_per_unit
FROM dim_product__rename_column
)

, dim_product__convert_boolean AS(
  SELECT
    *
    , CASE 
      WHEN is_chiller_stock_boolean IS TRUE THEN 'Chiller Stock'
      WHEN is_chiller_stock_boolean IS FALSE THEN 'Not Chiller Stock'
      WHEN is_chiller_stock_boolean IS NULL THEN 'Undefined'
      ELSE 'Invalid' END
     AS is_chiller_stock
  FROM dim_product__cast_type
)

SELECT
  dim_product.product_key
  , dim_product.product_name
  , dim_product.is_chiller_stock
  , COALESCE(dim_product.brand_name, 'Undefined') AS brand_name
  , dim_product.supplier_key
  , COALESCE(dim_supplier.supplier_name, 'Invalid') AS supplier_name
  , dim_supplier.supplier_category_key
  , dim_supplier.supplier_category_name
  , dim_supplier.delivery_method_key
  , dim_supplier.delivery_method_name
  , dim_supplier.delivery_city_key
  , dim_supplier.delivery_city_name
  , dim_supplier.delivery_state_province_key
  , dim_supplier.delivery_state_province_name
  , dim_supplier.postal_city_key
  , dim_supplier.postal_city_name
  , dim_supplier.postal_state_province_key
  , dim_supplier.postal_state_province_name
  , dim_product.product_color_key
  , dim_color.color_name AS product_color_name
  , dim_product.unit_package_type_key
  , dim_unit_package_type.package_type_name AS unit_package_type_name
  , dim_product.outer_package_type_key
  , dim_outer_package_type.package_type_name AS outer_package_type_name
  , dim_product.size
  , dim_product.tax_rate
  , dim_product.unit_price
  , dim_product.recommended_retail_price
  , dim_product.typical_weight_per_unit
FROM dim_product__convert_boolean AS dim_product
LEFT JOIN {{ ref('dim_supplier') }} AS dim_supplier
  ON dim_product.supplier_key = dim_supplier.supplier_key
LEFT JOIN {{ ref('stg_dim_color') }} AS dim_color
  ON dim_product.product_color_key = dim_color.color_key
LEFT JOIN {{ ref('stg_dim_package_type') }} AS dim_unit_package_type
  ON dim_product.unit_package_type_key = dim_unit_package_type.package_type_key
LEFT JOIN {{ ref('stg_dim_package_type') }} AS dim_outer_package_type
  ON dim_product.outer_package_type_key = dim_outer_package_type.package_type_key