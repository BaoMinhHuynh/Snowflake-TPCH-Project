WITH stg_product AS (
    SELECT *
    FROM {{ref('stg_product')}}
)
SELECT 
    product_key,
    product_name,
    mfgr,
    brand,
    product_type,
    product_size,
    container,
    retail_price,
    product_comment
FROM stg_product