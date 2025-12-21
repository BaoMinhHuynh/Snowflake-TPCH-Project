WITH product__source AS (
    SELECT * 
    FROM {{ source('tpch_staging', 'PART') }}
),
    product__rename AS (
        SELECT 
            P_PARTKEY AS product_key,
            P_NAME AS product_name,
            P_MFGR AS mfgr,
            P_BRAND AS brand,
            P_TYPE AS product_type,
            P_SIZE AS product_size,
            P_CONTAINER AS container,
            P_RETAILPRICE AS retail_price,
            P_COMMENT AS product_comment 
        FROM product__source
    ),
    product__cast AS (
        SELECT 
            CAST(product_key AS INTEGER) AS product_key,
            CAST(product_name AS STRING) AS product_name,
            CAST(mfgr AS STRING) AS mfgr,
            CAST(brand AS STRING) AS brand,
            CAST(product_type AS STRING) AS product_type,
            CAST(product_size AS INTEGER) AS product_size,
            CAST(retail_price AS NUMERIC(12,2)) AS retail_price,
            CAST(container AS STRING) AS container,
            CAST(product_comment AS STRING) AS product_comment
        FROM product__rename
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
FROM product__cast