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
            P_COMMENT AS product_comment 
        FROM product__source
    ),
    product__cast AS (
        SELECT 
            CAST(product_key AS INTEGER) AS product_key,
            CAST(product_name AS STRING) AS product_name,
            CAST(mfgr AS STRING) AS mfgr,
            CAST(product_type AS STRING) AS product_type,
            CAST(product_size AS INTEGER) AS product_size,
            CAST(container AS STRING) AS container,
            CAST(product_comment AS STRING) AS product_comment
        FROM product__rename
    )
SELECT 
    product_key,
    product_name,
    mfgr,
    product_type,
    product_size,
    container,
    product_comment
FROM product__cast