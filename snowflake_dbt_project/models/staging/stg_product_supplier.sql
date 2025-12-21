WITH product_supplier__source AS (
    SELECT * 
    FROM {{ source('tpch_staging', 'PARTSUPP') }}
),
    product_supplier__rename AS (
        SELECT 
            PS_PARTKEY AS product_key,
            PS_SUPPKEY AS supplier_key,
            PS_AVAILQTY AS availqty,
            PS_SUPPLYCOST AS supply_cost,
            PS_COMMENT AS product_supplier_comment
        FROM product_supplier__source
    ),
    product_supplier__cast AS (
        SELECT
            CAST(product_key AS INTEGER) AS product_key,
            CAST(supplier_key AS INTEGER) AS supplier_key,
            CAST(availqty AS INTEGER) AS availqty,
            CAST(supply_cost AS NUMERIC(12, 2)) AS supply_cost,
            CAST(product_supplier_comment AS STRING) AS product_supplier_comment
        FROM product_supplier__rename
    )
SELECT 
    product_key,
    supplier_key,
    availqty,
    supply_cost,
    product_supplier_comment
FROM product_supplier__cast