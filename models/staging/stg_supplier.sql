WITH supplier__source AS (
    SELECT * 
    FROM {{ source('tpch_staging', 'SUPPLIER') }}
),
    supplier__rename AS (
        SELECT 
            S_SUPPKEY AS supplier_key,
            S_NAME AS supplier_name,
            S_ADDRESS AS supplier_address,
            S_NATIONKEY AS nation_key,
            S_PHONE AS supplier_phone,
            S_ACCTBAL AS supplier_acctbal,
            S_COMMENT AS supplier_comment
        FROM supplier__source
    ),
    supplier__cast AS (
        SELECT 
            CAST(supplier_key AS INTEGER) AS supplier_key,
            CAST(supplier_name AS STRING) AS supplier_name,
            CAST(supplier_address AS STRING) AS supplier_address,
            CAST(supplier_phone AS STRING) AS supplier_phone,
            CAST(nation_key AS INTEGER) AS nation_key,
            CAST(supplier_acctbal AS NUMERIC(12, 2)) AS supplier_acctbal,
            CAST(supplier_comment AS STRING) AS supplier_comment
        FROM supplier__rename
    )
SELECT 
    stg_supplier.supplier_key,
    stg_supplier.supplier_name,
    stg_supplier.supplier_address,
    stg_supplier.supplier_phone,
    stg_supplier.nation_key,
    stg_supplier.supplier_acctbal,
    stg_supplier.supplier_comment,
    COALESCE(stg_nation.nation_name, 'N/A') AS nation_name,
    COALESCE(stg_nation.nation_comment, 'N/A') AS nation_comment,
    stg_nation.region_name,
    stg_nation.region_comment
FROM supplier__cast AS stg_supplier
LEFT JOIN  {{ref('stg_nation')}} AS stg_nation 
ON stg_supplier.nation_key = stg_nation.nation_key
