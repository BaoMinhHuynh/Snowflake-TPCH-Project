WITH customer__source AS (
    SELECT * 
    FROM {{ source('tpch_staging', 'CUSTOMER') }}
),
    customer__rename AS (
        SELECT 
            C_CUSTKEY AS customer_key,
            C_NAME AS customer_name,
            C_ADDRESS AS customer_address,
            C_NATIONKEY AS nation_key,
            C_PHONE AS customer_phone,
            C_ACCTBAL AS customer_acctbal,
            C_MKTSEGMENT AS customer_mktsegment,
            C_COMMENT AS customer_comment
        FROM customer__source
    ),
    customer__cast AS (
        SELECT 
            CAST(customer_key AS INTEGER) AS customer_key,
            CAST(customer_name AS STRING) AS customer_name,
            CAST(customer_address AS STRING) AS customer_address,
            CAST(customer_phone AS STRING) AS customer_phone,
            CAST(nation_key AS INTEGER) AS nation_key,
            CAST(customer_acctbal AS NUMERIC(12, 2)) AS customer_acctbal,
            CAST(customer_mktsegment AS STRING) AS customer_mktsegment,
            CAST(customer_comment AS STRING) AS customer_comment
        FROM customer__rename
    )
SELECT 
    stg_customer.customer_key,
    stg_customer.customer_name,
    stg_customer.customer_address,
    stg_customer.customer_phone,
    stg_customer.nation_key,
    stg_customer.customer_acctbal,
    stg_customer.customer_mktsegment,
    stg_customer.customer_comment,
    COALESCE(stg_nation.nation_name, 'N/A') AS nation_name,
    COALESCE(stg_nation.nation_comment, 'N/A') AS nation_comment,
    stg_nation.region_name,
    stg_nation.region_comment
FROM customer__cast AS stg_customer
LEFT JOIN  {{ref('stg_nation')}} AS stg_nation 
ON stg_customer.nation_key = stg_nation.nation_key