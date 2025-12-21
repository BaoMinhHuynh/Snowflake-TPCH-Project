WITH stg_customer AS (
    SELECT *
    FROM {{ref('stg_customer')}}
)
SELECT 
    customer_key,
    customer_name,
    customer_address,
    customer_phone,
    nation_key,
    customer_acctbal,
    customer_mktsegment,
    customer_comment,
    nation_name,
    nation_comment,
    region_name,
    region_comment 
FROM stg_customer