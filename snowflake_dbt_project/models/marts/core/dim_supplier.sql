WITH stg_supplier AS (
    SELECT * 
    FROM {{ref('stg_supplier')}}
)
SELECT 
    supplier_key,
    supplier_name,
    supplier_address,
    supplier_phone,
    nation_key,
    supplier_acctbal,
    supplier_comment,
    nation_name,
    nation_comment,
    region_name,
    region_comment
FROM stg_supplier

