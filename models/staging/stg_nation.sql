WITH nation__source AS (
    SELECT * 
    FROM {{ source('tpch_staging', 'NATION') }}
),
    nation__rename AS (
        SELECT 
            N_NATIONKEY AS nation_key,
            N_REGIONKEY AS region_key,
            N_NAME AS nation_name,
            N_COMMENT AS nation_comment
        FROM nation__source
    ),
    nation__cast AS (
        SELECT
            CAST(nation_key AS INTEGER) AS nation_key,
            CAST(nation_name AS STRING) AS nation_name,
            CAST(region_key AS INTEGER) AS region_key,
            CAST(nation_comment AS STRING) AS nation_comment
        FROM nation__rename
    )
SELECT 
    stg_nation.nation_key,
    stg_nation.nation_name,
    stg_nation.region_key,
    stg_nation.nation_comment,
    COALESCE(stg_region.region_name,'N/A') AS region_name,
    COALESCE(stg_region.region_comment,'N/A') AS region_comment
FROM nation__cast AS stg_nation
LEFT JOIN {{ref('stg_region')}} AS stg_region
ON stg_nation.region_key = stg_region.region_key