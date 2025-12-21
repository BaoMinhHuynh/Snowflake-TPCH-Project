WITH region__source AS (
    SELECT * 
    FROM {{ source('tpch_staging', 'REGION') }}
),
    region__rename AS (
        SELECT 
            R_REGIONKEY AS region_key,
            R_NAME AS region_name,
            R_COMMENT AS region_comment
        FROM region__source
    ),
    region__cast AS (
        SELECT
            CAST(region_key AS INTEGER) AS region_key,
            CAST(region_name AS STRING) AS region_name,
            CAST(region_comment AS STRING) AS region_comment
        FROM region__rename
    )
SELECT 
    region_key,
    region_name,
    region_comment
FROM region__cast