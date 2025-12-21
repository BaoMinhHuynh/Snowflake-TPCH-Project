WITH order__source AS (
    SELECT * 
    FROM {{ source('tpch_staging', 'ORDERS') }}
),
    order__rename AS (
        SELECT 
            O_ORDERKEY AS order_key,
            O_CUSTKEY AS customer_key,
            O_ORDERSTATUS AS order_status,
            O_TOTALPRICE AS total_price,
            O_ORDERDATE AS order_date,
            O_ORDERPRIORITY AS order_priority,
            O_CLERK AS clerk,
            O_SHIPPRIORITY AS ship_priority,
            O_COMMENT AS order_comment
        FROM order__source
    ),
    order__cast AS (
        SELECT
            CAST(order_key AS INTEGER) AS order_key,
            CAST(customer_key AS INTEGER) AS customer_key,
            CAST(order_status AS STRING) AS order_status,
            CAST(total_price AS NUMERIC(12, 2)) AS total_price,
            CAST(order_date AS DATE) AS order_date,
            CAST(order_priority AS STRING) AS order_priority,
            CAST(clerk AS STRING) AS clerk,
            CAST(ship_priority AS INTEGER) AS ship_priority,
            CAST(order_comment AS STRING) AS order_comment
        FROM order__rename
    )
SELECT 
    order_key,
    customer_key,
    order_status,
    total_price,
    order_date,
    order_priority,
    clerk,
    CAST(SPLIT_PART(clerk, '#', 2) AS STRING) AS o_clerk_id,
    ship_priority,
    order_comment
FROM order__cast