WITH order_line_item__source AS (
    SELECT *
    FROM {{ source('tpch_staging', 'LINEITEM') }}
),
    order_line_time__rename AS (
        SELECT 
            L_ORDERKEY AS order_key,
            L_PARTKEY AS product_key,
            L_SUPPKEY AS supplier_key,
            L_LINENUMBER AS line_number,
            L_QUANTITY AS quantity,
            L_EXTENDEDPRICE AS extended_price,
            L_DISCOUNT AS discount,
            L_TAX AS tax, 
            L_RETURNFLAG AS return_flag,
            L_LINESTATUS AS order_line_status,
            L_SHIPDATE AS ship_date,
            L_COMMITDATE AS commit_date,
            L_RECEIPTDATE AS receipt_date,
            L_SHIPINSTRUCT AS ship_instruct,
            L_SHIPMODE AS ship_mode,
            L_COMMENT AS order_line_item_comment
        FROM order_line_item__source
    ),
    order_line_item__cast AS(
        SELECT
            CAST(order_key AS INTEGER) AS order_key,
            CAST(product_key AS INTEGER) AS product_key,
            CAST(supplier_key AS INTEGER) AS supplier_key,
            CAST(line_number AS INTEGER) AS line_number,
            CAST(quantity AS NUMERIC(12, 2)) AS quantity,
            CAST(extended_price AS NUMERIC(12,2)) AS extended_price,
            CAST(discount AS NUMERIC(12,2)) AS discount,
            CAST(tax AS NUMERIC(12,2)) AS tax,
            CAST(return_flag AS STRING) AS return_flag,
            CAST(order_line_status AS STRING) AS order_line_status,
            CAST(ship_date AS DATE) AS ship_date,
            CAST(commit_date AS DATE) AS commit_date,
            CAST(receipt_date AS DATE) AS receipt_date,
            CAST(ship_instruct AS STRING) AS ship_instruct,
            CAST(ship_mode AS STRING) AS ship_mode,
            CAST(order_line_item_comment AS STRING) AS order_line_item_comment
        FROM order_line_time__rename
    )
SELECT 
    stg_order_line_item.order_key,
    stg_order_line_item.product_key,
    stg_order_line_item.supplier_key,
    stg_order_line_item.line_number,
    stg_order_line_item.quantity,
    stg_order_line_item.extended_price,
    stg_order_line_item.discount,
    stg_order_line_item.tax,
    stg_order_line_item.return_flag,
    stg_order_line_item.order_line_status,
    stg_order_line_item.ship_date,
    stg_order_line_item.commit_date,
    stg_order_line_item.receipt_date,
    stg_order_line_item.ship_instruct,
    stg_order_line_item.ship_mode,
    stg_order_line_item.order_line_item_comment,
    stg_order.customer_key,
    stg_order.order_date,
    stg_order.order_status,
    stg_order.order_priority,
    stg_product_supplier.supply_cost
FROM order_line_item__cast AS stg_order_line_item 
LEFT JOIN {{ref('stg_order')}}  AS stg_order
ON stg_order_line_item.order_key = stg_order.order_key
LEFT JOIN {{ref('stg_product_supplier')}} AS stg_product_supplier
ON stg_order_line_item.product_key = stg_product_supplier.product_key
AND stg_order_line_item.supplier_key = stg_product_supplier.supplier_key
