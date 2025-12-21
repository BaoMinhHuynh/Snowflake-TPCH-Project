WITH stg_order_line_item AS (
    SELECT *
    FROM {{ref('stg_order_line_item')}}
),
    order_line_item_enrichment AS (
        SELECT *,
            extended_price * (1 - discount ) AS net_price,
            extended_price * (1 - discount ) * (1 + tax) AS final_price,
            DATEDIFF(day, commit_date, receipt_date) AS ship_delay_days
        FROM stg_order_line_item
    )
SELECT
    {{ dbt_utils.generate_surrogate_key(['order_key', 'line_number']) }} AS order_line_item_key,
    order_key,
    product_key,
    customer_key,
    supplier_key,
    {{ dbt_utils.generate_surrogate_key(['return_flag', 'order_line_status','ship_mode','ship_instruct']) }} AS order_line_fulfillment_key,
    {{ dbt_utils.generate_surrogate_key(['order_status', 'order_priority']) }} AS order_attribute_key,
    line_number,
    quantity,
    extended_price,
    discount,
    tax,
    supply_cost,
    net_price,
    final_price,
    ship_date,
    commit_date,
    receipt_date,  
    order_date,
    ship_delay_days,
    order_line_item_comment
FROM order_line_item_enrichment