WITH stg_order_line_item AS (
    SELECT *
    FROM {{ref('stg_order_line_item')}}
),
    order_line_item__derived AS (
        SELECT 
            return_flag,
            CASE 
                WHEN return_flag = 'R' THEN 'Returned'
                WHEN return_flag = 'A' THEN 'Accepted'
                WHEN return_flag = 'N' THEN 'No Return'
                ELSE 'N/A'
            END AS return_flag_desc,
            order_line_status,
            CASE 
                WHEN order_line_status = 'O' THEN 'Open'
                WHEN order_line_status = 'F' THEN 'Fulfilled'
                ELSE 'N/A'
            END AS order_line_status_desc,
            ship_mode,
            ship_instruct
        FROM stg_order_line_item
    )
SELECT
    {{ dbt_utils.generate_surrogate_key(['return_flag', 'order_line_status','ship_mode','ship_instruct']) }} AS order_line_fulfillment_key,
    a.return_flag,
    a.return_flag_desc,
    b.order_line_status,
    b.order_line_status_desc,
    c.ship_mode,
    d.ship_instruct
FROM (SELECT DISTINCT return_flag, return_flag_desc FROM order_line_item__derived ) AS a
CROSS JOIN (SELECT DISTINCT order_line_status, order_line_status_desc FROM order_line_item__derived ) AS b
CROSS JOIN (SELECT DISTINCT ship_mode FROM order_line_item__derived ) AS c  
CROSS JOIN (SELECT DISTINCT ship_instruct FROM order_line_item__derived ) AS d