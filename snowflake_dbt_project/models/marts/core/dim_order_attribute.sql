WITH stg_order AS (
    SELECT * 
    FROM {{ref('stg_order')}}
),
    order_dirived AS (
        SELECT
            order_status,
            CASE
                WHEN order_status = 'O' THEN 'Open'
                WHEN order_status = 'F' THEN 'Filled'
                WHEN order_status = 'P' THEN 'Partial'
                ELSE 'N/A'
            END AS order_status_desc,
            order_priority,
            CASE
                WHEN order_priority = '1-URGENT' THEN 1
                WHEN order_priority = '2-HIGH' THEN 2
                WHEN order_priority = '3-MEDIUM' THEN 3
                WHEN order_priority = '4-NOT SPECIFIED' THEN 4
                WHEN order_priority = '5-LOW' THEN 5 
                ELSE -1    
            END AS order_priority_rank,
            ship_priority
        FROM stg_order
    )

SELECT 
    {{ dbt_utils.generate_surrogate_key(['order_status', 'order_priority']) }} AS order_attribute_key,
    a.order_status,
    a.order_status_desc,
    b.order_priority,
    b.order_priority_rank,
    c.ship_priority
FROM (SELECT DISTINCT order_status, order_status_desc FROM order_dirived) AS a
CROSS JOIN (SELECT DISTINCT order_priority, order_priority_rank FROM order_dirived) AS b
CROSS JOIN (SELECT DISTINCT ship_priority  FROM order_dirived) AS c