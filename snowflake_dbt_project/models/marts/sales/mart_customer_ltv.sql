WITH order_level AS (
    SELECT
        order_key,
        customer_key,
        order_date,
        sum(final_price) AS order_value
    FROM {{ref('fact_order_line_item')}}
    GROUP BY 
        order_key,
        customer_key,
        order_date
),
customer_metrics AS (
    SELECT 
        customer_key,
        COUNT(DISTINCT order_key) AS total_order,
        ROUND(SUM(order_value),2) AS total_spent,
        ROUND(AVG(order_value),2) AS avg_order_value,
        MAX(order_date) AS last_order_date,
        MIN(order_date) AS first_order_date,
        DATEDIFF(day, MIN(order_date), MAX(order_date)) AS customer_tenure_days,
        MAX(order_date) >= DATEADD(day, -90, (SELECT MAX(order_date) FROM order_level)) AS is_active
    FROM order_level
    GROUP BY customer_key
)
SELECT 
    cus_metrics.customer_key,
    dim_cus.customer_name,
    dim_cus.nation_name,
    dim_cus.region_name,
    dim_cus.customer_mktsegment,
    cus_metrics.total_order,
    cus_metrics.total_spent,
    cus_metrics.avg_order_value,
    cus_metrics.first_order_date,
    cus_metrics.last_order_date,
    cus_metrics.customer_tenure_days,
    CASE
        WHEN cus_metrics.total_spent > 500000 THEN 'VIP'
        WHEN cus_metrics.total_spent > 200000 THEN 'GOLD'
        WHEN cus_metrics.total_spent > 100000 THEN 'SILVER'
        WHEN cus_metrics.total_spent > 50000 THEN 'BRONZE'
        ELSE 'STANDARD'
    END AS customer_tier,
    is_active
FROM customer_metrics AS cus_metrics 
LEFT JOIN {{ref('dim_customer')}} AS dim_cus 
ON cus_metrics.customer_key = dim_cus.customer_key
        