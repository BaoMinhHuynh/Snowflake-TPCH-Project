WITH order_level AS (
    SELECT
        order_key,
        customer_key,
        order_date AS summary_date,
        sum(final_price) AS order_value
    FROM {{ref('fact_order_line_item')}}
    GROUP BY 
        order_key,
        customer_key,
        summary_date
)
SELECT 
    summary_date,
    YEAR(summary_date) AS order_year,
    MONTH(summary_date) AS order_month,
    QUARTER(summary_date) AS order_quarter,
    COUNT(DISTINCT order_key) AS total_order,
    COUNT(DISTINCT customer_key) AS total_customer,
    ROUND(SUM(order_value), 2) AS total_revenue,
    ROUND(AVG(order_value), 2) AS avg_order_value,
    ROUND(MIN(order_value), 2) AS min_order_value,
    ROUND(MAX(order_value), 2) AS max_order_value,
FROM order_level
GROUP BY
    summary_date,
    order_year,
    order_month,
    order_quarter
ORDER BY summary_date