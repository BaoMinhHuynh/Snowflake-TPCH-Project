WITH date_spine AS (
    -- Gọi macro của dbt_utils để tạo dòng từ 1990 đến 2025
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('1990-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    ) }}
)
SELECT
    date_day AS date_iso, -- Cột gốc kiểu DATE
    -- Các cột format theo chuẩn Snowflake
    TO_CHAR(date_day, 'YYYY-MM-DD') AS date_string,
    DAYNAME(date_day) AS day_of_week_short, -- Mon, Tue, Wed...
    -- Map tên viết tắt sang tên đầy đủ
    DECODE(DAYNAME(date_day), 
         'Sun', 'Sunday', 
         'Sat', 'Saturday', 
         'Mon', 'Monday', 
         'Tue', 'Tuesday', 
         'Wed', 'Wednesday', 
         'Thu', 'Thursday', 
         'Fri', 'Friday') AS day_of_week,
    -- Logic cuối tuần (0=Chủ nhật, 6=Thứ bảy trong Snowflake mặc định)
    CASE 
        WHEN DAYOFWEEK(date_day) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday' 
    END AS is_weekday_or_weekend,
    DATE_TRUNC('MONTH', date_day) AS year_month,
    EXTRACT(MONTH FROM date_day) AS month_number,
    TO_CHAR(date_day, 'MMMM') AS month_name, -- January, February...
    DATE_TRUNC('YEAR', date_day) AS year,
    EXTRACT(YEAR FROM date_day) AS year_number,
    DATE_TRUNC('QUARTER', date_day) AS quarter,
    EXTRACT(QUARTER FROM date_day) AS quarter_number
FROM date_spine