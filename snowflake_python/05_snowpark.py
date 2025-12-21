import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, max as max_, count, sum as sum_, avg,
    datediff, current_date, date_trunc
)
import matplotlib.pyplot as plt
from dotenv import load_dotenv
def get_session():
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv()) 
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    connection_parameters = {
        "account": account,
        "user": user,
        "password": password,
        "role": "ACCOUNTADMIN",
        "warehouse": warehouse,
        "database": database,
        "schema": schema
    }
    
    session = Session.builder.configs(connection_parameters).create()
    print('✅ Kết nối Snowflake thành công!')
    return session
# ============================================================
# 5.1 Customer Segmentation với RFM
# ============================================================
def run_rfm_segmentation(session):
    customers = session.table("CUSTOMER")
    orders = session.table("ORDERS")
    rfm_df = (customers
        .join(orders, customers["C_CUSTKEY"] == orders["O_CUSTKEY"], "left")
        .group_by("C_CUSTKEY", "C_NAME")
        .agg([
            max_("O_ORDERDATE").alias("LAST_ORDER_DATE"),  
            count("O_ORDERKEY").alias("FREQUENCY"),        
            sum_("O_TOTALPRICE").alias("MONETARY")         
        ])
        .with_column("RECENCY_DAYS", 
                     datediff("day", col("LAST_ORDER_DATE"), current_date())) 
    )
    rfm_clean = rfm_df.fillna({"MONETARY": 0.0, "RECENCY_DAYS": 0})
    rfm_clean.write.mode("overwrite").save_as_table("CUSTOMER_RFM_SCORES")
    # Show sample
    rfm_clean.filter(col("FREQUENCY") > 0).show(10)
def analyze_sales_trend(session):
    orders = session.table("ORDERS")
    # Monthly aggregation
    monthly_sales = (orders
        .with_column("MONTH", date_trunc("month", col("O_ORDERDATE")))
        .group_by("MONTH")
        .agg([
            count("O_ORDERKEY").alias("ORDER_COUNT"),
            sum_("O_TOTALPRICE").alias("TOTAL_REVENUE"),
            avg("O_TOTALPRICE").alias("AVG_ORDER_VALUE")
        ])
        .sort("MONTH")
    )
    monthly_sales.write.mode("overwrite").save_as_table("MONTHLY_SALES_STATS")
    print("✅ Đã lưu bảng thống kê: MONTHLY_SALES_STATS")
    monthly_sales.show(10)
    # 4. Convert to pandas for visualization
    df_pandas = monthly_sales.to_pandas()
    plt.figure(figsize=(12, 6))
            
            # Vẽ đường Revenue
    plt.plot(df_pandas['MONTH'], df_pandas['TOTAL_REVENUE'], marker='o', linestyle='-', color='b')
    plt.title('Monthly Sales Revenue Trend')
    plt.xlabel('Month')
    plt.ylabel('Total Revenue')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Hiển thị biểu đồ
    plt.show()
    print("✅ Đã hiển thị biểu đồ!")
 
if __name__ == "__main__":
    session = get_session()
    # run_rfm_segmentation(session)
    analyze_sales_trend(session)
    session.close()
    