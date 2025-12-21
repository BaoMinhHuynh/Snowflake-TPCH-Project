❄️ Analytics Engineer Project : Snowflake, dbt & Snowpark Python

1. Tổng quan dự án (Project Overview)
   Dự án này xây dựng một luồng xử lý dữ liệu (Data Pipeline) hoàn chỉnh sử dụng Modern Data Stack. Mục tiêu là phân tích dữ liệu bán hàng (TPC-H dataset), phân khúc khách hàng và dự báo xu hướng doanh thu.

Công nghệ sử dụng:

Data Warehouse: Snowflake

Transformation: dbt (data build tool)

Analytics & ML: Snowpark Python

Environment Management: Python-dotenv

2. Cấu trúc dự án

├── dbt_project/ # Source code dbt (Models, Seeds, Tests)

├── python/ # Scripts Python & Snowpark

│ ├── 05_snowpark.py # Phân tích RFM & Sales Trend

│ ├── 05_analysis.ipynb # Jupyter Notebook (nếu có)

│ └── .env # Biến môi trường (Không commit file này)

├── sql/ # Các file SQL bổ trợ

│ └── 05_udfs.sql # User Defined Functions

└── README.md # Tài liệu hướng dẫn
