# ❄️ Analytics Engineer Project : Snowflake, dbt & Snowpark Python

1. Tổng quan dự án (Project Overview)

   Dự án này xây dựng một luồng xử lý dữ liệu (Data Pipeline) hoàn chỉnh sử dụng Modern Data Stack. Mục tiêu là phân tích dữ liệu bán hàng (TPC-H dataset), phân khúc khách hàng và dự báo xu hướng doanh thu.

Công nghệ sử dụng:

Data Warehouse: Snowflake

Transformation: dbt (data build tool)

Analytics & ML: Snowpark Python

Environment Management: Python-dotenv

2. Cấu trúc dự án

```text
Snowflake-TPCH-Project/
│
├── dbt/                        # Source code dbt (Data Transformation)
│   ├── analysis/
│   ├── models/                 # Chứa các file .sql biến đổi dữ liệu (Staging -> Marts)
│   ├── seeds/                  # Chứa file csv dữ liệu thô (nếu có)
│   ├── tests/                  # Các file test logic dữ liệu
│   └── dbt_project.yml         # Cấu hình chính của dbt
│
├── python/                     # Source code Python & Snowpark
│   ├── .env                    # Biến môi trường (Credentials - KHÔNG UP FILE NÀY)
│   ├── 05_snowpark.py          # Script chạy RFM Analysis & Sales Trend
│   └── requirements.txt        # Danh sách thư viện cần cài
│
├── sql/                        # Các scripts quản trị Database (Infrastructure)
│   ├── 01_init_setup.sql       # Tạo Database, Warehouse, Schema
│   ├── 02_security.sql         # Phân quyền (Grants), tạo User/Role
│   └── 05_udfs.sql             # Tạo hàm User Defined Functions
│
└── README.md                   # Tài liệu hướng dẫn dự án
```
