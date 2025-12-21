-- UDF 1: Phân loại khách hàng theo revenue
CREATE OR REPLACE FUNCTION UDF_CUSTOMER_TIER(total_revenue NUMBER)
RETURNS STRING
AS
$$
    CASE
        WHEN total_revenue >= 500000 THEN 'VIP'
        WHEN total_revenue >= 200000 THEN 'GOLD'
        WHEN total_revenue >= 100000 THEN 'SILVER'
        WHEN total_revenue >= 50000  THEN 'BRONZE'
        ELSE 'STANDARD'
    END
$$;
-- Test UDF 1: Áp dụng lên bảng RFM vừa tạo 
SELECT 
    C_CUSTKEY, 
    C_NAME, 
    MONETARY,
    UDF_CUSTOMER_TIER(MONETARY) as MEMBER_TIER
FROM CUSTOMER_RFM_SCORES
WHERE MONETARY IS NOT NULL
LIMIT 10;
-- UDF 2: Validate phone number
CREATE OR REPLACE FUNCTION UDF_VALIDATE_PHONE(phone STRING)
RETURNS BOOLEAN
AS
$$
    REGEXP_LIKE(phone, '^[0-9]{10,15}$')
$$;

-- UDF 3: Validate email
CREATE OR REPLACE FUNCTION UDF_VALIDATE_EMAIL(email STRING)
RETURNS BOOLEAN
AS
$$
    REGEXP_LIKE(
        email,
        '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    )
$$;
-- TEST UDF2, 3
SELECT 
    column1 as test_phone,
    UDF_VALIDATE_PHONE(column1) as is_phone_valid,
    column2 as test_email,
    UDF_VALIDATE_EMAIL(column2) as is_email_valid
FROM VALUES 
    ('0901234567', 'test@gmail.com'),       
    ('123', 'invalid-email'),               
    ('098765432112', 'user@company.vn')   