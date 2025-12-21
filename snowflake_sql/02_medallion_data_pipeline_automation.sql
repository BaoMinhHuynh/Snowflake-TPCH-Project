LIST @TPCH_ANALYTICS_DB.STAGING.TPCH_DATA_STAGE;
USE DATABASE TPCH_ANALYTICS_DB;
USE SCHEMA STAGING;
CREATE OR REPLACE FILE FORMAT TPCH_CSV_FORMAT
TYPE = CSV
SKIP_HEADER = 1
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;
CREATE OR REPLACE TABLE RAW_ORDERS_LANDING (
    O_ORDERKEY      NUMBER(38,0),
    O_CUSTKEY       NUMBER(38,0),
    O_ORDERSTATUS   VARCHAR(1),
    O_TOTALPRICE    NUMBER(15,2),
    O_ORDERDATE     DATE,
    O_ORDERPRIORITY VARCHAR(15),
    O_CLERK         VARCHAR(15),
    O_SHIPPRIORITY  NUMBER(38,0),
    O_COMMENT       VARCHAR(79),
    INGESTION_TIME  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_NAME       VARCHAR(256)
);
-- Snowpipe automatically loads data when new files appear in the stage

CREATE OR REPLACE PIPE PIPE_LOAD_ORDERS
AUTO_INGEST = TRUE -- Automate the data ingestion process
AS
COPY INTO RAW_ORDERS_LANDING (
    O_ORDERKEY,
    O_CUSTKEY,
    O_ORDERSTATUS,
    O_TOTALPRICE,
    O_ORDERDATE,
    O_ORDERPRIORITY,
    O_CLERK,
    O_SHIPPRIORITY,
    O_COMMENT,
    FILE_NAME
)
FROM (
    SELECT 
        $1::NUMBER(38,0),
        $2::NUMBER(38,0),
        $3::VARCHAR(1),
        $4::NUMBER(15,2),
        $5::DATE,
        $6::VARCHAR(15),
        $7::VARCHAR(15),
        $8::NUMBER(38,0),
        $9::VARCHAR(79),
        METADATA$FILENAME
    FROM @TPCH_ANALYTICS_DB.STAGING.TPCH_DATA_STAGE
)
FILE_FORMAT = TPCH_CSV_FORMAT
--Check loaded data
SELECT COUNT(*) AS ROWS_LOADED FROM RAW_ORDERS_LANDING
-- Create STREAM on RAW_ORDERS_LANDING (CDC - Change Data Capture)
CREATE OR REPLACE STREAM STREAM_RAW_ORDERS_LANDING 
ON TABLE RAW_ORDERS_LANDING
SHOW_INITIAL_ROWS = FALSE;  -- Set to TRUE if you want to process existing data as changes

-- View stream metadata
SHOW STREAMS LIKE 'STREAM_RAW_ORDERS_LANDING';

-- Check if stream has data (CDC changes)
SELECT COUNT(*) AS PENDING_CHANGES FROM STREAM_RAW_ORDERS_LANDING;

-- Create Stored Procedure for CDC MERGE Logic
-- ============================================================================
-- This procedure processes the stream and merges changes into ORDER STAGING table
CREATE OR REPLACE PROCEDURE SP_MERGE_ORDERS_STAGING()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    pending_count NUMBER;
    rows_merged NUMBER;
BEGIN
    -- Check stream
    SELECT COUNT(*) INTO :pending_count
    FROM STREAM_RAW_ORDERS_LANDING;

    IF (pending_count = 0) THEN
        RETURN 'No changes detected in stream.';
    END IF;

    MERGE INTO TPCH_ANALYTICS_DB.STAGING.ORDERS tgt
    USING (
        SELECT
            O_ORDERKEY,
            O_CUSTKEY,
            O_ORDERSTATUS,
            O_TOTALPRICE,
            O_ORDERDATE,
            O_ORDERPRIORITY,
            O_CLERK,
            O_SHIPPRIORITY,
            O_COMMENT,
            INGESTION_TIME
        FROM STREAM_RAW_ORDERS_LANDING
        WHERE METADATA$ACTION = 'INSERT'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY O_ORDERKEY
            ORDER BY INGESTION_TIME DESC
        ) = 1
    ) src
    ON tgt.O_ORDERKEY = src.O_ORDERKEY

    WHEN MATCHED THEN UPDATE SET
        tgt.O_CUSTKEY        = src.O_CUSTKEY,
        tgt.O_ORDERSTATUS    = src.O_ORDERSTATUS,
        tgt.O_TOTALPRICE     = src.O_TOTALPRICE,
        tgt.O_ORDERDATE      = src.O_ORDERDATE,
        tgt.O_ORDERPRIORITY  = src.O_ORDERPRIORITY,
        tgt.O_CLERK          = src.O_CLERK,
        tgt.O_SHIPPRIORITY   = src.O_SHIPPRIORITY,
        tgt.O_COMMENT        = src.O_COMMENT

    WHEN NOT MATCHED THEN INSERT (
        O_ORDERKEY,
        O_CUSTKEY,
        O_ORDERSTATUS,
        O_TOTALPRICE,
        O_ORDERDATE,
        O_ORDERPRIORITY,
        O_CLERK,
        O_SHIPPRIORITY,
        O_COMMENT
    )
    VALUES (
        src.O_ORDERKEY,
        src.O_CUSTKEY,
        src.O_ORDERSTATUS,
        src.O_TOTALPRICE,
        src.O_ORDERDATE,
        src.O_ORDERPRIORITY,
        src.O_CLERK,
        src.O_SHIPPRIORITY,
        src.O_COMMENT
    );

    rows_merged := SQLROWCOUNT;

    RETURN 'Merged ' || rows_merged || ' rows into STAGING.ORDERS.';
END;
$$;

-- Create TASK to Automate CDC Pipeline
-- This task runs every 5 minutes to process new data from the stream

CREATE OR REPLACE TASK TASK_CDC_MERGE_ORDERS
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'  -- Run every 5 minutes
    -- Or use CRON: SCHEDULE = 'USING CRON */5 * * * * UTC'
    WHEN SYSTEM$STREAM_HAS_DATA('STREAM_RAW_ORDERS_LANDING')  -- Only run if stream has data
AS
    CALL SP_MERGE_ORDERS_STAGING();

-- Resume the task to start it
ALTER TASK TASK_CDC_MERGE_ORDERS RESUME;

-- Simulate Continuous Ingestion (Testing)
-- 1. Add new data to landing table (simulating Snowpipe load)
INSERT INTO RAW_ORDERS_LANDING (
    O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE,
    O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, FILE_NAME
)
SELECT 
    O_ORDERKEY,  -- Offset to avoid duplicates
    O_CUSTKEY,
    O_ORDERSTATUS,
    O_TOTALPRICE,
    O_ORDERDATE,
    O_ORDERPRIORITY,
    O_CLERK,
    O_SHIPPRIORITY,
    O_COMMENT,
    'manual_test_batch_' || CURRENT_TIMESTAMP()::VARCHAR
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS
LIMIT 1000;

select count(*) from RAW_ORDERS_LANDING;

-- 2. Check stream (should show new changes)
SELECT COUNT(*) AS NEW_CHANGES FROM STREAM_RAW_ORDERS_LANDING;

SELECT * FROM STREAM_RAW_ORDERS_LANDING;

-- 3. Manually trigger CDC task (or wait for scheduled run)
EXECUTE TASK TASK_CDC_MERGE_ORDERS;

-- 4. Verify ORDER STAGING table updated
SELECT 
    COUNT(*) AS TOTAL_ROWS,
FROM TPCH_ANALYTICS_DB.STAGING.ORDERS;

-- 5. Check stream consumed
SELECT COUNT(*) FROM STREAM_RAW_ORDERS_LANDING;
-- ============================================================================
-- CREATE RAW_CUSTOMER_LANDING
CREATE OR REPLACE TABLE RAW_CUSTOMER_LANDING (
    C_CUSTKEY     NUMBER(38,0),
    C_NAME        VARCHAR(25),
    C_ADDRESS     VARCHAR(40),
    C_NATIONKEY   NUMBER(38,0),
    C_PHONE       VARCHAR(15),
    C_ACCTBAL     NUMBER(15,2),
    C_MKTSEGMENT  VARCHAR(10),
    C_COMMENT     VARCHAR(117),
    INGESTION_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_NAME      VARCHAR(256)
);
-- Snowpipe automatically loads data when new files appear in the stage
CREATE OR REPLACE PIPE PIPE_LOAD_CUSTOMER
AUTO_INGEST = TRUE
AS
COPY INTO RAW_CUSTOMER_LANDING (
    C_CUSTKEY,
    C_NAME,
    C_ADDRESS,
    C_NATIONKEY,
    C_PHONE,
    C_ACCTBAL,
    C_MKTSEGMENT,
    C_COMMENT,
    FILE_NAME
)
FROM (
    SELECT
        $1::NUMBER,
        $2::VARCHAR,
        $3::VARCHAR,
        $4::NUMBER,
        $5::VARCHAR,
        $6::NUMBER(15,2),
        $7::VARCHAR,
        $8::VARCHAR,
        METADATA$FILENAME
    FROM @TPCH_ANALYTICS_DB.STAGING.TPCH_DATA_STAGE/customer
);
-- Create STREAM on RAW_CUSTOMER_LANDING (CDC - Change Data Capture)
CREATE OR REPLACE STREAM STREAM_RAW_CUSTOMER_LANDING
ON TABLE RAW_CUSTOMER_LANDING
SHOW_INITIAL_ROWS = FALSE;
-- Create Stored Procedure for CDC MERGE Logic
-- ============================================================================
-- This procedure processes the stream and merges changes into CUSTOMER STAGING table
CREATE OR REPLACE PROCEDURE SP_MERGE_CUSTOMER_STAGING()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    pending_count NUMBER;
    rows_merged NUMBER;
BEGIN
    SELECT COUNT(*) INTO :pending_count
    FROM STREAM_RAW_CUSTOMER_LANDING;

    IF (pending_count = 0) THEN
        RETURN 'No changes detected in CUSTOMER stream.';
    END IF;

    MERGE INTO TPCH_ANALYTICS_DB.STAGING.CUSTOMER tgt
    USING (
        SELECT
            C_CUSTKEY,
            C_NAME,
            C_ADDRESS,
            C_NATIONKEY,
            C_PHONE,
            C_ACCTBAL,
            C_MKTSEGMENT,
            C_COMMENT,
            INGESTION_TIME,
            METADATA$ACTION
        FROM STREAM_RAW_CUSTOMER_LANDING
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY C_CUSTKEY
            ORDER BY INGESTION_TIME DESC
        ) = 1
    ) src
    ON tgt.C_CUSTKEY = src.C_CUSTKEY

    WHEN MATCHED AND src.METADATA$ACTION = 'DELETE' THEN
        DELETE

    WHEN MATCHED THEN UPDATE SET
        tgt.C_NAME        = src.C_NAME,
        tgt.C_ADDRESS     = src.C_ADDRESS,
        tgt.C_NATIONKEY   = src.C_NATIONKEY,
        tgt.C_PHONE       = src.C_PHONE,
        tgt.C_ACCTBAL     = src.C_ACCTBAL,
        tgt.C_MKTSEGMENT  = src.C_MKTSEGMENT,
        tgt.C_COMMENT     = src.C_COMMENT

    WHEN NOT MATCHED AND src.METADATA$ACTION = 'INSERT' THEN
        INSERT (
            C_CUSTKEY,
            C_NAME,
            C_ADDRESS,
            C_NATIONKEY,
            C_PHONE,
            C_ACCTBAL,
            C_MKTSEGMENT,
            C_COMMENT
        )
        VALUES (
            src.C_CUSTKEY,
            src.C_NAME,
            src.C_ADDRESS,
            src.C_NATIONKEY,
            src.C_PHONE,
            src.C_ACCTBAL,
            src.C_MKTSEGMENT,
            src.C_COMMENT
        );

    rows_merged := SQLROWCOUNT;

    RETURN 'Merged ' || rows_merged || ' rows into STAGING.CUSTOMER.';
END;
$$;
-- Create TASK to Automate CDC Pipeline
-- This task runs every 5 minutes to process new data from the stream
CREATE OR REPLACE TASK TASK_CDC_MERGE_CUSTOMER
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('STREAM_RAW_CUSTOMER_LANDING')
AS
    CALL SP_MERGE_CUSTOMER_STAGING();
-- ============================================================================
-- CREATE RAW_LINEITEM_LANDING
CREATE OR REPLACE TABLE RAW_LINEITEM_LANDING (
    L_ORDERKEY      NUMBER(38,0),
    L_PARTKEY       NUMBER(38,0),
    L_SUPPKEY       NUMBER(38,0),
    L_LINENUMBER    NUMBER(38,0),
    L_QUANTITY      NUMBER(15,2),
    L_EXTENDEDPRICE NUMBER(15,2),
    L_DISCOUNT      NUMBER(15,2),
    L_TAX           NUMBER(15,2),
    L_RETURNFLAG    VARCHAR(1),
    L_LINESTATUS    VARCHAR(1),
    L_SHIPDATE      DATE,
    L_COMMITDATE    DATE,
    L_RECEIPTDATE   DATE,
    L_SHIPINSTRUCT  VARCHAR(25),
    L_SHIPMODE      VARCHAR(10),
    L_COMMENT       VARCHAR(44),
    INGESTION_TIME  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_NAME       VARCHAR(256)
);
-- Snowpipe automatically loads data when new files appear in the stage
CREATE OR REPLACE PIPE PIPE_LOAD_LINEITEM
AUTO_INGEST = TRUE
AS
COPY INTO RAW_LINEITEM_LANDING (
    L_ORDERKEY,
    L_PARTKEY,
    L_SUPPKEY,
    L_LINENUMBER,
    L_QUANTITY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_TAX,
    L_RETURNFLAG,
    L_LINESTATUS,
    L_SHIPDATE,
    L_COMMITDATE,
    L_RECEIPTDATE,
    L_SHIPINSTRUCT,
    L_SHIPMODE,
    L_COMMENT,
    FILE_NAME
)
FROM (
    SELECT
        $1::NUMBER,
        $2::NUMBER,
        $3::NUMBER,
        $4::NUMBER,
        $5::NUMBER(15,2),
        $6::NUMBER(15,2),
        $7::NUMBER(15,2),
        $8::NUMBER(15,2),
        $9::VARCHAR,
        $10::VARCHAR,
        $11::DATE,
        $12::DATE,
        $13::DATE,
        $14::VARCHAR,
        $15::VARCHAR,
        $16::VARCHAR,
        METADATA$FILENAME
    FROM @TPCH_ANALYTICS_DB.STAGING.TPCH_DATA_STAGE/lineitem
);
-- Create STREAM on RAW_LINEITEM_LANDING (CDC - Change Data Capture)
CREATE OR REPLACE STREAM STREAM_RAW_LINEITEM_LANDING
ON TABLE RAW_LINEITEM_LANDING
SHOW_INITIAL_ROWS = FALSE;
-- Create Stored Procedure for CDC MERGE Logic
-- ============================================================================
-- This procedure processes the stream and merges changes into CUSTOMER STAGING table
CREATE OR REPLACE PROCEDURE SP_MERGE_LINEITEM_STAGING()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    pending_count NUMBER;
    rows_merged NUMBER;
BEGIN
    SELECT COUNT(*) INTO :pending_count
    FROM STREAM_RAW_LINEITEM_LANDING;

    IF (pending_count = 0) THEN
        RETURN 'No changes detected in LINEITEM stream.';
    END IF;

    MERGE INTO TPCH_ANALYTICS_DB.STAGING.LINEITEM tgt
    USING (
        SELECT
            L_ORDERKEY,
            L_LINENUMBER,
            L_PARTKEY,
            L_SUPPKEY,
            L_QUANTITY,
            L_EXTENDEDPRICE,
            L_DISCOUNT,
            L_TAX,
            L_RETURNFLAG,
            L_LINESTATUS,
            L_SHIPDATE,
            L_COMMITDATE,
            L_RECEIPTDATE,
            L_SHIPINSTRUCT,
            L_SHIPMODE,
            L_COMMENT,
            INGESTION_TIME,
            METADATA$ACTION
        FROM STREAM_RAW_LINEITEM_LANDING
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY L_ORDERKEY, L_LINENUMBER
            ORDER BY INGESTION_TIME DESC
        ) = 1
    ) src
    ON tgt.L_ORDERKEY   = src.L_ORDERKEY
   AND tgt.L_LINENUMBER = src.L_LINENUMBER

    WHEN MATCHED AND src.METADATA$ACTION = 'DELETE' THEN
        DELETE

    WHEN MATCHED THEN UPDATE SET
        tgt.L_PARTKEY        = src.L_PARTKEY,
        tgt.L_SUPPKEY        = src.L_SUPPKEY,
        tgt.L_QUANTITY       = src.L_QUANTITY,
        tgt.L_EXTENDEDPRICE  = src.L_EXTENDEDPRICE,
        tgt.L_DISCOUNT       = src.L_DISCOUNT,
        tgt.L_TAX            = src.L_TAX,
        tgt.L_RETURNFLAG     = src.L_RETURNFLAG,
        tgt.L_LINESTATUS     = src.L_LINESTATUS,
        tgt.L_SHIPDATE       = src.L_SHIPDATE,
        tgt.L_COMMITDATE     = src.L_COMMITDATE,
        tgt.L_RECEIPTDATE    = src.L_RECEIPTDATE,
        tgt.L_SHIPINSTRUCT   = src.L_SHIPINSTRUCT,
        tgt.L_SHIPMODE       = src.L_SHIPMODE,
        tgt.L_COMMENT        = src.L_COMMENT

    WHEN NOT MATCHED AND src.METADATA$ACTION = 'INSERT' THEN
        INSERT (
            L_ORDERKEY,
            L_LINENUMBER,
            L_PARTKEY,
            L_SUPPKEY,
            L_QUANTITY,
            L_EXTENDEDPRICE,
            L_DISCOUNT,
            L_TAX,
            L_RETURNFLAG,
            L_LINESTATUS,
            L_SHIPDATE,
            L_COMMITDATE,
            L_RECEIPTDATE,
            L_SHIPINSTRUCT,
            L_SHIPMODE,
            L_COMMENT
        )
        VALUES (
            src.L_ORDERKEY,
            src.L_LINENUMBER,
            src.L_PARTKEY,
            src.L_SUPPKEY,
            src.L_QUANTITY,
            src.L_EXTENDEDPRICE,
            src.L_DISCOUNT,
            src.L_TAX,
            src.L_RETURNFLAG,
            src.L_LINESTATUS,
            src.L_SHIPDATE,
            src.L_COMMITDATE,
            src.L_RECEIPTDATE,
            src.L_SHIPINSTRUCT,
            src.L_SHIPMODE,
            src.L_COMMENT
        );

    rows_merged := SQLROWCOUNT;

    RETURN 'Merged ' || rows_merged || ' rows into STAGING.LINEITEM.';
END;
$$;
-- Create TASK to Automate CDC Pipeline
-- This task runs every 5 minutes to process new data from the stream
CREATE OR REPLACE TASK TASK_CDC_MERGE_LINEITEM
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('STREAM_RAW_LINEITEM_LANDING')
AS
    CALL SP_MERGE_LINEITEM_STAGING();