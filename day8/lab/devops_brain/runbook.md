# Pipeline Overview

This pipeline processes transaction data, transforms it, and loads it into bronze, silver, and gold tables. It runs to ensure data is available for downstream analytics and reporting. If it stops, critical business metrics and reports will be unavailable.

## Pipeline Steps

1. Connect to the DuckDB database using `get_connection()`.
2. Set up necessary tables using `setup_tables()`.
3. Load merchant data into the `merchants` table using `load_merchants()`.
4. Load transaction data into the `bronze_transactions` table using `load_bronze()`.
5. Transform bronze transactions to silver using `transform_bronze_to_silver()`.
6. Load transformed data into the `silver_transactions` table using `load_silver()`.
7. Compute merchant performance metrics using `compute_merchant_performance()`.
8. Compute daily summary metrics using `compute_daily_summary()`.
9. Load performance and summary data into gold tables using `load_gold()`.

## Schedule / Trigger

This pipeline runs every hour, triggered by a cron job.

## Failure Modes

1. **Database Connection Failure**
   - **Root Cause:** DuckDB service is down.
   - **Symptom:** `get_connection()` fails.
2. **Table Creation Failure**
   - **Root Cause:** Syntax error in SQL.
   - **Symptom:** `setup_tables()` throws an exception.
3. **Merchant Data Load Failure**
   - **Root Cause:** Corrupt merchant data.
   - **Symptom:** `load_merchants()` fails to insert data.
4. **Bronze Table Load Failure**
   - **Root Cause:** Invalid transaction data.
   - **Symptom:** `load_bronze()` fails to insert data.
5. **Silver Table Transformation Failure**
   - **Root Cause:** Missing merchant data for transactions.
   - **Symptom:** `transform_bronze_to_silver()` produces incorrect output.

## Recovery Actions

1. **Database Connection Failure**
   - Check DuckDB service status.
   - Restart the service if necessary.
   - Retry the pipeline.
2. **Table Creation Failure**
   - Review SQL syntax in `setup_tables()`.
   - Correct the syntax and retry.
3. **Merchant Data Load Failure**
   - Validate merchant data for corruption.
   - Clean the data and retry `load_merchants()`.
4. **Bronze Table Load Failure**
   - Validate transaction data for correctness.
   - Clean the data and retry `load_bronze()`.
5. **Silver Table Transformation Failure**
   - Ensure all merchants are present in the `merchants` table.
   - Retry `transform_bronze_to_silver()`.

## Known Bugs

- Hardcoded AWS credentials in the code.
- Lack of null handling in `transform_bronze_to_silver()`.

## Escalation Contacts

1. **On-call DE:** Priya Nair (priya.nair@sigmadatatech.in, +91-98400-11111)
2. **Tech Lead:** Arjun Mehta (arjun.mehta@sigmadatatech.in)
3. **Platform Manager:** Kavya Reddy (kavya.reddy@sigmadatatech.in)

## Data Quality Checks

- Verify the count of records in `bronze_transactions`, `silver_transactions`, `gold_merchant_performance`, and `gold_daily_summary`.
- Ensure `quality_flag` is set correctly in `silver_transactions`.
- Check for any NULL values in critical fields.