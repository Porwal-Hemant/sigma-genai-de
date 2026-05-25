WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

merchants AS (
    SELECT
        MERCHANT_ID AS merchant_id,
        MERCHANT_NAME AS merchant_name,
        CATEGORY AS category,
        CITY AS city
    FROM {{ source('sigma_de', 'dim_merchant') }}
),

merchant_metrics AS (
    SELECT
        t.merchant_id,
        m.merchant_name,
        m.category,
        m.city,
        SUM(CASE WHEN t.status = 'COMPLETED' THEN t.amount ELSE 0 END) AS total_revenue,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN t.status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
        COUNT(DISTINCT t.customer_id) AS unique_customers,
        AVG(CASE WHEN t.status = 'COMPLETED' THEN t.amount END) AS avg_transaction_value
    FROM transactions t
    JOIN merchants m
        ON t.merchant_id = m.merchant_id
    GROUP BY
        t.merchant_id,
        m.merchant_name,
        m.category,
        m.city
)

SELECT
    merchant_id,
    merchant_name,
    category,
    city,
    total_revenue,
    total_transactions,
    failed_count,
    ROUND((failed_count * 100.0) / NULLIF(total_transactions, 0), 2) AS failure_rate_pct,
    avg_transaction_value,
    unique_customers
FROM merchant_metrics
