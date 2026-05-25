WITH filtered_transactions AS (
    SELECT
        transaction_id,
        amount,
        status,
        merchant_id,
        customer_id,
        transaction_date,
        payment_method
    FROM {{ ref('stg_fact_transactions') }}
    WHERE status IN ('COMPLETED', 'FAILED')
),

merchant_details AS (
    SELECT
        merchant_id,
        merchant_name,
        category,
        city,
        onboarded_date
    FROM {{ ref('dim_merchant') }}
),

aggregated_metrics AS (
    SELECT
        ft.merchant_id,
        m.merchant_name,
        COUNT(ft.transaction_id) AS total_transactions,
        COUNT(CASE WHEN ft.status = 'COMPLETED' THEN 1 END) AS completed_transactions,
        COUNT(CASE WHEN ft.status = 'FAILED' THEN 1 END) AS failed_count,
        SUM(CASE WHEN ft.status = 'COMPLETED' THEN ft.amount ELSE 0 END) AS total_revenue,
        AVG(CASE WHEN ft.status = 'COMPLETED' THEN ft.amount ELSE NULL END) AS avg_transaction_value,
        COUNT(DISTINCT ft.customer_id) AS unique_customers
    FROM filtered_transactions ft
    JOIN merchant_details m ON ft.merchant_id = m.merchant_id
    GROUP BY ft.merchant_id, m.merchant_name
)

SELECT
    merchant_id,
    merchant_name,
    total_transactions,
    completed_transactions,
    failed_count,
    total_revenue,
    avg_transaction_value,
    unique_customers,
    (failed_count::FLOAT / total_transactions) * 100 AS failure_rate_pct
FROM aggregated_metrics
