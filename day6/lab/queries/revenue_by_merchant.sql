SELECT m.merchant_name,
       SUM(t.amount) as total_revenue,
       COUNT(*) as txn_count
FROM fact_transactions t, dim_merchant m
WHERE t.merchant_id = m.merchant_id
AND t.transaction_date > '2024-01-01'
GROUP BY m.merchant_name
ORDER BY total_revenue
LIMIT 10;
