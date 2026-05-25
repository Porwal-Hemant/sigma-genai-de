SELECT m.category,
       t.customer_id,
       SUM(t.amount) as category_revenue,
       COUNT(*) as transaction_count
FROM fact_transactions t
JOIN dim_merchant m ON t.merchant_id = m.merchant_id
WHERE t.transaction_date >= '2024-01-01'
GROUP BY m.category
ORDER BY category_revenue ASC;
