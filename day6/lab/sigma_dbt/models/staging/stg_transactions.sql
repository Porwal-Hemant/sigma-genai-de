WITH raw_transactions AS (
    SELECT
        TRANSACTION_ID,
        CAST(AMOUNT AS NUMBER(10, 2)) AS AMOUNT,
        STATUS,
        MERCHANT_ID,
        CUSTOMER_ID,
        CAST(TRANSACTION_DATE AS DATE) AS TRANSACTION_DATE,
        PAYMENT_METHOD
    FROM {{ source('sigma_de', 'fact_transactions') }}
),

cleaned_transactions AS (
    SELECT
        TRANSACTION_ID AS transaction_id,
        AMOUNT AS amount,
        STATUS AS status,
        MERCHANT_ID AS merchant_id,
        CUSTOMER_ID AS customer_id,
        TRANSACTION_DATE AS transaction_date,
        PAYMENT_METHOD AS payment_method,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM raw_transactions
    WHERE MERCHANT_ID NOT ILIKE 'TEST_%'
)

SELECT * FROM cleaned_transactions
