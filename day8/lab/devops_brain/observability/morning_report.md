# DataOps Morning Report — 2023-10-05

### Pipeline Status
**DEGRADED**  
The pipeline is currently degraded due to a significant dataset drift and a high failure rate in the Gold Layer.

### 5 Key Findings
- **Silver Layer Quality:**  
  - Total rows: 14  
  - Columns with nulls: []  
  - Transaction status breakdown: {'COMPLETED': 11, 'FAILED': 2, 'PENDING': 1}  
  - Amount range: 65.0 to 3400.0  
  - Amount mean: 1002.86  
  - **Observation:** The small number of rows might indicate a data ingestion issue.
- **Bronze → Silver Drift:**  
  - Dataset drifted: True  
  - Drift share: 0.43  
  - Drifted columns: ['transaction_id', 'merchant_id', 'customer_id']  
  - **Observation:** A drift share of 0.43 is significant and needs investigation.
- **Gold Layer:**  
  - Active merchants: 8  
  - Total revenue: 13161.0  
  - Average failure rate: 18.75%  
  - Highest failure rate: 100.0% (Zomato)  
  - **Observation:** Zomato has a 100% failure rate, which is critical and needs immediate attention.
- **Transaction Status:**  
  - PENDING transactions: 1  
  - **Observation:** There is 1 transaction still pending, which could affect the overall data accuracy.
- **Amount Mean:**  
  - Amount mean: 1002.86  
  - **Observation:** The mean transaction amount is relatively high, which is expected but should be monitored.

### Alerts to Watch
- **Bronze → Silver Drift:**  
  - Any increase in the drift share or additional drifted columns.
- **Gold Layer Failure Rate:**  
  - Any merchant showing a failure rate of 100%.
- **Pending Transactions:**  
  - Any increase in the number of pending transactions.

### Recommended Actions
- **Investigate Dataset Drift:**  
  - Review the drifted columns ('transaction_id', 'merchant_id', 'customer_id') to understand the cause and impact.
- **Address High Failure Rate:**  
  - Focus on Zomato to identify and resolve the 100% failure rate issue.
- **Monitor Pending Transactions:**  
  - Ensure that the pending transaction is processed and resolved before 10 AM.