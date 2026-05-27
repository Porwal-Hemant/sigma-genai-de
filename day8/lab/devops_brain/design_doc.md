# Pipeline Design Document

## What This Pipeline Does
This pipeline ingests transaction data, cleans it, and transforms it into two gold layers: merchant performance metrics and daily transaction summaries.

## Data Flow Diagram

```
+----------------+       +--------------------+       +--------------------+       +--------------------+
| Source         |       | Bronze Layer       |       | Silver Layer       |       | Gold Layer         |
| (Dirty/Clean)  |----->  | bronze_transactions|----->  | silver_transactions |----->  | gold_merchant_perf  |
|                |        |                    |        |                     |        | gold_daily_summary  |
+----------------+       +--------------------+       +--------------------+       +--------------------+
```

## Key Design Decisions
- **Layered Approach**: The pipeline uses a three-layer approach (Bronze, Silver, Gold) to ensure data quality and transformation are modular and maintainable.
- **Data Validation**: Negative amounts and duplicate transactions are filtered out in the transformation phase to ensure data integrity.
- **Timestamps**: Each layer includes ingestion timestamps to track data freshness and processing time.
- **Aggregation**: Aggregations are performed in memory to ensure efficient computation before writing to the gold layer.

## Known Limitations
- **Data Volume**: The current implementation may not scale well for very large datasets due to in-memory aggregations.
- **Error Handling**: Limited error handling in data ingestion may lead to data loss if exceptions occur.
- **Data Freshness**: The pipeline runs once daily, which may not meet real-time analytics needs.
- **Schema Changes**: Adding new fields to the source data requires updating the pipeline code and database schema.

## Dependencies
- **DuckDB**: The pipeline relies on DuckDB for data storage and querying.
- **MERCHANTS**: A predefined list of merchants used for enriching transaction data.
- **TRANSACTIONS_CLEAN and TRANSACTIONS_DIRTY**: Source data files containing transaction records.
- **AWS S3**: Although not used in the provided code, the pipeline is configured to interact with an S3 bucket for potential future enhancements.