import logging
import shutil
import uuid
import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit, when, sum, count, max, first, last, mode
from pyspark.sql.types import DecimalType, DateType, StringType, StructType, StructField

logging.basicConfig(level=logging.INFO)

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    try:
        logging.info("[Stage: Ingest Bronze] Starting ingestion")
        raw_transactions = (spark.read
                          .option("header", "true")
                           .option("inferSchema", "false")
                           .parquet(input_path))
        
        logging.info(f"[Stage: Ingest Bronze] Input count: {raw_transactions.count():,}")

        enriched_transactions = (raw_transactions
                               .withColumn("ingestion_timestamp", current_timestamp())
                                 .withColumn("source_file", input_file_name())
                               .withColumn("pipeline_run_id", lit(run_id)))

        partition_path = f"{output_path}/ingestion_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        enriched_transactions.write.partitionBy("ingestion_date").mode("overwrite").parquet(output_path)
        logging.info("[Stage: Ingest Bronze] Ingestion completed successfully")
    except Exception as e:
        logging.error(f"[Stage: Ingest Bronze] Error: {e}")
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    try:
        logging.info("[Stage: Transform Silver] Starting transformation")
        bronze_transactions = (spark.read
                             .option("header", "true")
                               .parquet(bronze_path)
                             .where(f"ingestion_date = '{run_date}'"))

        logging.info(f"[Stage: Transform Silver] Input count: {bronze_transactions.count():,}")

        silver_transactions = (bronze_transactions
                             .withColumn("amount", col("amount").cast(DecimalType(18,4)))
                               .withColumn("transaction_date", col("transaction_date").cast(DateType()))
                             .withColumn("transaction_id", col("transaction_id").cast(StringType()))
                              .withColumn("merchant_id", col("merchant_id").cast(StringType()))
                              .withColumn("customer_id", col("customer_id").cast(StringType())))

        filtered_transactions = (silver_transactions
                               .where(col("transaction_id").isNotNull())
                                .where(col("amount") > 0))

        logging.info(f"[Stage: Transform Silver] After filter count: {filtered_transactions.count():,}")

        deduped_transactions = (filtered_transactions
                               .withColumn("rank", 
                                             when(col("ingestion_timestamp") == 
                                                  col("ingestion_timestamp").max().over(window.partitionBy("transaction_id")), 1).otherwise(0))
                                 .where(col("rank") == 1)
                                .drop("rank"))

        logging.info(f"[Stage: Transform Silver] After dedup count: {deduped_transactions.count():,}")

        merchants = (spark.read
                   .option("header", "true")
                   .csv(merchants_path)
                   .hint("broadcast"))

        joined_transactions = (deduped_transactions
                             .join(merchants, deduped_transactions.col("merchant_id") == merchants.col("merchant_id"), "left")
                               .withColumn("quality_flag", 
                                          when(col("merchant_id").isNotNull(), "CLEAN")
                                         .otherwise("UNMATCHED")))

        partition_path = f"{output_path}/transaction_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        joined_transactions.write.partitionBy("transaction_date").mode("overwrite").parquet(output_path)
        logging.info("[Stage: Transform Silver] Transformation completed successfully")
    except Exception as e:
        logging.error(f"[Stage: Transform Silver] Error: {e}")
        raise

def run_gold(spark, silver_path, gold_output_dir, run_date):
    try:
        logging.info("[Stage: Run Gold] Starting gold layer processing")
        build_merchant_performance(spark, silver_path, f"{gold_output_dir}/merchant_performance", run_date)
        build_customer_ltv(spark, silver_path, f"{gold_output_dir}/customer_ltv")
        build_daily_summary(spark, silver_path, f"{gold_output_dir}/daily_summary", run_date)
        
        run_metadata = {
            "run_date": run_date,
            "gold_output_dir": gold_output_dir,
            "functions_run": ["build_merchant_performance", "build_customer_ltv", "build_daily_summary"],
            "timestamp": datetime.datetime.now().isoformat(),
            "run_status": "SUCCESS"
        }
        
        spark.sparkContext.parallelize([run_metadata]).toDF().write.mode('overwrite').json(f"{gold_output_dir}/run_metadata")
        logging.info("[Stage: Run Gold] Gold layer processing completed successfully")
    except Exception as e:
        logging.error(f"[Stage: Run Gold] Error: {e}")
        run_metadata = {
            "run_date": run_date,
            "gold_output_dir": gold_output_dir,
            "functions_run": ["build_merchant_performance", "build_customer_ltv", "build_daily_summary"],
            "timestamp": datetime.datetime.now().isoformat(),
            "run_status": "FAILED",
            "error_message": str(e)
        }
        
        spark.sparkContext.parallelize([run_metadata]).toDF().write.mode('overwrite').json(f"{gold_output_dir}/run_metadata")
        raise

def build_merchant_performance(spark, silver_path, output_path, run_date):
    try:
        logging.info("[Stage: Build Merchant Performance] Starting merchant performance aggregation")
        silver_df = spark.read.parquet(silver_path)
        silver_df = silver_df.filter(col("transaction_date") == run_date)  # Partition pruning

        merchant_performance_df = silver_df.groupBy("merchant_id", "merchant_name", "category", "city", "transaction_date") \
            .agg(
                sum(when(col("status") == "COMPLETED", col("amount")).otherwise(0)).alias("total_revenue"),
                count("*").alias("txn_count"),
                (count(when(col("status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct")
            )
        
        logging.info(f"[Stage: Build Merchant Performance] Output count: {merchant_performance_df.count():,}")

        partition_path = f"{output_path}/transaction_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        merchant_performance_df.repartition("transaction_date") \
            .write.mode('overwrite').partitionBy("transaction_date").parquet(output_path)
        logging.info("[Stage: Build Merchant Performance] Merchant performance aggregation completed successfully")
    except Exception as e:
        logging.error(f"[Stage: Build Merchant Performance] Error: {e}")
        raise

def build_customer_ltv(spark, silver_path, output_path):
    try:
        logging.info("[Stage: Build Customer LTV] Starting customer LTV aggregation")
        silver_df = spark.read.parquet(silver_path)

        customer_ltv_df = silver_df.groupBy("customer_id") \
           .agg(
                sum(when(col("status") == "COMPLETED", col("amount"))).alias("total_spent"),
                count("*").alias("total_txns"),
                avg(when(col("status") == "COMPLETED", col("amount"))).alias("avg_txn_value"),
                first("transaction_date").alias("first_txn_date"),
                last("transaction_date").alias("last_txn_date"),
                mode("payment_method").alias("preferred_payment_method")
            )
        
        logging.info(f"[Stage: Build Customer LTV] Output count: {customer_ltv_df.count():,}")

        shutil.rmtree(output_path, ignore_errors=True)
        
        customer_ltv_df.write.mode('overwrite').parquet(output_path)
        logging.info("[Stage: Build Customer LTV] Customer LTV aggregation completed successfully")
    except Exception as e:
        logging.error(f"[Stage: Build Customer LTV] Error: {e}")
        raise

def build_daily_summary(spark, silver_path, output_path, run_date):
    try:
        logging.info("[Stage: Build Daily Summary] Starting daily summary aggregation")
        silver_df = spark.read.parquet(silver_path)
        silver_df = silver_df.filter(col("transaction_date") == run_date)  # Partition pruning

        daily_summary_df = silver_df.groupBy("transaction_date") \
           .agg(
                sum(when(col("status") == "COMPLETED", col("amount"))).alias("total_revenue"),
                count("*").alias("total_txns"),
                count(distinct("customer_id")).alias("unique_customers"),
                count(distinct("merchant_id")).alias("unique_merchants"),
                (count(when(col("status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct")
            )
        
        logging.info(f"[Stage: Build Daily Summary] Output count: {daily_summary_df.count():,}")

        partition_path = f"{output_path}/transaction_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        daily_summary_df.repartition("transaction_date") \
            .write.mode('overwrite').partitionBy("transaction_date").parquet(output_path)
        logging.info("[Stage: Build Daily Summary] Daily summary aggregation completed successfully")
    except Exception as e:
        logging.error(f"[Stage: Build Daily Summary] Error: {e}")
        raise

def main():
    spark = (SparkSession.builder
           .appName("SigmaPay Pipeline")
             .getOrCreate())

    input_path = sys.argv[1]  # e.g., "s3://bucket/bronze/transactions_raw"
    bronze_output_path = sys.argv[2]  # e.g., "s3://bucket/bronze/output"
    merchants_path = sys.argv[3]  # e.g., "s3://bucket/silver/payment_fee_lookup"
    silver_output_path = sys.argv[4]  # e.g., "s3://bucket/silver/output"
    gold_output_path = sys.argv[5]  # e.g., "s3://bucket/gold/output"
    run_date = sys.argv[6]  # e.g., "2023-10-01"
    run_id = str(uuid.uuid4())

    try:
        ingest_bronze(spark, input_path, bronze_output_path, run_date, run_id)
        transform_silver(spark, bronze_output_path, merchants_path, silver_output_path, run_date)
        run_gold(spark, silver_output_path, gold_output_path, run_date)
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()
