"""
Sigma DataTech Transaction Analytics Pipeline
Day 7 Pipeline Brain - reviewed and production-hardened version.

Architecture: Bronze -> Silver -> Gold medallion pipeline.
"""

import argparse
import datetime
import json
import logging
import shutil
import uuid
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    count,
    countDistinct,
    current_timestamp,
    desc,
    first,
    input_file_name,
    last,
    lit,
    row_number,
    sum,
    when,
)
from pyspark.sql.types import DateType, DecimalType, StringType
from pyspark.sql.window import Window


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("sigma_transaction_pipeline")


BRONZE_REQUIRED_COLUMNS = {
    "transaction_id",
    "amount",
    "status",
    "merchant_id",
    "customer_id",
    "transaction_date",
    "payment_method",
}

MERCHANT_REQUIRED_COLUMNS = {
    "merchant_id",
    "merchant_name",
    "category",
    "city",
}

SILVER_REQUIRED_COLUMNS = BRONZE_REQUIRED_COLUMNS | MERCHANT_REQUIRED_COLUMNS | {
    "ingestion_timestamp",
    "pipeline_run_id",
    "quality_flag",
}


def validate_schema(df: DataFrame, required_columns: Iterable[str], stage_name: str) -> None:
    """Raise a clear error if a required source column is missing."""
    missing_columns = sorted(set(required_columns) - set(df.columns))
    if missing_columns:
        raise ValueError(f"[Stage: {stage_name}] Missing columns: {missing_columns}")


def log_count(df: DataFrame, stage_name: str, label: str, counts: dict) -> int:
    """Count a DataFrame once, log it, and keep the value for run metadata."""
    row_count = df.count()
    counts[f"{stage_name}.{label}"] = row_count
    LOGGER.info("[Stage: %s] %s: %,d rows", stage_name, label, row_count)
    return row_count


def delete_partition(output_path: str, partition_column: str, partition_value: str) -> None:
    """Delete the target partition before overwrite so same-day reruns are idempotent."""
    partition_path = f"{output_path}/{partition_column}={partition_value}"
    shutil.rmtree(partition_path, ignore_errors=True)


def write_metadata(spark: SparkSession, output_path: str, run_metadata: dict) -> None:
    """Write the run summary JSON at the end of the pipeline."""
    metadata_json = json.dumps(run_metadata, sort_keys=True)
    metadata_path = f"{output_path}/run_metadata/run_date={run_metadata['run_date']}"
    spark.createDataFrame([(metadata_json,)], ["value"]).write.mode("overwrite").text(metadata_path)


def ingest_bronze(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    run_date: str,
    run_id: str,
    counts: dict,
) -> None:
    """Load raw daily transactions and write the Bronze partition."""
    stage_name = "ingest_bronze"
    try:
        raw_transactions = (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .csv(input_path)
        )
        validate_schema(raw_transactions, BRONZE_REQUIRED_COLUMNS, stage_name)
        log_count(raw_transactions, stage_name, "input_count", counts)

        enriched_transactions = (
            raw_transactions.withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", input_file_name())
            .withColumn("pipeline_run_id", lit(run_id))
            .withColumn("ingestion_date", lit(run_date))
        )
        log_count(enriched_transactions, stage_name, "output_count", counts)

        delete_partition(output_path, "ingestion_date", run_date)
        enriched_transactions.write.mode("overwrite").partitionBy("ingestion_date").parquet(output_path)
    except Exception as exc:
        LOGGER.exception("[Stage: %s] Failed after counts=%s", stage_name, counts)
        raise exc


def transform_silver(
    spark: SparkSession,
    bronze_path: str,
    merchants_path: str,
    output_path: str,
    run_date: str,
    counts: dict,
) -> None:
    """Clean, deduplicate, and enrich Bronze transactions into Silver."""
    stage_name = "transform_silver"
    try:
        bronze_transactions = spark.read.parquet(bronze_path).filter(col("ingestion_date") == run_date)
        validate_schema(bronze_transactions, BRONZE_REQUIRED_COLUMNS, stage_name)
        log_count(bronze_transactions, stage_name, "input_count", counts)

        typed_transactions = (
            bronze_transactions.withColumn("amount", col("amount").cast(DecimalType(18, 4)))
            .withColumn("transaction_date", col("transaction_date").cast(DateType()))
            .withColumn("transaction_id", col("transaction_id").cast(StringType()))
            .withColumn("merchant_id", col("merchant_id").cast(StringType()))
            .withColumn("customer_id", col("customer_id").cast(StringType()))
            .withColumn("status", col("status").cast(StringType()))
        )
        log_count(typed_transactions, stage_name, "after_cast_count", counts)

        filtered_transactions = typed_transactions.filter(
            col("transaction_id").isNotNull()
            & col("merchant_id").isNotNull()
            & col("customer_id").isNotNull()
            & col("transaction_date").isNotNull()
            & col("status").isNotNull()
            & col("amount").isNotNull()
            & (col("amount") >= 0)
        )
        log_count(filtered_transactions, stage_name, "after_filter_count", counts)

        dedupe_window = Window.partitionBy("transaction_id").orderBy(desc("ingestion_timestamp"))
        deduped_transactions = (
            filtered_transactions.withColumn("row_num", row_number().over(dedupe_window))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )
        log_count(deduped_transactions, stage_name, "after_dedup_count", counts)

        merchants = spark.read.option("header", "true").csv(merchants_path)
        validate_schema(merchants, MERCHANT_REQUIRED_COLUMNS, stage_name)

        joined_transactions = (
            deduped_transactions.join(broadcast(merchants), "merchant_id", "left")
            .withColumn(
                "quality_flag",
                when(col("merchant_name").isNotNull(), lit("CLEAN")).otherwise(lit("UNMATCHED")),
            )
        )
        validate_schema(joined_transactions, SILVER_REQUIRED_COLUMNS, stage_name)
        log_count(joined_transactions, stage_name, "output_count", counts)

        delete_partition(output_path, "transaction_date", run_date)
        joined_transactions.write.mode("overwrite").partitionBy("transaction_date").parquet(output_path)
    except Exception as exc:
        LOGGER.exception("[Stage: %s] Failed after counts=%s", stage_name, counts)
        raise exc


def build_merchant_performance(
    spark: SparkSession,
    silver_path: str,
    output_path: str,
    run_date: str,
    counts: dict,
) -> None:
    """Build daily merchant-level revenue and reliability metrics."""
    stage_name = "build_merchant_performance"
    try:
        silver_df = spark.read.parquet(silver_path).filter(col("transaction_date") == run_date)
        validate_schema(silver_df, SILVER_REQUIRED_COLUMNS, stage_name)
        log_count(silver_df, stage_name, "input_count", counts)

        merchant_performance_df = silver_df.groupBy(
            "merchant_id",
            "merchant_name",
            "category",
            "city",
            "transaction_date",
        ).agg(
            sum(when(col("status") == "COMPLETED", col("amount")).otherwise(0)).alias("total_revenue"),
            count("*").alias("txn_count"),
            (count(when(col("status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct"),
        )
        log_count(merchant_performance_df, stage_name, "output_count", counts)

        delete_partition(output_path, "transaction_date", run_date)
        merchant_performance_df.write.mode("overwrite").partitionBy("transaction_date").parquet(output_path)
    except Exception as exc:
        LOGGER.exception("[Stage: %s] Failed after counts=%s", stage_name, counts)
        raise exc


def build_customer_ltv(
    spark: SparkSession,
    silver_path: str,
    output_path: str,
    run_date: str,
    counts: dict,
) -> None:
    """Build customer value metrics for the current daily partition."""
    stage_name = "build_customer_ltv"
    try:
        silver_df = spark.read.parquet(silver_path).filter(col("transaction_date") == run_date)
        validate_schema(silver_df, SILVER_REQUIRED_COLUMNS, stage_name)
        log_count(silver_df, stage_name, "input_count", counts)

        customer_ltv_df = silver_df.groupBy("customer_id").agg(
            sum(when(col("status") == "COMPLETED", col("amount")).otherwise(0)).alias("total_spent"),
            count("*").alias("total_txns"),
            avg(when(col("status") == "COMPLETED", col("amount"))).alias("avg_txn_value"),
            first("transaction_date").alias("first_txn_date"),
            last("transaction_date").alias("last_txn_date"),
            first("payment_method").alias("preferred_payment_method"),
        )
        log_count(customer_ltv_df, stage_name, "output_count", counts)

        delete_partition(output_path, "run_date", run_date)
        customer_ltv_df.withColumn("run_date", lit(run_date)).write.mode("overwrite").partitionBy("run_date").parquet(output_path)
    except Exception as exc:
        LOGGER.exception("[Stage: %s] Failed after counts=%s", stage_name, counts)
        raise exc


def build_daily_summary(
    spark: SparkSession,
    silver_path: str,
    output_path: str,
    run_date: str,
    counts: dict,
) -> None:
    """Build one daily aggregate row across all merchants."""
    stage_name = "build_daily_summary"
    try:
        silver_df = spark.read.parquet(silver_path).filter(col("transaction_date") == run_date)
        validate_schema(silver_df, SILVER_REQUIRED_COLUMNS, stage_name)
        log_count(silver_df, stage_name, "input_count", counts)

        daily_summary_df = silver_df.groupBy("transaction_date").agg(
            sum(when(col("status") == "COMPLETED", col("amount")).otherwise(0)).alias("total_revenue"),
            count("*").alias("total_txns"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("merchant_id").alias("unique_merchants"),
            (count(when(col("status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct"),
        )
        log_count(daily_summary_df, stage_name, "output_count", counts)

        delete_partition(output_path, "transaction_date", run_date)
        daily_summary_df.write.mode("overwrite").partitionBy("transaction_date").parquet(output_path)
    except Exception as exc:
        LOGGER.exception("[Stage: %s] Failed after counts=%s", stage_name, counts)
        raise exc


def run_gold(
    spark: SparkSession,
    silver_path: str,
    merchant_performance_path: str,
    customer_ltv_path: str,
    daily_summary_path: str,
    run_date: str,
    counts: dict,
) -> None:
    """Run all Gold table builders."""
    stage_name = "run_gold"
    try:
        build_merchant_performance(spark, silver_path, merchant_performance_path, run_date, counts)
        build_customer_ltv(spark, silver_path, customer_ltv_path, run_date, counts)
        build_daily_summary(spark, silver_path, daily_summary_path, run_date, counts)
    except Exception as exc:
        LOGGER.exception("[Stage: %s] Gold layer failed", stage_name)
        raise exc


def parse_args() -> argparse.Namespace:
    """Read all paths and run settings from runtime parameters."""
    parser = argparse.ArgumentParser(description="Run the Sigma transaction analytics pipeline.")
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--bronze-output-path", required=True)
    parser.add_argument("--merchants-path", required=True)
    parser.add_argument("--silver-output-path", required=True)
    parser.add_argument("--merchant-performance-path", required=True)
    parser.add_argument("--customer-ltv-path", required=True)
    parser.add_argument("--daily-summary-path", required=True)
    parser.add_argument("--metadata-output-path", required=True)
    parser.add_argument("--run-date", required=True)
    return parser.parse_args()


def main() -> None:
    """Run the full Bronze -> Silver -> Gold pipeline and write run metadata."""
    args = parse_args()
    spark = SparkSession.builder.appName("SigmaPay Pipeline").getOrCreate()
    run_id = str(uuid.uuid4())
    started_at = datetime.datetime.now(datetime.timezone.utc)
    counts = {}
    run_metadata = {
        "pipeline_name": "sigma_transaction_analytics",
        "run_date": args.run_date,
        "run_id": run_id,
        "run_status": "SUCCESS",
        "error_message": None,
        "started_at": started_at.isoformat(),
        "completed_at": None,
        "row_counts": counts,
    }

    try:
        ingest_bronze(spark, args.input_path, args.bronze_output_path, args.run_date, run_id, counts)
        transform_silver(
            spark,
            args.bronze_output_path,
            args.merchants_path,
            args.silver_output_path,
            args.run_date,
            counts,
        )
        run_gold(
            spark,
            args.silver_output_path,
            args.merchant_performance_path,
            args.customer_ltv_path,
            args.daily_summary_path,
            args.run_date,
            counts,
        )
    except Exception as exc:
        run_metadata["run_status"] = "FAILED"
        run_metadata["error_message"] = str(exc)
        raise
    finally:
        run_metadata["completed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        write_metadata(spark, args.metadata_output_path, run_metadata)


if __name__ == "__main__":
    main()
