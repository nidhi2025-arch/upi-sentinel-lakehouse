"""Silver layer data quality checks for synthetic UPI transactions.

This notebook implements PyDeequ-style rules without the external library.
The source data is Synthetic Data.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import List, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


BRONZE_PATH = os.environ.get("UPI_BRONZE_PATH", "delta/bronze/upi_transactions")
SILVER_PATH = os.environ.get("UPI_SILVER_PATH", "delta/silver/upi_transactions")
QUARANTINE_PATH = os.environ.get("UPI_QUARANTINE_PATH", "delta/silver/quarantine")
CHECKPOINT_PATH = os.environ.get("UPI_SILVER_CHECKPOINT", "delta/_checkpoints/silver_upi_transactions")
PRIMARY_SOURCE_FILE = Path(os.environ.get("UPI_SAMPLE_PATH", "sample_data/upi_transactions.csv"))
FALLBACK_SOURCE_FILE = Path("sample_data/upi_transactions_sample.csv")


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("upi-sentinel-silver-quality").getOrCreate()


def resolve_source_path() -> Path:
    if PRIMARY_SOURCE_FILE.exists():
        return PRIMARY_SOURCE_FILE
    return FALLBACK_SOURCE_FILE


def load_bronze_data(spark: SparkSession) -> DataFrame:
    if Path(BRONZE_PATH).exists():
        return spark.read.format("delta").load(BRONZE_PATH)
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(str(resolve_source_path()))
    )


def add_quality_rules(df: DataFrame) -> DataFrame:
    """Attach 12+ rule columns and a compact failure summary."""
    email_like_vpa = r"^[a-z0-9._%+-]+@[a-z0-9.-]+$"
    future_cutoff = F.current_timestamp()

    base = (
        df.withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("transaction_id_not_null", F.col("transaction_id").isNotNull())
        .withColumn("utr_not_null", F.col("utr_number").isNotNull())
        .withColumn("amount_positive", F.col("amount").cast("double") > F.lit(0))
        .withColumn("sender_vpa_valid", F.lower(F.col("sender_vpa")).rlike(email_like_vpa))
        .withColumn("receiver_vpa_valid", F.lower(F.col("receiver_vpa")).rlike(email_like_vpa))
        .withColumn("timestamp_not_future", F.col("timestamp") <= future_cutoff)
        .withColumn("user_id_not_null", F.col("user_id").isNotNull())
        .withColumn(
            "status_valid",
            F.upper(F.col("status")).isin("SUCCESS", "FAILED"),
        )
        .withColumn(
            "failure_reason_valid",
            F.when(F.upper(F.col("status")) == "FAILED", F.col("failure_reason").isNotNull() & (F.length(F.col("failure_reason")) > 0))
            .otherwise(F.lit(True)),
        )
        .withColumn("device_id_not_null", F.col("device_id").isNotNull())
        .withColumn("ip_address_valid", F.col("ip_address").rlike(r"^(\d{1,3}\.){3}\d{1,3}$"))
        .withColumn("merchant_category_not_null", F.col("merchant_category").isNotNull())
        .withColumn("location_city_not_null", F.col("location_city").isNotNull())
        .withColumn(
            "amount_reasonable",
            F.col("amount").cast("double").between(0.01, 1_000_000),
        )
    )

    duplicate_window = Window.partitionBy("utr_number")
    with_dupes = base.withColumn("utr_duplicate_count", F.count("*").over(duplicate_window))
    def safe_bool(col_name: str):
        return F.coalesce(F.col(col_name), F.lit(False))

    validated = with_dupes.withColumn("utr_unique", F.col("utr_duplicate_count") == 1).withColumn(
        "_is_valid",
        safe_bool("transaction_id_not_null")
        & safe_bool("utr_not_null")
        & safe_bool("utr_unique")
        & safe_bool("amount_positive")
        & safe_bool("sender_vpa_valid")
        & safe_bool("receiver_vpa_valid")
        & safe_bool("timestamp_not_future")
        & safe_bool("user_id_not_null")
        & safe_bool("status_valid")
        & safe_bool("failure_reason_valid")
        & safe_bool("device_id_not_null")
        & safe_bool("ip_address_valid")
        & safe_bool("merchant_category_not_null")
        & safe_bool("location_city_not_null")
        & safe_bool("amount_reasonable"),
    )

    rule_cols = [
        "transaction_id_not_null",
        "utr_not_null",
        "utr_unique",
        "amount_positive",
        "sender_vpa_valid",
        "receiver_vpa_valid",
        "timestamp_not_future",
        "user_id_not_null",
        "status_valid",
        "failure_reason_valid",
        "device_id_not_null",
        "ip_address_valid",
        "merchant_category_not_null",
        "location_city_not_null",
        "amount_reasonable",
    ]
    failure_array = F.array(
        *[
            F.when(~F.coalesce(F.col(rule), F.lit(False)), F.lit(rule))
            for rule in rule_cols
        ]
    )
    return (
        validated
        .withColumn("_dq_failures_raw", failure_array)
        .withColumn("_dq_failures", F.expr("filter(_dq_failures_raw, x -> x is not null)"))
        .drop("_dq_failures_raw")
    )


def split_valid_invalid(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    valid_df = df.filter(F.col("_is_valid") == F.lit(True)).drop("utr_duplicate_count")
    invalid_df = df.filter(F.coalesce(F.col("_is_valid"), F.lit(False)) == F.lit(False)).drop("utr_duplicate_count")
    return valid_df, invalid_df


def summarize_quality(df: DataFrame) -> DataFrame:
    return df.agg(
        F.count("*").alias("total_rows"),
        F.sum(F.when(F.col("_is_valid"), 1).otherwise(0)).alias("valid_rows"),
        F.sum(F.when(~F.col("_is_valid"), 1).otherwise(0)).alias("invalid_rows"),
    )


def write_silver_outputs(valid_df: DataFrame, invalid_df: DataFrame) -> None:
    (
        valid_df.write.format("delta")
        .mode("overwrite")
        .partitionBy("status")
        .save(SILVER_PATH)
    )
    (
        invalid_df.write.format("delta")
        .mode("overwrite")
        .save(QUARANTINE_PATH)
    )


def main() -> None:
    spark = get_spark()
    bronze_df = load_bronze_data(spark)
    scored_df = add_quality_rules(bronze_df)
    valid_df, invalid_df = split_valid_invalid(scored_df)

    write_silver_outputs(valid_df, invalid_df)
    summarize_quality(scored_df).show(truncate=False)

    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS silver.upi_transactions USING DELTA LOCATION '{SILVER_PATH}'"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS silver.quarantine USING DELTA LOCATION '{QUARANTINE_PATH}'"
    )


if __name__ == "__main__":
    main()
