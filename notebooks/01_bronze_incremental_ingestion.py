"""Bronze layer incremental ingestion for synthetic UPI data.

Compatible with Databricks Community Edition and local Spark sessions.
The source data is Synthetic Data.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


BRONZE_TABLE_NAME = "bronze.upi_transactions"
BRONZE_TABLE_PATH = os.environ.get("UPI_BRONZE_PATH", "delta/bronze/upi_transactions")
CHECKPOINT_PATH = os.environ.get("UPI_BRONZE_CHECKPOINT", "delta/_checkpoints/bronze_upi_transactions")
PRIMARY_SOURCE_FILE = Path(os.environ.get("UPI_SAMPLE_PATH", "sample_data/upi_transactions.csv"))
FALLBACK_SOURCE_FILE = Path("sample_data/upi_transactions_sample.csv")


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("upi-sentinel-bronze-ingestion").getOrCreate()


def resolve_source_root_and_pattern() -> tuple[str, str]:
    if PRIMARY_SOURCE_FILE.exists():
        return str(PRIMARY_SOURCE_FILE.parent), PRIMARY_SOURCE_FILE.name
    return str(FALLBACK_SOURCE_FILE.parent), FALLBACK_SOURCE_FILE.name


def read_source_stream(spark: SparkSession, source_root: str, file_pattern: str) -> DataFrame:
    """Try Auto Loader style ingestion first, then fall back to regular file streaming."""
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", str(Path(BRONZE_TABLE_PATH) / "_schema"))
        .option("header", "true")
        .option("pathGlobFilter", file_pattern)
    )
    try:
        return reader.load(source_root)
    except Exception:
        return (
            spark.readStream
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("pathGlobFilter", file_pattern)
            .load(source_root)
        )


def prepare_bronze(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withWatermark("timestamp", "10 minutes")
        .dropDuplicates(["transaction_id"])
    )


def write_bronze_stream(df: DataFrame, checkpoint_path: str, bronze_path: str):
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(once=True)
        .start(bronze_path)
    )


def main() -> None:
    spark = get_spark()
    source_root, file_pattern = resolve_source_root_and_pattern()
    source_df = read_source_stream(spark, source_root, file_pattern)
    bronze_df = prepare_bronze(source_df)

    query = write_bronze_stream(bronze_df, CHECKPOINT_PATH, BRONZE_TABLE_PATH)
    query.awaitTermination()

    spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE_NAME}
        USING DELTA
        LOCATION '{BRONZE_TABLE_PATH}'
        """
    )


if __name__ == "__main__":
    main()
