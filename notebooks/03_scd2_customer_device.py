"""SCD Type 2 for customer/device dimension using Delta MERGE.

The source data is Synthetic Data.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    from delta.tables import DeltaTable
except Exception:  # pragma: no cover - keeps the script importable without Delta locally
    DeltaTable = None


SOURCE_PATH = os.environ.get("UPI_CUSTOMER_SOURCE_PATH", str(Path("sample_data") / "customer_dim.csv"))
SCD2_PATH = os.environ.get("UPI_SCD2_PATH", "delta/dim/customer_device_scd2")
CHECKPOINT_PATH = os.environ.get("UPI_SCD2_CHECKPOINT", "delta/_checkpoints/customer_device_scd2")


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("upi-sentinel-scd2").getOrCreate()


def load_customer_source(spark: SparkSession) -> DataFrame:
    return spark.read.option("header", "true").option("inferSchema", "true").csv(SOURCE_PATH)


def normalize_customer_source(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("effective_start_date", F.current_date())
        .withColumn("effective_end_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("device_id"), F.lit("")),
                    F.coalesce(F.col("location_city"), F.lit("")),
                    F.coalesce(F.col("bank"), F.lit("")),
                    F.coalesce(F.col("kyc_status"), F.lit("")),
                ),
                256,
            ),
        )
    )


def detect_scd2_changes(source_df: DataFrame, current_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Return rows to expire and rows to insert for SCD2 maintenance."""
    source_norm = normalize_customer_source(source_df)

    current_current = current_df.filter(F.col("is_current") == F.lit(True))
    joined = source_norm.alias("s").join(
        current_current.select(F.col("user_id"), F.col("record_hash").alias("current_record_hash")).alias("t"),
        on="user_id",
        how="left",
    )
    new_rows = joined.filter(F.col("t.current_record_hash").isNull())
    changed_rows = joined.filter(
        F.col("t.current_record_hash").isNotNull() & (F.col("s.record_hash") != F.col("t.current_record_hash"))
    )
    insert_projection = [
        F.col("user_id"),
        F.col("name"),
        F.col("bank"),
        F.col("kyc_status"),
        F.col("device_id"),
        F.col("location_city"),
        F.col("effective_start_date"),
        F.col("effective_end_date"),
        F.col("is_current"),
        F.col("record_hash"),
    ]
    return (
        changed_rows.select(F.col("user_id"), F.col("record_hash"), F.col("effective_start_date")),
        new_rows.select(*insert_projection).unionByName(changed_rows.select(*insert_projection)),
    )


def initialize_table(spark: SparkSession, source_df: DataFrame) -> None:
    normalized = normalize_customer_source(source_df)
    (
        normalized.write.format("delta")
        .mode("overwrite")
        .save(SCD2_PATH)
    )
    spark.sql("CREATE DATABASE IF NOT EXISTS dim")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS dim.customer_device_scd2 USING DELTA LOCATION '{SCD2_PATH}'"
    )


def merge_customer_dim(spark: SparkSession, source_df: DataFrame) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS dim")

    if DeltaTable is None or not Path(SCD2_PATH).exists():
        initialize_table(spark, source_df)
        return

    current_df = spark.read.format("delta").load(SCD2_PATH)
    expire_df, insert_df = detect_scd2_changes(source_df, current_df)

    if expire_df.rdd.isEmpty() and insert_df.rdd.isEmpty():
        return

    delta_table = DeltaTable.forPath(spark, SCD2_PATH)
    if not expire_df.rdd.isEmpty():
        delta_table.alias("t").merge(
            expire_df.alias("s"),
            "t.user_id = s.user_id AND t.is_current = true",
        ).whenMatchedUpdate(
            condition="t.record_hash <> s.record_hash",
            set={
                "effective_end_date": "current_date()",
                "is_current": "false",
            },
        ).execute()

    new_versions = insert_df.select(
        "user_id",
        "name",
        "bank",
        "kyc_status",
        "device_id",
        "location_city",
        "effective_start_date",
        "effective_end_date",
        "is_current",
        "record_hash",
    )
    (
        new_versions.write.format("delta")
        .mode("append")
        .save(SCD2_PATH)
    )


def main() -> None:
    spark = get_spark()
    source_df = load_customer_source(spark)
    merge_customer_dim(spark, source_df)
    spark.sql(
        "CREATE OR REPLACE TEMP VIEW customer_dim_current AS "
        f"SELECT * FROM delta.`{SCD2_PATH}` WHERE is_current = true"
    )
    spark.sql("SELECT COUNT(*) AS active_customer_count FROM customer_dim_current").show()


if __name__ == "__main__":
    main()
