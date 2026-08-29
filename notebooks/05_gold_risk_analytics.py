"""Gold layer analytics for synthetic UPI fraud detection.

Creates curated tables:
- gold.fraud_transactions
- gold.mule_accounts
- gold.daily_merchant_fraud_kpi

The source data is Synthetic Data.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


FRAUD_SCORED_PATH = os.environ.get("UPI_FRAUD_SCORED_PATH", "delta/gold/fraud_scored_transactions")
FRAUD_TXN_PATH = os.environ.get("UPI_GOLD_FRAUD_TXN_PATH", "delta/gold/fraud_transactions")
MULE_ACCOUNTS_PATH = os.environ.get("UPI_GOLD_MULE_PATH", "delta/gold/mule_accounts")
MERC_KPI_PATH = os.environ.get("UPI_GOLD_KPI_PATH", "delta/gold/daily_merchant_fraud_kpi")


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("upi-sentinel-gold-analytics").getOrCreate()


def load_scored_data(spark: SparkSession) -> DataFrame:
    return spark.read.format("delta").load(FRAUD_SCORED_PATH)


def build_gold_tables(spark: SparkSession) -> None:
    scored_df = load_scored_data(spark).cache()
    scored_df.count()

    fraud_txn_df = scored_df.filter(F.col("is_fraud") == F.lit(True))
    mule_accounts_df = (
        scored_df.filter(F.col("device_mule_check") == F.lit(True))
        .groupBy("user_id")
        .agg(
            F.count("*").alias("flagged_transaction_count"),
            F.countDistinct("device_id").alias("distinct_devices"),
            F.countDistinct("location_city").alias("distinct_cities"),
            F.max("risk_score").alias("max_risk_score"),
            F.min("timestamp").alias("first_flagged_timestamp"),
            F.max("timestamp").alias("last_flagged_timestamp"),
        )
    )

    merchant_kpi_df = (
        fraud_txn_df.groupBy("transaction_date", "merchant_category")
        .agg(
            F.count("*").alias("fraud_transaction_count"),
            F.round(F.sum(F.col("amount").cast("double")), 2).alias("fraud_amount"),
            F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
            F.countDistinct("user_id").alias("distinct_users"),
        )
        .withColumn("computed_at", F.current_timestamp())
    )

    fraud_txn_df.write.format("delta").mode("overwrite").partitionBy("transaction_date").save(FRAUD_TXN_PATH)
    mule_accounts_df.write.format("delta").mode("overwrite").save(MULE_ACCOUNTS_PATH)
    merchant_kpi_df.write.format("delta").mode("overwrite").partitionBy("transaction_date").save(MERC_KPI_PATH)

    for path, table_name in [
        (FRAUD_TXN_PATH, "gold.fraud_transactions"),
        (MULE_ACCOUNTS_PATH, "gold.mule_accounts"),
        (MERC_KPI_PATH, "gold.daily_merchant_fraud_kpi"),
    ]:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS gold")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{path}'")

    try:
        spark.sql(f"OPTIMIZE delta.`{FRAUD_TXN_PATH}` ZORDER BY (user_id)")
        spark.sql(f"OPTIMIZE delta.`{MULE_ACCOUNTS_PATH}` ZORDER BY (user_id)")
        spark.sql(f"OPTIMIZE delta.`{MERC_KPI_PATH}` ZORDER BY (merchant_category)")
    except Exception:
        # Community Edition or local Spark may not support OPTIMIZE.
        pass

    scored_df.unpersist()


def main() -> None:
    spark = get_spark()
    build_gold_tables(spark)


if __name__ == "__main__":
    main()
