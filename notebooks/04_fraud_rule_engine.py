"""Fraud rule engine for synthetic UPI transactions.

Rules implemented:
- velocity_check
- geo_anomaly
- device_mule_check
- failure_burst_check

The source data is Synthetic Data.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


SILVER_PATH = os.environ.get("UPI_SILVER_PATH", "delta/silver/upi_transactions")
FRAUD_SCORED_PATH = os.environ.get("UPI_FRAUD_SCORED_PATH", "delta/gold/fraud_scored_transactions")


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("upi-sentinel-fraud-engine").getOrCreate()


def load_silver_data(spark: SparkSession) -> DataFrame:
    return spark.read.format("delta").load(SILVER_PATH)


def engineer_features(df: DataFrame) -> DataFrame:
    ts_col = F.col("timestamp").cast("timestamp")
    unix_ts = F.unix_timestamp(ts_col)

    user_window_1m = Window.partitionBy("user_id").orderBy(unix_ts).rangeBetween(-60, 0)
    user_window_ordered = Window.partitionBy("user_id").orderBy(unix_ts)
    device_window_10m = Window.partitionBy("device_id").orderBy(unix_ts).rangeBetween(-600, 0)
    failure_seq = Window.partitionBy("user_id").orderBy(unix_ts)

    base = df.withColumn("timestamp", ts_col).withColumn("transaction_date", F.to_date("timestamp"))

    with_velocity = base.withColumn(
        "velocity_amount_1m",
        F.sum(F.col("amount").cast("double")).over(user_window_1m),
    ).withColumn("velocity_check", F.col("velocity_amount_1m") > F.lit(50000))

    with_geo = (
        with_velocity
        .withColumn("prev_city", F.lag("location_city").over(user_window_ordered))
        .withColumn("prev_ts", F.lag("timestamp").over(user_window_ordered))
        .withColumn("next_city", F.lead("location_city").over(user_window_ordered))
        .withColumn("next_ts", F.lead("timestamp").over(user_window_ordered))
        .withColumn(
            "geo_anomaly",
            (
                ((F.col("prev_city").isNotNull()) & (F.col("prev_city") != F.col("location_city")) & ((F.unix_timestamp("timestamp") - F.unix_timestamp("prev_ts")) <= 300))
                |
                ((F.col("next_city").isNotNull()) & (F.col("next_city") != F.col("location_city")) & ((F.unix_timestamp("next_ts") - F.unix_timestamp("timestamp")) <= 300))
            ),
        )
    )

    with_mule = with_geo.withColumn(
        "device_users_10m",
        F.size(F.collect_set("user_id").over(device_window_10m)),
    ).withColumn("device_mule_check", F.col("device_users_10m") >= F.lit(3))

    with_failure_seq = (
        with_mule
        .withColumn("status_upper", F.upper("status"))
        .withColumn("lag1", F.lag("status_upper", 1).over(failure_seq))
        .withColumn("lag2", F.lag("status_upper", 2).over(failure_seq))
        .withColumn("lag3", F.lag("status_upper", 3).over(failure_seq))
        .withColumn("lag4", F.lag("status_upper", 4).over(failure_seq))
        .withColumn("lag5", F.lag("status_upper", 5).over(failure_seq))
        .withColumn("lag5_ts", F.lag("timestamp", 5).over(failure_seq))
        .withColumn(
            "failure_burst_check",
            (
                (F.col("status_upper") == F.lit("SUCCESS"))
                & (F.col("lag1") == F.lit("FAILED"))
                & (F.col("lag2") == F.lit("FAILED"))
                & (F.col("lag3") == F.lit("FAILED"))
                & (F.col("lag4") == F.lit("FAILED"))
                & (F.col("lag5") == F.lit("FAILED"))
                & ((F.unix_timestamp("timestamp") - F.unix_timestamp("lag5_ts")) <= 600)
            ),
        )
    )

    risk_score = (
        F.when(F.col("velocity_check"), F.lit(40)).otherwise(F.lit(0))
        + F.when(F.col("geo_anomaly"), F.lit(25)).otherwise(F.lit(0))
        + F.when(F.col("device_mule_check"), F.lit(40)).otherwise(F.lit(0))
        + F.when(F.col("failure_burst_check"), F.lit(35)).otherwise(F.lit(0))
    )

    reason_array = F.array(
        F.when(F.col("velocity_check"), F.lit("velocity_check")),
        F.when(F.col("geo_anomaly"), F.lit("geo_anomaly")),
        F.when(F.col("device_mule_check"), F.lit("device_mule_check")),
        F.when(F.col("failure_burst_check"), F.lit("failure_burst_check")),
    )

    return (
        with_failure_seq
        .withColumn("risk_score", F.least(risk_score, F.lit(100)))
        .withColumn("fraud_reasons_raw", reason_array)
        .withColumn("fraud_reasons", F.expr("filter(fraud_reasons_raw, x -> x is not null)"))
        .drop("fraud_reasons_raw")
        .withColumn("fraud_reason", F.concat_ws(",", F.col("fraud_reasons")))
        .withColumn("is_fraud", F.col("risk_score") > F.lit(0))
    )


def write_fraud_scores(df: DataFrame) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("transaction_date")
        .save(FRAUD_SCORED_PATH)
    )


def main() -> None:
    spark = get_spark()
    silver_df = load_silver_data(spark)
    scored_df = engineer_features(silver_df)
    write_fraud_scores(scored_df)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS gold.fraud_scored_transactions USING DELTA LOCATION '{FRAUD_SCORED_PATH}'"
    )


if __name__ == "__main__":
    main()
