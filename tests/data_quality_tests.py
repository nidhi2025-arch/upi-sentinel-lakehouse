"""Pytest-style tests for the synthetic UPI lakehouse logic."""

from __future__ import annotations

import csv
import importlib.util
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, relative_path: str):
    module_path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


silver_module = load_module("silver_quality", "notebooks/02_silver_data_quality.py")
scd2_module = load_module("scd2_customer", "notebooks/03_scd2_customer_device.py")
generator_module = load_module("upi_generator", "data_generator/generate_upi_transactions.py")


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("upi-sentinel-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_null_check_and_duplicate_utr(spark):
    rows = [
        {
            "transaction_id": "TXN1",
            "utr_number": "UTR1",
            "user_id": "U1",
            "timestamp": "2025-08-01 10:00:00",
            "amount": 100.0,
            "sender_vpa": "alice.upi@sbi",
            "receiver_vpa": "merchant.pay@hdfc",
            "merchant_category": "grocery",
            "location_city": "Mumbai",
            "device_id": "dev-1",
            "ip_address": "1.2.3.4",
            "status": "SUCCESS",
            "failure_reason": "",
        },
        {
            "transaction_id": None,
            "utr_number": "UTR1",
            "user_id": "U2",
            "timestamp": "2025-08-01 10:01:00",
            "amount": 50.0,
            "sender_vpa": "bob.upi@sbi",
            "receiver_vpa": "merchant.pay@hdfc",
            "merchant_category": "grocery",
            "location_city": "Delhi",
            "device_id": "dev-2",
            "ip_address": "1.2.3.5",
            "status": "FAILED",
            "failure_reason": "invalid_upi_pin",
        },
    ]
    df = spark.createDataFrame(rows)
    scored = silver_module.add_quality_rules(df)
    invalid_rows = scored.filter((F.col("utr_number") == "UTR1") & (F.col("_is_valid") == F.lit(False)))
    assert invalid_rows.count() == 1


def test_duplicate_utr_check_flags_both_rows(spark):
    rows = [
        {
            "transaction_id": "TXN1",
            "utr_number": "DUP-UTR",
            "user_id": "U1",
            "timestamp": "2025-08-01 10:00:00",
            "amount": 100.0,
            "sender_vpa": "alice.upi@sbi",
            "receiver_vpa": "merchant.pay@hdfc",
            "merchant_category": "grocery",
            "location_city": "Mumbai",
            "device_id": "dev-1",
            "ip_address": "1.2.3.4",
            "status": "SUCCESS",
            "failure_reason": "",
        },
        {
            "transaction_id": "TXN2",
            "utr_number": "DUP-UTR",
            "user_id": "U1",
            "timestamp": "2025-08-01 10:02:00",
            "amount": 150.0,
            "sender_vpa": "alice.upi@sbi",
            "receiver_vpa": "merchant.pay@hdfc",
            "merchant_category": "grocery",
            "location_city": "Mumbai",
            "device_id": "dev-1",
            "ip_address": "1.2.3.4",
            "status": "SUCCESS",
            "failure_reason": "",
        },
    ]
    df = spark.createDataFrame(rows)
    scored = silver_module.add_quality_rules(df)
    invalid_count = scored.filter(F.col("_is_valid") == F.lit(False)).count()
    assert invalid_count == 2


def test_scd2_change_detection(spark):
    current_rows = [
        {
            "user_id": "U1",
            "name": "Alice",
            "bank": "sbi",
            "kyc_status": "FULL_KYC",
            "device_id": "dev-1",
            "location_city": "Mumbai",
            "effective_start_date": "2025-07-01",
            "effective_end_date": None,
            "is_current": True,
            "record_hash": "oldhash",
        }
    ]
    source_rows = [
        {
            "user_id": "U1",
            "name": "Alice",
            "bank": "sbi",
            "kyc_status": "FULL_KYC",
            "device_id": "dev-2",
            "location_city": "Delhi",
        }
    ]
    current_df = spark.createDataFrame(current_rows)
    source_df = spark.createDataFrame(source_rows)
    expire_df, insert_df = scd2_module.detect_scd2_changes(source_df, current_df)
    assert expire_df.count() == 1
    assert insert_df.count() == 1


def test_generated_transaction_ids_are_unique(tmp_path):
    output_dir = tmp_path / "sample_data"
    generator_module.generate_dataset(
        output_dir=output_dir,
        total_transactions=1000,
        total_customers=200,
        transactions_file="upi_transactions.csv",
        customers_file="customer_dim.csv",
    )

    transaction_file = output_dir / "upi_transactions.csv"
    with transaction_file.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    transaction_ids = [row["transaction_id"] for row in rows]
    assert len(transaction_ids) == len(set(transaction_ids))
