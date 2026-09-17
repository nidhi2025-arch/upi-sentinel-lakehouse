"""Tests for the lightweight local dashboard data and fraud-rule layer."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.dashboard_data import (
    REQUIRED_COLUMNS,
    apply_fraud_rules,
    build_dashboard_bundle,
    csv_bytes,
    generate_deterministic_source,
    quality_checks,
    standardize_columns,
)


ROOT = Path(__file__).resolve().parents[1]


def test_deterministic_source_and_layer_counts() -> None:
    first = generate_deterministic_source()
    second = generate_deterministic_source()
    pd.testing.assert_frame_equal(first, second)
    assert len(first) == 5000
    assert first["transaction_id"].nunique() == 4980


def test_all_four_local_fraud_rules_are_represented() -> None:
    source = standardize_columns(generate_deterministic_source())
    valid, _, _ = quality_checks(source)
    scored = apply_fraud_rules(valid)
    for rule in ["velocity_check", "geo_anomaly", "device_mule_check", "failure_burst_check"]:
        assert bool(scored[rule].any()), rule
    assert scored["risk_score"].between(0, 100).all()
    assert scored.loc[scored["is_fraud"], "fraud_reason"].ne("No rule triggered").all()


def test_quality_checks_quarantine_invalid_rows() -> None:
    source = standardize_columns(generate_deterministic_source(10))
    source.loc[0, "transaction_id"] = None
    source.loc[1, "amount"] = -1
    source.loc[2, "status"] = "UNKNOWN"
    valid, quarantine, results = quality_checks(source)
    assert len(quarantine) >= 3
    assert len(valid) < len(source)
    assert "status_valid" in results["rule"].tolist()


def test_empty_data_is_safe() -> None:
    empty = pd.DataFrame(columns=REQUIRED_COLUMNS)
    standardized = standardize_columns(empty)
    scored = apply_fraud_rules(standardized)
    assert scored.empty
    assert set(REQUIRED_COLUMNS).issubset(scored.columns)


def test_dashboard_bundle_uses_sample_and_calculates_kpis() -> None:
    bundle = build_dashboard_bundle(ROOT)
    assert len(bundle.bronze) > 0
    assert len(bundle.silver) > 0
    assert "risk_score" in bundle.scored.columns
    assert len(bundle.gold) == int(bundle.scored["is_fraud"].sum())


def test_csv_download_payload_contains_filtered_schema() -> None:
    bundle = build_dashboard_bundle(ROOT)
    fraud = bundle.gold.head(3)
    payload = csv_bytes(fraud, ["transaction_id", "risk_score"])
    assert payload.startswith(b"transaction_id,risk_score")
    assert payload.count(b"\n") == len(fraud) + 1
