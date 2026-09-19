"""Data loading, quality checks, local fraud rules, and dashboard metrics.

The local dashboard is intentionally independent from Spark. It consumes
compatible CSV/Parquet outputs when present and otherwise uses the checked-in
Synthetic Data sample or a deterministic Synthetic Data fallback.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import io
import json
from pathlib import Path
from typing import Iterable

import pandas as pd


REQUIRED_COLUMNS = [
    "transaction_id",
    "user_id",
    "timestamp",
    "amount",
    "merchant_category",
    "location_city",
    "device_id",
    "status",
]

RULE_WEIGHTS = {
    "velocity_check": 40,
    "geo_anomaly": 25,
    "device_mule_check": 40,
    "failure_burst_check": 35,
}

RULE_LABELS = {
    "velocity_check": "Velocity check",
    "geo_anomaly": "Geo anomaly",
    "device_mule_check": "Device mule check",
    "failure_burst_check": "Failure burst check",
}

ALIASES = {
    "txn_id": "transaction_id",
    "transactionid": "transaction_id",
    "transaction": "transaction_id",
    "upi_id": "user_id",
    "customer_id": "user_id",
    "customer": "user_id",
    "payer_city": "location_city",
    "payer_location": "location_city",
    "city": "location_city",
    "device": "device_id",
    "deviceid": "device_id",
    "transaction_amount": "amount",
    "txn_amount": "amount",
    "date": "timestamp",
    "event_time": "timestamp",
    "merchant": "merchant_category",
}


@dataclass(frozen=True)
class DashboardBundle:
    """All dashboard-ready layers and the provenance shown in the UI."""

    bronze: pd.DataFrame
    silver: pd.DataFrame
    quarantine: pd.DataFrame
    scored: pd.DataFrame
    gold: pd.DataFrame
    source_label: str
    source_note: str
    refresh_timestamp: pd.Timestamp
    delta_outputs_detected: bool
    customer_scd2_available: bool


def _cities() -> list[str]:
    return ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Pune", "Chennai", "Kolkata", "Jaipur"]


def _merchant_categories() -> list[str]:
    return ["Retail", "Food", "Travel", "Utilities", "Gaming", "Healthcare"]


def generate_deterministic_source(unique_rows: int = 4980) -> pd.DataFrame:
    """Generate a repeatable raw Synthetic Data frame with all fraud signals."""
    indexes = range(unique_rows)
    cities = _cities()
    merchants = _merchant_categories()
    timestamps = pd.date_range("2026-08-30 09:00:00", periods=unique_rows, freq="17s")
    data = pd.DataFrame(
        {
            "transaction_id": [f"TXN-{i:08d}" for i in indexes],
            "utr_number": [f"UTR-{i:08d}" for i in indexes],
            "user_id": [f"USR{i % 850:06d}" for i in indexes],
            "timestamp": timestamps,
            "amount": [float((i * 7919) % 34991 + 10) for i in indexes],
            "sender_vpa": [f"user{i % 850:04d}@syntheticupi" for i in indexes],
            "receiver_vpa": [f"merchant{i % 30:03d}@syntheticbank" for i in indexes],
            "merchant_category": [merchants[i % len(merchants)] for i in indexes],
            "location_city": [cities[i % len(cities)] for i in indexes],
            "device_id": [f"dev-{i % 120:04d}" for i in indexes],
            "ip_address": [f"10.{(i % 200) + 1}.{(i * 3) % 250 + 1}.{(i * 7) % 250 + 1}" for i in indexes],
            "status": ["FAILED" if i % 31 == 0 else "SUCCESS" for i in indexes],
            "failure_reason": ["invalid_upi_pin" if i % 31 == 0 else "" for i in indexes],
        }
    )

    # Eight deterministic clusters make every notebook rule visible locally.
    if unique_rows >= 56:
        velocity_indexes = list(range(50, 56))
        data.loc[velocity_indexes, "user_id"] = "USR_VELOCITY"
        data.loc[velocity_indexes, "timestamp"] = pd.date_range("2026-08-30 12:00:00", periods=6, freq="8s")
        data.loc[velocity_indexes, "amount"] = [9500, 9600, 9700, 9800, 9900, 10000]
    if unique_rows >= 102:
        geo_indexes = [100, 101]
        data.loc[geo_indexes, "user_id"] = "USR_GEO"
        data.loc[geo_indexes, "timestamp"] = [pd.Timestamp("2026-08-30 13:00:00"), pd.Timestamp("2026-08-30 13:04:00")]
        data.loc[geo_indexes, "location_city"] = ["Mumbai", "Delhi"]
    if unique_rows >= 123:
        mule_indexes = [120, 121, 122]
        data.loc[mule_indexes, "user_id"] = ["USR_MULE_A", "USR_MULE_B", "USR_MULE_C"]
        data.loc[mule_indexes, "device_id"] = "dev-MULE"
        data.loc[mule_indexes, "timestamp"] = pd.date_range("2026-08-30 14:00:00", periods=3, freq="3min")
    if unique_rows >= 136:
        brute_indexes = [130, 131, 132, 133, 134, 135]
        data.loc[brute_indexes, "user_id"] = "USR_BRUTE"
        data.loc[brute_indexes, "timestamp"] = pd.date_range("2026-08-30 15:00:00", periods=6, freq="1min")
        data.loc[brute_indexes, "status"] = ["FAILED"] * 5 + ["SUCCESS"]
        data.loc[brute_indexes, "failure_reason"] = ["invalid_upi_pin"] * 5 + [""]
        data.loc[brute_indexes, "location_city"] = "Pune"
    return pd.concat([data, data.iloc[:20].copy()], ignore_index=True)


def _read_frame(path: Path) -> pd.DataFrame | None:
    try:
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path)
        if path.suffix.lower() in {".parquet", ".pq"}:
            return pd.read_parquet(path)
    except Exception:
        return None
    return None


def read_uploaded_frame(payload: bytes, filename: str) -> pd.DataFrame:
    """Read a supported upload without touching the host filesystem."""
    suffix = Path(filename).suffix.lower()
    stream = io.BytesIO(payload)
    if suffix == ".csv":
        return pd.read_csv(stream)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(stream)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(stream)
    if suffix == ".json":
        parsed = json.loads(payload.decode("utf-8"))
        if isinstance(parsed, dict):
            parsed = parsed.get("data", parsed.get("transactions", parsed))
        return pd.DataFrame(parsed)
    raise ValueError("Supported file types are CSV, XLSX, JSON, and Parquet.")


def _find_source(root: Path) -> tuple[pd.DataFrame | None, str, str]:
    candidates = [
        root / "sample_data" / "upi_transactions.csv",
        root / "sample_data" / "upi_transactions_sample.csv",
    ]
    for path in candidates:
        if path.exists():
            frame = _read_frame(path)
            if frame is not None and not frame.empty:
                label = "Generated Synthetic Data CSV" if path.name == "upi_transactions.csv" else "Checked-in Synthetic Data sample"
                return frame, label, f"Loaded {path.as_posix()}"
    return None, "Deterministic Synthetic Data fallback", "No compatible CSV source was available; generated a repeatable local fallback."


def _find_optional_output(root: Path, stem: str) -> pd.DataFrame | None:
    candidates = [
        root / "sample_data" / f"{stem}.csv",
        root / "sample_data" / f"{stem}.parquet",
        root / "delta" / "gold" / f"{stem}.csv",
        root / "delta" / "gold" / f"{stem}.parquet",
        root / "gold" / f"{stem}.csv",
        root / "gold" / f"{stem}.parquet",
    ]
    for path in candidates:
        if path.exists():
            frame = _read_frame(path)
            if frame is not None and not frame.empty:
                return frame
    return None


def standardize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize generator, notebook, and local dashboard naming differences."""
    data = frame.copy()
    data.columns = [str(column).strip().lower() for column in data.columns]
    data = data.rename(columns={key: value for key, value in ALIASES.items() if key in data.columns})
    for column in REQUIRED_COLUMNS:
        if column not in data.columns:
            data[column] = pd.NA

    data["timestamp"] = pd.to_datetime(data["timestamp"], errors="coerce")
    data["amount"] = pd.to_numeric(data["amount"], errors="coerce")
    data["status"] = data["status"].astype("string").str.upper()
    data["merchant_category"] = data["merchant_category"].astype("string")
    data["location_city"] = data["location_city"].astype("string")
    data["payer_location"] = data.get("payer_location", data["location_city"]).fillna(data["location_city"])
    data["payee_location"] = data.get("payee_location", data["location_city"]).fillna(data["location_city"])
    data["failure_reason"] = data.get("failure_reason", pd.Series("", index=data.index)).fillna("").astype("string")
    data["device_id"] = data["device_id"].astype("string")
    data["user_id"] = data["user_id"].astype("string")
    data["transaction_id"] = data["transaction_id"].astype("string")
    device_change = data["device_change_flag"] if "device_change_flag" in data.columns else pd.Series(False, index=data.index)
    origin = data["data_origin"] if "data_origin" in data.columns else pd.Series("Synthetic Data", index=data.index)
    data["device_change_flag"] = device_change.fillna(False).astype(bool)
    data["data_origin"] = origin.fillna("Synthetic Data")
    return data


def missing_required_columns(frame: pd.DataFrame) -> list[str]:
    """Return required canonical fields absent before dashboard defaults are added."""
    names = {str(column).strip().lower() for column in frame.columns}
    canonical = {ALIASES.get(name, name) for name in names}
    return [column for column in REQUIRED_COLUMNS if column not in canonical]


def quality_checks(data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return valid Silver rows, quarantined rows, and rule-level results."""
    work = data.copy()
    checks = {
        "transaction_id_not_null": work["transaction_id"].notna() & work["transaction_id"].ne(""),
        "utr_number_not_null": work.get("utr_number", pd.Series(pd.NA, index=work.index)).notna(),
        "amount_positive": work["amount"].gt(0),
        "sender_vpa_valid": work.get("sender_vpa", pd.Series("", index=work.index)).astype(str).str.match(r"^[A-Za-z0-9._-]+@[A-Za-z0-9.-]+$"),
        "receiver_vpa_valid": work.get("receiver_vpa", pd.Series("", index=work.index)).astype(str).str.match(r"^[A-Za-z0-9._-]+@[A-Za-z0-9.-]+$"),
        "timestamp_not_future": work["timestamp"].le(pd.Timestamp.now()),
        "user_id_not_null": work["user_id"].notna() & work["user_id"].ne(""),
        "status_valid": work["status"].isin(["SUCCESS", "FAILED"]),
        "failure_reason_valid": (work["status"] != "FAILED") | work["failure_reason"].ne(""),
        "device_id_not_null": work["device_id"].notna() & work["device_id"].ne(""),
        "merchant_category_not_null": work["merchant_category"].notna() & work["merchant_category"].ne(""),
        "location_city_not_null": work["location_city"].notna() & work["location_city"].ne(""),
    }
    utr = work.get("utr_number", pd.Series(pd.NA, index=work.index))
    checks["utr_unique"] = ~utr.duplicated(keep=False)
    check_frame = pd.DataFrame(checks, index=work.index).fillna(False)
    work["quarantine_reason"] = check_frame.apply(
        lambda row: "; ".join(name for name, passed in row.items() if not passed), axis=1
    )
    work["_is_valid"] = work["quarantine_reason"].eq("")
    valid = work[work["_is_valid"]].drop_duplicates("transaction_id", keep="first").copy()
    quarantine = work[~work["_is_valid"]].copy()
    rule_rows = []
    for name, passed in check_frame.items():
        failed_count = int((~passed).sum())
        rule_rows.append({
            "rule": name,
            "passed_records": int(passed.sum()),
            "failed_records": failed_count,
            "pass_rate_pct": round(float(passed.mean() * 100), 2) if len(passed) else 100.0,
            "status": "PASS" if failed_count == 0 else "WARNING",
        })
    return valid, quarantine, pd.DataFrame(rule_rows)


def _velocity_flags(data: pd.DataFrame) -> pd.Series:
    flags = pd.Series(False, index=data.index)
    for _, group in data.groupby("user_id", dropna=False):
        ordered = group.sort_values("timestamp")
        totals = ordered.set_index("timestamp")["amount"].rolling("60s", closed="both").sum()
        flags.loc[ordered.index] = totals.to_numpy() > 50000
    return flags


def _geo_flags(data: pd.DataFrame) -> pd.Series:
    flags = pd.Series(False, index=data.index)
    for _, group in data.groupby("user_id", dropna=False):
        ordered = group.sort_values("timestamp")
        previous_delta = ordered["timestamp"].diff().dt.total_seconds()
        next_delta = ordered["timestamp"].shift(-1).sub(ordered["timestamp"]).dt.total_seconds()
        previous = ordered["location_city"].ne(ordered["location_city"].shift()) & previous_delta.le(300)
        following = ordered["location_city"].ne(ordered["location_city"].shift(-1)) & next_delta.le(300)
        flags.loc[ordered.index] = (previous | following).fillna(False).to_numpy()
    return flags


def _mule_flags(data: pd.DataFrame) -> pd.Series:
    flags = pd.Series(False, index=data.index)
    for _, group in data.groupby("device_id", dropna=False):
        ordered = group.sort_values("timestamp")
        window: deque[tuple[pd.Timestamp, str]] = deque()
        for row_index, row in ordered.iterrows():
            current_time = row["timestamp"]
            while window and (current_time - window[0][0]).total_seconds() > 600:
                window.popleft()
            window.append((current_time, str(row["user_id"])))
            if len({user_id for _, user_id in window}) >= 3:
                flags.loc[row_index] = True
    return flags


def _failure_flags(data: pd.DataFrame) -> pd.Series:
    flags = pd.Series(False, index=data.index)
    for _, group in data.groupby("user_id", dropna=False):
        ordered = group.sort_values("timestamp")
        statuses = ordered["status"].astype(str).tolist()
        times = ordered["timestamp"].tolist()
        for position in range(5, len(ordered)):
            previous_failed = statuses[position - 5 : position] == ["FAILED"] * 5
            within_ten_minutes = (times[position] - times[position - 5]).total_seconds() <= 600
            if statuses[position] == "SUCCESS" and previous_failed and within_ten_minutes:
                flags.loc[ordered.index[position]] = True
    return flags


def apply_fraud_rules(data: pd.DataFrame) -> pd.DataFrame:
    """Apply faithful local equivalents of notebook 04's four rules."""
    scored = data.copy().sort_values("timestamp").reset_index(drop=True)
    scored["velocity_check"] = _velocity_flags(scored)
    scored["geo_anomaly"] = _geo_flags(scored)
    scored["device_mule_check"] = _mule_flags(scored)
    scored["failure_burst_check"] = _failure_flags(scored)
    scored["risk_score"] = sum(
        scored[name].astype(int) * weight for name, weight in RULE_WEIGHTS.items()
    ).clip(upper=100).astype(int)
    scored["fraud_reason"] = scored.apply(
        lambda row: ", ".join(label for name, label in RULE_LABELS.items() if bool(row[name]))
        or "No rule triggered",
        axis=1,
    )
    scored["is_fraud"] = scored["risk_score"].gt(0)
    scored["risk_level"] = pd.cut(
        scored["risk_score"], bins=[-1, 0, 24, 49, 79, 100],
        labels=["None", "Low", "Medium", "High", "Critical"],
    ).astype(str)
    scored["transaction_date"] = scored["timestamp"].dt.date.astype("string")
    return scored


def _missing_rule_names(scored: pd.DataFrame) -> list[str]:
    return [name for name in RULE_WEIGHTS if not bool(scored[name].any())]


def apply_local_demo_overlay(data: pd.DataFrame, missing_rules: Iterable[str]) -> pd.DataFrame:
    """Create deterministic rule demonstrations only when the source lacks a signal."""
    enriched = data.copy()
    indexes = list(enriched.index)
    if not indexes:
        return enriched
    if "velocity_check" in missing_rules and len(indexes) >= 6:
        selected = indexes[:6]
        enriched.loc[selected, "user_id"] = "USR_DEMO_VELOCITY"
        enriched.loc[selected, "timestamp"] = pd.date_range("2026-08-30 12:00:00", periods=6, freq="8s")
        enriched.loc[selected, "amount"] = [9500, 9600, 9700, 9800, 9900, 10000]
    if "geo_anomaly" in missing_rules and len(indexes) >= 2:
        selected = indexes[-2:]
        enriched.loc[selected, "user_id"] = "USR_DEMO_GEO"
        enriched.loc[selected, "timestamp"] = [pd.Timestamp("2026-08-30 13:00:00"), pd.Timestamp("2026-08-30 13:04:00")]
        enriched.loc[selected, "location_city"] = ["Mumbai", "Delhi"]
    if "device_mule_check" in missing_rules and len(indexes) >= 3:
        selected = indexes[6:9]
        enriched.loc[selected, "user_id"] = ["USR_DEMO_MULE_A", "USR_DEMO_MULE_B", "USR_DEMO_MULE_C"]
        enriched.loc[selected, "device_id"] = "dev-DEMO-MULE"
        enriched.loc[selected, "timestamp"] = pd.date_range("2026-08-30 14:00:00", periods=3, freq="3min")
    if "failure_burst_check" in missing_rules and len(indexes) >= 6:
        selected = indexes[12:18]
        enriched.loc[selected, "user_id"] = "USR_DEMO_BRUTE"
        enriched.loc[selected, "timestamp"] = pd.date_range("2026-08-30 15:00:00", periods=6, freq="1min")
        enriched.loc[selected, "status"] = ["FAILED"] * 5 + ["SUCCESS"]
        enriched.loc[selected, "failure_reason"] = ["invalid_upi_pin"] * 5 + [""]
    enriched["data_origin"] = "Synthetic Data local rule demonstration overlay"
    return enriched


def _build_bundle_from_source(
    root: Path,
    source: pd.DataFrame,
    source_label: str,
    source_note: str,
    use_optional_gold: bool = True,
) -> DashboardBundle:
    bronze = standardize_columns(source)
    valid, quarantine, _ = quality_checks(bronze)
    silver = valid.drop_duplicates("transaction_id", keep="first").copy()
    scored = apply_fraud_rules(silver)
    overlay_used = False
    missing_rules = _missing_rule_names(scored)
    if missing_rules and source_label != "Deterministic Synthetic Data fallback":
        silver = apply_local_demo_overlay(silver, missing_rules)
        scored = apply_fraud_rules(silver)
        overlay_used = True

    optional_gold = _find_optional_output(root, "fraud_transactions") if use_optional_gold else None
    delta_detected = (root / "delta").exists() if use_optional_gold else False
    if optional_gold is not None:
        candidate = standardize_columns(optional_gold)
        if "is_fraud" in candidate.columns:
            gold = candidate[candidate["is_fraud"].astype(bool)].copy()
            source_note += " Compatible local Gold output was loaded for fraud rows."
        else:
            gold = scored[scored["is_fraud"]].copy()
    else:
        gold = scored[scored["is_fraud"]].copy()

    if overlay_used:
        source_note += " Missing rule signals were filled with a clearly labeled deterministic local demonstration overlay."
    if delta_detected and optional_gold is None:
        source_note += " Delta folders were detected, but no pandas-readable Gold CSV/Parquet output was available; the dashboard uses the local demonstration path."
    scd2_available = any((root / path).exists() for path in ["sample_data/customer_dim.csv", "delta/dim/customer_device_scd2"])
    return DashboardBundle(
        bronze=bronze,
        silver=scored,
        quarantine=quarantine,
        scored=scored,
        gold=gold,
        source_label=source_label,
        source_note=source_note,
        refresh_timestamp=pd.Timestamp.now(),
        delta_outputs_detected=delta_detected,
        customer_scd2_available=scd2_available,
    )


def build_dashboard_bundle_from_frame(
    root: Path,
    source: pd.DataFrame,
    source_label: str = "Uploaded Synthetic Data",
    source_note: str = "Loaded from the current browser session.",
) -> DashboardBundle:
    """Build dashboard layers from an uploaded or programmatically supplied frame."""
    if source.empty:
        raise ValueError("The uploaded file contains no rows.")
    return _build_bundle_from_source(root, source, source_label, source_note, use_optional_gold=False)


def build_dashboard_bundle(root: Path) -> DashboardBundle:
    source, source_label, source_note = _find_source(root)
    if source is None:
        source = generate_deterministic_source()
    return _build_bundle_from_source(root, source, source_label, source_note)


def quality_summary(data: pd.DataFrame) -> pd.DataFrame:
    """Return compact quality results for the Pipeline section."""
    _, _, summary = quality_checks(data)
    return summary


def high_risk_users(scored: pd.DataFrame) -> pd.DataFrame:
    fraud = scored[scored["is_fraud"]]
    if fraud.empty:
        return pd.DataFrame(columns=["user_id", "fraud_count", "flagged_amount", "max_risk_score"])
    return (
        fraud.groupby("user_id", as_index=False)
        .agg(fraud_count=("transaction_id", "count"), flagged_amount=("amount", "sum"), max_risk_score=("risk_score", "max"))
        .sort_values(["max_risk_score", "fraud_count", "flagged_amount"], ascending=False)
    )


def csv_bytes(data: pd.DataFrame, columns: list[str] | None = None) -> bytes:
    """Create a deterministic UTF-8 CSV payload for Streamlit downloads."""
    selected = data[columns].copy() if columns else data.copy()
    return selected.to_csv(index=False).encode("utf-8")
