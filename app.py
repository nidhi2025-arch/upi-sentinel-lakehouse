"""Local browser demo for the UPI Sentinel Lakehouse.

This application uses Synthetic Data only. It provides a small pandas-based
demonstration of the same quality and fraud rules implemented with PySpark in
the Databricks-compatible notebooks.
"""

from __future__ import annotations

from collections import deque
from pathlib import Path

import pandas as pd
import streamlit as st


APP_ROOT = Path(__file__).resolve().parent
DEFAULT_SAMPLE = APP_ROOT / "sample_data" / "upi_transactions_sample.csv"
VPA_PATTERN = r"^[A-Za-z0-9._-]+@[A-Za-z0-9.-]+$"


@st.cache_data(show_spinner=False)
def load_transactions(file_bytes: bytes | None = None) -> pd.DataFrame:
    """Load the checked-in sample or an uploaded CSV into a normalized frame."""
    if file_bytes is None:
        return pd.read_csv(DEFAULT_SAMPLE)

    from io import BytesIO

    return pd.read_csv(BytesIO(file_bytes))


def _mark_velocity(df: pd.DataFrame) -> pd.Series:
    flags = pd.Series(False, index=df.index)
    for _, group in df.groupby("user_id", dropna=False):
        ordered = group.sort_values("timestamp")
        totals = (
            ordered.set_index("timestamp")["amount"]
            .rolling("60s", closed="both")
            .sum()
        )
        flags.loc[ordered.index] = totals.to_numpy() > 50000
    return flags


def _mark_geo_anomaly(df: pd.DataFrame) -> pd.Series:
    flags = pd.Series(False, index=df.index)
    for _, group in df.groupby("user_id", dropna=False):
        ordered = group.sort_values("timestamp")
        city_changed = ordered["location_city"].ne(ordered["location_city"].shift())
        within_five = ordered["timestamp"].diff().dt.total_seconds().le(300)
        flags.loc[ordered.index] = (city_changed & within_five).fillna(False).to_numpy()
    return flags


def _mark_mule_accounts(df: pd.DataFrame) -> pd.Series:
    flags = pd.Series(False, index=df.index)
    for _, group in df.groupby("device_id", dropna=False):
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


def _mark_failure_burst(df: pd.DataFrame) -> pd.Series:
    flags = pd.Series(False, index=df.index)
    for _, group in df.groupby("user_id", dropna=False):
        ordered = group.sort_values("timestamp")
        statuses = ordered["status"].astype(str).str.upper().tolist()
        times = ordered["timestamp"].tolist()
        for position in range(5, len(ordered)):
            five_failures = statuses[position - 5 : position] == ["FAILED"] * 5
            within_ten = (times[position] - times[position - 5]).total_seconds() <= 600
            if statuses[position] == "SUCCESS" and five_failures and within_ten:
                flags.loc[ordered.index[position]] = True
    return flags


def score_transactions(raw: pd.DataFrame) -> pd.DataFrame:
    """Apply local demo versions of the four Spark fraud rules."""
    df = raw.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["status"] = df["status"].astype(str).str.upper()
    df = df.sort_values("timestamp").reset_index(drop=True)

    df["velocity_check"] = _mark_velocity(df)
    df["geo_anomaly"] = _mark_geo_anomaly(df)
    df["device_mule_check"] = _mark_mule_accounts(df)
    df["failure_burst_check"] = _mark_failure_burst(df)

    rule_columns = [
        "velocity_check",
        "geo_anomaly",
        "device_mule_check",
        "failure_burst_check",
    ]
    rule_labels = {
        "velocity_check": "velocity_check",
        "geo_anomaly": "geo_anomaly",
        "device_mule_check": "device_mule_check",
        "failure_burst_check": "failure_burst_check",
    }
    weights = {
        "velocity_check": 35,
        "geo_anomaly": 30,
        "device_mule_check": 25,
        "failure_burst_check": 25,
    }
    df["risk_score"] = sum(df[column].astype(int) * weights[column] for column in rule_columns)
    df["risk_score"] = df["risk_score"].clip(upper=100).astype(int)
    df["fraud_reason"] = df.apply(
        lambda row: ",".join(
            rule_labels[column] for column in rule_columns if bool(row[column])
        ),
        axis=1,
    )
    df["is_fraud"] = df["risk_score"].gt(0)
    df["transaction_date"] = df["timestamp"].dt.date.astype("string")
    return df


def quality_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Return the local equivalents of the Silver data-quality checks."""
    checks = {
        "transaction_id_not_null": df["transaction_id"].notna(),
        "utr_number_not_null": df["utr_number"].notna(),
        "utr_number_unique": ~df["utr_number"].duplicated(keep=False),
        "amount_positive": df["amount"].gt(0),
        "sender_vpa_valid": df["sender_vpa"].astype(str).str.match(VPA_PATTERN),
        "receiver_vpa_valid": df["receiver_vpa"].astype(str).str.match(VPA_PATTERN),
        "timestamp_not_future": df["timestamp"].le(pd.Timestamp.now()),
        "user_id_not_null": df["user_id"].notna(),
        "status_valid": df["status"].isin(["SUCCESS", "FAILED"]),
        "device_id_not_null": df["device_id"].notna(),
        "ip_address_present": df["ip_address"].notna(),
        "merchant_category_not_null": df["merchant_category"].notna(),
        "location_city_not_null": df["location_city"].notna(),
    }
    return pd.DataFrame(
        [
            {
                "rule": name,
                "passed_records": int(mask.fillna(False).sum()),
                "failed_records": int((~mask.fillna(False)).sum()),
                "pass_rate_pct": round(float(mask.fillna(False).mean() * 100), 2),
            }
            for name, mask in checks.items()
        ]
    )


def render_app() -> None:
    st.set_page_config(
        page_title="UPI Sentinel Lakehouse",
        page_icon="UPI",
        layout="wide",
    )
    st.title("UPI Sentinel Lakehouse")
    st.caption("Local browser demo | Synthetic Data only | Spark/Delta production path remains in notebooks")

    with st.sidebar:
        st.header("Demo Controls")
        uploaded = st.file_uploader("Upload a compatible synthetic CSV", type="csv")
        st.markdown("**Pipeline order**")
        st.markdown("1. Generator  2. Bronze  3. Silver  4. SCD2  5. Fraud  6. Gold")
        st.info("This browser demo does not claim to be real banking data.")

    source = load_transactions(uploaded.getvalue() if uploaded else None)
    scored = score_transactions(source)
    quality = quality_summary(scored)

    st.success(f"Loaded {len(scored):,} Synthetic Data transactions from " + ("uploaded CSV." if uploaded else "the checked-in sample."))

    metric_columns = st.columns(5)
    metric_columns[0].metric("Transactions", f"{len(scored):,}")
    metric_columns[1].metric("Fraud flagged", f"{int(scored['is_fraud'].sum()):,}")
    metric_columns[2].metric("Fraud amount", f"{scored.loc[scored['is_fraud'], 'amount'].sum():,.2f}")
    metric_columns[3].metric("Mule signals", f"{int(scored['device_mule_check'].sum()):,}")
    metric_columns[4].metric("Quality pass rate", f"{quality['pass_rate_pct'].mean():.1f}%")

    tab_overview, tab_fraud, tab_quality, tab_data = st.tabs(
        ["Overview", "Fraud Signals", "Data Quality", "Transactions"]
    )

    with tab_overview:
        left, right = st.columns(2)
        with left:
            st.subheader("Daily transaction volume")
            daily = scored.groupby("transaction_date").size().rename("transactions")
            st.line_chart(daily)
        with right:
            st.subheader("Fraud by merchant category")
            merchant = (
                scored[scored["is_fraud"]]
                .groupby("merchant_category")
                .agg(fraud_transactions=("transaction_id", "count"), fraud_amount=("amount", "sum"))
                .sort_values("fraud_transactions", ascending=False)
            )
            st.bar_chart(merchant["fraud_transactions"])
            st.dataframe(merchant.reset_index(), use_container_width=True, hide_index=True)

    with tab_fraud:
        rule_counts = pd.DataFrame(
            {
                "rule": ["velocity_check", "geo_anomaly", "device_mule_check", "failure_burst_check"],
                "flagged_transactions": [int(scored[name].sum()) for name in [
                    "velocity_check", "geo_anomaly", "device_mule_check", "failure_burst_check"
                ]],
            }
        ).set_index("rule")
        st.subheader("Fraud rule activity")
        st.bar_chart(rule_counts)
        st.subheader("Highest-risk users")
        top_users = (
            scored[scored["is_fraud"]]
            .groupby("user_id")
            .agg(
                fraud_transactions=("transaction_id", "count"),
                fraud_amount=("amount", "sum"),
                max_risk_score=("risk_score", "max"),
            )
            .sort_values(["max_risk_score", "fraud_transactions"], ascending=False)
            .head(10)
            .reset_index()
        )
        st.dataframe(top_users, use_container_width=True, hide_index=True)

    with tab_quality:
        st.subheader("Silver-style data-quality checks")
        st.dataframe(quality, use_container_width=True, hide_index=True)
        st.caption("Rows failing these checks would be quarantined by the Spark Silver notebook.")

    with tab_data:
        min_score, max_score = st.slider("Risk-score range", 0, 100, (0, 100))
        selected_status = st.multiselect("Status", ["SUCCESS", "FAILED"], default=["SUCCESS", "FAILED"])
        selected_categories = st.multiselect(
            "Merchant category",
            sorted(scored["merchant_category"].dropna().unique()),
            default=sorted(scored["merchant_category"].dropna().unique()),
        )
        filtered = scored[
            scored["risk_score"].between(min_score, max_score)
            & scored["status"].isin(selected_status)
            & scored["merchant_category"].isin(selected_categories)
        ]
        columns = [
            "transaction_id", "timestamp", "user_id", "amount", "merchant_category",
            "location_city", "device_id", "status", "risk_score", "fraud_reason", "is_fraud",
        ]
        st.caption(f"Showing {len(filtered):,} of {len(scored):,} transactions")
        st.dataframe(filtered[columns].sort_values("risk_score", ascending=False), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    render_app()
