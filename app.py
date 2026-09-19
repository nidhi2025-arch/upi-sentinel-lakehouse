"""UPI Sentinel Lakehouse local portfolio dashboard.

This browser application is a lightweight pandas demonstration of the
repository's Spark/Delta architecture. All records are Synthetic Data only;
it does not execute the complete Databricks pipeline inside Streamlit.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from src.dashboard_data import (
    DashboardBundle,
    RULE_LABELS,
    build_dashboard_bundle,
    build_dashboard_bundle_from_frame,
    csv_bytes,
    high_risk_users,
    missing_required_columns,
    read_uploaded_frame,
    quality_summary,
)


ROOT = Path(__file__).resolve().parent


st.set_page_config(
    page_title="UPI Sentinel Lakehouse",
    page_icon="U",
    layout="wide",
    initial_sidebar_state="expanded",
)


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Space+Grotesk:wght@500;600;700&display=swap');
        :root { --navy:#0b1220; --navy2:#111c32; --ink:#172033; --muted:#64748b; --blue:#5b72e8; --teal:#20b8aa; --red:#e34d5b; --amber:#e6a52d; --green:#23936b; }
        html, body, [class*="css"] { font-family:'DM Sans',sans-serif; }
        h1, h2, h3, h4 { font-family:'Space Grotesk',sans-serif; color:var(--ink); }
        .block-container { max-width:1500px; padding:1.4rem 2.2rem 3rem; }
        .hero { background:linear-gradient(115deg,#0b1220 0%,#172954 54%,#3157a6 100%); border-radius:24px; padding:2.35rem 2.6rem; color:white; box-shadow:0 18px 42px rgba(11,18,32,.25); margin-bottom:1.35rem; overflow:hidden; }
        .hero:after { content:''; display:block; height:4px; width:160px; background:linear-gradient(90deg,#4ECDC4,#FFE66D); margin-top:1.4rem; border-radius:5px; }
        .hero h1 { color:white; margin:.3rem 0 0; font-size:clamp(2rem,4vw,3.35rem); letter-spacing:.035em; }
        .hero p { color:#dbe7ff; margin:.55rem 0 0; font-size:1.05rem; }
        .eyebrow, .section-kicker { text-transform:uppercase; letter-spacing:.16em; font-size:.72rem; font-weight:700; }
        .eyebrow { color:#a9bbff; }
        .section-kicker { color:#5b72e8; margin:.15rem 0 .45rem; }
        .badge { display:inline-block; padding:.28rem .65rem; border-radius:999px; font-size:.72rem; font-weight:700; letter-spacing:.06em; background:#fff3bf; color:#6b5000; }
        .health { display:inline-block; padding:.28rem .65rem; border-radius:999px; font-size:.72rem; font-weight:700; letter-spacing:.06em; background:#d5f7e8; color:#116b4b; }
        .health-warn { background:#fff0c7; color:#835600; }
        .info-card, .metric-card, .stage-card, .stack-card { background:#fff; border-radius:15px; padding:1rem 1.15rem; box-shadow:0 8px 24px rgba(15,23,42,.07); border:1px solid #e8edf5; }
        .stack-card { background:#f0f2f6; min-height:108px; border-top:4px solid #5b72e8; }
        .stack-name { font-family:'Space Grotesk',sans-serif; font-weight:700; color:var(--ink); margin-top:.2rem; }
        .stack-detail, .muted { color:var(--muted); font-size:.82rem; line-height:1.45; }
        .metric-card { border-left:5px solid #5b72e8; min-height:104px; }
        .metric-label { color:var(--muted); text-transform:uppercase; letter-spacing:.08em; font-size:.7rem; font-weight:700; }
        .metric-value { color:var(--ink); font-family:'Space Grotesk',sans-serif; font-size:1.72rem; font-weight:700; margin-top:.25rem; }
        .metric-detail { color:var(--muted); font-size:.78rem; margin-top:.2rem; }
        .stage-card { min-height:130px; border-top:5px solid #20b8aa; }
        .stage-card h4 { margin:.25rem 0; font-size:1rem; }
        .stage-count { font-family:'Space Grotesk',sans-serif; font-size:1.6rem; font-weight:700; color:var(--ink); }
        .note { background:#eef5ff; border-left:4px solid #5b72e8; border-radius:10px; padding:.8rem 1rem; color:#314563; font-size:.84rem; }
        .critical { background:#fff0f1; border-left-color:#e34d5b; }
        [data-testid="stMetric"] { background:#fff; border-radius:15px; border-left:5px solid #5b72e8; box-shadow:0 8px 24px rgba(15,23,42,.07); padding:1rem; }
        [data-testid="stTabs"] button { font-family:'Space Grotesk',sans-serif; font-weight:700; }
        @media (max-width: 700px) { .block-container { padding:1rem .8rem 2rem; } .hero { padding:1.65rem 1.25rem; border-radius:18px; } .hero h1 { font-size:2rem; } }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(show_spinner=False)
def load_bundle() -> DashboardBundle:
    return build_dashboard_bundle(ROOT)


@st.cache_data(show_spinner=False)
def load_uploaded_bundle(payload: bytes, filename: str) -> DashboardBundle:
    frame = read_uploaded_frame(payload, filename)
    return build_dashboard_bundle_from_frame(
        ROOT,
        frame,
        source_label=f"Uploaded {filename} (Synthetic Data analysis)",
        source_note="The uploaded file is analyzed in this browser session and is not stored by the dashboard.",
    )


def inr(value: float) -> str:
    numeric = float(value)
    sign = "-" if numeric < 0 else ""
    integer, fraction = f"{abs(numeric):.2f}".split(".")
    if len(integer) > 3:
        last_three = integer[-3:]
        remaining = integer[:-3]
        groups = []
        while remaining:
            groups.insert(0, remaining[-2:])
            remaining = remaining[:-2]
        integer = ",".join(groups + [last_three])
    return f"{sign}INR {integer}.{fraction}"


def metric_card(label: str, value: str, detail: str, color: str = "#5b72e8") -> None:
    st.markdown(
        f'<div class="metric-card" style="border-left-color:{color}">'
        f'<div class="metric-label">{label}</div><div class="metric-value">{value}</div>'
        f'<div class="metric-detail">{detail}</div></div>',
        unsafe_allow_html=True,
    )


def pipeline_graph() -> None:
    st.graphviz_chart(
        '''digraph {
            graph [bgcolor="transparent", rankdir=LR, pad="0.2", nodesep="0.35"];
            node [shape=box, style="rounded,filled", fontname="Arial", fontsize=13, margin="0.2,0.15", color="#ffffff"];
            edge [color="#64748b", penwidth=2, arrowsize=.75];
            Generator [label="SYNTHETIC\\nGENERATOR", fillcolor="#64748b", fontcolor="white"];
            Bronze [label="BRONZE\\nRAW", fillcolor="#e34d5b", fontcolor="white"];
            Silver [label="SILVER\\nVALIDATED", fillcolor="#20b8aa", fontcolor="white"];
            Gold [label="GOLD\\nRISK SCORED", fillcolor="#e6a52d", fontcolor="#3b2c00"];
            Dashboard [label="DASHBOARD", fillcolor="#5b72e8", fontcolor="white"];
            Generator -> Bronze -> Silver -> Gold -> Dashboard;
        }''',
        width="stretch",
    )


def hero(bundle: DashboardBundle) -> None:
    st.markdown(
        '<div class="hero"><div class="eyebrow">Synthetic Data Engineering Control Plane</div>'
        '<h1>UPI SENTINEL LAKEHOUSE</h1>'
        '<p>Synthetic UPI Fraud Detection and Risk Analytics</p></div>',
        unsafe_allow_html=True,
    )
    source_status = "READY - local pandas demonstration"
    if bundle.delta_outputs_detected:
        source_status = "READY - Delta folders detected; local-compatible outputs preferred"
    left, right = st.columns([3, 1])
    with left:
        st.markdown('<span class="badge">SYNTHETIC DATA ONLY</span>', unsafe_allow_html=True)
        st.markdown('<span class="health">PIPELINE STATUS: ' + source_status + '</span>', unsafe_allow_html=True)
    with right:
        st.caption(f"Last data refresh: {bundle.refresh_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")


def render_overview(bundle: DashboardBundle) -> None:
    hero(bundle)
    valid = bundle.silver
    fraud = bundle.gold
    high_risk = valid[valid["risk_level"].isin(["High", "Critical"])]
    st.markdown('<div class="section-kicker">1 / Executive overview</div>', unsafe_allow_html=True)
    cards = st.columns(6)
    values = [
        ("Valid transactions", f"{len(valid):,}", "Silver-ready records", "#5b72e8"),
        ("Flagged transactions", f"{len(fraud):,}", "Rule-based alerts", "#e34d5b"),
        ("Fraud rate", f"{(len(fraud) / len(valid) * 100) if len(valid) else 0:.2f}%", "Flagged / valid", "#e6a52d"),
        ("Flagged amount", inr(fraud["amount"].sum()), "Synthetic flagged value", "#e6a52d"),
        ("Average risk score", f"{valid['risk_score'].mean() if len(valid) else 0:.1f}", "All Silver records", "#20b8aa"),
        ("High-risk users", f"{high_risk['user_id'].nunique():,}", "High and Critical", "#764ba2"),
    ]
    for column, (label, value, detail, color) in zip(cards, values):
        with column:
            metric_card(label, value, detail, color)

    st.markdown("### Business problem")
    problem_left, problem_right = st.columns([1.15, 1])
    with problem_left:
        st.markdown(
            '<div class="info-card"><h4>Why this lakehouse matters</h4>'
            '<p class="muted">High-volume payment telemetry needs reliable ingestion, quality gates, customer-history tracking, transparent fraud rules, and risk-ready analytics. This portfolio project shows that flow with Synthetic Data only.</p>'
            '<p class="muted"><b>Dashboard scope:</b> the local app calculates a repeatable pandas demonstration. The complete PySpark, Delta Lake, incremental ingestion, and SCD Type 2 implementation remains in the notebooks and must run in Spark or Databricks.</p></div>',
            unsafe_allow_html=True,
        )
    with problem_right:
        st.markdown("### Technology stack")
        tech = st.columns(4)
        for column, name, detail, color in zip(
            tech,
            ["Python", "PySpark", "Delta Lake", "ETL Testing"],
            ["Data generation", "Distributed rules", "ACID lakehouse", "Quality gates"],
            ["#5b72e8", "#764ba2", "#20b8aa", "#e34d5b"],
        ):
            with column:
                st.markdown(
                    f'<div class="stack-card" style="border-top-color:{color}"><div class="stack-name">{name}</div><div class="stack-detail">{detail}</div></div>',
                    unsafe_allow_html=True,
                )

    st.markdown("### Bronze to Gold architecture")
    pipeline_graph()
    st.caption("The dashboard demonstrates the architecture locally; it does not claim that Streamlit executed the complete Spark/Delta pipeline.")


def render_pipeline(bundle: DashboardBundle) -> None:
    st.markdown('<div class="section-kicker">2 / Pipeline and data quality</div>', unsafe_allow_html=True)
    st.markdown('<div class="note">Source: <b>' + bundle.source_label + '</b>. ' + bundle.source_note + '</div>', unsafe_allow_html=True)
    layers = [
        ("Bronze ingestion", len(bundle.bronze), "Raw loaded records", "#e34d5b"),
        ("Silver validation", len(bundle.silver), "Valid, deduplicated records", "#20b8aa"),
        ("Quarantine", len(bundle.quarantine), "Records failing quality rules", "#e6a52d"),
        ("Gold analytics", len(bundle.gold), "Fraud-flagged records", "#5b72e8"),
    ]
    columns = st.columns(4)
    for column, (name, count, detail, color) in zip(columns, layers):
        with column:
            st.markdown(
                f'<div class="stage-card" style="border-top-color:{color}"><div class="section-kicker">{name}</div><div class="stage-count">{count:,}</div><div class="muted">{detail}</div></div>',
                unsafe_allow_html=True,
            )

    duplicate_rows = int(bundle.bronze["transaction_id"].duplicated(keep=False).sum())
    quality = quality_summary(bundle.bronze)
    valid_rate = len(bundle.silver) / len(bundle.bronze) * 100 if len(bundle.bronze) else 100
    q1, q2, q3, q4 = st.columns(4)
    q1.metric("Valid versus invalid", f"{len(bundle.silver):,} / {len(bundle.quarantine):,}")
    q2.metric("Duplicate rows", f"{duplicate_rows:,}")
    q3.metric("Quality pass", f"{valid_rate:.2f}%")
    q4.metric("SCD Type 2 status", "AVAILABLE" if bundle.customer_scd2_available else "DATABRICKS OUTPUT")

    st.markdown("### End-to-end flow")
    st.graphviz_chart(
        '''digraph { graph [bgcolor="transparent", rankdir=LR]; node [shape=box, style="rounded,filled", fontname="Arial", color="white"]; edge [color="#64748b"]; Generator [label="Synthetic Generator", fillcolor="#64748b", fontcolor="white"]; Bronze [label="Bronze Ingestion", fillcolor="#e34d5b", fontcolor="white"]; Silver [label="Silver Validation", fillcolor="#20b8aa", fontcolor="white"]; Quarantine [label="Quarantine", fillcolor="#e6a52d", fontcolor="#3b2c00"]; SCD2 [label="SCD Type 2", fillcolor="#764ba2", fontcolor="white"]; Fraud [label="Fraud Engine", fillcolor="#e34d5b", fontcolor="white"]; Gold [label="Gold Analytics", fillcolor="#5b72e8", fontcolor="white"]; Dashboard [label="Dashboard", fillcolor="#0b1220", fontcolor="white"]; Generator -> Bronze -> Silver; Silver -> Quarantine; Silver -> SCD2 -> Gold; Silver -> Fraud -> Gold -> Dashboard; }''',
        width="stretch",
    )
    left, right = st.columns([1.25, 1])
    with left:
        st.markdown("### Data-quality rules")
        st.dataframe(quality, width="stretch", hide_index=True)
    with right:
        st.markdown("### Quarantine reason distribution")
        if bundle.quarantine.empty:
            st.success("No records are currently quarantined.")
        else:
            reasons = bundle.quarantine["quarantine_reason"].str.split("; ").explode().value_counts()
            st.bar_chart(reasons)
            st.dataframe(bundle.quarantine[["transaction_id", "quarantine_reason"]].head(50), width="stretch", hide_index=True)

    with st.expander("How the Spark/Delta implementation works"):
        st.code(
            """# Silver quality gate\nvalid = silver.filter(F.col('_is_valid') == True)\nquarantine = silver.filter(F.col('_is_valid') == False)\n\n# SCD Type 2 preserves old device/location versions\ndelta_table.alias('target').merge(\n    source.alias('source'),\n    'target.user_id = source.user_id AND target.is_current = true'\n).whenMatchedUpdate(...).whenNotMatchedInsert(...).execute()\n\n# Gold is built from the fraud-scored Delta output\nfraud_transactions.write.format('delta').mode('overwrite').save(GOLD_PATH)""",
            language="python",
        )
        st.caption("The complete runnable implementations are notebooks 01 through 05. These snippets are explanatory; this local tab reads pandas-compatible data only.")


def filter_fraud_data(data: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, object]]:
    """Render filters and return the filtered scored frame."""
    with st.expander("Filters", expanded=True):
        dates = pd.to_datetime(data["timestamp"], errors="coerce").dt.date
        date_min, date_max = dates.min(), dates.max()
        selected_dates = st.date_input("Date range", value=(date_min, date_max), min_value=date_min, max_value=date_max)
        cities = st.multiselect("City", sorted(data["location_city"].dropna().astype(str).unique()), default=sorted(data["location_city"].dropna().astype(str).unique()))
        merchants = st.multiselect("Merchant category", sorted(data["merchant_category"].dropna().astype(str).unique()), default=sorted(data["merchant_category"].dropna().astype(str).unique()))
        statuses = st.multiselect("Transaction status", sorted(data["status"].dropna().astype(str).unique()), default=sorted(data["status"].dropna().astype(str).unique()))
        reason_options = ["Any reason", *sorted(RULE_LABELS.values()), "No rule triggered"]
        reasons = st.multiselect("Fraud reason", reason_options, default=["Any reason"])
        risk_levels = st.multiselect("Risk level", ["None", "Low", "Medium", "High", "Critical"], default=["Medium", "High", "Critical"])
        user_search = st.text_input("User ID search", placeholder="Example: USR000001")
        amount_min, amount_max = float(data["amount"].min()), float(data["amount"].max())
        amount_range = st.slider("Amount range", min_value=amount_min, max_value=amount_max, value=(amount_min, amount_max))

    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
        start_date, end_date = selected_dates
    else:
        start_date = end_date = selected_dates
    mask = (
        dates.between(start_date, end_date)
        & data["location_city"].astype(str).isin(cities)
        & data["merchant_category"].astype(str).isin(merchants)
        & data["status"].astype(str).isin(statuses)
        & data["risk_level"].isin(risk_levels)
        & data["amount"].between(amount_range[0], amount_range[1])
    )
    if user_search:
        mask &= data["user_id"].astype(str).str.contains(user_search, case=False, na=False)
    if reasons and "Any reason" not in reasons:
        reason_mask = data["fraud_reason"].astype(str).apply(lambda value: any(reason in value for reason in reasons))
        mask &= reason_mask
    elif not reasons:
        mask &= False
    return data[mask].copy(), {"start_date": start_date, "end_date": end_date}


def render_fraud(bundle: DashboardBundle) -> None:
    st.markdown('<div class="section-kicker">3 / Fraud analytics</div>', unsafe_allow_html=True)
    filtered, _ = filter_fraud_data(bundle.scored)
    fraud = filtered[filtered["is_fraud"]].copy()
    st.caption(f"Local pandas rule evaluation: {len(filtered):,} records match the selected filters, including {len(fraud):,} flagged transactions.")
    if filtered.empty:
        st.info("No records match the selected filters. Broaden the filters to continue.")
        return

    cards = st.columns(4)
    cards[0].metric("Filtered transactions", f"{len(filtered):,}")
    cards[1].metric("Filtered fraud count", f"{len(fraud):,}")
    cards[2].metric("Filtered fraud amount", inr(fraud["amount"].sum()))
    cards[3].metric("Average filtered risk", f"{filtered['risk_score'].mean():.1f}")

    trend = fraud.assign(date=fraud["timestamp"].dt.date).groupby("date").agg(fraud_count=("transaction_id", "count"), fraud_amount=("amount", "sum"))
    city = fraud.groupby("location_city").agg(fraud_count=("transaction_id", "count"), fraud_amount=("amount", "sum")).sort_values("fraud_amount", ascending=False)
    merchant = fraud.groupby("merchant_category").size().sort_values(ascending=False).rename("fraud_count")
    charts = st.columns(2)
    with charts[0]:
        st.markdown("### Fraud trend over time")
        st.line_chart(trend, width="stretch")
    with charts[1]:
        st.markdown("### Fraud amount by city")
        st.bar_chart(city["fraud_amount"], width="stretch")
    charts = st.columns(2)
    with charts[0]:
        st.markdown("### Fraud by merchant category")
        st.bar_chart(merchant, width="stretch")
    with charts[1]:
        st.markdown("### Risk-score distribution")
        st.bar_chart(filtered["risk_score"].value_counts().sort_index(), width="stretch")

    rule_counts = pd.Series({label: int(fraud[name].sum()) for name, label in RULE_LABELS.items()})
    st.markdown("### Fraud-rule distribution")
    st.bar_chart(rule_counts, width="stretch")
    st.markdown("### Top high-risk users")
    st.dataframe(high_risk_users(fraud).head(15), width="stretch", hide_index=True)
    st.markdown("### High-risk transactions")
    display_columns = ["transaction_id", "user_id", "timestamp", "amount", "location_city", "device_id", "merchant_category", "fraud_reason", "risk_score", "risk_level"]
    st.dataframe(fraud.sort_values(["risk_score", "timestamp"], ascending=[False, False])[display_columns].head(100), width="stretch", hide_index=True)
    st.download_button(
        "Download filtered high-risk CSV",
        csv_bytes(fraud, display_columns),
        file_name="upi_sentinel_filtered_high_risk.csv",
        mime="text/csv",
        width="stretch",
    )


def render_monitoring(bundle: DashboardBundle) -> None:
    st.markdown('<div class="section-kicker">4 / Monitoring, investigation and testing</div>', unsafe_allow_html=True)
    fraud = bundle.gold.copy()
    if fraud.empty:
        st.success("No active fraud alerts in the current Synthetic Data output.")
        return
    fraud["investigation_status"] = fraud["risk_score"].map(lambda score: "Escalated" if score >= 80 else "Review" if score >= 40 else "New")
    top, middle, bottom = st.columns(3)
    with top:
        st.markdown('<span class="health">MONITORING READY</span>', unsafe_allow_html=True)
        st.caption("Simulated alert queue, not a live stream")
    with middle:
        st.markdown('<span class="health">QUALITY GATES LOADED</span>', unsafe_allow_html=True)
        st.caption(f"{len(bundle.quarantine):,} quarantined records")
    with bottom:
        st.markdown('<span class="health">SYNTHETIC DATA</span>', unsafe_allow_html=True)
        st.caption("No real customer or banking data")

    search, sort = st.columns([2, 1])
    with search:
        query = st.text_input("Search alert queue", placeholder="Transaction ID, user ID, city or reason")
    with sort:
        sort_by = st.selectbox("Sort alerts by", ["risk_score", "amount", "timestamp"])
    alerts = fraud.copy()
    if query:
        searchable = alerts.astype(str).agg(" ".join, axis=1)
        alerts = alerts[searchable.str.contains(query, case=False, na=False)]
    alerts = alerts.sort_values(sort_by, ascending=False)
    columns = ["transaction_id", "user_id", "timestamp", "amount", "location_city", "device_id", "fraud_reason", "risk_score", "risk_level", "investigation_status"]
    st.markdown("### Latest high-risk alerts")
    st.dataframe(alerts[columns].head(100), width="stretch", hide_index=True)

    st.markdown("### Validation summary")
    quality = quality_summary(bundle.bronze)
    test_columns = st.columns(4)
    with test_columns[0]:
        if bundle.quarantine.empty:
            st.success("Data quality\n\nPASS")
        else:
            st.warning(f"Data quality\n\nWARNING: {len(bundle.quarantine):,} quarantined")
    with test_columns[1]:
        required_rules = {"velocity_check", "geo_anomaly", "device_mule_check", "failure_burst_check"}
        passed = required_rules.issubset(bundle.scored.columns)
        (st.success if passed else st.error)("Fraud logic\n\nPASS" if passed else "Fraud logic\n\nERROR")
    with test_columns[2]:
        duplicate_count = int(bundle.bronze["transaction_id"].duplicated(keep=False).sum())
        (st.success if duplicate_count == 0 else st.warning)(f"Duplicate gate\n\n{duplicate_count:,} duplicate rows")
    with test_columns[3]:
        pass_rate = quality["pass_rate_pct"].mean() if not quality.empty else 100
        (st.success if pass_rate == 100 else st.warning)(f"Rule checks\n\n{pass_rate:.2f}% pass")

    with st.expander("How this becomes real-time-ready"):
        st.markdown(
            "The local page is simulated monitoring over deterministic Synthetic Data. A production extension would publish UPI events to Kafka, use Spark Structured Streaming or Databricks Auto Loader for Bronze ingestion, apply stateful watermark-aware rules in Silver, and serve Gold Delta outputs to this dashboard. No streaming service is claimed to be running here."
        )


def main() -> None:
    inject_styles()
    if st.sidebar.button("Refresh data"):
        load_bundle.clear()
        st.rerun()
    st.sidebar.markdown("### UPI Sentinel Lakehouse")
    st.sidebar.caption("Professional Synthetic Data risk-monitoring demo")
    uploaded_file = st.sidebar.file_uploader(
        "Upload transaction data",
        type=["csv", "xlsx", "json", "parquet"],
        help="Supported formats: CSV, XLSX, JSON, and Parquet. Uploaded data should be Synthetic Data or a non-sensitive demo export.",
    )
    if uploaded_file is not None:
        st.sidebar.caption(f"Loaded in memory: {uploaded_file.name}")
    st.sidebar.markdown("**Dashboard source**")
    st.sidebar.info("Lightweight pandas demonstration with optional Spark/Delta outputs. The complete PySpark pipeline remains in the notebooks.")
    if uploaded_file is not None:
        try:
            uploaded_payload = uploaded_file.getvalue()
            uploaded_frame = read_uploaded_frame(uploaded_payload, uploaded_file.name)
            missing = missing_required_columns(uploaded_frame)
            if missing:
                st.sidebar.warning("Missing columns: " + ", ".join(missing) + ". Those rows will be quarantined until the fields are provided.")
            bundle = load_uploaded_bundle(uploaded_payload, uploaded_file.name)
        except (ValueError, TypeError, UnicodeDecodeError, OSError) as exc:
            st.sidebar.error(f"Could not analyze this file: {exc}")
            st.info("The dashboard is showing its built-in deterministic Synthetic Data sample instead.")
            bundle = load_bundle()
        except Exception:
            st.sidebar.error("Could not analyze this file. Check its format and required columns.")
            st.info("The dashboard is showing its built-in deterministic Synthetic Data sample instead.")
            bundle = load_bundle()
    else:
        try:
            bundle = load_bundle()
        except Exception:
            st.error("The dashboard could not load its built-in Synthetic Data sample. Please refresh the app.")
            st.stop()

    sections = st.tabs([
        "1. EXECUTIVE OVERVIEW",
        "2. PIPELINE & DATA QUALITY",
        "3. FRAUD ANALYTICS",
        "4. MONITORING & TESTING",
    ])
    with sections[0]:
        render_overview(bundle)
    with sections[1]:
        render_pipeline(bundle)
    with sections[2]:
        render_fraud(bundle)
    with sections[3]:
        render_monitoring(bundle)


if __name__ == "__main__":
    main()
