"""
streamlit_dashboard.py — FluxCart Live Web Dashboard
=====================================================

PURPOSE:
    A live web dashboard that reads the CSV files written by export.py
    and displays charts that auto-refresh every 30 seconds.

    This runs in your browser at http://localhost:8501
    Every refresh cycle re-reads the CSV files from disk and redraws
    all charts with the latest data.

HOW TO RUN:
    Step 1 — Install dependencies (one time only):
        pip3 install streamlit plotly pandas --break-system-packages

    Step 2 — Make sure the pipeline and exporter are running:
        Terminal 1: python3 run_pipeline.py
        Terminal 2: python3 powerbi/export.py

    Step 3 — Start the dashboard:
        Terminal 3: streamlit run streamlit_dashboard.py

    Step 4 — Open browser:
        http://localhost:8501
        Charts refresh automatically every 30 seconds.
        Or click "Refresh" in the top right corner for instant update.

WHAT YOU SEE:
    Row 1 — KPI metrics bar
        Total behavior events | Total revenue | Total fraud alerts | Fulfillment rate

    Row 2 — Revenue Over Time (line chart) | Fraud Alerts Table (colored by severity)

    Row 3 — Top Products (bar chart) | Warehouse Activity (bar chart)

    Row 4 — Fulfillment Rate Over Time (line chart) | Order Status Breakdown (pie chart)

REQUIRES:
    - export.py must be running and have written at least one cycle of CSV files
    - powerbi/data/analytics_summary.csv
    - powerbi/data/fraud_alerts.csv
    - powerbi/data/inventory_summary.csv
"""

# =============================================================================
# IMPORTS
# =============================================================================

import streamlit as st
# streamlit — the web dashboard framework
# st.title(), st.metric(), st.plotly_chart() etc. build the UI
# Everything in this file is a streamlit call

import pandas as pd
# pandas — reads CSV files into DataFrames
# A DataFrame is like an Excel table in Python — rows and columns
# We use it to filter, sort, and aggregate the CSV data

import plotly.express as px
# plotly.express — creates interactive charts (line, bar, pie)
# "express" is the high-level API — one function call per chart
# Charts are interactive in the browser — hover, zoom, pan

import os
# os.path — for building file paths to the CSV files

import time
# time — used for the last-updated timestamp display

from datetime import datetime
# datetime — for formatting timestamps in the UI


# =============================================================================
# PAGE CONFIG — must be the FIRST streamlit call
# =============================================================================

st.set_page_config(
    page_title = "FluxCart Live Dashboard",
    page_icon  = "🛒",
    layout     = "wide",
    # layout="wide" uses the full browser width — better for dashboards
)


# =============================================================================
# CSV FILE PATHS
# =============================================================================
# These must match the paths in export.py exactly.

BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = "/Volumes/Work/Kafka_foundational/fluxcart-pipeline/bi_integration/data"

ANALYTICS_CSV = os.path.join(DATA_DIR, "analytics_summary.csv")
FRAUD_CSV     = os.path.join(DATA_DIR, "fraud_alerts.csv")
INVENTORY_CSV = os.path.join(DATA_DIR, "inventory_summary.csv")


# =============================================================================
# AUTO REFRESH INTERVAL
# =============================================================================

REFRESH_SECONDS = 30
# How often the dashboard re-reads the CSV files and redraws charts.
# 30 seconds = charts are at most 30 seconds behind the CSV files.
# The CSV files themselves are updated every 60 seconds by export.py.
# Lower this if you want more frequent updates (minimum ~5 seconds).


# =============================================================================
# DATA LOADERS
# =============================================================================
# Each function reads one CSV file and returns a pandas DataFrame.
# @st.cache_data(ttl=REFRESH_SECONDS) caches the result for REFRESH_SECONDS.
# After REFRESH_SECONDS, the cache expires and the file is re-read from disk.
# This is how auto-refresh works — the cache expires and new data is loaded.

@st.cache_data(ttl=REFRESH_SECONDS)
def load_analytics() -> pd.DataFrame:
    """
    Read analytics_summary.csv into a DataFrame.

    Returns empty DataFrame with correct columns if file does not exist yet.
    This prevents the dashboard from crashing before export.py has written
    its first cycle.
    """
    if not os.path.exists(ANALYTICS_CSV):
        # File does not exist yet — export.py has not run its first cycle
        # Return an empty DataFrame with the correct column names
        return pd.DataFrame(columns=[
            "timestamp", "behavior_events", "top_action", "top_product",
            "top_category", "order_events", "avg_order_value", "total_revenue",
            "orders_placed", "orders_delivered", "orders_cancelled",
        ])

    df = pd.read_csv(ANALYTICS_CSV)
    # pd.read_csv() reads the entire CSV file into a DataFrame
    # Each column header becomes a DataFrame column
    # Each row becomes a DataFrame row

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    # Convert the timestamp string to a proper datetime object
    # This lets Plotly draw a proper time-series axis on line charts

    df = df.sort_values("timestamp")
    # Sort by time so line charts go left to right chronologically

    return df


@st.cache_data(ttl=REFRESH_SECONDS)
def load_fraud() -> pd.DataFrame:
    """
    Read fraud_alerts.csv into a DataFrame.
    Returns empty DataFrame if file does not exist yet.
    """
    if not os.path.exists(FRAUD_CSV):
        return pd.DataFrame(columns=[
            "timestamp", "alert_id", "rule", "severity",
            "user_id", "payment_id", "order_id", "amount", "detail",
        ])

    df = pd.read_csv(FRAUD_CSV)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp", ascending=False)
    # Most recent alerts at the top of the table

    return df


@st.cache_data(ttl=REFRESH_SECONDS)
def load_inventory() -> pd.DataFrame:
    """
    Read inventory_summary.csv into a DataFrame.
    Returns empty DataFrame if file does not exist yet.
    """
    if not os.path.exists(INVENTORY_CSV):
        return pd.DataFrame(columns=[
            "timestamp", "order_events", "units_ordered", "units_delivered",
            "units_cancelled", "fulfillment_rate", "top_warehouse",
            "top_product", "top_category", "orders_placed",
            "orders_delivered", "orders_cancelled",
        ])

    df = pd.read_csv(INVENTORY_CSV)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")

    return df


# =============================================================================
# SEVERITY COLOR MAP — for fraud alerts table
# =============================================================================

SEVERITY_COLORS = {
    "HIGH":   "🔴",
    "MEDIUM": "🟠",
    "LOW":    "🟡",
}
# Maps severity level to a colored emoji dot
# Shown in the fraud alerts table so HIGH alerts stand out immediately


# =============================================================================
# DASHBOARD LAYOUT
# =============================================================================

def render_dashboard():
    """
    Build and render the complete dashboard.
    Called on every page load and every auto-refresh cycle.
    """

    # ── Load all data ─────────────────────────────────────────────────────
    analytics = load_analytics()
    fraud     = load_fraud()
    inventory = load_inventory()

    # ── Header ────────────────────────────────────────────────────────────
    st.title("🛒 FluxCart Live Pipeline Dashboard")

    # Show when data was last refreshed
    last_refresh = datetime.utcnow().strftime("%H:%M:%S UTC")
    st.caption(f"Auto-refreshes every {REFRESH_SECONDS}s  |  Last loaded: {last_refresh}  |  CSV rows: analytics={len(analytics)}  fraud={len(fraud)}  inventory={len(inventory)}")

    # Show a warning if CSV files are not ready yet
    if len(analytics) == 0:
        st.warning(
            "⏳ Waiting for data... "
            "Make sure `python3 run_pipeline.py` and `python3 powerbi/export.py` are running. "
            "Data appears after the first export cycle (~60 seconds)."
        )
        st.stop()
        # st.stop() halts rendering here — nothing below is drawn
        # until data is available

    st.divider()

    # =========================================================================
    # ROW 1 — KPI METRICS
    # =========================================================================
    # Four big numbers at the top — quick health check at a glance

    st.subheader("📊 Pipeline KPIs")

    col1, col2, col3, col4, col5 = st.columns(5)
    # st.columns(5) creates 5 equal-width columns side by side

    with col1:
        total_behavior = int(analytics["behavior_events"].sum())
        # .sum() adds up all values in the column across all rows
        st.metric(
            label = "Total Behavior Events",
            value = f"{total_behavior:,}",
            # :, formats numbers with commas: 12345 → 12,345
        )

    with col2:
        total_revenue = analytics["total_revenue"].sum()
        st.metric(
            label = "Total Revenue",
            value = f"Rs.{total_revenue:,.0f}",
            # .0f = no decimal places — Rs.5,826,625 not Rs.5826625.97
        )

    with col3:
        total_alerts = len(fraud)
        high_alerts  = len(fraud[fraud["severity"] == "HIGH"])
        # fraud["severity"] == "HIGH" creates a boolean mask
        # fraud[mask] filters to only HIGH severity rows
        st.metric(
            label = "Fraud Alerts",
            value = total_alerts,
            delta = f"{high_alerts} HIGH risk",
            delta_color = "inverse",
            # delta_color="inverse" makes the delta RED (bad) instead of green
        )

    with col4:
        avg_fulfillment = inventory["fulfillment_rate"].mean()
        # .mean() = average across all rows
        st.metric(
            label = "Avg Fulfillment Rate",
            value = f"{avg_fulfillment:.1f}%",
        )

    with col5:
        total_orders = int(analytics["order_events"].sum())
        st.metric(
            label = "Total Orders",
            value = f"{total_orders:,}",
        )

    st.divider()

    # =========================================================================
    # ROW 2 — REVENUE OVER TIME  |  FRAUD ALERTS TABLE
    # =========================================================================

    col_left, col_right = st.columns([3, 2])
    # [3, 2] = left column is 60% wide, right column is 40% wide

    with col_left:
        st.subheader("💰 Revenue Over Time")

        fig_revenue = px.line(
            analytics,
            x     = "timestamp",
            y     = "total_revenue",
            # x axis = time, y axis = revenue
            title = "Total Revenue per 10s Window",
            labels = {
                "timestamp":     "Time",
                "total_revenue": "Revenue (Rs.)",
            },
            markers = True,
            # markers=True shows a dot at each data point
            # Makes it easy to see individual window values
        )
        fig_revenue.update_traces(line_color="#00C4B4", line_width=2)
        # Teal colored line — matches the FluxCart theme

        fig_revenue.update_layout(
            xaxis_title = "Time",
            yaxis_title = "Revenue (Rs.)",
            hovermode   = "x unified",
            # hovermode="x unified" shows all values at the same timestamp
            # when you hover over the chart
        )
        st.plotly_chart(fig_revenue, use_container_width=True)
        # use_container_width=True stretches the chart to fill the column

    with col_right:
        st.subheader("🚨 Fraud Alerts")

        if len(fraud) == 0:
            st.success("✅ No fraud alerts detected yet")
        else:
            # Add severity emoji column for visual scanning
            fraud_display = fraud[["timestamp", "severity", "rule", "user_id", "amount"]].copy()
            fraud_display["severity"] = fraud_display["severity"].map(
                lambda s: f"{SEVERITY_COLORS.get(s, '⚪')} {s}"
                # Map each severity to emoji + text: "🔴 HIGH", "🟠 MEDIUM"
            )
            fraud_display["amount"] = fraud_display["amount"].map(
                lambda a: f"Rs.{a:,.0f}"
                # Format amounts: 441104 → Rs.4,41,104
            )
            fraud_display["timestamp"] = fraud_display["timestamp"].dt.strftime("%H:%M:%S")
            # Show only the time part — date is the same for all rows

            fraud_display.columns = ["Time", "Severity", "Rule", "User", "Amount"]
            # Rename columns for clean display in the table

            st.dataframe(
                fraud_display,
                use_container_width = True,
                hide_index          = True,
                # hide_index=True removes the 0,1,2,3 row numbers
            )

    st.divider()

    # =========================================================================
    # ROW 3 — TOP PRODUCTS  |  WAREHOUSE ACTIVITY
    # =========================================================================

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("📦 Top Products by Orders")

        # Count how many times each product appears as top_product
        product_counts = (
            analytics["top_product"]
            .value_counts()
            # value_counts() counts occurrences of each unique value
            # {"elec-003": 12, "home-004": 9, "clth-001": 7}
            .reset_index()
            # reset_index() converts the Series to a DataFrame with columns
            .head(10)
            # Show top 10 products only
        )
        product_counts.columns = ["product", "windows_as_top"]
        # Rename columns — "windows_as_top" = how many 10s windows
        # this product was the #1 product

        fig_products = px.bar(
            product_counts,
            x      = "product",
            y      = "windows_as_top",
            title  = "Products Most Often Ranked #1",
            labels = {"product": "Product ID", "windows_as_top": "Times Top Product"},
            color  = "windows_as_top",
            color_continuous_scale = "teal",
            # Color bars by value — darker = more times at top
        )
        fig_products.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig_products, use_container_width=True)

    with col_right:
        st.subheader("🏭 Warehouse Activity")

        warehouse_counts = (
            inventory["top_warehouse"]
            .value_counts()
            .reset_index()
            .head(10)
        )
        warehouse_counts.columns = ["warehouse", "times_busiest"]

        fig_warehouse = px.bar(
            warehouse_counts,
            x      = "warehouse",
            y      = "times_busiest",
            title  = "Busiest Warehouses",
            labels = {"warehouse": "Warehouse", "times_busiest": "Times Busiest"},
            color  = "times_busiest",
            color_continuous_scale = "blues",
        )
        fig_warehouse.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig_warehouse, use_container_width=True)

    st.divider()

    # =========================================================================
    # ROW 4 — FULFILLMENT RATE OVER TIME  |  ORDER STATUS BREAKDOWN
    # =========================================================================

    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.subheader("🚚 Fulfillment Rate Over Time")

        fig_fulfillment = px.line(
            inventory,
            x       = "timestamp",
            y       = "fulfillment_rate",
            title   = "Fulfillment Rate % per 10s Window",
            labels  = {
                "timestamp":        "Time",
                "fulfillment_rate": "Fulfillment Rate (%)",
            },
            markers = True,
        )
        fig_fulfillment.update_traces(line_color="#FF6B6B", line_width=2)
        # Red line for fulfillment — easy to distinguish from revenue chart

        fig_fulfillment.add_hline(
            y          = 100,
            line_dash  = "dash",
            line_color = "green",
            annotation_text = "100% target",
        )
        # Dashed green line at 100% — shows the target fulfillment rate
        # In practice it will be well below 100% which is expected

        fig_fulfillment.update_layout(
            xaxis_title = "Time",
            yaxis_title = "Fulfillment Rate (%)",
            hovermode   = "x unified",
        )
        st.plotly_chart(fig_fulfillment, use_container_width=True)

    with col_right:
        st.subheader("🥧 Order Status Breakdown")

        # Sum up all order status columns across the full history
        status_data = {
            "placed":    int(analytics["orders_placed"].sum()),
            "delivered": int(analytics["orders_delivered"].sum()),
            "cancelled": int(analytics["orders_cancelled"].sum()),
        }
        # Build a small DataFrame for the pie chart
        status_df = pd.DataFrame(
            list(status_data.items()),
            columns=["status", "count"]
        )

        fig_pie = px.pie(
            status_df,
            values = "count",
            names  = "status",
            title  = "Order Status Distribution",
            color  = "status",
            color_discrete_map = {
                "placed":    "#00C4B4",   # teal
                "delivered": "#2ECC71",   # green
                "cancelled": "#E74C3C",   # red
            },
            # Fixed colors so placed=teal, delivered=green, cancelled=red
            # regardless of which slice is largest
        )
        fig_pie.update_traces(textposition="inside", textinfo="percent+label")
        # Show percentage and label inside each slice
        st.plotly_chart(fig_pie, use_container_width=True)

    st.divider()

    # =========================================================================
    # ROW 5 — BEHAVIOR EVENTS OVER TIME  |  AVG ORDER VALUE OVER TIME
    # =========================================================================

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("👤 User Behavior Events Over Time")

        fig_behavior = px.area(
            analytics,
            x      = "timestamp",
            y      = "behavior_events",
            title  = "Behavior Events per 10s Window",
            labels = {
                "timestamp":       "Time",
                "behavior_events": "Events",
            },
        )
        fig_behavior.update_traces(
            line_color  = "#9B59B6",
            fillcolor   = "rgba(155, 89, 182, 0.2)",
            # Semi-transparent purple fill — area chart looks good for event volume
        )
        st.plotly_chart(fig_behavior, use_container_width=True)

    with col_right:
        st.subheader("💳 Average Order Value Over Time")

        fig_aov = px.line(
            analytics,
            x       = "timestamp",
            y       = "avg_order_value",
            title   = "Avg Order Value (Rs.) per 10s Window",
            labels  = {
                "timestamp":       "Time",
                "avg_order_value": "Avg Order Value (Rs.)",
            },
            markers = True,
        )
        fig_aov.update_traces(line_color="#F39C12", line_width=2)
        # Orange line for order value
        st.plotly_chart(fig_aov, use_container_width=True)

    # =========================================================================
    # FOOTER
    # =========================================================================

    st.divider()
    st.caption(
        f"FluxCart Pipeline Dashboard  |  "
        f"Data from: {DATA_DIR}  |  "
        f"Refresh interval: {REFRESH_SECONDS}s  |  "
        f"Analytics rows: {len(analytics)}  |  "
        f"Fraud rows: {len(fraud)}  |  "
        f"Inventory rows: {len(inventory)}"
    )


# =============================================================================
# AUTO REFRESH
# =============================================================================

def main():
    """
    Render the dashboard and schedule the next auto-refresh.

    st_autorefresh() reloads the entire page every REFRESH_SECONDS * 1000
    milliseconds. On each reload, the @st.cache_data(ttl=...) cache has
    expired so load_analytics(), load_fraud(), load_inventory() all
    re-read the latest CSV files from disk.

    This is how values update automatically — the page reloads, cache
    expires, new CSV data is read, charts are redrawn with fresh data.
    """

    # ── Auto refresh ──────────────────────────────────────────────────────
    # streamlit_autorefresh is a small package that triggers page reloads.
    # Install it with: pip3 install streamlit-autorefresh
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(
            interval = REFRESH_SECONDS * 1000,
            # interval is in milliseconds — multiply seconds by 1000
            key      = "fluxcart_autorefresh",
        )
    except ImportError:
        # If streamlit-autorefresh is not installed, show a manual refresh note
        st.info(
            f"💡 For auto-refresh, run: `pip3 install streamlit-autorefresh`  |  "
            f"Currently: manual refresh only (press R or click ⟳ in browser)"
        )

    # ── Render the full dashboard ─────────────────────────────────────────
    render_dashboard()


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    main()
    
'''
Refresh Timing — How It Actually Works
export.py        → appends new rows to CSV every 60 seconds
Streamlit cache  → expires every 30 seconds
Page auto-reload → every 30 seconds

So the cycle is:
0s   — export.py writes new rows to CSV
30s  — Streamlit reloads, cache expires, reads new CSV → charts update
60s  — export.py writes more rows
90s  — Streamlit reloads again → charts update again
Effective update frequency = every 30 seconds on screen, new data every 60 seconds from Kafka.

The Revenue Chart Drop
You will notice the revenue line drops sharply — that is because only 5 analytics rows are loaded so far. As the pipeline keeps running and more rows accumulate over the next 10-15 minutes, the line chart will smooth out and show a proper trend.

The Complete FluxCart Pipeline Is Done
✅  3-broker Kafka cluster (KRaft, no ZooKeeper)
✅  5 topics with partitions and replication
✅  Producer — 50 events/second
✅  Analytics consumer — behavior + revenue reports
✅  Fraud consumer — real time detection + alerts
✅  Inventory consumer — stock movement + warehouses
✅  run_pipeline.py — one command for everything
✅  Terminal dashboard — unified live view
✅  export.py — CSV files every 60 seconds
✅  Streamlit dashboard — live web charts auto-refreshing

'''