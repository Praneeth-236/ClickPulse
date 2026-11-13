"""ClickPulse Streamlit dashboard powered by Apache Druid."""

import os
from datetime import timedelta

import pandas as pd
import requests
import streamlit as st

DRUID_SQL_URL = os.getenv(
    "DRUID_SQL_URL",
    "http://localhost:8888/druid/v2/sql"
)

DATASOURCE = "retailmart_clickstream"


@st.cache_data(ttl=30)
def run_query(query: str) -> list:
    """Execute a SQL query against Apache Druid."""
    response = requests.post(
        DRUID_SQL_URL,
        json={"query": query},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def sql_string(value: str) -> str:
    """Safely escape a string for a SQL literal."""
    return "'" + value.replace("'", "''") + "'"


def get_date_range():
    rows = run_query(
        f"""
        SELECT
            MIN(__time) AS min_time,
            MAX(__time) AS max_time
        FROM {DATASOURCE}
        """
    )

    if not rows or not rows[0]["min_time"]:
        return None, None

    min_date = pd.to_datetime(
        rows[0]["min_time"], utc=True
    ).date()

    max_date = pd.to_datetime(
        rows[0]["max_time"], utc=True
    ).date()

    return min_date, max_date


def get_devices():
    rows = run_query(
        f"""
        SELECT DISTINCT device
        FROM {DATASOURCE}
        WHERE device IS NOT NULL
        ORDER BY device
        """
    )

    return [row["device"] for row in rows]


def build_filters(start_date, end_date, devices):
    """Build SQL conditions from dashboard filters."""

    next_day = end_date + timedelta(days=1)

    conditions = [
        f"__time >= TIMESTAMP '{start_date} 00:00:00'",
        f"__time < TIMESTAMP '{next_day} 00:00:00'",
    ]

    if devices:
        device_values = ", ".join(
            sql_string(device) for device in devices
        )
        conditions.append(f"device IN ({device_values})")

    return " AND ".join(conditions)


def main():
    st.set_page_config(
        page_title="ClickPulse Analytics",
        page_icon="📊",
        layout="wide",
    )

    st.title("ClickPulse — Clickstream Analytics")
    st.caption("Real-time analytics powered by Apache Druid")

    try:
        min_date, max_date = get_date_range()
        devices = get_devices()

    except requests.RequestException as error:
        st.error(f"Unable to connect to Apache Druid: {error}")
        st.stop()

    if min_date is None:
        st.warning("No data available in the Druid datasource.")
        st.stop()

    # Sidebar filters
    st.sidebar.header("Dashboard Filters")

    selected_dates = st.sidebar.date_input(
        "Select date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    selected_devices = st.sidebar.multiselect(
        "Select device",
        options=devices,
        default=devices,
    )

    if len(selected_dates) != 2:
        st.info("Select both a start date and an end date.")
        st.stop()

    start_date, end_date = selected_dates

    if start_date > end_date:
        st.error("Start date cannot be after end date.")
        st.stop()

    if not selected_devices:
        st.info("Select at least one device to view analytics.")
        st.stop()

    filters = build_filters(
        start_date,
        end_date,
        selected_devices,
    )

    # Revenue and purchase KPIs
    kpi_query = f"""
        SELECT
            COUNT(*) AS purchase_count,
            COALESCE(SUM(price * quantity), 0) AS total_revenue
        FROM {DATASOURCE}
        WHERE event_type = 'purchase'
        AND {filters}
    """

    # Revenue by category
    category_query = f"""
        SELECT
            category,
            SUM(price * quantity) AS revenue
        FROM {DATASOURCE}
        WHERE event_type = 'purchase'
        AND category IS NOT NULL
        AND {filters}
        GROUP BY category
        ORDER BY revenue DESC
    """

    try:
        kpi_rows = run_query(kpi_query)
        category_rows = run_query(category_query)

    except requests.RequestException as error:
        st.error(f"Druid query failed: {error}")
        st.stop()

    purchase_count = int(kpi_rows[0]["purchase_count"] or 0)
    total_revenue = float(kpi_rows[0]["total_revenue"] or 0)

    # KPI cards
    col1, col2 = st.columns(2)

    col1.metric(
        "Total Revenue",
        f"{total_revenue:,.2f}",
    )

    col2.metric(
        "Total Purchases",
        f"{purchase_count:,}",
    )

    st.divider()

    # Revenue chart
    st.subheader("Revenue by Category")

    if category_rows:
        category_df = pd.DataFrame(category_rows)

        st.bar_chart(
            category_df,
            x="category",
            y="revenue",
            x_label="Product Category",
            y_label="Revenue",
        )
    else:
        st.info("No purchase revenue for the selected filters.")

    st.caption(
        f"Showing data from {start_date} to {end_date} | "
        f"Datasource: {DATASOURCE}"
    )


if __name__ == "__main__":
    main()
