#!/usr/bin/env python3
"""
Streamlit Web Dashboard for Parquet + DuckDB PM Data
Works perfectly in GitHub Codespaces.
"""

import os
import streamlit as st
import pandas as pd
from datetime import datetime
import plotly.express as px

from pm_query import query_data  # your fast parquet/DuckDB engine

# --------------------------------------
# Streamlit Page Config
# --------------------------------------
st.set_page_config(
    page_title="PM Dashboard",
    layout="wide",
)

st.title("📡 PM Dashboard (Parquet + DuckDB)")

STORE_ROOT = "./pm_store"
st.sidebar.write(f"📁 Parquet Store: `{STORE_ROOT}`")

# --------------------------------------
# Scan NE partitions
# --------------------------------------
def scan_ne(store_root):
    if not os.path.isdir(store_root):
        return []
    nes = []
    for p in os.listdir(store_root):
        if p.startswith("NE="):
            nes.append(p.split("=", 1)[1])
    return sorted(nes)

ne_list = scan_ne(STORE_ROOT)

if not ne_list:
    st.warning("No NE partitions found. Run pm_ingest.py first.")
    st.stop()

# --------------------------------------
# Sidebar Filters
# --------------------------------------
st.sidebar.header("Filters")

ne = st.sidebar.selectbox("Select NE", ne_list)

kpi_text = st.sidebar.text_input(
    "KPIs (comma separated)",
    "QFACTOR-AVG,PREFEC-AVG"
)
kpis = [k.strip() for k in kpi_text.split(",") if k.strip()]

tp_filter = st.sidebar.text_input("TP contains (optional)", "")

start = st.sidebar.text_input("Start Time (YYYY-MM-DD HH:MM)", "")
end = st.sidebar.text_input("End Time (YYYY-MM-DD HH:MM)", "")

run_btn = st.sidebar.button("Run Query")

# --------------------------------------
# Logic
# --------------------------------------
if run_btn:
    st.write("### Query Results")

    df = query_data(
        root=STORE_ROOT,
        ne=ne,
        kpis=kpis,
        start=start if start.strip() else None,
        end=end if end.strip() else None,
        tp_contains=tp_filter if tp_filter.strip() else None,
    )

    if df.empty:
        st.warning("No data found for selected filters.")
        st.stop()

    st.success(f"Loaded {df.shape[0]} rows.")

    # --------------------------------------
    # Plot with Plotly
    # --------------------------------------
    for k in kpis:
        if k in df.columns:
            fig = px.line(
                df.reset_index(),
                x="Time",
                y=k,
                title=f"{k} Over Time",
                markers=True
            )
            st.plotly_chart(fig, use_container_width=True)

    # --------------------------------------
    # Raw Data Table
    # --------------------------------------
    with st.expander("Show Raw Data"):
        st.dataframe(df.reset_index())

