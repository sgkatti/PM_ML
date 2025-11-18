#!/usr/bin/env python3
"""
pm_dashboard_streamlit_v1.py

Streamlit-based PM Dashboard (Parquet + DuckDB)
- KPI multi-select (populates after TP selection)
- TP Category -> TP list (CAT-3 logic)
- Overlay toggle (single chart vs per-KPI)
- Uses pm_query.query_data() to fetch final data

Requirements:
  pip install streamlit pandas pyarrow duckdb plotly

Author: ChatGPT for Sanjeev
"""
import os
import streamlit as st
import pandas as pd
from datetime import datetime
import plotly.express as px

# local query engine (uses duckdb if available)
from pm_query import query_data

# --------- Page config ----------
st.set_page_config(page_title="PM Dashboard", layout="wide")
st.title("📡 PM Dashboard (Parquet + DuckDB Backed)")

STORE_ROOT = "./pm_store"
st.sidebar.write(f"📁 Parquet Store: `{STORE_ROOT}`")

# --------- Helpers: TP Category (CAT-3) ----------
def extract_tp_category(tp: str) -> str:
    """Robust telecom-grade TP category extraction (CAT-3)."""
    tp_upper = str(tp).upper()

    # 1) Primary tokens to search anywhere
    primary_tokens = ["OCH", "OTS", "OTU", "ODU", "OSC", "LINEIN", "LINEOUT", "100GBE", "10GBE", "PORT"]
    for token in primary_tokens:
        if f":{token}:" in tp_upper or token in tp_upper:
            return token

    # 2) If colon-separated, use second token
    parts = tp_upper.split(":")
    if len(parts) >= 2 and parts[1].strip():
        return parts[1].strip()

    # 3) Fallback: prefix
    return tp_upper[:10]

# --------- Helper: list NE partitions ----------
def scan_ne(store_root):
    if not os.path.isdir(store_root):
        return []
    nes = []
    for p in os.listdir(store_root):
        if p.startswith("NE="):
            nes.append(p.split("=", 1)[1])
    return sorted(nes)

# --------- Helper: get TP list & category map for a NE ----------
def get_tp_info(store_root, ne):
    """
    Fast distinct TP extraction using duckdb (reads parquet).
    Returns (tps_sorted, category_map {category: [tp,...]}).
    Defensive: falls back to scanning parquet files with pandas if duckdb not available.
    """
    parquet_glob = os.path.join(store_root, f"NE={ne}", "*", "*.parquet")
    try:
        import duckdb
        q = f"SELECT DISTINCT TP FROM parquet_scan('{parquet_glob}') WHERE TP IS NOT NULL"
        df = duckdb.query(q).to_df()
        tps = sorted(df["TP"].dropna().unique()) if not df.empty else []
    except Exception:
        # fallback: scan a few parquet files with pandas
        tps = []
        base = os.path.join(store_root, f"NE={ne}")
        if os.path.isdir(base):
            for root, _, files in os.walk(base):
                for f in files:
                    if f.endswith(".parquet"):
                        try:
                            p = os.path.join(root, f)
                            tmp = pd.read_parquet(p, engine="pyarrow", columns=["TP"])
                            if "TP" in tmp.columns:
                                tps.extend(tmp["TP"].dropna().astype(str).unique().tolist())
                        except Exception:
                            continue
                if len(tps) > 500:
                    break
        tps = sorted(list(set(tps)))

    category_map = {}
    for tp in tps:
        cat = extract_tp_category(tp)
        category_map.setdefault(cat, []).append(tp)

    # sort the lists
    for k in list(category_map.keys()):
        category_map[k] = sorted(category_map[k])

    return sorted(tps), category_map

# --------- Helper: lightweight KPI discovery for NE+TP ----------
def get_kpi_list(store_root, ne, tp):
    """
    Returns a sorted list of numeric KPI column names for given NE+TP.
    Uses a small parquet sample via DuckDB for speed.
    """
    parquet_glob = os.path.join(store_root, f"NE={ne}", "*", "*.parquet")
    try:
        import duckdb
        # read small sample of rows matching the TP to detect numeric columns
        sql = f"""
            SELECT *
            FROM parquet_scan('{parquet_glob}')
            WHERE TP = '{tp}'
            LIMIT 10
        """
        df = duckdb.query(sql).to_df()
    except Exception:
        # fallback: read up to a couple of parquet files and sample
        df = pd.DataFrame()
        base = os.path.join(store_root, f"NE={ne}")
        if os.path.isdir(base):
            files_seen = 0
            for root, _, files in os.walk(base):
                for f in files:
                    if f.endswith(".parquet"):
                        p = os.path.join(root, f)
                        try:
                            tmp = pd.read_parquet(p, engine="pyarrow", columns=None)
                            if "TP" in tmp.columns:
                                tmp = tmp[tmp["TP"].astype(str) == tp]
                            df = pd.concat([df, tmp.head(10)], ignore_index=True) if not df.empty else tmp.head(10)
                            files_seen += 1
                        except Exception:
                            continue
                if files_seen >= 3:
                    break

    if df.empty:
        return []

    # choose numeric-like columns (exclude NE, TP, Time)
    exclude = {"NE", "TP", "Time"}
    numeric_cols = []
    for c in df.columns:
        if c in exclude:
            continue
        try:
            # attempt to coerce sample to numeric — if many values become numeric it's a KPI
            sample = pd.to_numeric(df[c].replace({"NS": pd.NA}), errors="coerce")
            non_na = sample.dropna()
            if non_na.shape[0] >= 1:
                numeric_cols.append(c)
        except Exception:
            continue

    return sorted(numeric_cols)

# --------- Sidebar: NE selection ----------
ne_list = scan_ne(STORE_ROOT)
if not ne_list:
    st.sidebar.warning("No NE partitions found. Run pm_ingest.py first.")
    st.stop()

st.sidebar.header("Filters")
ne = st.sidebar.selectbox("Select NE", ne_list)

# --------- Load TP info and show TP category + TP list ----------
with st.spinner("Loading TP categories..."):
    all_tps, category_map = get_tp_info(STORE_ROOT, ne)

if not all_tps:
    st.sidebar.warning("No TPs found for this NE.")
    st.stop()

tp_categories = sorted(category_map.keys())
tp_cat = st.sidebar.selectbox("TP Category", tp_categories)

tp_list = category_map.get(tp_cat, [])
tp_selected = st.sidebar.selectbox("Select TP", tp_list)

# --------- Load KPI list for selected NE+TP (lightweight) ----------
with st.spinner("Loading KPI list for selected TP..."):
    kpi_list = get_kpi_list(STORE_ROOT, ne, tp_selected)

if not kpi_list:
    st.sidebar.warning("No KPIs detected for this NE+TP (check partitions or ingestion).")

# KPI multiselect (now populated)
kpis = st.sidebar.multiselect(
    "Select KPIs",
    options=kpi_list,
    default=[kpi_list[0]] if kpi_list else [],
    help="Pick one or more KPIs to plot"
)

# Overlay toggle
overlay = st.sidebar.checkbox("Overlay multiple KPIs (single chart)", False)

# Time inputs (text for simplicity); can be upgraded to date_input/time_input
start_str = st.sidebar.text_input("Start Time (YYYY-MM-DD HH:MM)", "")
end_str = st.sidebar.text_input("End Time (YYYY-MM-DD HH:MM)", "")

# Run button
run_btn = st.sidebar.button("Run Query")

# ---------- Main query / plotting ----------
if run_btn:
    if not kpis:
        st.error("Please select at least one KPI before running the query.")
        st.stop()

    # build start/end
    start = start_str.strip() or None
    end = end_str.strip() or None

    st.subheader("Query Results")
    with st.spinner("Running fast query..."):
        try:
            df = query_data(
                root=STORE_ROOT,
                ne=ne,
                kpis=kpis,
                start=start,
                end=end,
                tp_contains=tp_selected,
            )
        except Exception as e:
            st.error(f"Query failed: {e}")
            st.stop()

    if df.empty:
        st.warning("No data matched the filters.")
        st.stop()

    st.success(f"Loaded {df.shape[0]:,} rows for NE={ne}, TP={tp_selected}")

    # Reset index may already be datetime index; ensure 'Time' column exists for plotting
    if "Time" not in df.reset_index().columns:
        df = df.reset_index().rename_axis('Time').reset_index()

    df_plot = df.reset_index() if "Time" in df.columns else df.reset_index()

    # plotting
    if overlay:
        # plot multiple KPIs on single chart
        fig = px.line(
            df_plot,
            x="Time",
            y=kpis,
            title=f"Overlay KPIs for {tp_selected}",
            markers=False,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        # one chart per KPI (scatter/line)
        for k in kpis:
            if k not in df_plot.columns:
                continue
            fig = px.scatter(
                df_plot,
                x="Time",
                y=k,
                title=f"{k} over time — TP: {tp_selected}",
                height=300,
            )
            st.plotly_chart(fig, use_container_width=True)

    with st.expander("Show Raw Data (first 500 rows)"):
        st.dataframe(df.head(500))

# Helpful footnote / instructions
st.markdown("---")
st.markdown(
    "Tip: select NE → TP Category → TP. KPI list loads automatically for the selected TP. "
    "Use the overlay toggle to combine KPIs on a single chart."
)
