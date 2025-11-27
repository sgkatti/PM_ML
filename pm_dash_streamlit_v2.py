#!/usr/bin/env python3
"""
pm_dashboard_streamlit_v3.py

PM Dashboard V3 — Enhancements:
1) NE list filtered to only NEs that have OTS-type TPs.
2) New mode: "Multiple OTS TPs, single KPI" overlay.
   - Compare performance of a single KPI across many OTS TPs.
3) Retains V2 features:
   - Parquet + DuckDB backend
   - Time-series regularization (continuous lines)
   - Single-TP / multi-KPI mode
"""

# ============================================================
#                   IMPORTS & CONFIGURATION
# ============================================================

import os
import streamlit as st
import pandas as pd
import plotly.express as px

from pm_query import query_data

# Streamlit page
st.set_page_config(page_title="PM Dashboard V3", layout="wide")
st.title("📡 PM Dashboard (Parquet + DuckDB Backed) — V3")

STORE_ROOT = "./pm_store"
st.sidebar.write(f"📁 Parquet Store Directory: `{STORE_ROOT}`")


# ============================================================
#              TIME SERIES REGULARIZATION HELPER
# ============================================================

def regularize_time(df: pd.DataFrame, freq: str = "15T") -> pd.DataFrame:
    """
    Ensure a continuous time grid for plotting by:
    - Normalizing Time column
    - Removing duplicate timestamps
    - Reindexing to regular grid
    - Interpolating + edge-filling

    This is only for visualization; raw data remains untouched.
    """
    # Ensure a Time column exists
    if "Time" not in df.columns:
        df = df.reset_index().rename(columns={"index": "Time"})

    df = df.copy()
    df["Time"] = pd.to_datetime(df["Time"], errors="coerce")
    df = df.dropna(subset=["Time"])
    df = df.sort_values("Time")

    if df.empty:
        return df

    # Remove duplicate timestamps (keep first)
    df = df.drop_duplicates(subset=["Time"], keep="first")

    # Build regular time index
    full_index = pd.date_range(
        start=df["Time"].min(),
        end=df["Time"].max(),
        freq=freq,
    )

    df = df.set_index("Time").reindex(full_index)
    df.index.name = "Time"

    # Interpolate in time, then fill edges
    df = df.interpolate(method="time")
    df = df.ffill().bfill()

    return df.reset_index().rename(columns={"index": "Time"})


# ============================================================
#         TP CATEGORY EXTRACTION (CAT-3 TELECOM LOGIC)
# ============================================================

def extract_tp_category(tp: str) -> str:
    """
    Heuristic TP category extraction.

    Example:
      "OTSI-4-13-L1:OTSI:NEND:RCV"  -> "OTS" or "OTSI" depending on naming
      "LINEIN:OCH"                  -> "OCH"
    """
    tp_upper = str(tp).upper()

    primary_tokens = [
        "OTS", "OTSI", "OCH", "OTU", "ODU", "OSC",
        "LINEIN", "LINEOUT", "100GBE", "10GBE", "PORT"
    ]

    for token in primary_tokens:
        if f":{token}:" in tp_upper or token in tp_upper:
            # normalise OTSI to OTS for filtering purposes
            if token == "OTSI":
                return "OTS"
            return token

    parts = tp_upper.split(":")
    if len(parts) >= 2 and parts[1].strip():
        return parts[1].strip()

    return tp_upper[:10]


# ============================================================
#               NE SCANNING / FILTER BY CATEGORY
# ============================================================

def scan_ne(store_root):
    """Return list of NE IDs from NE=xxxx directories."""
    if not os.path.isdir(store_root):
        return []
    return sorted(
        p.split("=", 1)[1]
        for p in os.listdir(store_root)
        if p.startswith("NE=")
    )


def get_tp_info(store_root, ne):
    """
    Discover TP list and category map for a given NE.

    Returns:
      tps_sorted, category_map {category: [tp,...]}
    """
    parquet_glob = os.path.join(store_root, f"NE={ne}", "*", "*.parquet")

    # Fast path — DuckDB
    try:
        import duckdb
        q = f"SELECT DISTINCT TP FROM parquet_scan('{parquet_glob}') WHERE TP IS NOT NULL"
        df = duckdb.query(q).to_df()
        tps = sorted(df["TP"].dropna().astype(str).unique()) if not df.empty else []
    except Exception:
        # Slow fallback
        tps = []
        base = os.path.join(store_root, f"NE={ne}")
        if os.path.isdir(base):
            for root, _, files in os.walk(base):
                for f in files:
                    if f.endswith(".parquet"):
                        p = os.path.join(root, f)
                        try:
                            tmp = pd.read_parquet(p, columns=["TP"], engine="pyarrow")
                            if "TP" in tmp.columns:
                                tps.extend(tmp["TP"].dropna().astype(str).unique())
                        except Exception:
                            continue
        tps = sorted(set(tps))

    category_map = {}
    for tp in tps:
        cat = extract_tp_category(tp)
        category_map.setdefault(cat, []).append(tp)

    for k in category_map:
        category_map[k] = sorted(category_map[k])

    return sorted(tps), category_map


def scan_ne_with_tp_category(store_root, required_cat: str):
    """
    Return list of NEs that have at least one TP whose category == required_cat.
    Used to filter NEs to only those that have OTS TPs.
    """
    candidates = scan_ne(store_root)
    filtered = []
    for ne in candidates:
        _, cat_map = get_tp_info(store_root, ne)
        if required_cat in cat_map:
            filtered.append(ne)
    return filtered


# ============================================================
#   KPI DISCOVERY: DETECT NUMERIC COLUMNS FOR SELECTED TP
# ============================================================

def get_kpi_list(store_root, ne, tp):
    """
    Detect numeric KPI columns for a given NE+TP.

    - DuckDB fast path
    - Pandas fallback (reads a few parquet files)
    """
    parquet_glob = os.path.join(store_root, f"NE={ne}", "*", "*.parquet")

    # Try DuckDB sample first
    try:
        import duckdb
        sql = f"""
            SELECT *
            FROM parquet_scan('{parquet_glob}')
            WHERE TP = '{tp}'
            LIMIT 10
        """
        df = duckdb.query(sql).to_df()
    except Exception:
        # Fallback: scan a few files
        df = pd.DataFrame()
        base = os.path.join(store_root, f"NE={ne}")
        if os.path.isdir(base):
            files_seen = 0
            for root, _, files in os.walk(base):
                for f in files:
                    if f.endswith(".parquet"):
                        p = os.path.join(root, f)
                        try:
                            tmp = pd.read_parquet(p, engine="pyarrow")
                            if "TP" in tmp.columns:
                                tmp = tmp[tmp["TP"].astype(str) == tp]
                            if not tmp.empty:
                                df = pd.concat(
                                    [df, tmp.head(10)],
                                    ignore_index=True
                                ) if not df.empty else tmp.head(10)
                                files_seen += 1
                        except Exception:
                            continue
                if files_seen >= 3:
                    break

    if df.empty:
        return []

    exclude = {"NE", "TP", "Time"}
    numeric_cols = []

    for c in df.columns:
        if c in exclude:
            continue
        try:
            sample = pd.to_numeric(df[c].replace({"NS": pd.NA}), errors="coerce")
            if sample.dropna().shape[0] >= 1:
                numeric_cols.append(c)
        except Exception:
            continue

    return sorted(numeric_cols)


# ============================================================
#                   SIDEBAR — MAIN FILTERS
# ============================================================

st.sidebar.header("Filters")

# 1) Mode selection
mode = st.sidebar.radio(
    "Plot mode",
    ("Single TP, multiple KPIs", "Multiple OTS TPs, single KPI"),
    index=0,
)

# 2) NE selection (filtered to only those with OTS TPs)
ne_list = scan_ne_with_tp_category(STORE_ROOT, required_cat="OTS")
if not ne_list:
    st.sidebar.warning("❗ No NE found with OTS-type TPs. Check pm_store.")
    st.stop()

ne = st.sidebar.selectbox("Select NE (with OTS present)", ne_list)

# 3) Load TP info for this NE
with st.spinner("Loading TP categories & TPs..."):
    all_tps, category_map = get_tp_info(STORE_ROOT, ne)

if not all_tps:
    st.sidebar.warning("❗ No TPs found for this NE.")
    st.stop()

tp_categories = sorted(category_map.keys())


# ============================================================
#            MODE A: SINGLE TP, MULTIPLE KPIs (OLD FLOW)
# ============================================================

if mode == "Single TP, multiple KPIs":
    # TP Category selection — any category
    tp_cat = st.sidebar.selectbox("TP Category", tp_categories, key="cat_single")

    tp_list = category_map.get(tp_cat, [])
    tp_selected = st.sidebar.selectbox("Select TP", tp_list, key="tp_single")

    # KPI list for this TP
    with st.spinner("Detecting KPIs..."):
        kpi_list = get_kpi_list(STORE_ROOT, ne, tp_selected)

    if not kpi_list:
        st.sidebar.warning("❗ No KPIs found for this TP.")
        st.stop()

    kpis = st.sidebar.multiselect(
        "Select KPIs",
        kpi_list,
        default=[kpi_list[0]] if kpi_list else [],
        key="kpi_multi",
    )

    overlay = st.sidebar.checkbox("Overlay multiple KPIs in one chart", False)

    start_str = st.sidebar.text_input("Start Time (YYYY-MM-DD HH:MM)", "", key="start_single")
    end_str = st.sidebar.text_input("End Time (YYYY-MM-DD HH:MM)", "", key="end_single")

    run_btn = st.sidebar.button("Run Query", key="run_single")

    # -------- EXECUTION: MODE A --------
    if run_btn:
        if not kpis:
            st.error("❗ Please select at least one KPI.")
            st.stop()

        start = start_str.strip() or None
        end = end_str.strip() or None

        st.subheader("Query Results — Single TP / Multi KPI")

        with st.spinner("Running query..."):
            df = query_data(
                root=STORE_ROOT,
                ne=ne,
                kpis=kpis,
                start=start,
                end=end,
                tp_contains=tp_selected,
            )

        if df.empty:
            st.warning("⚠ No data matched your filters.")
            st.stop()

        st.success(f"✅ Loaded {df.shape[0]:,} rows for NE={ne}, TP={tp_selected}")

        df_plot = df.reset_index().rename(columns={"index": "Time"})
        df_plot = regularize_time(df_plot, freq="15T")

        # Overlay mode: one chart with many KPIs
        if overlay:
            fig = px.line(
                df_plot,
                x="Time",
                y=kpis,
                title=f"Overlay KPIs for TP {tp_selected}",
                markers=True,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            # One chart per KPI
            for k in kpis:
                if k not in df_plot.columns:
                    continue
                fig = px.line(
                    df_plot,
                    x="Time",
                    y=k,
                    title=f"{k} over time — TP: {tp_selected}",
                    markers=True,
                    height=300,
                )
                st.plotly_chart(fig, use_container_width=True)

        with st.expander("Show Raw Data (first 500 rows)"):
            st.dataframe(df.head(500))


# ============================================================
#      MODE B: MULTIPLE OTS TPs, SINGLE KPI (NEW FLOW)
# ============================================================

else:
    # We restrict to OTS category only
    if "OTS" not in tp_categories:
        st.sidebar.warning("❗ This NE has no OTS-type TPs. Please pick another NE.")
        st.stop()

    tp_cat = "OTS"
    ots_tp_list = category_map.get("OTS", [])

    st.sidebar.markdown("**TP Category:** OTS")

    # Select-all option
    select_all_ots = st.sidebar.checkbox("Select all OTS TPs", value=True)

    if select_all_ots:
        selected_tps = ots_tp_list
    else:
        selected_tps = st.sidebar.multiselect(
            "Select OTS TPs",
            ots_tp_list,
            default=ots_tp_list[: min(3, len(ots_tp_list))],
            key="tp_multi",
        )

    if not selected_tps:
        st.sidebar.warning("❗ Please select at least one OTS TP.")
        st.stop()

    # KPI list from the first selected TP
    with st.spinner("Detecting KPIs from first TP..."):
        base_tp = selected_tps[0]
        kpi_list = get_kpi_list(STORE_ROOT, ne, base_tp)

    if not kpi_list:
        st.sidebar.warning("❗ No KPIs found for the selected OTS TP(s).")
        st.stop()

    kpi_single = st.sidebar.selectbox(
        "Select KPI (applied to all selected OTS TPs)",
        kpi_list,
        key="kpi_single",
    )

    start_str = st.sidebar.text_input("Start Time (YYYY-MM-DD HH:MM)", "", key="start_multi")
    end_str = st.sidebar.text_input("End Time (YYYY-MM-DD HH:MM)", "", key="end_multi")

    run_btn = st.sidebar.button("Run Query", key="run_multi")

    # -------- EXECUTION: MODE B --------
    if run_btn:
        start = start_str.strip() or None
        end = end_str.strip() or None

        st.subheader("Query Results — Multi OTS TPs / Single KPI")

        frames = []

        with st.spinner("Running queries per TP..."):
            for tp in selected_tps:
                df_tp = query_data(
                    root=STORE_ROOT,
                    ne=ne,
                    kpis=[kpi_single],
                    start=start,
                    end=end,
                    tp_contains=tp,
                )
                if df_tp.empty:
                    continue

                df_tp = df_tp.reset_index().rename(columns={"index": "Time"})
                # Keep only Time + KPI + real TP name
                df_tp = regularize_time(df_tp[["Time", kpi_single]], freq="15T")
                df_tp["TP"] = tp  # label for legend
                frames.append(df_tp)

        if not frames:
            st.warning("⚠ No data returned for selected TPs/KPI/time window.")
            st.stop()

        df_all = pd.concat(frames, ignore_index=True)

        st.success(
            f"✅ Loaded data for {len(frames)} OTS TPs "
            f"on NE={ne}, KPI={kpi_single}"
        )

        # Single chart: KPI vs Time, colored by TP
        fig = px.line(
            df_all,
            x="Time",
            y=kpi_single,
            color="TP",
            title=f"{kpi_single} over time — multiple OTS TPs on NE={ne}",
            markers=True,
        )
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Show Raw Data (first 500 rows)"):
            st.dataframe(df_all.head(500))


# ============================================================
#                        FOOTER MESSAGE
# ============================================================

st.markdown("---")
st.markdown(
    "**Usage:**\n"
    "- *Single TP, multiple KPIs*: classic view you already used.\n"
    "- *Multiple OTS TPs, single KPI*: compare one KPI across many OTS TPs for the same NE."
)
