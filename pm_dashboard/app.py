import streamlit as st
from loader import load_pm_data, list_nodes, list_tps
from plotter import plot_kpi_range, plot_basic_line
from config import KPI_MAP

st.set_page_config(page_title="PM Dashboard vNext", layout="wide")

st.title("📡 PM Telemetry Dashboard — ByteBaby vNext")

node = st.selectbox("Node", list_nodes())
tp = st.selectbox("TP / Interface", list_tps(node))

df = load_pm_data(node, tp)

if df is None or df.empty:
    st.warning("No PM data found.")
    st.stop()

kpi = st.selectbox("Select KPI", list(KPI_MAP.keys()))
kmap = KPI_MAP[kpi]

has_high = kmap["high"] in df.columns
has_low  = kmap["low"] in df.columns

if has_high and has_low:
    fig = plot_kpi_range(df, kpi, kmap["high"], kmap["low"])
else:
    fig = plot_basic_line(df, kpi)

st.plotly_chart(fig, use_container_width=True)


