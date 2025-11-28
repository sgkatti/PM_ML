import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

def plot_kpi_range(df, kpi, high, low):
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["Time"], y=df[high],
        mode="lines",
        name=f"{kpi} High", line=dict(width=1.2)
    ))
    fig.add_trace(go.Scatter(
        x=df["Time"], y=df[low],
        mode="lines",
        name=f"{kpi} Low", line=dict(width=1.2)
    ))

    fig.add_trace(go.Scatter(
        x=pd.concat([df["Time"], df["Time"][::-1]]),
        y=pd.concat([df[high], df[low][::-1]]),
        fill="toself",
        fillcolor="rgba(0,150,255,0.15)",
        line=dict(color="rgba(0,0,0,0)"),
        hoverinfo="skip",
        name=f"{kpi} Range"
    ))

    fig.update_layout(title=f"{kpi} High–Low Range")
    return fig

def plot_basic_line(df, kpi):
    return px.line(df, x="Time", y=kpi, title=f"{kpi} over Time")


