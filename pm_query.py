#!/usr/bin/env python3
"""
pm_query.py

Provides query_data(...) that returns a pandas DataFrame by reading
only required parquet partitions using DuckDB (fast) or pandas fallback.

Usage example:
    from pm_query import query_data
    df = query_data(root="./pm_store", ne="UEUNNORLIC0JCNA08801",
                    kpis=["QFACTOR-AVG","PREFEC-AVG"],
                    start="2025-06-10 00:00", end="2025-06-11 23:59",
                    tp_contains="OCH")
"""
import os
from pathlib import Path
import pandas as pd
import numpy as np

try:
    import duckdb
    _duckdb_available = True
except Exception:
    _duckdb_available = False

def _glob_parquet_paths(root, ne):
    base = Path(root) / f"NE={ne}"
    if not base.exists():
        return []
    # gather all parquet files recursively under NE=... folder
    return [str(p) for p in base.rglob("*.parquet")]

def query_data(root="./pm_store", ne=None, kpis=None, start=None, end=None, tp_contains=None, max_rows=0):
    """
    Query pre-ingested parquet store.
    - root: root folder containing NE=... partitions
    - ne: required (string)
    - kpis: list of KPI column names (strings)
    - start/end: datelike or None
    - tp_contains: substring filter for TP column
    - max_rows: if >0, return only last max_rows
    Returns pandas DataFrame with Time index and requested columns + NE,TP.
    """
    if ne is None:
        raise ValueError("NE must be specified for partitioned store queries (NE=...)")

    if kpis is None or len(kpis) == 0:
        raise ValueError("At least one KPI required")

    # Find parquet files for this NE
    paths = _glob_parquet_paths(root, ne)
    if not paths:
        return pd.DataFrame()

    # DuckDB path (fast)
    if _duckdb_available:
        # Build query
        cols = ["Time", "NE", "TP"] + kpis
        cols_quoted = ", ".join([f'"{c}"' for c in cols])
        # Create a table from the parquet files (duckdb supports reading a list)
        # We'll build a UNION all approach: duckdb can read a list via parquet_scan('file1','file2',...)
        files_list = ", ".join([f"'{p}'" for p in paths])
        sql = f"SELECT {cols_quoted} FROM parquet_scan([{files_list}])"
        where_clauses = []
        if start:
            where_clauses.append(f"Time >= TIMESTAMP '{pd.to_datetime(start)}'")
        if end:
            where_clauses.append(f"Time <= TIMESTAMP '{pd.to_datetime(end)}'")
        if tp_contains:
            where_clauses.append(f"TP LIKE '%{tp_contains}%'")
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        # Final ordering
        sql += " ORDER BY Time ASC"

        # Execute
        con = duckdb.connect()
        try:
            logdf = con.execute(sql).df()
        finally:
            con.close()
        if logdf.empty:
            return pd.DataFrame()
        # Ensure Time is parsed and set index
        if "Time" in logdf.columns:
            logdf["Time"] = pd.to_datetime(logdf["Time"], errors='coerce')
            logdf = logdf.dropna(subset=["Time"]).set_index("Time")
        # coerce KPI cols to numeric
        for k in kpis:
            if k in logdf.columns:
                logdf[k] = pd.to_numeric(logdf[k], errors='coerce')
        if max_rows and max_rows>0 and logdf.shape[0] > max_rows:
            logdf = logdf.tail(max_rows)
        return logdf

    # Fallback: pandas read_parquet on partitions (slower)
    frames = []
    for p in paths:
        try:
            df = pd.read_parquet(p)
            # keep only needed columns
            want = [c for c in ["Time","NE","TP"] + kpis if c in df.columns]
            if not want:
                continue
            df = df[want]
            df["Time"] = pd.to_datetime(df["Time"], errors='coerce')
            df = df.dropna(subset=["Time"]).set_index("Time")
            if tp_contains:
                df = df[df["TP"].astype(str).str.contains(tp_contains, na=False, case=False)]
            if start:
                df = df[df.index >= pd.to_datetime(start)]
            if end:
                df = df[df.index <= pd.to_datetime(end)]
            frames.append(df)
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames).sort_index()
    for k in kpis:
        if k in combined.columns:
            combined[k] = pd.to_numeric(combined[k], errors='coerce')
    if max_rows and combined.shape[0] > max_rows:
        combined = combined.tail(max_rows)
    return combined
