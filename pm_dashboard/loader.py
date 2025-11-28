import polars as pl
import glob
import os

DATA_ROOT = "/workspaces/PM_ML/pm_data"

def list_nodes():
    nodes = [p.split("node=")[-1] for p in glob.glob(f"{DATA_ROOT}/node=*")]
    return sorted(nodes)

def list_tps(node):
    pattern = f"{DATA_ROOT}/node={node}/year=*/month=*/day=*/*.parquet"
    files = glob.glob(pattern)
    if not files:
        return []
    df = pl.scan_parquet(files).collect()
    return sorted(df["TP"].unique().to_list()) if "TP" in df.columns else []

def load_pm_data(node, tp):
    pattern = f"{DATA_ROOT}/node={node}/year=*/month=*/day=*/*.parquet"
    files = glob.glob(pattern)
    if not files:
        return None
    df = pl.scan_parquet(files).filter(pl.col("TP") == tp).collect()
    return df.to_pandas()


