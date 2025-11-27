#!/usr/bin/env python3
"""
generate_tp_metadata.py

Scan pm_store, discover distinct TPs per NE, classify them into:
- category (OTS, OTSI, OCH, OTU, ODU, OSC, ETH, AMP, OTHER)
- role (transponder, amplifier, osc, client, line, unknown)

Write per-NE JSON:
  pm_store/NE=<NEID>/tp_meta.json

Run:
  python generate_tp_metadata.py --root ./pm_store
"""

import os
import json
import argparse
from datetime import datetime

import pandas as pd


def classify_tp_category(tp: str) -> str:
    """
    Classify TP into a high-level category string.
    """
    tp_upper = str(tp).upper()

    # Priority order
    if "OTS" in tp_upper or "OTSI" in tp_upper:
        return "OTS"
    if "OCH" in tp_upper:
        return "OCH"
    if "OTU" in tp_upper:
        return "OTU"
    if "ODU" in tp_upper:
        return "ODU"
    if "OSC" in tp_upper:
        return "OSC"
    if "AMP" in tp_upper or "BOOST" in tp_upper:
        return "AMP"
    if "ETH" in tp_upper or "GE" in tp_upper or "100GBE" in tp_upper or "10GBE" in tp_upper:
        return "ETH"

    return "OTHER"


def classify_tp_role(tp: str, category: str) -> str:
    """
    Rough classification of TP into functional role:
      - transponder
      - amplifier
      - osc
      - client
      - line
      - unknown
    """
    tp_upper = str(tp).upper()

    if category == "OSC" or "OSC" in tp_upper:
        return "osc"
    if category == "AMP" or "AMP" in tp_upper or "BOOST" in tp_upper or "PREAMP" in tp_upper:
        return "amplifier"
    if category in {"OTS", "OCH", "OTU", "ODU"}:
        # Treat as line-side / transponder-facing
        return "transponder"
    if category == "ETH" or "GE" in tp_upper or "ETH" in tp_upper:
        return "client"

    # If label suggests line, trunk, span, fiber
    if any(tok in tp_upper for tok in ["LINE", "SPAN", "FIBER", "TRUNK"]):
        return "line"

    return "unknown"


def get_distinct_tps_for_ne(store_root: str, ne: str):
    """
    Return set of distinct TP strings for NE=<ne>.
    Uses DuckDB if available; falls back to pandas scanning.
    """
    ne_dir = os.path.join(store_root, f"NE={ne}")
    if not os.path.isdir(ne_dir):
        return set()

    parquet_glob = os.path.join(ne_dir, "*", "*.parquet")
    tps = set()

    # Try DuckDB fast path
    try:
        import duckdb
        q = f"SELECT DISTINCT TP FROM parquet_scan('{parquet_glob}') WHERE TP IS NOT NULL"
        df = duckdb.query(q).to_df()
        if not df.empty and "TP" in df.columns:
            tps = set(df["TP"].dropna().astype(str).unique())
            return tps
    except Exception:
        pass

    # Fallback: scan via pandas
    for root, _, files in os.walk(ne_dir):
        for f in files:
            if f.endswith(".parquet"):
                p = os.path.join(root, f)
                try:
                    tmp = pd.read_parquet(p, columns=["TP"], engine="pyarrow")
                    if "TP" in tmp.columns:
                        tps.update(tmp["TP"].dropna().astype(str).unique())
                except Exception:
                    continue

    return tps


def build_metadata_for_ne(store_root: str, ne: str) -> dict:
    """
    Build metadata dict for a single NE.
    """
    tps = get_distinct_tps_for_ne(store_root, ne)
    meta_tps = {}
    categories = {}
    roles = {}

    for tp in sorted(tps):
        cat = classify_tp_category(tp)
        role = classify_tp_role(tp, cat)

        meta_tps[tp] = {
            "category": cat,
            "role": role,
        }

        categories.setdefault(cat, []).append(tp)
        roles.setdefault(role, []).append(tp)

    return {
        "ne": ne,
        "root": os.path.join(store_root, f"NE={ne}"),
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "tps": meta_tps,
        "categories": categories,
        "roles": roles,
    }


def save_metadata(store_root: str, ne: str, meta: dict):
    ne_dir = os.path.join(store_root, f"NE={ne}")
    os.makedirs(ne_dir, exist_ok=True)
    out_path = os.path.join(ne_dir, "tp_meta.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, sort_keys=True)
    print(f"[INFO] Saved metadata for NE={ne} -> {out_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate TP metadata per NE")
    parser.add_argument(
        "--root",
        default="./pm_store",
        help="Root directory for pm_store (default: ./pm_store)",
    )
    args = parser.parse_args()

    store_root = args.root
    if not os.path.isdir(store_root):
        print(f"[ERROR] Store root not found: {store_root}")
        return

    # Discover NEs
    nes = sorted(
        p.split("=", 1)[1]
        for p in os.listdir(store_root)
        if p.startswith("NE=")
    )
    if not nes:
        print("[WARN] No NE=xxxx directories found.")
        return

    print(f"[INFO] Found NEs: {', '.join(nes)}")

    for ne in nes:
        print(f"[INFO] Building metadata for NE={ne} ...")
        meta = build_metadata_for_ne(store_root, ne)
        save_metadata(store_root, ne, meta)

    print("[INFO] Done. Metadata generated for all NEs.")


if __name__ == "__main__":
    main()
