#!/usr/bin/env python3
"""
pm_ingest.py

Chunked CSV -> Parquet ingestion.
Partitioning: pm_store/NE=<NE>/date=<YYYY-MM-DD>/part-<uuid>.parquet

Usage:
    python pm_ingest.py --src "C:\path\to\csv_folder" [--out "./pm_store"] [--chunksize 50000]

Notes:
- Writes many small parquet parts per NE/date (DuckDB will read them fast).
- Does minimal memory usage; chunked CSV reads.
"""
import os
import sys
import argparse
import uuid
import time
from pathlib import Path
import pandas as pd
import numpy as np




import time
import math

def log_progress(prefix, current, total, start_time):
    """Prints progress with percentage + ETA."""
    elapsed = time.time() - start_time
    pct = (current / total) * 100

    # avoid division by zero
    if pct > 0:
        remaining = (elapsed / pct) * (100 - pct)
    else:
        remaining = 0

    # Format time
    def fmt(sec):
        if sec < 60:
            return f"{int(sec)}s"
        return f"{int(sec//60)}m {int(sec%60)}s"

    eta = fmt(remaining)
    elapsed_s = fmt(elapsed)

    print(f"{prefix} [{current:3}/{total:3}] {pct:5.1f}% | elapsed: {elapsed_s} | ETA: {eta}", end="\r")


def log_chunk(file_name, chunk_id, rows_in, rows_written):
    """Print useful details per chunk."""
    print(f"\n   ➤ {file_name} | chunk {chunk_id}: read {rows_in:,} rows → wrote {rows_written:,} rows")


# prefer pyarrow engine
PARQUET_ENGINE = "pyarrow"

SAMPLE_ROWS = 200

def clean_time(s):
    if pd.isna(s):
        return s
    s = str(s)
    # remove (GMT) etc.
    import re
    s = re.sub(r'\(.*?\)', '', s)
    s = re.sub(r'\[.*?\]', '', s)
    s = s.replace('/', '-')
    s = re.sub(r'(\d)\.(\d)\.(\d)', r'\1:\2:\3', s)
    return s.strip()

def make_output_path(base_out, ne, date_str):
    safe_ne = str(ne).replace('/', '_').replace('\\','_')
    p = Path(base_out) / f"NE={safe_ne}" / f"date={date_str}"
    p.mkdir(parents=True, exist_ok=True)
    return p

def ingest_folder(src_folder, out_folder="./pm_store", chunksize=50000, verbose=True):
    t0 = time.time()
    src = Path(src_folder)
    out = Path(out_folder)
    out.mkdir(parents=True, exist_ok=True)

    csv_files = sorted([p for p in src.iterdir() if p.is_file() and p.suffix.lower()=='.csv'])
    if verbose:
        print(f"[INGEST] Found {len(csv_files)} CSV files in {src_folder}")

    processed_files = 0
    for i, csv in enumerate(csv_files, 1):
        try:
            if verbose:
                print(f"[INGEST] ({i}/{len(csv_files)}) Scanning: {csv.name}")
            # read in chunks
            for chunk in pd.read_csv(csv, chunksize=chunksize, low_memory=False):
                # normalize column names
                chunk.columns = [str(c).strip() for c in chunk.columns]

                # find time column (case-insensitive)
                time_cols = [c for c in chunk.columns if c.strip().lower() == 'time']
                if not time_cols:
                    # skip chunk if no time column
                    continue
                time_col = time_cols[0]

                # clean times and derive date column
                chunk[time_col] = chunk[time_col].apply(clean_time)
                chunk[time_col] = pd.to_datetime(chunk[time_col], errors='coerce')
                chunk = chunk.dropna(subset=[time_col])
                if chunk.empty:
                    continue

                # force NE column to string if exists; else assign unknown
                if 'NE' in chunk.columns:
                    chunk['NE'] = chunk['NE'].astype(str)
                else:
                    chunk['NE'] = 'UNKNOWN_NE'

                # derive date string for partition
                chunk['__date'] = chunk[time_col].dt.strftime('%Y-%m-%d')

                # Replace 'NS' with NaN globally and coerce numeric columns later if needed
                chunk = chunk.replace({'NS': pd.NA, '': pd.NA, 'NA': pd.NA})

                # For each NE/date group in this chunk, write a parquet part
                group_cols = ['NE', '__date']
                for (ne, date_str), df_grp in chunk.groupby(group_cols):
                    if df_grp.empty:
                        continue
                    outp = make_output_path(out, ne, date_str)
                    part_name = f"part-{uuid.uuid4().hex}.parquet"
                    dest = outp / part_name
                    # write parquet (pyarrow)
                    try:
                        df_grp.to_parquet(dest, engine=PARQUET_ENGINE, index=False)
                        if verbose:
                            print(f"[INGEST] Wrote {dest} [{len(df_grp)} rows]")
                    except Exception as e:
                        print(f"[INGEST][ERROR] Failed to write {dest}: {e}")
                        # attempt fallback to fastparquet if available
                        try:
                            df_grp.to_parquet(dest, engine='fastparquet', index=False)
                        except Exception as e2:
                            print(f"[INGEST][ERROR] fastparquet also failed: {e2}")
                            continue

            processed_files += 1
        except Exception as e:
            print(f"[INGEST] Skipping file {csv.name} due to error: {e}")
            continue

    t = time.time() - t0
    print(f"[INGEST] Completed. Files processed: {processed_files}/{len(csv_files)} in {t:.2f}s")
    print(f"[INGEST] Parquet store at: {out.resolve()}")


def main():
    parser = argparse.ArgumentParser(description="Ingest CSV PM dumps into partitioned parquet store.")
    parser.add_argument("--src", required=True, help="Source folder containing CSVs")
    parser.add_argument("--out", default="./pm_store", help="Output parquet root folder")
    parser.add_argument("--chunksize", type=int, default=50000, help="CSV read chunksize")
    parser.add_argument("--quiet", action="store_true", help="Minimal logs")
    args = parser.parse_args()

    ingest_folder(args.src, args.out, args.chunksize, verbose=(not args.quiet))

if __name__ == "__main__":
    main()
