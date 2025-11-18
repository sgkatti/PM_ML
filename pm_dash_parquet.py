#!/usr/bin/env python3
"""
pm_dashboard_parquet_tk.py

Tkinter UI that queries pre-ingested parquet store (NE→date).
Relies on pm_query.query_data to fetch data quickly (DuckDB recommended).

Usage:
    python pm_dashboard_parquet_tk.py
"""
import os
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
from datetime import datetime
import matplotlib
matplotlib.use("TkAgg")
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

# import the query module we created
from pm_query import query_data

# You can keep debug in GUI as before
DEBUG = False
def log(msg):
    if DEBUG:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

class Dashboard:
    def __init__(self, root):
        self.root = root
        self.root.title("PM Dashboard (Parquet + DuckDB)")
        self.store_root = os.path.join(os.getcwd(), "pm_store")  # default
        self.kpi_cache = []     # we will populate by scanning store
        self.ne_cache = []      # list of NE partitions detected

        self._build_ui()
        self._build_plot()

    def _build_ui(self):
        left = ttk.Frame(self.root, padding=8)
        left.grid(row=0, column=0, sticky='ns')
        right = ttk.Frame(self.root, padding=8)
        right.grid(row=0, column=1, sticky='nsew')
        self.root.columnconfigure(1, weight=1)
        self.root.rowconfigure(0, weight=1)

        btn_frame = ttk.Frame(left)
        btn_frame.pack(fill='x')
        ttk.Button(btn_frame, text="Choose CSV folder (ingest option)", command=self.choose_folder).pack(side='left')
        ttk.Button(btn_frame, text="Scan Parquet Store", command=self.scan_store).pack(side='left')
        ttk.Button(btn_frame, text="Query & Plot", command=self.run_query).pack(side='left')

        # store path label
        ttk.Label(left, text="Parquet store root:").pack(anchor='w', pady=(8,0))
        self.store_lbl = ttk.Label(left, text=self.store_root)
        self.store_lbl.pack(anchor='w')

        # NE
        ttk.Label(left, text="NE:").pack(anchor='w', pady=(6,0))
        self.ne_cb = ttk.Combobox(left, values=["(All)"], state='readonly')
        self.ne_cb.pack(fill='x')

        # KPI selection
        ttk.Label(left, text="KPIs (comma separated):").pack(anchor='w', pady=(6,0))
        self.kpi_entry = ttk.Entry(left)
        self.kpi_entry.pack(fill='x')
        self.kpi_entry.insert(0, "QFACTOR-AVG,PREFEC-AVG")

        # TP filter
        ttk.Label(left, text="TP contains (optional):").pack(anchor='w', pady=(6,0))
        self.tp_entry = ttk.Entry(left)
        self.tp_entry.pack(fill='x')

        # time range
        ttk.Label(left, text="Start (YYYY-MM-DD HH:MM) optional").pack(anchor='w', pady=(6,0))
        self.start_entry = ttk.Entry(left)
        self.start_entry.pack(fill='x')
        ttk.Label(left, text="End (YYYY-MM-DD HH:MM) optional").pack(anchor='w', pady=(6,0))
        self.end_entry = ttk.Entry(left)
        self.end_entry.pack(fill='x')

        # debug toggle
        self.debug_var = tk.BooleanVar()
        ttk.Checkbutton(left, text="Debug logs", variable=self.debug_var, command=self.toggle_debug).pack(anchor='w', pady=(6,0))

    def _build_plot(self):
        self.fig = Figure(figsize=(9,6))
        self.ax = self.fig.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.root)
        self.canvas.get_tk_widget().grid(row=0, column=1, sticky='nsew')

    def toggle_debug(self):
        global DEBUG
        DEBUG = self.debug_var.get()
        log(f"Debug set {DEBUG}")

    def choose_folder(self):
        folder = filedialog.askdirectory()
        if not folder:
            return
        # offer to run ingest
        resp = messagebox.askyesno("Ingest?", f"Run ingestion on folder:\n{folder} ?\n(This writes parquet into ./pm_store/NE=... folders)")
        if resp:
            import subprocess, sys
            cmd = [sys.executable, "pm_ingest.py", "--src", folder, "--out", self.store_root]
            messagebox.showinfo("Ingest starting", "This may take time. Check console output.")
            log(f"Running ingest command: {' '.join(cmd)}")
            subprocess.run(cmd)
            messagebox.showinfo("Ingest", "Ingest finished (check console logs).")
            self.scan_store()

    def scan_store(self):
        root = self.store_root
        if not os.path.isdir(root):
            messagebox.showwarning("Store not found", f"Parquet store not found at {root}")
            return
        # list NE folders
        nes = []
        for p in os.listdir(root):
            if p.startswith("NE="):
                nes.append(p.split("=",1)[1])
        nes = sorted(nes)
        self.ne_cb['values'] = ["(All)"] + nes
        if nes:
            self.ne_cb.current(0)
        self.ne_cache = nes
        # Find kpi candidates by scanning a few parquet files (fast)
        sample_cols = set()
        for rootdir, dirs, files in os.walk(root):
            for f in files:
                if f.endswith(".parquet"):
                    try:
                        df = pd.read_parquet(os.path.join(rootdir,f), engine='pyarrow', columns=None)
                        sample_cols.update([c for c in df.columns if c not in ('Time','NE','TP')])
                        if len(sample_cols) > 200:
                            break
                    except Exception:
                        continue
            if len(sample_cols) > 200:
                break
        self.kpi_cache = sorted(sample_cols)
        log(f"Scan complete: {len(nes)} NEs, sample KPIs: {len(self.kpi_cache)}")
        messagebox.showinfo("Scan", f"Detected {len(nes)} NE partitions. Sample KPIs cached.")

    def run_query(self):
        ne = self.ne_cb.get()
        if ne in ("", "(All)"):
            ne = None
        kpi_text = self.kpi_entry.get().strip()
        if not kpi_text:
            messagebox.showwarning("KPIs", "Enter at least one KPI")
            return
        kpis = [k.strip() for k in kpi_text.split(",") if k.strip()]
        tp = self.tp_entry.get().strip() or None
        start = self.start_entry.get().strip() or None
        end = self.end_entry.get().strip() or None
        if ne is None:
            messagebox.showwarning("NE required", "Please select a specific NE (partitioned store requires NE).")
            return
        log(f"Querying NE={ne} KPIs={kpis} TP={tp} start={start} end={end}")
        df = query_data(root=self.store_root, ne=ne, kpis=kpis, start=start, end=end, tp_contains=tp)
        if df.empty:
            messagebox.showinfo("No results", "No rows matched query.")
            return
        # plot first few KPIs
        self.ax.clear()
        for k in kpis:
            if k in df.columns:
                s = df[k].dropna()
                if not s.empty:
                    self.ax.plot(s.index, s.values, label=k, marker='o', markersize=3, linewidth=1)
        self.ax.legend(loc='upper left', bbox_to_anchor=(1.02,1))
        self.ax.grid(True)
        self.fig.autofmt_xdate()
        self.canvas.draw()
        messagebox.showinfo("Done", f"Loaded {df.shape[0]} rows, plotted KPIs.")

if __name__ == "__main__":
    root = tk.Tk()
    root.geometry("1200x800")
    app = Dashboard(root)
    root.mainloop()
