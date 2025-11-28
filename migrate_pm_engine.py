import os
import shutil
import glob
import pandas as pd
import re
from datetime import datetime

# ------------------------------------------------------------
# CONFIG: You can modify these paths anytime
# ------------------------------------------------------------

RAW_PM_DATA_DIR = "PM_Files"             # Existing raw PM folder
TARGET_PM_DATA_DIR = "pm_data"              # New partitioned data folder
NEW_CODEBASE_FILE = "new_codebase.txt"      # File containing code modules
TARGET_CODEBASE_DIR = "pm_dashboard"        # Folder for generated codebase

SUPPORTED_EXT = [".csv", ".parquet"]        # Supported PM formats


# ------------------------------------------------------------
# UTILITIES
# ------------------------------------------------------------

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def extract_date(fname):
    """
    Extract YYYYMMDD from ANY filename segment.
    Works for filenames like:
      15mins_All_NE_20251106_103000.csv
      PM_AGRA01_20240718.csv
    """
    base = os.path.basename(fname)
    tokens = base.split("_")

    for t in tokens:
        if re.match(r"^\d{8}$", t):
            try:
                return datetime.strptime(t, "%Y%m%d")
            except:
                continue
    return None


def extract_node_from_csv(fname):
    """
    Extract NE name by reading only 1 row of CSV.
    Vendor-independent.
    Searches for common PM columns:
      NE, Node, NodeName, NetworkElement
    """
    try:
        df = pd.read_csv(fname, nrows=1)
        for col in ["NE", "Node", "NodeName", "NetworkElement"]:
            if col in df.columns:
                return str(df[col].iloc[0]).strip()
    except Exception as e:
        print(f"⚠ Could not read CSV for NE: {fname} — {e}")

    return "UNKNOWN"


def extract_node_from_parquet(fname):
    """Fallback for parquet if they have NE column."""
    try:
        df = pd.read_parquet(fname, columns=["NE"])
        return str(df["NE"].iloc[0]).strip()
    except:
        return "UNKNOWN"


# ------------------------------------------------------------
# PART 1: MIGRATE PM DATA → PARTITIONED STRUCTURE
# ------------------------------------------------------------

def migrate_pm_data():
    print("\n🚀 Migrating PM Files...")

    # Gather files
    files = []
    for ext in SUPPORTED_EXT:
        files.extend(glob.glob(os.path.join(RAW_PM_DATA_DIR, f"*{ext}")))

    print(f"Found {len(files)} raw PM files.")

    if not files:
        print("⚠ No PM files found. Migration aborted.")
        return

    for f in files:

        # Determine Node name
        if f.endswith(".csv"):
            node = extract_node_from_csv(f)
        elif f.endswith(".parquet"):
            node = extract_node_from_parquet(f)
        else:
            node = "UNKNOWN"

        # Extract date
        dt = extract_date(f)

        if not dt:
            print(f"⚠ Skipping: cannot extract date → {f}")
            continue

        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")

        # Build new folder
        new_dir = os.path.join(
            TARGET_PM_DATA_DIR,
            f"node={node}",
            f"year={year}",
            f"month={month}",
            f"day={day}"
        )
        ensure_dir(new_dir)

        # Copy file
        dest = os.path.join(new_dir, os.path.basename(f))
        shutil.copy2(f, dest)

        print(f"✔ {f} → {dest}")

    print("🎉 PM Data Migration Completed!")
    print("-" * 60)


# ------------------------------------------------------------
# PART 2: GENERATE NEW PM DASHBOARD CODEBASE
# ------------------------------------------------------------

def generate_codebase():
    print("\n🛠 Generating New PM Dashboard Codebase...")

    if not os.path.exists(NEW_CODEBASE_FILE):
        print(f"❌ Missing {NEW_CODEBASE_FILE}. Cannot generate modules.")
        return

    ensure_dir(TARGET_CODEBASE_DIR)

    with open(NEW_CODEBASE_FILE, "r", encoding="utf-8") as f:
        contents = f.read()

    # Split using module markers
    if "#MODULE:" in contents:
        blocks = contents.split("#MODULE:")
        for b in blocks[1:]:
            header, *body = b.split("\n", 1)
            module_name = header.strip()
            module_path = os.path.join(TARGET_CODEBASE_DIR, module_name)

            with open(module_path, "w", encoding="utf-8") as out:
                out.write(body[0])

            print(f"✔ Created module: {module_path}")

    else:
        # Write everything to a single file
        out_file = os.path.join(TARGET_CODEBASE_DIR, "generated_app.py")
        with open(out_file, "w", encoding="utf-8") as out:
            out.write(contents)
        print(f"✔ generated_app.py created")

    print("🎉 Code Generation Completed!")
    print("-" * 60)


# ------------------------------------------------------------
# MAIN ENTRY
# ------------------------------------------------------------

def main():
    print("\n============================================")
    print("BYTEBABY — PM_ML MIGRATION ENGINE vNext")
    print("============================================")

    migrate_pm_data()
    generate_codebase()

    print("\n✨ Migration Finished Successfully!")
    print("   New PM Dashboard Ready in pm_dashboard/")
    print("   Old tool remains untouched.")
    print("============================================\n")


if __name__ == "__main__":
    main()
