import os
import shutil
import glob
import pandas as pd
from datetime import datetime

# ------------------------------------------------------------
# CONFIG — You can modify these names/folders anytime
# ------------------------------------------------------------

RAW_PM_DATA_DIR = "pm_raw_data"             # Folder where old PM files are
TARGET_PM_DATA_DIR = "pm_data"              # New folder where partitioned data goes
NEW_CODEBASE_FILE = "new_codebase.txt"      # File containing new modules
TARGET_CODEBASE_DIR = "pm_dashboard"        # Folder where new modules will be created

SUPPORTED_EXT = [".parquet", ".csv"]        # Allowed PM data formats


# ------------------------------------------------------------
# UTILS
# ------------------------------------------------------------

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def extract_node(fname):
    """Extract node name from filename BEFORE first underscore."""
    base = os.path.basename(fname)
    return base.split("_")[0]

def extract_date(fname):
    """Extract date YYYYMMDD from filename AFTER underscore."""
    base = os.path.basename(fname)
    try:
        parts = base.split("_")
        date_str = parts[1].split(".")[0]
        dt = datetime.strptime(date_str, "%Y%m%d")
        return dt
    except:
        return None


# ------------------------------------------------------------
# PART 1: PM DATA MIGRATION
# ------------------------------------------------------------

def migrate_pm_data():
    print("\n🚀 Migrating PM Files...")

    # Collect raw files
    files = []
    for ext in SUPPORTED_EXT:
        files.extend(glob.glob(f"{RAW_PM_DATA_DIR}/*{ext}"))

    print(f"Found {len(files)} raw PM files.")

    if not files:
        print("⚠ No files found. Migration skipped.")
        return

    for f in files:
        node = extract_node(f)
        dt = extract_date(f)

        if not dt:
            print(f"⚠ Skipping: cannot extract date → {f}")
            continue

        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")

        # Build partitioned folder path
        new_dir = os.path.join(
            TARGET_PM_DATA_DIR,
            f"node={node}",
            f"year={year}",
            f"month={month}",
            f"day={day}"
        )
        ensure_dir(new_dir)

        # Copy file into partition
        dest = os.path.join(new_dir, os.path.basename(f))
        shutil.copy2(f, dest)

        print(f"✔ {f} → {dest}")

    print("🎉 PM Data Migration Completed!")


# ------------------------------------------------------------
# PART 2: CODE GENERATION FROM new_codebase.txt
# ------------------------------------------------------------

def generate_codebase():
    print("\n🛠 Generating New PM Dashboard Codebase...")

    if not os.path.exists(NEW_CODEBASE_FILE):
        print(f"❌ new_codebase.txt not found in current folder!")
        return

    ensure_dir(TARGET_CODEBASE_DIR)

    with open(NEW_CODEBASE_FILE, "r", encoding="utf-8") as f:
        contents = f.read()

    # Check for modular split markers
    if "#MODULE:" in contents:
        segments = contents.split("#MODULE:")
        for block in segments[1:]:
            header, *body = block.split("\n", 1)
            module_name = header.strip()
            module_path = os.path.join(TARGET_CODEBASE_DIR, module_name)

            with open(module_path, "w", encoding="utf-8") as out:
                out.write(body[0])

            print(f"✔ Generated module: {module_path}")

    else:
        # Fallback: write as single file
        out_file = os.path.join(TARGET_CODEBASE_DIR, "generated_app.py")
        with open(out_file, "w", encoding="utf-8") as out:
            out.write(contents)
        print(f"✔ New dashboard code generated: {out_file}")

    print("🎉 Code Generation Completed!")


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

def main():
    print("\n============================================")
    print("BYTEBABY — PM_ML MIGRATION ENGINE vNext")
    print("============================================")

    migrate_pm_data()
    generate_codebase()

    print("\n✨ Migration Complete. New PM Dashboard Ready!")
    print("   Old tool remains untouched.")
    print("============================================\n")


if __name__ == "__main__":
    main()
