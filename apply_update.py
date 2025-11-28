import argparse

def load_updates(update_file):
    """
    Reads update rules from update.txt.
    Rules must be separated by blank lines:

    FIND: something
    REPLACE: something
    """
    rules = []
    find = None
    replace = None

    with open(update_file, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.rstrip("\n")

            if stripped.startswith("FIND:"):
                find = stripped.replace("FIND:", "").strip()

            elif stripped.startswith("REPLACE:"):
                replace = stripped.replace("REPLACE:", "").strip()

            elif stripped.strip() == "" and find and replace:
                rules.append((find, replace))
                find, replace = None, None

        # Catch last block if no blank line
        if find and replace:
            rules.append((find, replace))

    return rules


def apply_updates(original_code, rules):
    for find_text, repl_text in rules:
        if find_text in original_code:
            original_code = original_code.replace(find_text, repl_text)
            print(f"[OK] Updated: {find_text[:40]}...")
        else:
            print(f"[WARN] FIND text not found:\n  {find_text}\n")

    return original_code


def main():
    parser = argparse.ArgumentParser(description="Auto-update Python script using update definitions")
    parser.add_argument("script", help="Python file to patch (e.g., pm_dashboard_streamlit_v3_1.py)")
    parser.add_argument("updates", help="Update instruction file (e.g., update.txt)")
    parser.add_argument("--output", default="pm_dashboard_streamlit_v3_1_patched.py",
                        help="Output file name")

    args = parser.parse_args()

    # Load original script
    with open(args.script, "r", encoding="utf-8") as f:
        original_code = f.read()

    # Load rules
    rules = load_updates(args.updates)
    print(f"Loaded {len(rules)} update rule(s)")

    # Apply updates
    updated_code = apply_updates(original_code, rules)

    # Save new file
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(updated_code)

    print(f"\n✔ Patch complete → {args.output}")


if __name__ == "__main__":
    main()
