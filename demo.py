import sys
import json
import io
from pathlib import Path
import shutil
from contextlib import redirect_stdout, redirect_stderr

# Import the pipeline logic from run.py to ensure identical execution
try:
    from run import run_pipeline
except ImportError:
    print("Error: Could not import run_pipeline from run.py. Make sure run.py is in the same directory.")
    sys.exit(1)

# Configuration: Expected outcomes for each scenario
EXPECTED = {
    "s01":                        "AUTO_POST",
    "s02":                        "HOLD_FOR_APPROVAL",
    "s03_qty_variance":           "HOLD_FOR_APPROVAL",
    "s04_price_variance":         "HOLD_FOR_APPROVAL",
    "s05_bad_total":              "HOLD_FOR_APPROVAL",
    "s06_duplicate":              "BLOCK",
    "s07_credit_note":            "ROUTE_APPROVAL",
    "s08_multi_currency":         "ROUTE_APPROVAL",
    "s09_tax_rate_mismatch":      "HOLD_FOR_APPROVAL",
    "s10_new_vendor":             "ROUTE_TO_DEPT_HEAD",
    "s11_bank_change_high_value": "ESCALATE_TO_FINANCE_APPROVER",
    "s12_low_ocr_confidence":     "ROUTE_TO_MANUAL_REVIEW",
    "s13_no_po_invoice":          "ROUTE_TO_DEPT_HEAD",
    "s14_split_deliveries":       "AUTO_POST",
    "s15_clean_small_invoice":    "AUTO_POST",
}

# Map requirements keys to actual folder names if they differ
# Based on file structure provided in context
FOLDER_MAP = {
    "s02_no_grn": "s02",
    # Add others if folder names don't match exactly.
    # Assuming keys in EXPECTED match directory names for now, except s02.
}


def get_bundle_path(scenario_key):
    folder_name = FOLDER_MAP.get(scenario_key, scenario_key)
    return Path("input_bundles") / folder_name


def run_scenario(scenario_key, expected_action):
    bundle_path = get_bundle_path(scenario_key)

    if not bundle_path.exists():
        return "MISSING_BUNDLE", f"Bundle {bundle_path} not found"

    # Capture stdout/stderr to keep the demo output clean
    capture_out = io.StringIO()
    capture_err = io.StringIO()

    try:
        with redirect_stdout(capture_out), redirect_stderr(capture_err):
            # 1. Run the full pipeline
            # This will create a new run directory in runs/
            # We need to find the latest run dir created to read the result

            # Note: run_pipeline() prints "Run ID: <id>" to stdout.
            # We can capture that or just look for the newest folder in runs/

            run_pipeline(str(bundle_path))

        # 2. Find the artifact (posting_payload.json)
        # Strategy: Look for the most recently modified folder in runs/
        # that matches the scenario name
        runs_root = Path("runs")
        candidates = list(runs_root.glob(f"{bundle_path.name}_*"))
        if not candidates:
            return "ERROR", "No run directory created"

        # Sort by modification time, newest first
        latest_run = max(candidates, key=lambda p: p.stat().st_mtime)
        payload_path = latest_run / "posting_payload.json"

        if not payload_path.exists():
            return "ERROR", "posting_payload.json missing"

        # 3. Read the actual action
        data = json.loads(payload_path.read_text(encoding="utf-8"))
        actual_action = data.get("action", "UNKNOWN")

        return actual_action, None

    except SystemExit as e:
        return "CRASH", f"Pipeline exited with code {e.code}\nLogs:\n{capture_out.getvalue()}\n{capture_err.getvalue()}"
    except Exception as e:
        return "CRASH", str(e)


def print_row(scenario, expected, actual, status):
    # Truncate strings for table formatting
    scen_str = scenario[:30].ljust(30)
    exp_str = expected[:25].ljust(25)
    act_str = str(actual)[:25].ljust(25)
    print(f"{scen_str} {exp_str} {act_str} {status}")


def main():

    runs_path = Path("runs")
    runs_path.mkdir(exist_ok=True)

    print("\nIIPS Demo Results")
    print("═" * 75)
    print(f"{'Scenario'.ljust(30)} {'Expected'.ljust(25)} {'Actual'.ljust(25)} PASS/FAIL")
    print("─" * 75)

    passed_count = 0
    total_count = len(EXPECTED)
    failed_details = []

    for scenario_key, expected_action in EXPECTED.items():
        actual_action, error_msg = run_scenario(scenario_key, expected_action)

        if actual_action == expected_action:
            status = "PASS"
            passed_count += 1
        else:
            status = "FAIL"
            failed_details.append((scenario_key, expected_action, actual_action, error_msg))

        print_row(scenario_key, expected_action, actual_action, status)

    print("═" * 75)
    print(f"Result: {passed_count}/{total_count} passed")

    if failed_details:
        print("\n--- Failure Details ---")
        for key, exp, act, err in failed_details:
            print(f"\nScenario: {key}")
            print(f"  Expected: {exp}")
            print(f"  Actual:   {act}")
            if err:
                print(f"  Error:    {err}")
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()