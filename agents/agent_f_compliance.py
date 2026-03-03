import argparse
import json
import re
from pathlib import Path
import yaml

# --- Helpers ---

def read_json(path: Path):
    """Safely reads a JSON file."""
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))

def write_json(path: Path, payload):
    """Writes data to a JSON file, creating parents if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=4, ensure_ascii=False) + "\n", encoding="utf-8")

def read_yaml(path: Path):
    """Safely reads a YAML file."""
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}

# --- Logic Functions ---

def get_vendor_country(vendor_id: str, vendor_master: list):
    """Determines country code (US, GB, DE) based on Vendor Master address."""
    if not vendor_id:
        return None, None

    # Find the specific vendor in the master list
    vendor_record = next((v for v in vendor_master if v.get("vendor_id") == vendor_id), None)
    if not vendor_record:
        return None, None

    # Get the address and normalize it to uppercase
    address = vendor_record.get("address", "").upper()

    # Simple text matching to guess the country
    if "USA" in address or address.endswith(" US"):
        return "US", vendor_record
    if "UK" in address or "GB" in address or address.endswith(" GB"):
        return "GB", vendor_record
    if "GERMANY" in address or "DEUTSCHLAND" in address or address.endswith(" DE"):
        return "DE", vendor_record

    return "UNKNOWN", vendor_record

def validate_tax_id_format(tax_id: str, country_code: str) -> bool:
    """Checks if a Tax ID matches the strict pattern for that country."""
    if not tax_id:
        return False

    # Define the strict rules for each country
    patterns = {
        "US": r"^US-\d{2}-\d{7}$",    # Example: US-12-1234567
        "GB": r"^GB\d{9}$",           # Example: GB123456789
        "DE": r"^DE\d{9}$"            # Example: DE123456789
    }

    # Get the rule for identified country
    pattern = patterns.get(country_code)

    if pattern:
        return bool(re.match(pattern, tax_id))

    # If country unknown, we assume True (can't fail what we don't know)
    return True

# --- Main Execution ---

def run_agent_f(args):
    run_dir = Path(args.run_dir).resolve()

    # Input files
    extracted_path = run_dir / "extracted_invoice.json"
    vendor_res_path = run_dir / "vendor_resolution_result.json"
    context_path = run_dir / "context_packet.json"  # Fixed name

    # Config files
    tax_rules_path = run_dir / "tax_rules.yaml"
    vendor_master_path = run_dir / "vendor_master.json"
    policy_path = run_dir / "approval_policy.yaml"

    print(f"[Agent F] Starting Tax Compliance. Run: {run_dir.name}")

    # Load into memory
    extracted_data = read_json(extracted_path)
    resolution_data = read_json(vendor_res_path)
    vendor_master = read_json(vendor_master_path)
    tax_rules = read_yaml(tax_rules_path)
    policy = read_yaml(policy_path)

    findings = []
    validation_result = {
        "status": "PASS",
        "details": {}
    }

    # 3. Get Vendor Info (Result from Agent C)
    vendor_id = resolution_data.get("matched_vendor_id")
    if not vendor_id:
        print("   -> Skipping: No matched vendor found.")
        validation_result["status"] = "SKIPPED"
        write_json(run_dir / "tax_validation_result.json", validation_result)
        return

    # 4. Determine Jurisdiction
    country_code, vendor_record = get_vendor_country(vendor_id, vendor_master)
    print(f"   -> Vendor Jurisdiction: {country_code}")

    if not country_code or country_code == "UNKNOWN":
        findings.append({
            "agent": "F",
            "code": "UNKNOWN_JURISDICTION",
            "severity": "MEDIUM",
            "message": f"Could not determine jurisdiction for vendor {vendor_id}",
            "evidence": {"address": vendor_record.get("address", "N/A")}
        })
        validation_result["status"] = "FLAGGED"

    # 5. Validate Tax ID Format
    master_tax_id = vendor_record.get("tax_id") if vendor_record else None

    if master_tax_id and country_code in ["US", "GB", "DE"]:
        if not validate_tax_id_format(master_tax_id, country_code):
            findings.append({
                "agent": "F",
                "code": "INVALID_TAX_ID",
                "severity": "MEDIUM",
                "message": f"Tax ID {master_tax_id} format invalid for {country_code}",
                "evidence": {"tax_id": master_tax_id, "expected_format": country_code}
            })
            validation_result["status"] = "FLAGGED"

    # 6. Validate Tax Rates
    header = extracted_data.get("header", {})

    # Handle numbers safely
    try:
        total_amount = float(header.get("total_amount", 0) or 0)
        tax_amount = float(header.get("tax_amount", 0) or 0)
    except (ValueError, TypeError):
        total_amount = 0.0
        tax_amount = 0.0

    # Calculate Subtotal (assuming Total = Subtotal + Tax)
    subtotal = float(header.get("subtotal", 0) or 0)
    if subtotal == 0 and total_amount > 0:
        subtotal = total_amount - tax_amount

    # MATH LOGIC
    if subtotal > 0:
        actual_rate = tax_amount / subtotal

        # Find expected rate in our YAML rules
        rule = next((r for r in tax_rules if r['country_code'] == country_code), None)

        if rule:
            expected_rate = float(rule.get('standard_rate', 0))
            reduced_rate = float(rule.get('reduced_rate', 0))

            # Get allowed difference from policy (default 0.05)
            tolerance = float(policy.get('tax_rules', {}).get('allowable_tax_diff', 0.05))

            diff_std = abs(actual_rate - expected_rate)
            diff_red = abs(actual_rate - reduced_rate)

            # Check if it matches EITHER standard OR reduced rate
            match_std = diff_std <= tolerance
            match_red = diff_red <= tolerance

            print(f"   -> Tax Check: Actual {actual_rate:.2%} vs Expected {expected_rate:.2%} (Diff: {diff_std:.4f})")

            if not match_std and not match_red:
                # MAJOR ERROR FOUND
                findings.append({
                    "agent": "F",
                    "code": "TAX_RATE_MISMATCH",
                    "severity": "HIGH",
                    "message": f"Tax rate {actual_rate:.1%} does not match standard ({expected_rate:.1%}) or reduced ({reduced_rate:.1%}) for {country_code}",
                    "evidence": {
                        "subtotal": subtotal,
                        "tax_amount": tax_amount,
                        "actual_rate": round(actual_rate, 4),
                        "expected_rate": expected_rate
                    }
                })
                validation_result["status"] = "FAIL"

    # 7. Write Outputs

    # A. Save specific Tax Result
    validation_result["details"] = {
        "country": country_code,
        "tax_id_valid": True if not any(f['code'] == 'INVALID_TAX_ID' for f in findings) else False
    }
    write_json(run_dir / "tax_validation_result.json", validation_result)

    # B. Append to master findings list
    findings_path = run_dir / "findings.json"
    existing_findings = read_json(findings_path)

    if isinstance(existing_findings, list):
        existing_findings.extend(findings)
    else:
        existing_findings = findings

    if findings:
        write_json(findings_path, existing_findings)

    # C. Update Context Packet (The "Case File")
    context = read_json(context_path)
    context["tax_validation"] = validation_result
    write_json(context_path, context)

    print(f"[Agent F] Complete. Findings: {len(findings)}")


if __name__ == "__main__":
    # Auto-detect latest run if no args provided (Development Helper)
    default_run_dir = None
    try:
        base_dir = Path(__file__).resolve().parent.parent
        runs_dir = base_dir / "runs"
        if runs_dir.exists():
            # Sort by creation time to find the newest
            all_runs = sorted([d for d in runs_dir.iterdir() if d.is_dir() and d.name.startswith("run_")],
                              key=lambda x: x.stat().st_ctime)
            if all_runs:
                default_run_dir = str(all_runs[-1])
    except Exception:
        pass

    parser = argparse.ArgumentParser(description="Agent F: Compliance & Tax Validation")
    parser.add_argument("--run-dir", default=default_run_dir, help="Path to the specific run directory")

    args = parser.parse_args()

    if not args.run_dir:
        print("❌ Error: No run directory provided and none could be auto-detected.")
    else:
        try:
            run_agent_f(args)
        except Exception as e:
            print(f"❌ Error: {e}")