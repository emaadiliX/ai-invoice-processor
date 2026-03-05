import argparse
import json
import re
from pathlib import Path
import yaml


def read_json(path: Path):
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def read_yaml(path: Path):
    if not path or not path.exists():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return data if isinstance(data, dict) else {}


def find_first_existing(paths):
    for p in paths:
        if p and p.exists():
            return p
    return None


def get_manifest(bundle_dir: Path):
    mp = bundle_dir / "manifest.yaml"
    return read_yaml(mp) if mp.exists() else {}


def resolve_extraction(bundle_dir: Path, run_dir: Path | None, explicit: str | None):
    return find_first_existing([
        Path(explicit).resolve() if explicit else None,
        (bundle_dir / "mock_extraction.json").resolve(),
        (bundle_dir / "extracted_invoice.json").resolve(),
        (run_dir / "extracted_invoice.json").resolve() if run_dir else None,
    ])


def resolve_vendor_master(bundle_dir: Path, manifest: dict, explicit: str | None):
    return find_first_existing([
        Path(explicit).resolve() if explicit else None,
        (bundle_dir / "vendor_master.json").resolve(),
        (bundle_dir / manifest.get("vendor_master_file", "")).resolve() if manifest.get("vendor_master_file") else None,
        (bundle_dir.parent / "shared" / "vendor_master.json").resolve(),
        (bundle_dir.parent / "vendor_master.json").resolve(),
    ])


def resolve_tax_rules(bundle_dir: Path, manifest: dict, explicit: str | None):
    return find_first_existing([
        Path(explicit).resolve() if explicit else None,
        (bundle_dir / "tax_rules.yaml").resolve(),
        (bundle_dir / manifest.get("tax_rules_file", "")).resolve() if manifest.get("tax_rules_file") else None,
        (bundle_dir.parent / "shared" / "tax_rules.yaml").resolve(),
    ])


def resolve_policy(bundle_dir: Path, manifest: dict, explicit: str | None):
    return find_first_existing([
        Path(explicit).resolve() if explicit else None,
        (bundle_dir / manifest.get("approval_policy_file", "")).resolve() if manifest.get("approval_policy_file") else None,
        (bundle_dir.parent / "shared" / "approval_policy.yaml").resolve(),
        (bundle_dir.parent.parent / "policy" / "approval_policy.yaml").resolve(),
    ])


def resolve_vendor_resolution(bundle_dir: Path, run_dir: Path | None, explicit: str | None):
    return find_first_existing([
        Path(explicit).resolve() if explicit else None,
        (run_dir / "vendor_resolution_result.json").resolve() if run_dir else None,
        (bundle_dir / "vendor_resolution_result.json").resolve(),
    ])


def get_vendor_country(vendor_id: str, vendor_master: list):
    if not vendor_id:
        return None, None

    vendor_record = next((v for v in vendor_master if v.get("vendor_id") == vendor_id), None)
    if not vendor_record:
        return None, None

    address = vendor_record.get("address", "").upper()

    if "USA" in address or address.endswith(" US"):
        return "US", vendor_record
    if "UK" in address or "GB" in address or address.endswith(" GB"):
        return "GB", vendor_record
    if "GERMANY" in address or "DEUTSCHLAND" in address or address.endswith(" DE"):
        return "DE", vendor_record

    return "UNKNOWN", vendor_record


def validate_tax_id_format(tax_id: str, country_code: str) -> bool:
    if not tax_id:
        return False

    patterns = {
        "US": r"^US-\d{2}-\d{7}$",
        "GB": r"^GB\d{9}$",
        "DE": r"^DE\d{9}$"
    }

    pattern = patterns.get(country_code)
    if pattern:
        return bool(re.match(pattern, tax_id))

    return True


def run_agent_f(args):
    bundle_dir = Path(args.bundle_dir).resolve()
    run_dir = Path(args.run_dir).resolve() if args.run_dir else None
    out_dir = Path(args.out_dir).resolve() if args.out_dir else (run_dir or bundle_dir)
    manifest = get_manifest(bundle_dir)

    print(f"[Agent F] Starting Tax Compliance. Bundle: {bundle_dir.name}")

    extraction_path = resolve_extraction(bundle_dir, run_dir, args.extracted_invoice)
    if not extraction_path:
        raise FileNotFoundError("No extracted_invoice.json or mock_extraction.json found.")

    vendor_res_path = resolve_vendor_resolution(bundle_dir, run_dir, args.vendor_resolution)
    policy_path = resolve_policy(bundle_dir, manifest, args.policy)
    tax_rules_path = resolve_tax_rules(bundle_dir, manifest, args.tax_rules)
    vendor_master_path = resolve_vendor_master(bundle_dir, manifest, args.vendor_master)

    extracted_data = read_json(extraction_path)
    resolution_data = read_json(vendor_res_path) if vendor_res_path else {}
    vendor_master_data = read_json(vendor_master_path) if vendor_master_path else []
    vendor_master = vendor_master_data if isinstance(vendor_master_data, list) else []
    tax_rules = read_yaml(tax_rules_path) if tax_rules_path else {}
    policy = read_yaml(policy_path) if policy_path else {}

    context_path = out_dir / "context_packet.json"
    findings_path = out_dir / "findings.json"

    findings = []
    validation_result = {
        "status": "PASS",
        "details": {}
    }

    vendor_id = resolution_data.get("matched_vendor_id")
    if not vendor_id:
        print("   -> Skipping: No matched vendor found.")
        validation_result["status"] = "SKIPPED"
        write_json(out_dir / "tax_validation_result.json", validation_result)
        return {
            "result_path": str(out_dir / "tax_validation_result.json"),
            "context_path": str(context_path),
            "findings_path": str(findings_path),
            "status": "SKIPPED",
            "passed": True,
            "finding_codes": [],
        }

    country_code, vendor_record = get_vendor_country(vendor_id, vendor_master)
    print(f"   -> Vendor Jurisdiction: {country_code}")

    if not country_code or country_code == "UNKNOWN":
        findings.append({
            "agent": "F",
            "code": "UNKNOWN_JURISDICTION",
            "severity": "MEDIUM",
            "message": f"Could not determine jurisdiction for vendor {vendor_id}",
            "evidence": {"address": vendor_record.get("address", "N/A") if vendor_record else "N/A"},
            "recommended_action": "verify_vendor_jurisdiction"
        })
        validation_result["status"] = "FLAGGED"

    master_tax_id = vendor_record.get("tax_id") if vendor_record else None

    if master_tax_id and country_code in ["US", "GB", "DE"]:
        if not validate_tax_id_format(master_tax_id, country_code):
            findings.append({
                "agent": "F",
                "code": "INVALID_TAX_ID",
                "severity": "MEDIUM",
                "message": f"Tax ID {master_tax_id} format invalid for {country_code}",
                "evidence": {"tax_id": master_tax_id, "expected_format": country_code},
                "recommended_action": "verify_tax_id"
            })
            validation_result["status"] = "FLAGGED"

    try:
        total_amount = float(extracted_data.get("total_amount", 0) or 0)
        tax_amount = float(extracted_data.get("tax_amount", 0) or 0)
    except (ValueError, TypeError):
        total_amount = 0.0
        tax_amount = 0.0

    subtotal = float(extracted_data.get("subtotal", 0) or 0)
    if subtotal == 0 and total_amount > 0:
        subtotal = total_amount - tax_amount

    if subtotal > 0:
        actual_rate = tax_amount / subtotal

        rules_list = tax_rules.get("tax_rules", [])
        rule = next((r for r in rules_list if r['country_code'] == country_code), None)

        if rule:
            expected_rate = float(rule.get('standard_rate', 0))
            reduced_rate = float(rule.get('reduced_rate', 0))
            tolerance = float(policy.get('tax_rules', {}).get('allowable_tax_diff', 0.05))

            diff_std = abs(actual_rate - expected_rate)
            diff_red = abs(actual_rate - reduced_rate)

            match_std = diff_std <= tolerance
            match_red = diff_red <= tolerance

            print(f"   -> Tax Check: Actual {actual_rate:.2%} vs Expected {expected_rate:.2%} (Diff: {diff_std:.4f})")

            if not match_std and not match_red:
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
                    },
                    "recommended_action": "escalate_tax_mismatch"
                })
                validation_result["status"] = "FAIL"

    validation_result["details"] = {
        "country": country_code,
        "tax_id_valid": not any(f['code'] == 'INVALID_TAX_ID' for f in findings)
    }
    write_json(out_dir / "tax_validation_result.json", validation_result)

    existing_findings = read_json(findings_path)
    if isinstance(existing_findings, list):
        existing_findings.extend(findings)
    else:
        existing_findings = findings

    if findings:
        write_json(findings_path, existing_findings)

    context = read_json(context_path)
    context["tax_validation"] = validation_result
    write_json(context_path, context)

    print(f"[Agent F] Complete. Findings: {len(findings)}")

    passed = len(findings) == 0

    return {
        "result_path": str(out_dir / "tax_validation_result.json"),
        "context_path": str(context_path),
        "findings_path": str(findings_path),
        "status": validation_result["status"],
        "passed": passed,
        "finding_codes": [f["code"] for f in findings],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Agent F: Compliance & Tax Validation")
    parser.add_argument("--bundle-dir", required=True)
    parser.add_argument("--extracted-invoice", default=None)
    parser.add_argument("--vendor-resolution", default=None)
    parser.add_argument("--vendor-master", default=None)
    parser.add_argument("--tax-rules", default=None)
    parser.add_argument("--policy", default=None)
    parser.add_argument("--run-dir", default=None)
    parser.add_argument("--out-dir", default=None)
    result = run_agent_f(parser.parse_args())
    print(json.dumps(result, indent=2))
