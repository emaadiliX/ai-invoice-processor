import argparse
import csv
import json
from pathlib import Path

import yaml
from rapidfuzz import fuzz

DEFAULT_THRESHOLD = 0.75


def read_json(path: Path):
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
    for path in paths:
        if path and path.exists():
            return path
    return None


def get_manifest(bundle_dir: Path):
    manifest_path = bundle_dir / "manifest.yaml"
    return read_yaml(manifest_path) if manifest_path.exists() else {}


def resolve_vendor_master(bundle_dir: Path, manifest: dict, explicit: str | None):
    # FIXED: Added (bundle_dir / "vendor_master.json") to candidates
    candidates = [
        Path(explicit).resolve() if explicit else None,
        (bundle_dir / "vendor_master.json").resolve(),  # <--- NEW: Check run dir first
        (bundle_dir / manifest.get("vendor_master_file", "")).resolve() if manifest.get("vendor_master_file") else None,
        (bundle_dir.parent / "shared" / "vendor_master.json").resolve(),
        (bundle_dir.parent / "vendor_master.json").resolve(),
    ]
    path = find_first_existing(candidates)
    if not path:
        raise FileNotFoundError(f"Vendor master file not found. Searched: {[str(c) for c in candidates if c]}")
    return path


def resolve_policy(bundle_dir: Path, manifest: dict, explicit: str | None):
    return find_first_existing(
        [
            Path(explicit).resolve() if explicit else None,
            (bundle_dir / manifest.get("approval_policy_file", "")).resolve() if manifest.get("approval_policy_file") else None,
            (bundle_dir.parent / "shared" / "approval_policy.yaml").resolve(),
            (bundle_dir.parent / "policy" / "approval_policy.yaml").resolve(),
        ]
    )


def resolve_extraction(bundle_dir: Path, explicit: str | None):
    return find_first_existing(
        [
            Path(explicit).resolve() if explicit else None,
            (bundle_dir / "mock_extraction.json").resolve(),
            (bundle_dir / "extracted_invoice.json").resolve(),
        ]
    )


def load_vendor_master(path: Path):
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    rows = read_json(path)
    if not isinstance(rows, list):
        raise ValueError("Vendor master JSON must be a list.")
    return [row for row in rows if isinstance(row, dict)]


def pick_vendor_name(invoice_payload: dict):
    for key in ("vendor_name", "seller_name", "supplier_name", "vendor", "supplier"):
        value = invoice_payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            nested = value.get("name") or value.get("vendor_name") or value.get("legal_name")
            if isinstance(nested, str) and nested.strip():
                return nested.strip()
    return None


def vendor_name(record: dict):
    return record.get("name") or record.get("vendor_name") or record.get("legal_name") or record.get("display_name")


def vendor_id(record: dict):
    return record.get("vendor_id") or record.get("id") or record.get("supplier_id")


def confidence_threshold(policy: dict):
    vendor = policy.get("vendor") if isinstance(policy.get("vendor"), dict) else {}
    raw = vendor.get("vendor_match_min_confidence", DEFAULT_THRESHOLD)
    try:
        return max(0.0, min(1.0, float(raw)))
    except (TypeError, ValueError):
        return DEFAULT_THRESHOLD


def resolve_vendor(vendor_text: str | None, master_rows: list[dict], threshold: float, top_k: int):
    vendor_text = (vendor_text or "").strip()
    if not vendor_text:
        return {
            "input_vendor_name": None,
            "matched_vendor_id": None,
            "matched_vendor_name": None,
            "confidence": 0.0,
            "threshold": threshold,
            "risk_flag": True,
            "status": "MISSING_INPUT_VENDOR_NAME",
            "top_candidates": [],
        }

    candidates = []
    for row in master_rows:
        name = vendor_name(row)
        ident = vendor_id(row)
        if not isinstance(name, str) or not name.strip() or not isinstance(ident, str) or not ident.strip():
            continue
        score = round(fuzz.WRatio(vendor_text, name.strip()) / 100.0, 4)
        candidates.append({"vendor_id": ident.strip(), "vendor_name": name.strip(), "confidence": score})

    candidates.sort(key=lambda x: (-x["confidence"], x["vendor_id"]))
    if not candidates:
        return {
            "input_vendor_name": vendor_text,
            "matched_vendor_id": None,
            "matched_vendor_name": None,
            "confidence": 0.0,
            "threshold": threshold,
            "risk_flag": True,
            "status": "NEW_VENDOR",
            "top_candidates": [],
        }

    best = candidates[0]
    weak = best["confidence"] < threshold
    return {
        "input_vendor_name": vendor_text,
        "matched_vendor_id": best["vendor_id"],
        "matched_vendor_name": best["vendor_name"],
        "confidence": best["confidence"],
        "threshold": threshold,
        "risk_flag": weak,
        "status": "WEAK_MATCH" if weak else "MATCHED",
        "top_candidates": candidates[:top_k],
    }


def build_finding(result: dict):
    if not result["risk_flag"]:
        return None
    mapping = {
        "MISSING_INPUT_VENDOR_NAME": ("VENDOR_NAME_MISSING", "HIGH", "Vendor name missing from extracted invoice."),
        "NEW_VENDOR": ("VENDOR_NOT_FOUND", "HIGH", "No vendor match found in vendor master."),
        "WEAK_MATCH": ("VENDOR_MATCH_WEAK", "MEDIUM", "Vendor confidence below threshold."),
    }
    code, severity, message = mapping[result["status"]]
    return {
        "agent": "C",
        "code": code,
        "severity": severity,
        "message": message,
        "evidence": {
            "input_vendor_name": result["input_vendor_name"],
            "matched_vendor_id": result["matched_vendor_id"],
            "matched_vendor_name": result["matched_vendor_name"],
            "confidence": result["confidence"],
            "threshold": result["threshold"],
        },
        "recommended_action": "manual_review",
    }


def run_agent_c(args):
    bundle_dir = Path(args.bundle_dir).resolve()
    out_dir = Path(args.out_dir).resolve() if args.out_dir else bundle_dir
    manifest = get_manifest(bundle_dir)

    vm_path = resolve_vendor_master(bundle_dir, manifest, args.vendor_master)
    policy = read_yaml(resolve_policy(bundle_dir, manifest, args.policy))
    threshold = confidence_threshold(policy)
    extraction_path = resolve_extraction(bundle_dir, args.extracted_invoice)
    invoice_payload = read_json(extraction_path) if extraction_path else {}

    result = resolve_vendor(
        args.vendor_name or pick_vendor_name(invoice_payload),
        load_vendor_master(vm_path),
        threshold,
        args.top_k,
    )

    write_json(out_dir / "vendor_resolution_result.json", result)

    context_path = out_dir / "context_packet.json"
    context = {}
    if context_path.exists():
        existing_context = read_json(context_path)
        if isinstance(existing_context, dict):
            context = existing_context
    context["vendor_resolution"] = {k: result[k] for k in ("input_vendor_name", "matched_vendor_id", "matched_vendor_name", "confidence", "threshold", "risk_flag", "status")}
    context["vendor_candidates"] = result["top_candidates"]
    write_json(context_path, context)

    finding = build_finding(result)
    if finding:
        findings_path = out_dir / "findings.json"
        findings = []
        if findings_path.exists():
            existing_findings = read_json(findings_path)
            if isinstance(existing_findings, list):
                findings = existing_findings
        findings.append(finding)
        write_json(findings_path, findings)

    return {
        "result_path": str(out_dir / "vendor_resolution_result.json"),
        "context_path": str(out_dir / "context_packet.json"),
        "findings_path": str(out_dir / "findings.json"),
        "risk_flag": result["risk_flag"],
        "status": result["status"],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Agent C: Vendor resolution via fuzzy matching")
    parser.add_argument("--bundle-dir", required=True)
    parser.add_argument("--extracted-invoice", default=None)
    parser.add_argument("--vendor-master", default=None)
    parser.add_argument("--policy", default=None)
    parser.add_argument("--vendor-name", default=None)
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--top-k", type=int, default=3)
    print(json.dumps(run_agent_c(parser.parse_args()), indent=2))