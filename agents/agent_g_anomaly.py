import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path

import yaml


#shared I/O helpers (mirrors Agent C)

def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2,
                    ensure_ascii=False) + "\n", encoding="utf-8")


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


#path resolvers (mirrors Agent C)

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
        (bundle_dir / manifest.get("vendor_master_file", "")
         ).resolve() if manifest.get("vendor_master_file") else None,
        (bundle_dir.parent / "shared" / "vendor_master.json").resolve(),
        (bundle_dir.parent / "vendor_master.json").resolve(),
    ])


def resolve_policy(bundle_dir: Path, manifest: dict, explicit: str | None):
    return find_first_existing([
        Path(explicit).resolve() if explicit else None,
        (bundle_dir / manifest.get("approval_policy_file", "")).resolve() if manifest.get(
            "approval_policy_file") else None,
        (bundle_dir.parent / "shared" / "approval_policy.yaml").resolve(),
        (bundle_dir.parent.parent / "policy" / "approval_policy.yaml").resolve(),
    ])


#finding builder (mirrors Agent C _build_finding)

def make_finding(code: str, severity: str, message: str, evidence: dict, action: str = "manual_review"):
    return {
        "agent": "G",
        "code": code,
        "severity": severity,
        "message": message,
        "evidence": evidence,
        "recommended_action": action,
    }


#field aliasing (policy uses "invoice_number"; extraction uses "invoice_id")

FIELD_ALIASES = {
    "invoice_number": "invoice_id",
}


def get_field(doc: dict, key: str):
    if key in doc:
        return doc[key]
    alias = FIELD_ALIASES.get(key)
    if alias and alias in doc:
        return doc[alias]
    return None


#detection 1: duplicate invoice (historical)

def collect_history(history_dir: Path | None, lookback_days: int,
                    exclude_dir: Path | None = None,
                    scenario_prefix: str | None = None,
                    skip_date_filter: bool = False) -> list[dict]:
    """Recursively scan history_dir for extracted_invoice.json files within the lookback window."""
    if not history_dir or not history_dir.exists():
        return []
    cutoff = date.today() - timedelta(days=lookback_days)
    results = []
    for history_file in history_dir.rglob("extracted_invoice.json"):
        if exclude_dir and history_file.resolve().is_relative_to(exclude_dir.resolve()):
            continue
        if scenario_prefix and history_file.parent.name.startswith(scenario_prefix + "_"):
            continue
        try:
            doc = read_json(history_file)
            inv_date_raw = doc.get("invoice_date")
            if inv_date_raw:
                try:
                    inv_date = datetime.strptime(
                        inv_date_raw, "%Y-%m-%d").date()
                    if not skip_date_filter and inv_date < cutoff:
                        continue  # older than lookback window — skip
                except ValueError:
                    pass  # unparseable date: include conservatively
            results.append({"path": str(history_file), "doc": doc})
        except Exception:
            continue
    return results


def check_duplicate(invoice: dict, history_dirs: list[Path | None], policy: dict,
                    run_dir: Path | None = None,
                    scenario_prefix: str | None = None,
                    bundle_history_dirs: list[Path | None] | None = None) -> list[dict]:
    dup_config = policy.get("duplicate") or {}
    lookback_days = int(dup_config.get("lookback_days", 90))
    match_keys = dup_config.get("match_keys") or [
        "vendor_id", "invoice_number", "invoice_date", "total_amount"
    ]

    history = []
    for hdir in history_dirs:
        history.extend(collect_history(hdir, lookback_days, exclude_dir=run_dir,
                                       scenario_prefix=scenario_prefix))
    for hdir in (bundle_history_dirs or []):
        history.extend(collect_history(
            hdir, lookback_days, skip_date_filter=True))

    for entry in history:
        prior = entry["doc"]
        if all(get_field(invoice, k) == get_field(prior, k) for k in match_keys):
            return [make_finding(
                "DUPLICATE_INVOICE", "CRITICAL",
                f"Invoice matches a previously processed invoice (matched on: {', '.join(match_keys)}).",
                {
                    "match_keys": match_keys,
                    "prior_invoice_path": entry["path"],
                    **{f"matched_{k}": get_field(invoice, k) for k in match_keys},
                },
                "block_payment",
            )]
    return []


#detection 2: bank account change

def check_bank_change(invoice: dict, vendor_master: list, policy: dict,
                      resolved_vendor_id: str | None = None) -> list[dict]:
    vendor_id = invoice.get("vendor_id") or resolved_vendor_id
    if not vendor_id or not vendor_master:
        return []

    vendor = next((v for v in vendor_master if v.get(
        "vendor_id") == vendor_id), None)
    if not vendor or not vendor.get("bank_change_flag", False):
        return []

    total = invoice.get("total_amount") or 0.0
    risk = policy.get("risk") or {}
    high_value_threshold = float(
        risk.get("bank_change_high_value_threshold", 5000))

    if total > high_value_threshold:
        return [make_finding(
            "BANK_CHANGE_HIGH_VALUE", "CRITICAL",
            f"Vendor {vendor_id} has a recent bank account change and invoice total "
            f"({total}) exceeds the high-value threshold ({high_value_threshold}).",
            {
                "vendor_id": vendor_id,
                "bank_change_flag": True,
                "total_amount": total,
                "high_value_threshold": high_value_threshold,
            },
            "escalate_to_finance_approver",
        )]

    return [make_finding(
        "BANK_ACCOUNT_CHANGE", "HIGH",
        f"Vendor {vendor_id} has a recent bank account change flagged in vendor master.",
        {
            "vendor_id": vendor_id,
            "bank_change_flag": True,
            "total_amount": total,
        },
        "verify_bank_details",
    )]


#detection 3: near approval limit

def check_near_limit(invoice: dict, policy: dict) -> list[dict]:
    thresholds = policy.get("thresholds") or {}
    risk = policy.get("risk") or {}

    senior_limit = float(thresholds.get("senior_approval_above", 10000))
    near_pct = float(risk.get("near_approval_limit_pct", 98))
    near_threshold = senior_limit * (near_pct / 100)

    total = invoice.get("total_amount") or 0.0
    if near_threshold <= total < senior_limit:
        return [make_finding(
            "NEAR_APPROVAL_LIMIT", "MEDIUM",
            f"Invoice total ({total}) is within {100 - near_pct:.0f}% of the senior "
            f"approval limit ({senior_limit}).",
            {
                "total_amount": total,
                "senior_approval_above": senior_limit,
                "near_limit_threshold": near_threshold,
                "near_limit_pct": near_pct,
            },
        )]
    return []


#orchestrate all checks

def detect_anomalies(invoice: dict, vendor_master: list, policy: dict,
                     history_dirs: list[Path | None], run_dir: Path | None = None,
                     scenario_prefix: str | None = None,
                     bundle_history_dirs: list[Path | None] | None = None,
                     resolved_vendor_id: str | None = None) -> list[dict]:
    findings = []

    # 1. Duplicate check (compares invoice fields against historical records)
    findings.extend(check_duplicate(invoice, history_dirs, policy,
                                    run_dir=run_dir, scenario_prefix=scenario_prefix,
                                    bundle_history_dirs=bundle_history_dirs))

    # 2. Bank & Limit Checks
    findings.extend(check_bank_change(invoice, vendor_master, policy,
                                      resolved_vendor_id=resolved_vendor_id))
    findings.extend(check_near_limit(invoice, policy))

    return findings


#output writers (mirrors Agent C)

def update_context_packet(out_dir: Path, summary: dict):
    context_path = out_dir / "context_packet.json"
    context = {}
    if context_path.exists():
        existing = read_json(context_path)
        if isinstance(existing, dict):
            context = existing
    context["anomaly_detection"] = summary
    write_json(context_path, context)


def append_findings(out_dir: Path, new_findings: list[dict]):
    if not new_findings:
        return
    findings_path = out_dir / "findings.json"
    findings = []
    if findings_path.exists():
        existing = read_json(findings_path)
        if isinstance(existing, list):
            findings = existing
    findings.extend(new_findings)
    write_json(findings_path, findings)


#main entry

def run_agent_g(args):
    bundle_dir = Path(args.bundle_dir).resolve()
    run_dir = Path(args.run_dir).resolve() if args.run_dir else None
    out_dir = Path(args.out_dir).resolve(
    ) if args.out_dir else (run_dir or bundle_dir)
    history_dir = Path(args.history_dir).resolve(
    ) if args.history_dir else None
    manifest = get_manifest(bundle_dir)

    extraction_path = resolve_extraction(
        bundle_dir, run_dir, args.extracted_invoice)
    if not extraction_path:
        raise FileNotFoundError(
            "No extracted_invoice.json or mock_extraction.json found.")
    invoice = read_json(extraction_path)

    # Use purchase order's vendor_id as fallback when invoice.vendor_id is null
    resolved_vendor_id = None
    if run_dir:
        po_path = run_dir / "purchase_order.json"
        if po_path.exists():
            try:
                po = read_json(po_path)
                resolved_vendor_id = po.get("vendor_id")
            except Exception:
                pass

    policy_path = resolve_policy(bundle_dir, manifest, args.policy)
    policy = read_yaml(policy_path) if policy_path else {}

    vendor_master_path = resolve_vendor_master(
        bundle_dir, manifest, args.vendor_master)
    vendor_master = read_json(vendor_master_path) if vendor_master_path else []

    # Build list of directories to scan for duplicate history
    history_dirs = [history_dir]
    bundle_history_dirs = []
    dup_hist = manifest.get("duplicate_history_dir")
    if dup_hist:
        bundle_history = bundle_dir / dup_hist
        if bundle_history.exists():
            bundle_history_dirs.append(bundle_history)

    # Extract scenario prefix from run directory name to skip re-runs
    scenario_prefix = None
    if run_dir:
        run_name = run_dir.name
        # Run dirs follow pattern: {scenario_name}_{YYYYMMDD}_{HHMMSS}
        parts = run_name.rsplit("_", 2)
        if len(parts) >= 3:
            scenario_prefix = parts[0]

    findings = detect_anomalies(invoice, vendor_master, policy, history_dirs,
                                run_dir=run_dir, scenario_prefix=scenario_prefix,
                                bundle_history_dirs=bundle_history_dirs,
                                resolved_vendor_id=resolved_vendor_id)

    critical = [f for f in findings if f["severity"] == "CRITICAL"]
    high = [f for f in findings if f["severity"] == "HIGH"]
    medium = [f for f in findings if f["severity"] == "MEDIUM"]
    low = [f for f in findings if f["severity"] == "LOW"]
    clear = len(findings) == 0

    summary = {
        "invoice_id": invoice.get("invoice_id"),
        "clear": clear,
        "finding_count": len(findings),
        "critical": len(critical),
        "high": len(high),
        "medium": len(medium),
        "low": len(low),
        "finding_codes": [f["code"] for f in findings],
    }

    write_json(out_dir / "anomaly_detection_result.json",
               {"summary": summary, "findings": findings})
    update_context_packet(out_dir, summary)
    append_findings(out_dir, findings)

    return {
        "result_path": str(out_dir / "anomaly_detection_result.json"),
        "context_path": str(out_dir / "context_packet.json"),
        "findings_path": str(out_dir / "findings.json"),
        "clear": clear,
        "finding_codes": summary["finding_codes"],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Agent G: Duplicate & anomaly detection")
    parser.add_argument("--bundle-dir", required=True)
    parser.add_argument("--extracted-invoice", default=None)
    parser.add_argument("--run-dir", default=None)
    parser.add_argument("--vendor-master", default=None)
    parser.add_argument("--policy", default=None)
    parser.add_argument("--history-dir", default=None)
    parser.add_argument("--out-dir", default=None)
    import sys

    result = run_agent_g(parser.parse_args())
    print(json.dumps(result, indent=2))