import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path

import yaml

# Shared I/O helpers


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


# Path resolvers

def resolve_extraction(bundle_dir, run_dir, explicit):
    return find_first_existing([
        Path(explicit).resolve() if explicit else None,
        (bundle_dir / "mock_extraction.json").resolve(),
        (bundle_dir / "extracted_invoice.json").resolve(),
        (run_dir / "extracted_invoice.json").resolve() if run_dir else None,
    ])


def resolve_findings(bundle_dir, run_dir, out_dir):
    return find_first_existing([
        (run_dir / "findings.json").resolve() if run_dir else None,
        (out_dir / "findings.json").resolve(),
        (bundle_dir / "findings.json").resolve(),
    ])


def resolve_policy(bundle_dir, manifest, explicit):
    return find_first_existing([
        Path(explicit).resolve() if explicit else None,
        (bundle_dir / manifest.get("approval_policy_file", "")
         ).resolve() if manifest.get("approval_policy_file") else None,
        (bundle_dir.parent / "shared" / "approval_policy.yaml").resolve(),
        (bundle_dir.parent.parent / "policy" / "approval_policy.yaml").resolve(),
    ])


def resolve_optional(run_dir, bundle_dir, filename):
    return find_first_existing([
        (run_dir / filename).resolve() if run_dir else None,
        (bundle_dir / filename).resolve(),
    ])


# Finding deduplication and sorting

SEVERITY_ORDER = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}


def deduplicate_findings(findings):
    """Remove duplicate findings based on (agent, code, severity) tuple."""
    seen = set()
    result = []
    for f in findings:
        key = (f.get("agent", ""), f.get("code", ""), f.get("severity", ""))
        if key not in seen:
            seen.add(key)
            result.append(f)
    return result


def sort_findings(findings):
    """Sort findings by agent, code, severity for deterministic processing."""
    return sorted(findings, key=lambda f: (
        f.get("agent", ""),
        f.get("code", ""),
        SEVERITY_ORDER.get(f.get("severity", "LOW"), 4),
    ))


# Decision finalization

def finalize_decision(approval_packet, findings, match_result, policy, invoice):
    """Decide the final action based on H's recommendation, findings, and invoice data."""
    h_action = approval_packet.get("recommended_action", "HOLD_FOR_APPROVAL")
    h_assigned_to = approval_packet.get("assigned_to", "ap_manager")
    h_reason = approval_packet.get("approval_reason", "")

    routing = policy.get("routing") or {}
    roles = routing.get("approval_roles") or {}
    ap_bot = roles.get("auto_approve", "ap_bot")

    has_currency_conversion = match_result.get(
        "currency_conversion") is not None
    is_full_match = match_result.get("overall_status") == "FULL_MATCH"
    is_clean = len(findings) == 0
    finding_codes = {f.get("code") for f in findings}

    # AUTO_APPROVE: post to ERP, or route if multi-currency
    if h_action == "AUTO_APPROVE":
        if has_currency_conversion:
            return ("ROUTE_APPROVAL", h_assigned_to,
                    ["Multi-currency invoice requires approval routing"])
        return ("AUTO_POST", ap_bot, [])

    # HOLD_FOR_APPROVAL: try to refine the action
    if h_action == "HOLD_FOR_APPROVAL":
        if is_clean and is_full_match and not has_currency_conversion:
            return ("AUTO_POST", ap_bot, [])
        if "CREDIT_NOTE_DETECTED" in finding_codes:
            return ("ROUTE_APPROVAL", h_assigned_to,
                    ["Credit note requires approval routing"])
        if has_currency_conversion:
            return ("ROUTE_APPROVAL", h_assigned_to,
                    ["Multi-currency invoice requires approval routing"])
        vendor_issue_codes = {"VENDOR_MATCH_WEAK", "VENDOR_NOT_FOUND", "NEW_VENDOR"}
        if finding_codes & vendor_issue_codes:
            return ("ROUTE_TO_DEPT_HEAD", roles.get("dept_head", "dept_head"),
                    ["Vendor resolution required — routing to department head"])
        if not invoice.get("po_reference"):
            if "MISSING_PO_REFERENCE" in finding_codes or "NO_PO_MATCH" in finding_codes:
                return ("ROUTE_TO_DEPT_HEAD", roles.get("dept_head", "dept_head"),
                        ["No PO — requires department head approval"])
        return ("HOLD_FOR_APPROVAL", h_assigned_to,
                [h_reason] if h_reason else ["Invoice has exceptions requiring approval"])

    # HOLD_FOR_MANUAL_REVIEW: normalize action name, handle special codes
    if h_action == "HOLD_FOR_MANUAL_REVIEW":
        if "CREDIT_NOTE_DETECTED" in finding_codes:
            return ("ROUTE_APPROVAL", h_assigned_to,
                    ["Credit note requires approval routing"])
        if has_currency_conversion:
            return ("ROUTE_APPROVAL", h_assigned_to,
                    ["Multi-currency invoice requires approval routing"])
        return ("ROUTE_TO_MANUAL_REVIEW", h_assigned_to,
                [h_reason] if h_reason else [])

    # ESCALATE_TO_RISK_OFFICER: reroute bank changes to finance
    if h_action == "ESCALATE_TO_RISK_OFFICER":
        bank_codes = {"BANK_CHANGE_HIGH_VALUE", "BANK_ACCOUNT_CHANGE"}
        if finding_codes & bank_codes:
            return ("ESCALATE_TO_FINANCE_APPROVER",
                    roles.get("senior_approval", "finance_controller"),
                    ["Bank account change on high-value invoice"])
        return (h_action, h_assigned_to, [h_reason] if h_reason else [])

    # BLOCK and everything else: passthrough
    return (h_action, h_assigned_to, [h_reason] if h_reason else [])


# Determinism hash

def compute_determinism_hash(findings, invoice, match_result, vendor_resolution,
                             approval_packet, action):
    """SHA-256 hash of all decision inputs for deterministic re-runs."""
    hash_input = {
        "finding_codes": sorted(f.get("code", "") for f in findings),
        "finding_severities": sorted(f.get("severity", "") for f in findings),
        "invoice_id": invoice.get("invoice_id"),
        "total_amount": invoice.get("total_amount"),
        "currency": invoice.get("currency"),
        "po_reference": invoice.get("po_reference"),
        "match_status": match_result.get("overall_status"),
        "currency_conversion_present": match_result.get("currency_conversion") is not None,
        "vendor_status": vendor_resolution.get("status"),
        "h_recommended_action": approval_packet.get("recommended_action"),
        "action": action,
    }
    canonical = json.dumps(hash_input, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# Output builders

def build_posting_payload(invoice, action, hold_reasons, assigned_to):
    """Build the final posting_payload.json content."""
    approval_required = action != "AUTO_POST"
    posting_blocked = action == "BLOCK"

    return {
        "invoice_id": invoice.get("invoice_id"),
        "action": action,
        "action_reason": hold_reasons[0] if hold_reasons else "Clean invoice — auto posting",
        "vendor_id": invoice.get("vendor_id"),
        "vendor_name": invoice.get("vendor_name"),
        "total_amount": invoice.get("total_amount"),
        "currency": invoice.get("currency", "USD"),
        "po_reference": invoice.get("po_reference"),
        "line_items": invoice.get("line_items", []),
        "assigned_to": assigned_to,
        "approval_required": approval_required,
        "hold_reasons": hold_reasons,
        "posting_blocked": posting_blocked,
    }


def build_audit_log_md(invoice, context_packet, findings, action, assigned_to,
                       hold_reasons, determinism_hash, evidence_paths, orchestrator_summary):
    """Build the audit_log.md markdown content."""
    invoice_id = invoice.get("invoice_id", "N/A")
    vendor_name = invoice.get("vendor_name", "N/A")
    vendor_id = invoice.get("vendor_id", "N/A")
    total = invoice.get("total_amount", 0.0)
    currency = invoice.get("currency", "USD")
    po_ref = invoice.get("po_reference", "N/A")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = []

    # Header
    lines.append(f"# Audit Log — Invoice {invoice_id}")
    lines.append("")
    lines.append(f"**Generated:** {timestamp}")
    lines.append(f"**Determinism Hash:** `{determinism_hash}`")
    lines.append("")

    # Run metadata table
    lines.append("## Run Metadata")
    lines.append("")
    lines.append("| Field | Value |")
    lines.append("|---|---|")
    lines.append(f"| Invoice ID | {invoice_id} |")
    lines.append(f"| Vendor | {vendor_name} ({vendor_id}) |")
    if isinstance(total, (int, float)):
        lines.append(f"| Total Amount | {currency} {total:,.2f} |")
    else:
        lines.append(f"| Total Amount | {currency} N/A |")
    lines.append(f"| PO Reference | {po_ref} |")
    lines.append(f"| Final Action | **{action}** |")
    lines.append(f"| Assigned To | {assigned_to} |")
    lines.append(
        f"| Hold Reasons | {'; '.join(hold_reasons) if hold_reasons else 'None'} |")
    lines.append("")

    # Agent trace table (9 rows, one per agent A-I)
    lines.append("## Agent Trace")
    lines.append("")
    lines.append("| Step | Agent | Context Key | Status | Detail |")
    lines.append("|---|---|---|---|---|")

    # Agent A — Intake (stored at root level of context_packet)
    run_id = context_packet.get("run_id")
    if run_id:
        lines.append(
            f"| 1 | A — Intake | run_id / status | completed | run_id={run_id} |")
    else:
        lines.append("| 1 | A — Intake | run_id / status | NOT_RUN | — |")

    # Agent B — Extraction (check if files key exists)
    files = context_packet.get("files")
    if files:
        lines.append(
            f"| 2 | B — Extraction | files | completed | {len(files)} files |")
    else:
        lines.append("| 2 | B — Extraction | files | NOT_RUN | — |")

    # Agent C — Vendor Resolution
    vendor_res = context_packet.get("vendor_resolution")
    if vendor_res and isinstance(vendor_res, dict):
        lines.append(
            f"| 3 | C — Vendor Resolution | vendor_resolution | {vendor_res.get('status', 'N/A')} | confidence={vendor_res.get('confidence', 'N/A')} |")
    else:
        lines.append(
            "| 3 | C — Vendor Resolution | vendor_resolution | NOT_RUN | — |")

    # Agent D — Field Validation
    field_val = context_packet.get("field_validation")
    if field_val and isinstance(field_val, dict):
        d_status = "PASS" if field_val.get(
            "passed") else f"{field_val.get('finding_count', 0)} findings"
        lines.append(
            f"| 4 | D — Field Validation | field_validation | {d_status} | passed={field_val.get('passed', 'N/A')} |")
    else:
        lines.append(
            "| 4 | D — Field Validation | field_validation | NOT_RUN | — |")

    # Agent E — Matching
    matching = context_packet.get("matching")
    if matching and isinstance(matching, dict):
        lines.append(
            f"| 5 | E — Matching | matching | {matching.get('overall_status', 'N/A')} | match_type={matching.get('match_type', 'N/A')} |")
    else:
        lines.append("| 5 | E — Matching | matching | NOT_RUN | — |")

    # Agent F — Tax Compliance
    tax_val = context_packet.get("tax_validation")
    if tax_val and isinstance(tax_val, dict):
        lines.append(
            f"| 6 | F — Tax Compliance | tax_validation | {tax_val.get('status', 'N/A')} | jurisdiction={tax_val.get('jurisdiction', 'N/A')} |")
    else:
        lines.append(
            "| 6 | F — Tax Compliance | tax_validation | NOT_RUN | — |")

    # Agent G — Anomaly Detection
    anomaly = context_packet.get("anomaly_detection")
    if anomaly and isinstance(anomaly, dict):
        g_status = "CLEAR" if anomaly.get(
            "clear") else f"{anomaly.get('finding_count', 0)} findings"
        g_detail = ", ".join(anomaly.get("finding_codes", [])) or "—"
        lines.append(
            f"| 7 | G — Anomaly Detection | anomaly_detection | {g_status} | {g_detail} |")
    else:
        lines.append(
            "| 7 | G — Anomaly Detection | anomaly_detection | NOT_RUN | — |")

    # Agent H — Exception Triage
    triage = context_packet.get("exception_triage")
    if triage and isinstance(triage, dict):
        lines.append(
            f"| 8 | H — Exception Triage | exception_triage | {triage.get('recommended_action', 'N/A')} | assigned_to={triage.get('assigned_to', 'N/A')} |")
    else:
        lines.append(
            "| 8 | H — Exception Triage | exception_triage | NOT_RUN | — |")

    # Agent I — Orchestrator (this agent)
    lines.append(
        f"| 9 | I — Orchestrator | orchestrator | {orchestrator_summary.get('action', 'N/A')} | assigned_to={orchestrator_summary.get('assigned_to', 'N/A')} |")

    lines.append("")

    # Final decision block
    lines.append("## Final Decision")
    lines.append("")
    lines.append("| Field | Value |")
    lines.append("|---|---|")
    lines.append(f"| Action | {action} |")
    lines.append(f"| Assigned To | {assigned_to} |")
    lines.append(
        f"| Approval Required | {'Yes' if action != 'AUTO_POST' else 'No'} |")
    lines.append(
        f"| Posting Blocked | {'Yes' if action == 'BLOCK' else 'No'} |")
    lines.append(f"| Determinism Hash | `{determinism_hash}` |")
    lines.append("")

    # Full findings list
    lines.append(f"## Findings ({len(findings)})")
    lines.append("")
    if findings:
        lines.append("| # | Agent | Code | Severity | Message |")
        lines.append("|---|---|---|---|---|")
        for i, f in enumerate(findings, 1):
            agent = f.get("agent", "?")
            code = f.get("code", "?")
            sev = f.get("severity", "?")
            msg = f.get("message", "").replace("|", "\\|")
            lines.append(f"| {i} | {agent} | {code} | {sev} | {msg} |")
    else:
        lines.append("No findings — invoice is clean.")
    lines.append("")

    # Evidence file paths
    lines.append("## Evidence File Paths")
    lines.append("")
    for filename in sorted(evidence_paths.keys()):
        lines.append(f"- **{filename}:** `{evidence_paths[filename]}`")
    lines.append("")

    return "\n".join(lines)


def build_metrics(invoice, findings, match_result, vendor_resolution, action, determinism_hash):
    """Build the metrics.json content."""
    # Calculate average extraction confidence
    scores = invoice.get("confidence_scores") or {}
    conf_avg = None
    if scores and isinstance(scores, dict):
        numeric = [v for v in scores.values() if isinstance(v, (int, float))]
        if numeric:
            conf_avg = round(sum(numeric) / len(numeric), 4)

    # Count findings by severity
    sev_counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for f in findings:
        sev = f.get("severity", "LOW")
        if sev in sev_counts:
            sev_counts[sev] += 1

    # Map action to human-readable outcome
    outcome_map = {
        "AUTO_POST": "auto_posted",
        "BLOCK": "blocked",
        "ESCALATE_TO_RISK_OFFICER": "escalated",
        "ESCALATE_TO_FINANCE_APPROVER": "escalated",
        "HOLD_FOR_MANUAL_REVIEW": "manual_review",
        "ROUTE_TO_MANUAL_REVIEW": "manual_review",
        "HOLD_FOR_APPROVAL": "held_for_approval",
        "ROUTE_APPROVAL": "routed_for_approval",
        "ROUTE_TO_DEPT_HEAD": "routed_to_dept_head",
    }

    return {
        "extraction_confidence_avg": conf_avg,
        "vendor_match_confidence": vendor_resolution.get("confidence"),
        "match_type": match_result.get("match_type"),
        "match_status": match_result.get("overall_status"),
        "total_findings": len(findings),
        "findings_by_severity": sev_counts,
        "final_action": action,
        "processing_outcome": outcome_map.get(action, "unknown"),
        "determinism_hash": determinism_hash,
    }


# Context packet updater

def update_context_packet(out_dir, summary):
    context_path = out_dir / "context_packet.json"
    context = {}
    if context_path.exists():
        existing = read_json(context_path)
        if isinstance(existing, dict):
            context = existing
    context["orchestrator"] = summary
    write_json(context_path, context)


# Main entry

def run_agent_i(args):
    bundle_dir = Path(args.bundle_dir).resolve()
    run_dir = Path(args.run_dir).resolve() if args.run_dir else None
    out_dir = Path(args.out_dir).resolve(
    ) if args.out_dir else (run_dir or bundle_dir)
    manifest = get_manifest(bundle_dir)

    # Load inputs

    # Extracted invoice (required)
    extraction_path = resolve_extraction(bundle_dir, run_dir, None)
    if not extraction_path:
        raise FileNotFoundError(
            "No extracted_invoice.json or mock_extraction.json found.")
    invoice = read_json(extraction_path)

    # Accumulated findings from agents C-G
    findings_path = resolve_findings(bundle_dir, run_dir, out_dir)
    if findings_path and findings_path.exists():
        findings_loaded = read_json(findings_path)
        if not isinstance(findings_loaded, list):
            findings_loaded = []
    else:
        findings_loaded = []

    # Match result from Agent E
    match_result = {}
    mr_path = resolve_optional(run_dir, bundle_dir, "match_result.json")
    if mr_path and mr_path.exists():
        try:
            match_result = read_json(mr_path)
        except Exception:
            match_result = {}

    # Vendor resolution from Agent C
    vendor_resolution = {}
    vr_path = resolve_optional(
        run_dir, bundle_dir, "vendor_resolution_result.json")
    if vr_path and vr_path.exists():
        try:
            vendor_resolution = read_json(vr_path)
        except Exception:
            vendor_resolution = {}

    # Approval packet from Agent H
    approval_packet = {}
    ap_path = resolve_optional(run_dir, bundle_dir, "approval_packet.json")
    if ap_path and ap_path.exists():
        try:
            approval_packet = read_json(ap_path)
        except Exception:
            approval_packet = {}

    # Context packet (accumulated from all prior agents)
    context_packet = {}
    cp_path = resolve_optional(run_dir, bundle_dir, "context_packet.json")
    if cp_path and cp_path.exists():
        try:
            context_packet = read_json(cp_path)
        except Exception:
            context_packet = {}

    # Approval policy
    policy_path = resolve_policy(bundle_dir, manifest, args.policy)
    policy = read_yaml(policy_path) if policy_path else {}

    # Deduplicate and sort findings

    findings = sort_findings(deduplicate_findings(findings_loaded))

    # Finalize decision

    action, assigned_to, hold_reasons = finalize_decision(
        approval_packet, findings, match_result, policy, invoice,
    )

    # Compute determinism hash

    determinism_hash = compute_determinism_hash(
        findings, invoice, match_result, vendor_resolution,
        approval_packet, action,
    )

    # Collect evidence paths

    evidence_files = [
        "extracted_invoice.json", "findings.json", "match_result.json",
        "vendor_resolution_result.json", "approval_packet.json",
        "context_packet.json", "validation_result.json",
        "tax_validation_result.json", "anomaly_detection_result.json",
        "exceptions.md",
    ]
    evidence_paths = {}
    for filename in evidence_files:
        p = resolve_optional(run_dir, bundle_dir, filename)
        if not p:
            candidate = out_dir / filename
            if candidate.exists():
                p = candidate
        evidence_paths[filename] = str(p) if p else "NOT_FOUND"

    # Build summary

    orchestrator_summary = {
        "invoice_id": invoice.get("invoice_id"),
        "action": action,
        "assigned_to": assigned_to,
        "hold_reasons": hold_reasons,
        "total_findings": len(findings),
        "h_recommended_action": approval_packet.get("recommended_action"),
        "determinism_hash": determinism_hash,
        "timestamp": datetime.now().isoformat(),
    }

    # Build outputs

    posting_payload = build_posting_payload(
        invoice, action, hold_reasons, assigned_to)

    audit_log_content = build_audit_log_md(
        invoice, context_packet, findings, action, assigned_to,
        hold_reasons, determinism_hash, evidence_paths, orchestrator_summary,
    )

    metrics = build_metrics(
        invoice, findings, match_result, vendor_resolution, action, determinism_hash,
    )

    # Write outputs

    write_json(out_dir / "posting_payload.json", posting_payload)
    (out_dir / "audit_log.md").write_text(audit_log_content, encoding="utf-8")
    write_json(out_dir / "metrics.json", metrics)

    # Update context_packet.json with orchestrator section
    update_context_packet(out_dir, orchestrator_summary)

    return {
        "posting_payload_path": str(out_dir / "posting_payload.json"),
        "audit_log_path": str(out_dir / "audit_log.md"),
        "metrics_path": str(out_dir / "metrics.json"),
        "context_path": str(out_dir / "context_packet.json"),
        "action": action,
        "assigned_to": assigned_to,
        "determinism_hash": determinism_hash,
        "total_findings": len(findings),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Agent I: Orchestrator — Final decision & output generation",
    )
    parser.add_argument("--bundle-dir", required=True)
    parser.add_argument("--run-dir", default=None)
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--policy", default=None)
    result = run_agent_i(parser.parse_args())
    print(json.dumps(result, indent=2))
