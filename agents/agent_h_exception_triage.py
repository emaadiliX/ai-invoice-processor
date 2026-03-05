import argparse
import json
from datetime import datetime
from pathlib import Path

import yaml

# ── shared I/O helpers (mirrors Agent G) ─────────────────────────────────────

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
    for p in paths:
        if p and p.exists():
            return p
    return None


def get_manifest(bundle_dir: Path):
    mp = bundle_dir / "manifest.yaml"
    return read_yaml(mp) if mp.exists() else {}


# ── path resolvers (mirrors Agent G) ─────────────────────────────────────────

def resolve_extraction(bundle_dir: Path, run_dir: Path | None, explicit: str | None):
    return find_first_existing([
        Path(explicit).resolve() if explicit else None,
        (bundle_dir / "mock_extraction.json").resolve(),
        (bundle_dir / "extracted_invoice.json").resolve(),
        (run_dir / "extracted_invoice.json").resolve() if run_dir else None,
    ])


def resolve_findings(bundle_dir: Path, run_dir: Path | None, out_dir: Path):
    return find_first_existing([
        (run_dir / "findings.json").resolve() if run_dir else None,
        (out_dir / "findings.json").resolve(),
        (bundle_dir / "findings.json").resolve(),
    ])


def resolve_policy(bundle_dir: Path, manifest: dict, explicit: str | None):
    return find_first_existing([
        Path(explicit).resolve() if explicit else None,
        (bundle_dir / manifest.get("approval_policy_file", "")).resolve() if manifest.get("approval_policy_file") else None,
        (bundle_dir.parent / "shared" / "approval_policy.yaml").resolve(),
        (bundle_dir.parent.parent / "policy" / "approval_policy.yaml").resolve(),
    ])


def resolve_optional(run_dir: Path | None, bundle_dir: Path, filename: str):
    return find_first_existing([
        (run_dir / filename).resolve() if run_dir else None,
        (bundle_dir / filename).resolve(),
    ])


# ── finding categorization ────────────────────────────────────────────────────

# Agent G finding codes that go to FRAUD_RISK
FRAUD_RISK_CODES = {"DUPLICATE_INVOICE", "NEAR_APPROVAL_LIMIT"}

# Agent G finding codes that go to VENDOR_RISK
VENDOR_RISK_CODES = {"BANK_ACCOUNT_CHANGE", "BANK_CHANGE_HIGH_VALUE"}

# Agent-letter → category for all other agents
AGENT_TO_CATEGORY = {
    "C": "VENDOR_RISK",
    "D": "DATA_QUALITY",
    "E": "MATCHING",
    "F": "COMPLIANCE",
}


def categorize_findings(findings: list[dict]) -> dict[str, list[dict]]:
    """Group findings into the five defined categories."""
    categories: dict[str, list[dict]] = {
        "DATA_QUALITY": [],
        "MATCHING": [],
        "VENDOR_RISK": [],
        "COMPLIANCE": [],
        "FRAUD_RISK": [],
    }
    for f in findings:
        agent = f.get("agent", "")
        code = f.get("code", "")
        if agent == "G":
            if code in FRAUD_RISK_CODES:
                categories["FRAUD_RISK"].append(f)
            else:
                # BANK_ACCOUNT_CHANGE, BANK_CHANGE_HIGH_VALUE and any other G code
                categories["VENDOR_RISK"].append(f)
        else:
            cat = AGENT_TO_CATEGORY.get(agent)
            if cat:
                categories[cat].append(f)
            else:
                # Unknown agent — fall back to DATA_QUALITY
                categories["DATA_QUALITY"].append(f)
    return categories


# ── severity helpers ──────────────────────────────────────────────────────────

SEVERITY_RANK = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}


def highest_severity(findings: list[dict]) -> str | None:
    if not findings:
        return None
    return max(findings, key=lambda f: SEVERITY_RANK.get(f.get("severity", "LOW"), 0))["severity"]


def severity_counts(findings: list[dict]) -> dict:
    counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for f in findings:
        sev = f.get("severity", "LOW")
        if sev in counts:
            counts[sev] += 1
    return counts


# ── routing logic ─────────────────────────────────────────────────────────────

def determine_routing(
    findings: list[dict], invoice: dict, policy: dict
) -> tuple[str, str, str, bool, str]:
    """
    Returns:
        (recommended_action, assigned_to, assigned_role, requires_approval, approval_reason)
    """
    thresholds = policy.get("thresholds") or {}
    routing = policy.get("routing") or {}
    roles = routing.get("approval_roles") or {}

    auto_approve_below = float(thresholds.get("auto_approve_below", 500))
    senior_approval_above = float(thresholds.get("senior_approval_above", 10000))

    ap_bot = roles.get("auto_approve", "ap_bot")
    ap_manager = roles.get("standard_approval", "ap_manager")
    finance_controller = roles.get("senior_approval", "finance_controller")
    risk_officer = routing.get("risk_escalation_role", "risk_officer")

    total = invoice.get("total_amount") or 0.0
    codes = {f.get("code", "") for f in findings}
    sevs = {f.get("severity", "") for f in findings}

    # Override: DUPLICATE → BLOCK, unless also a bank-change high-value case
    if "DUPLICATE_INVOICE" in codes and not (codes & {"BANK_CHANGE_HIGH_VALUE", "BANK_ACCOUNT_CHANGE"}):
        return (
            "BLOCK",
            risk_officer,
            "risk_escalation",
            True,
            "Duplicate invoice detected — payment blocked pending investigation.",
        )

    # Override: new/unresolved vendor → standard approval (refined later by orchestrator)
    new_vendor_codes = {"NEW_VENDOR", "VENDOR_NOT_FOUND", "WEAK_MATCH"}
    if codes & new_vendor_codes:
        return (
            "HOLD_FOR_APPROVAL",
            ap_manager,
            "standard_approval",
            True,
            "New or unresolved vendor — approval required.",
        )

    # Override: low OCR confidence → manual review
    low_ocr_codes = {"LOW_OCR_CONFIDENCE", "LOW_CONFIDENCE_FIELDS"}
    if codes & low_ocr_codes:
        return (
            "HOLD_FOR_MANUAL_REVIEW",
            ap_manager,
            "standard_approval",
            True,
            "Low OCR confidence on key fields — manual verification required.",
        )

    # CRITICAL finding → risk_officer
    if "CRITICAL" in sevs:
        return (
            "ESCALATE_TO_RISK_OFFICER",
            risk_officer,
            "risk_escalation",
            True,
            "Critical finding requires risk officer review.",
        )

    # Amount > senior threshold → finance_controller
    if total > senior_approval_above:
        return (
            "HOLD_FOR_APPROVAL",
            finance_controller,
            "senior_approval",
            True,
            f"Invoice total ({total}) exceeds senior approval threshold ({senior_approval_above}).",
        )

    # Has any findings → ap_manager
    if findings:
        return (
            "HOLD_FOR_APPROVAL",
            ap_manager,
            "standard_approval",
            True,
            "Invoice has exceptions requiring approval.",
        )

    # Amount above auto-approve threshold (clean) → ap_manager
    if total > auto_approve_below:
        return (
            "HOLD_FOR_APPROVAL",
            ap_manager,
            "standard_approval",
            True,
            f"Invoice total ({total}) exceeds auto-approval threshold ({auto_approve_below}).",
        )

    # Clean + under threshold → auto-approve
    return (
        "AUTO_APPROVE",
        ap_bot,
        "auto_approve",
        False,
        f"Invoice is clean and below auto-approval threshold ({auto_approve_below}).",
    )


# ── exceptions.md writer ──────────────────────────────────────────────────────

def build_exceptions_md(
    invoice: dict,
    findings: list[dict],
    categories: dict[str, list[dict]],
    recommended_action: str,
    assigned_to: str,
    approval_reason: str,
) -> str:
    invoice_id = invoice.get("invoice_id", "N/A")
    vendor_name = invoice.get("vendor_name", "N/A")
    vendor_id = invoice.get("vendor_id", "N/A")
    total = invoice.get("total_amount", 0.0)
    currency = invoice.get("currency", "USD")
    invoice_date = invoice.get("invoice_date", "N/A")
    top_severity = highest_severity(findings) or "NONE"

    lines = []
    lines.append("# Invoice Exception Report")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append("| Field | Value |")
    lines.append("|---|---|")
    lines.append(f"| Invoice ID | {invoice_id} |")
    lines.append(f"| Vendor | {vendor_name} ({vendor_id}) |")

    if isinstance(total, (int, float)):
        lines.append(f"| Amount | {currency} {total:,.2f} |")
    else:
        lines.append(f"| Amount | {currency} N/A |")
    lines.append(f"| Invoice Date | {invoice_date} |")
    lines.append(f"| Total Findings | {len(findings)} |")
    lines.append(f"| Highest Severity | {top_severity} |")
    lines.append(f"| Recommended Action | {recommended_action} |")
    lines.append(f"| Assigned To | {assigned_to} |")
    lines.append(f"| Reason | {approval_reason} |")
    lines.append("")

    if not findings:
        lines.append("## Result")
        lines.append("")
        lines.append("No exceptions — invoice is clean.")
        return "\n".join(lines)

    # Per-category tables
    lines.append("## Findings by Category")
    lines.append("")
    for cat, cat_findings in categories.items():
        if not cat_findings:
            continue
        lines.append(f"### {cat.replace('_', ' ').title()}")
        lines.append("")
        lines.append("| Severity | Code | Message | Recommended Action |")
        lines.append("|---|---|---|---|")
        for f in cat_findings:
            sev = f.get("severity", "")
            code = f.get("code", "")
            msg = f.get("message", "").replace("|", "\\|")
            action = f.get("recommended_action", "")
            lines.append(f"| {sev} | {code} | {msg} | {action} |")
        lines.append("")

    # Evidence summary
    lines.append("## Evidence Summary")
    lines.append("")
    for i, f in enumerate(findings, 1):
        evidence = f.get("evidence") or {}
        if evidence:
            lines.append(f"**Finding {i} — {f.get('code')}:**")
            lines.append("")
            lines.append("```json")
            lines.append(json.dumps(evidence, indent=2))
            lines.append("```")
            lines.append("")

    # Next steps
    lines.append("## Next Steps")
    lines.append("")
    if recommended_action == "BLOCK":
        lines.append("1. Do **not** process payment.")
        lines.append("2. Investigate duplicate invoice with vendor.")
        lines.append("3. Escalate to risk officer before any further action.")
    elif recommended_action == "AUTO_APPROVE":
        lines.append("1. Invoice cleared for automatic posting.")
        lines.append("2. No manual action required.")
    elif recommended_action == "ESCALATE_TO_RISK_OFFICER":
        lines.append("1. Route to risk officer for review.")
        lines.append("2. Do not approve until risk officer clears all critical findings.")
    else:
        lines.append(f"1. Route invoice to **{assigned_to}** for review.")
        lines.append("2. Address all findings listed above before approval.")
        lines.append("3. Update invoice status once resolved.")

    return "\n".join(lines)


# ── approval_packet builder ───────────────────────────────────────────────────

def build_approval_packet(
    invoice: dict,
    findings: list[dict],
    categories: dict[str, list[dict]],
    recommended_action: str,
    assigned_to: str,
    assigned_role: str,
    requires_approval: bool,
    approval_reason: str,
    supporting_evidence: list[dict],
) -> dict:
    by_category = {cat: len(f) for cat, f in categories.items() if f}
    return {
        "invoice_id": invoice.get("invoice_id"),
        "vendor_id": invoice.get("vendor_id"),
        "vendor_name": invoice.get("vendor_name"),
        "total_amount": invoice.get("total_amount"),
        "currency": invoice.get("currency", "USD"),
        "recommended_action": recommended_action,
        "assigned_to": assigned_to,
        "assigned_role": assigned_role,
        "exception_summary": {
            "total_findings": len(findings),
            "by_severity": severity_counts(findings),
            "by_category": by_category,
            "finding_codes": [f.get("code") for f in findings],
        },
        "requires_approval": requires_approval,
        "approval_reason": approval_reason,
        "supporting_evidence": supporting_evidence,
    }


# ── context packet updater (mirrors Agent G) ──────────────────────────────────

def update_context_packet(out_dir: Path, summary: dict):
    context_path = out_dir / "context_packet.json"
    context = {}
    if context_path.exists():
        existing = read_json(context_path)
        if isinstance(existing, dict):
            context = existing
    context["exception_triage"] = summary
    write_json(context_path, context)


# ── main entry ────────────────────────────────────────────────────────────────

def run_agent_h(args):
    bundle_dir = Path(args.bundle_dir).resolve()
    run_dir = Path(args.run_dir).resolve() if args.run_dir else None
    out_dir = Path(args.out_dir).resolve() if args.out_dir else (run_dir or bundle_dir)
    manifest = get_manifest(bundle_dir)

    # Load extracted invoice
    extraction_path = resolve_extraction(bundle_dir, run_dir, None)
    if not extraction_path:
        raise FileNotFoundError("No extracted_invoice.json or mock_extraction.json found.")
    invoice = read_json(extraction_path)

    # Load accumulated findings (may be empty if no prior agents ran)
    findings_path = resolve_findings(bundle_dir, run_dir, out_dir)
    findings: list[dict] = (
        read_json(findings_path)
        if findings_path and findings_path.exists()
        else []
    )

    # Load policy
    policy_path = resolve_policy(bundle_dir, manifest, args.policy)
    policy = read_yaml(policy_path) if policy_path else {}

    # Load optional supporting evidence files
    supporting_evidence = []
    for filename in ("match_result.json", "vendor_resolution_result.json"):
        p = resolve_optional(run_dir, bundle_dir, filename)
        if p and p.exists():
            try:
                supporting_evidence.append({"source": filename, "data": read_json(p)})
            except Exception:
                pass

    # Categorize findings
    categories = categorize_findings(findings)

    # Determine routing
    recommended_action, assigned_to, assigned_role, requires_approval, approval_reason = (
        determine_routing(findings, invoice, policy)
    )

    # Build outputs
    md_content = build_exceptions_md(
        invoice, findings, categories, recommended_action, assigned_to, approval_reason
    )
    packet = build_approval_packet(
        invoice, findings, categories,
        recommended_action, assigned_to, assigned_role,
        requires_approval, approval_reason, supporting_evidence,
    )

    # Write outputs
    md_path = out_dir / "exceptions.md"
    packet_path = out_dir / "approval_packet.json"
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(md_content, encoding="utf-8")
    write_json(packet_path, packet)

    # Update context_packet.json["exception_triage"]
    triage_summary = {
        "invoice_id": invoice.get("invoice_id"),
        "recommended_action": recommended_action,
        "assigned_to": assigned_to,
        "assigned_role": assigned_role,
        "requires_approval": requires_approval,
        "total_findings": len(findings),
        "highest_severity": highest_severity(findings),
        "finding_codes": [f.get("code") for f in findings],
    }
    update_context_packet(out_dir, triage_summary)

    return {
        "result_path": str(packet_path),
        "exceptions_path": str(md_path),
        "context_path": str(out_dir / "context_packet.json"),
        "findings_path": str(findings_path) if findings_path else None,
        "recommended_action": recommended_action,
        "assigned_to": assigned_to,
        "requires_approval": requires_approval,
        "finding_codes": triage_summary["finding_codes"],
    }


if __name__ == "__main__":
    import sys

    parser = argparse.ArgumentParser(description="Agent H: Exception triage & approval routing")
    parser.add_argument("--bundle-dir", required=True)
    parser.add_argument("--run-dir", default=None)
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--policy", default=None)
    result = run_agent_h(parser.parse_args())
    print(json.dumps(result, indent=2))
