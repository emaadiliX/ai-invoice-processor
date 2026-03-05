import argparse
import json
import re
from datetime import date, datetime
from pathlib import Path

import jsonschema
import yaml

TOLERANCE = 0.01
REQUIRED_FIELDS = ["invoice_id", "invoice_date", "vendor_name", "currency", "line_items", "subtotal", "total_amount"]


#shared I/O helpers (mirrors Agent C)

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


#path resolvers (mirrors Agent C)

def resolve_policy(bundle_dir: Path, manifest: dict, explicit: str | None):
    return find_first_existing([
        Path(explicit).resolve() if explicit else None,
        (bundle_dir / manifest.get("approval_policy_file", "")).resolve() if manifest.get("approval_policy_file") else None,
        (bundle_dir.parent / "shared" / "approval_policy.yaml").resolve(),
        (bundle_dir.parent.parent / "policy" / "approval_policy.yaml").resolve(),
    ])


def resolve_extraction(bundle_dir: Path, run_dir: Path | None, explicit: str | None):
    return find_first_existing([
        Path(explicit).resolve() if explicit else None,
        (bundle_dir / "mock_extraction.json").resolve(),
        (bundle_dir / "extracted_invoice.json").resolve(),
        (run_dir / "extracted_invoice.json").resolve() if run_dir else None,
    ])


def resolve_schema(explicit: str | None):
    return find_first_existing([
        Path(explicit).resolve() if explicit else None,
        (Path(__file__).parent.parent / "schemas" / "extracted_invoice_schema.json").resolve(),
    ])


#finding builder (mirrors Agent C _build_finding)

def make_finding(code: str, severity: str, message: str, evidence: dict, action: str = "manual_review"):
    return {
        "agent": "D",
        "code": code,
        "severity": severity,
        "message": message,
        "evidence": evidence,
        "recommended_action": action,
    }


#8 validation checks

def check_required_fields(invoice: dict, schema_path: Path | None) -> list[dict]:
    findings = []

    # Manual check for the 7 business-critical fields
    for field in REQUIRED_FIELDS:
        value = invoice.get(field)
        missing = value is None or (isinstance(value, (str, list)) and len(value) == 0)
        if missing:
            findings.append(make_finding(
                "MANDATORY_FIELD_MISSING", "HIGH",
                f"Required field '{field}' is missing or empty.",
                {"field": field},
                "fix_extraction",
            ))

    # Structural schema validation (informational supplement)
    if schema_path and schema_path.exists() and not findings:
        schema = read_json(schema_path)
        try:
            jsonschema.validate(instance=invoice, schema=schema)
        except jsonschema.ValidationError as exc:
            findings.append(make_finding(
                "MANDATORY_FIELD_MISSING", "HIGH",
                f"Schema validation failed: {exc.message}",
                {"schema_path": str(schema_path), "detail": exc.message},
                "fix_extraction",
            ))

    return findings


def check_dates(invoice: dict) -> list[dict]:
    findings = []
    today = date.today()

    def parse_date(value, field_name):
        if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            findings.append(make_finding(
                "INVALID_DATE_FORMAT", "MEDIUM",
                f"Field '{field_name}' has an invalid date format: '{value}'.",
                {"field": field_name, "value": value},
            ))
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            findings.append(make_finding(
                "INVALID_DATE_FORMAT", "MEDIUM",
                f"Field '{field_name}' cannot be parsed as a real date: '{value}'.",
                {"field": field_name, "value": value},
            ))
            return None

    inv_date = parse_date(invoice.get("invoice_date"), "invoice_date")
    if inv_date and inv_date > today:
        findings.append(make_finding(
            "FUTURE_INVOICE_DATE", "MEDIUM",
            f"Invoice date '{inv_date}' is in the future.",
            {"invoice_date": str(inv_date), "today": str(today)},
        ))

    due_raw = invoice.get("due_date")
    if due_raw:
        due_date = parse_date(due_raw, "due_date")
        if inv_date and due_date and due_date <= inv_date:
            findings.append(make_finding(
                "INVALID_DATE_FORMAT", "MEDIUM",
                f"due_date '{due_date}' is not after invoice_date '{inv_date}'.",
                {"invoice_date": str(inv_date), "due_date": str(due_date)},
            ))

    return findings


def check_currency(invoice: dict) -> list[dict]:
    currency = invoice.get("currency", "")
    if not isinstance(currency, str) or not re.fullmatch(r"[A-Z]{3}", currency):
        return [make_finding(
            "INVALID_CURRENCY_CODE", "MEDIUM",
            f"Currency '{currency}' is not a valid 3-letter ISO code.",
            {"currency": currency},
        )]
    return []


def check_line_item_math(invoice: dict) -> list[dict]:
    findings = []
    for item in invoice.get("line_items") or []:
        qty = item.get("quantity")
        price = item.get("unit_price")
        total = item.get("total")
        if None in (qty, price, total):
            continue
        expected = round(qty * price, 2)
        if abs(expected - total) > TOLERANCE:
            findings.append(make_finding(
                "LINE_ITEM_CALC_ERROR", "HIGH",
                f"Line {item.get('line_id')}: {qty} × {price} = {expected}, but total is {total}.",
                {"line_id": item.get("line_id"), "quantity": qty, "unit_price": price,
                 "expected_total": expected, "actual_total": total, "diff": round(total - expected, 4)},
            ))
    return findings


def check_subtotal(invoice: dict) -> list[dict]:
    line_items = invoice.get("line_items") or []
    subtotal = invoice.get("subtotal")
    if subtotal is None or not line_items:
        return []
    computed = round(sum(i.get("total", 0) for i in line_items), 2)
    if abs(computed - subtotal) > TOLERANCE:
        return [make_finding(
            "SUBTOTAL_MISMATCH", "HIGH",
            f"sum(line_items.total) = {computed} but subtotal = {subtotal}.",
            {"computed_subtotal": computed, "stated_subtotal": subtotal, "diff": round(subtotal - computed, 4)},
        )]
    return []


def check_header_total(invoice: dict) -> list[dict]:
    subtotal = invoice.get("subtotal")
    tax = invoice.get("tax_amount") or 0.0
    total = invoice.get("total_amount")
    if subtotal is None or total is None:
        return []
    expected = round(subtotal + tax, 2)
    if abs(expected - total) > TOLERANCE:
        return [make_finding(
            "HEADER_TOTAL_MISMATCH", "HIGH",
            f"subtotal({subtotal}) + tax({tax}) = {expected}, but total_amount = {total}.",
            {"subtotal": subtotal, "tax_amount": tax, "expected_total": expected,
             "actual_total": total, "diff": round(total - expected, 4)},
        )]
    return []


def check_po_required(invoice: dict, policy: dict) -> list[dict]:
    threshold = float((policy.get("compliance") or {}).get("require_po_for_invoices_above", 1000))
    total = invoice.get("total_amount") or 0.0
    po_ref = invoice.get("po_reference")
    if total > threshold and not po_ref:
        return [make_finding(
            "MISSING_PO_REFERENCE", "MEDIUM",
            f"Invoice total {total} exceeds ${threshold} but has no PO reference.",
            {"total_amount": total, "po_required_above": threshold, "po_reference": po_ref},
        )]
    return []


def check_credit_note(invoice: dict) -> list[dict]:
    total = invoice.get("total_amount")
    if isinstance(total, (int, float)) and total < 0:
        return [make_finding(
            "CREDIT_NOTE_DETECTED", "LOW",
            f"Invoice total is negative ({total}), indicating a credit note.",
            {"total_amount": total},
            "flag_as_credit_note",
        )]
    return []


def check_ocr_confidence(invoice: dict) -> list[dict]:
    low_conf = invoice.get("low_confidence_fields", [])
    if low_conf:
        return [make_finding(
            "LOW_OCR_CONFIDENCE", "MEDIUM",
            f"Low confidence detected in fields: {', '.join(low_conf)}",
            {"fields": low_conf},
            "manual_verification"
        )]
    return []

#orchestrate all checks 

def validate_invoice(invoice: dict, policy: dict, schema_path: Path | None) -> list[dict]:
    findings = []
    findings.extend(check_required_fields(invoice, schema_path))
    findings.extend(check_dates(invoice))
    findings.extend(check_currency(invoice))
    findings.extend(check_line_item_math(invoice))
    findings.extend(check_subtotal(invoice))
    findings.extend(check_header_total(invoice))
    findings.extend(check_po_required(invoice, policy))
    findings.extend(check_credit_note(invoice))
    findings.extend(check_ocr_confidence(invoice))
    return findings

#output writers (mirrors Agent C)

def update_context_packet(out_dir: Path, summary: dict):
    context_path = out_dir / "context_packet.json"
    context = {}
    if context_path.exists():
        existing = read_json(context_path)
        if isinstance(existing, dict):
            context = existing
    context["field_validation"] = summary
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

def run_agent_d(args):
    bundle_dir = Path(args.bundle_dir).resolve()
    run_dir = Path(args.run_dir).resolve() if args.run_dir else None
    out_dir = Path(args.out_dir).resolve() if args.out_dir else (run_dir or bundle_dir)
    manifest = get_manifest(bundle_dir)

    extraction_path = resolve_extraction(bundle_dir, run_dir, args.extracted_invoice)
    if not extraction_path:
        raise FileNotFoundError("No extracted_invoice.json or mock_extraction.json found.")
    invoice = read_json(extraction_path)

    policy_path = resolve_policy(bundle_dir, manifest, args.policy)
    policy = read_yaml(policy_path) if policy_path else {}
    schema_path = resolve_schema(args.schema)

    findings = validate_invoice(invoice, policy, schema_path)

    high = [f for f in findings if f["severity"] == "HIGH"]
    medium = [f for f in findings if f["severity"] == "MEDIUM"]
    low = [f for f in findings if f["severity"] == "LOW"]
    passed = len(findings) == 0

    summary = {
        "invoice_id": invoice.get("invoice_id"),
        "passed": passed,
        "finding_count": len(findings),
        "high": len(high),
        "medium": len(medium),
        "low": len(low),
        "finding_codes": [f["code"] for f in findings],
    }

    write_json(out_dir / "validation_result.json", {"summary": summary, "findings": findings})
    update_context_packet(out_dir, summary)
    append_findings(out_dir, findings)

    return {
        "result_path": str(out_dir / "validation_result.json"),
        "context_path": str(out_dir / "context_packet.json"),
        "findings_path": str(out_dir / "findings.json"),
        "passed": passed,
        "finding_codes": summary["finding_codes"],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Agent D: Invoice field validation")
    parser.add_argument("--bundle-dir", required=True)
    parser.add_argument("--extracted-invoice", default=None)
    parser.add_argument("--run-dir", default=None)
    parser.add_argument("--policy", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--out-dir", default=None)
    import sys
    result = run_agent_d(parser.parse_args())
    print(json.dumps(result, indent=2))
