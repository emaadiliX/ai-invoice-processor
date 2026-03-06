"""Agent E – Matching Engine: compares invoice lines against PO and GRN."""

import argparse
import json
from pathlib import Path

import yaml
from rapidfuzz import fuzz

FUZZY_THRESHOLD = 75


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


def resolve_extraction(bundle_dir, explicit):
    return find_first_existing([
        Path(explicit).resolve() if explicit else None,
        (bundle_dir / "mock_extraction.json").resolve(),
        (bundle_dir / "extracted_invoice.json").resolve(),
    ])


def resolve_purchase_order(bundle_dir, manifest, explicit):
    ref = manifest.get("purchase_order_file")
    candidates = [Path(explicit).resolve() if explicit else None]
    if ref:
        candidates.append((bundle_dir / ref).resolve())
        if ref.endswith(".txt"):
            candidates.append((bundle_dir / ref.rsplit(".txt", 1)[0]).resolve())
    candidates.append((bundle_dir / "purchase_order.json").resolve())
    return find_first_existing(candidates)


def resolve_grn_files(bundle_dir, manifest, explicit):
    if explicit:
        p = Path(explicit).resolve()
        return [p] if p.exists() else []

    grn_array = manifest.get("grn_files")
    if isinstance(grn_array, list) and grn_array:
        found = []
        for fname in grn_array:
            cands = [(bundle_dir / fname).resolve()]
            if fname.endswith(".txt"):
                cands.append((bundle_dir / fname.rsplit(".txt", 1)[0]).resolve())
            hit = find_first_existing(cands)
            if hit:
                found.append(hit)
        if found:
            return found

    ref = manifest.get("grn_file")
    candidates = []
    if ref:
        candidates.append((bundle_dir / ref).resolve())
        if ref.endswith(".txt"):
            candidates.append((bundle_dir / ref.rsplit(".txt", 1)[0]).resolve())
    candidates.append((bundle_dir / "grn.json").resolve())
    hit = find_first_existing(candidates)
    return [hit] if hit else []


def resolve_policy(bundle_dir, manifest, explicit):
    return find_first_existing([
        Path(explicit).resolve() if explicit else None,
        (bundle_dir / manifest.get("approval_policy_file", "")).resolve()
            if manifest.get("approval_policy_file") else None,
        (bundle_dir.parent / "shared" / "approval_policy.yaml").resolve(),
        (bundle_dir.parent / "policy" / "approval_policy.yaml").resolve(),
    ])


def resolve_exchange_rates(bundle_dir, manifest, explicit):
    return find_first_existing([
        Path(explicit).resolve() if explicit else None,
        (bundle_dir / manifest.get("exchange_rates_file", "")).resolve()
            if manifest.get("exchange_rates_file") else None,
        (bundle_dir / "exchange_rates.json").resolve(),
    ])


def get_tolerances(policy):
    tol = policy.get("tolerance") or {}
    tols = policy.get("tolerances") or {}
    if not isinstance(tol, dict):
        tol = {}
    if not isinstance(tols, dict):
        tols = {}
    return {
        "price_variance_pct": float(tol.get("price_variance_pct") or tols.get("price_variance_pct") or 2.0),
        "quantity_variance_pct": float(tol.get("quantity_variance_pct") or tols.get("qty_variance_pct") or 1.0),
    }


def convert_price(price, from_cur, to_cur, rates_data):
    ref = rates_data.get("reference_currency", "USD")
    rates = rates_data.get("rates", {})

    if from_cur == ref:
        price_in_ref, from_rate = price, 1.0
    else:
        from_rate = rates.get(from_cur)
        if from_rate is None:
            raise ValueError(f"No exchange rate found for {from_cur}")
        price_in_ref = price * from_rate

    if to_cur == ref:
        result, to_rate = price_in_ref, 1.0
    else:
        to_rate = rates.get(to_cur)
        if to_rate is None:
            raise ValueError(f"No exchange rate found for {to_cur}")
        result = price_in_ref / to_rate

    detail = {
        "invoice_currency": from_cur, "po_currency": to_cur,
        "reference_currency": ref,
        "invoice_to_ref_rate": from_rate,
        "ref_to_po_rate": 1.0 / to_rate if to_cur != ref else 1.0,
        "original_price": price, "converted_price": round(result, 4),
    }
    return round(result, 4), detail


def match_lines(invoice_lines, po_lines):
    po_by_id = {l["line_id"]: l for l in po_lines}
    used = set()
    pairs = []
    leftover = []

    for inv in invoice_lines:
        lid = inv.get("line_id")
        if lid in po_by_id and lid not in used:
            pairs.append({"invoice_line": inv, "po_line": po_by_id[lid], "matched_by": "line_id"})
            used.add(lid)
        else:
            leftover.append(inv)

    remaining_po = [l for l in po_lines if l["line_id"] not in used]
    for inv in leftover:
        desc = inv.get("description", "")
        best_score, best_po = 0, None
        for po in remaining_po:
            score = fuzz.WRatio(desc, po.get("description", ""))
            if score > best_score:
                best_score, best_po = score, po

        if best_po and best_score >= FUZZY_THRESHOLD:
            pairs.append({"invoice_line": inv, "po_line": best_po, "matched_by": "description_fuzzy"})
            remaining_po.remove(best_po)
        else:
            pairs.append({"invoice_line": inv, "po_line": None, "matched_by": "none"})

    return pairs


def aggregate_grn_quantities(grn_list):
    totals = {}
    for grn in grn_list:
        for item in grn.get("line_items", []):
            lid = item.get("line_id")
            if lid is not None:
                totals[lid] = totals.get(lid, 0) + item.get("quantity_received", 0)
    return totals


def make_finding(code, severity, message, evidence):
    return {
        "agent": "E", "code": code, "severity": severity,
        "message": message, "evidence": evidence,
        "recommended_action": "manual_review",
    }


def update_context_packet(out_dir, match_result):
    ctx_path = out_dir / "context_packet.json"
    ctx = {}
    if ctx_path.exists():
        loaded = read_json(ctx_path)
        if isinstance(loaded, dict):
            ctx = loaded
    ctx["matching"] = {
        "match_type": match_result["match_type"],
        "overall_status": match_result["overall_status"],
        "po_id": match_result["po_id"],
        "invoice_id": match_result["invoice_id"],
        "findings_count": match_result["findings_count"],
        "tolerances_applied": match_result["tolerances_applied"],
    }
    write_json(ctx_path, ctx)


def append_findings(out_dir, new_findings):
    if not new_findings:
        return
    findings_path = out_dir / "findings.json"
    existing = []
    if findings_path.exists():
        loaded = read_json(findings_path)
        if isinstance(loaded, list):
            existing = loaded
    existing.extend(new_findings)
    write_json(findings_path, existing)


def perform_matching(invoice, po, grn_list, tolerances, exchange_rates):
    invoice_id = invoice.get("invoice_id", "UNKNOWN")
    po_ref = invoice.get("po_reference")
    findings = []

    if po is None or not po_ref:
        findings.append(make_finding(
            "NO_PO_MATCH", "MEDIUM",
            f"Invoice {invoice_id} has no purchase order reference.",
            {"invoice_id": invoice_id, "po_reference": po_ref},
        ))
        return {
            "match_type": "NO_PO_MATCH", "overall_status": "NO_PO_MATCH",
            "po_id": None, "invoice_id": invoice_id,
            "currency_conversion": None, "line_results": [],
            "findings_count": 1, "tolerances_applied": tolerances,
        }, findings

    match_type = "3WAY" if grn_list else "2WAY"
    po_id = po.get("po_id", "UNKNOWN")

    inv_cur = invoice.get("currency", "USD")
    po_cur = po.get("currency", "USD")
    need_fx = inv_cur != po_cur
    fx_detail = None

    if need_fx and not exchange_rates:
        raise ValueError(f"Currency mismatch ({inv_cur} vs {po_cur}) but no exchange rates provided")

    pairs = match_lines(invoice.get("line_items", []), po.get("line_items", []))
    grn_qtys = aggregate_grn_quantities(grn_list) if grn_list else {}

    line_results = []
    any_mismatch = False

    for pair in pairs:
        inv_line = pair["invoice_line"]
        po_line = pair["po_line"]
        inv_lid = inv_line.get("line_id")

        if po_line is None:
            findings.append(make_finding(
                "UNMATCHED_INVOICE_LINE", "HIGH",
                f"Invoice line {inv_lid} has no matching PO line.",
                {"invoice_line_id": inv_lid, "description": inv_line.get("description")},
            ))
            line_results.append({
                "invoice_line_id": inv_lid, "po_line_id": None, "matched_by": "none",
                "invoice_qty": inv_line.get("quantity"), "po_qty": None,
                "qty_variance_pct": None, "qty_status": "FAIL",
                "invoice_unit_price": inv_line.get("unit_price"), "po_unit_price": None,
                "price_variance_pct": None, "price_status": "FAIL",
                "grn_qty_received": None, "grn_status": "N/A", "line_status": "MISMATCH",
            })
            any_mismatch = True
            continue

        inv_qty = inv_line.get("quantity", 0)
        po_qty = po_line.get("quantity_ordered", 0)
        inv_price = inv_line.get("unit_price", 0)
        po_price = po_line.get("agreed_unit_price", 0)
        po_lid = po_line.get("line_id")

        if need_fx:
            price_cmp, conv = convert_price(inv_price, inv_cur, po_cur, exchange_rates)
            if fx_detail is None:
                fx_detail = conv
        else:
            price_cmp = inv_price

        qty_var = round(abs(inv_qty - po_qty) / po_qty * 100, 2) if po_qty else 0.0
        qty_ok = qty_var <= tolerances["quantity_variance_pct"]

        price_var = round(abs(price_cmp - po_price) / po_price * 100, 2) if po_price else 0.0
        price_ok = price_var <= tolerances["price_variance_pct"]

        grn_received = grn_qtys.get(po_lid)
        if match_type == "3WAY" and grn_received is not None:
            grn_ok = inv_qty <= grn_received
            grn_status = "PASS" if grn_ok else "FAIL"
        elif match_type == "3WAY":
            grn_ok, grn_status = False, "FAIL"
        else:
            grn_ok, grn_status = True, "N/A"

        line_ok = qty_ok and price_ok and grn_status != "FAIL"
        if not line_ok:
            any_mismatch = True

        if not qty_ok:
            findings.append(make_finding(
                "QUANTITY_VARIANCE", "HIGH",
                f"Line {inv_lid}: quantity variance {qty_var}% exceeds tolerance {tolerances['quantity_variance_pct']}%.",
                {"invoice_line_id": inv_lid, "invoice_qty": inv_qty, "po_qty": po_qty,
                 "variance_pct": qty_var, "tolerance_pct": tolerances["quantity_variance_pct"]},
            ))
        if not price_ok:
            findings.append(make_finding(
                "PRICE_VARIANCE", "HIGH",
                f"Line {inv_lid}: price variance {price_var}% exceeds tolerance {tolerances['price_variance_pct']}%.",
                {"invoice_line_id": inv_lid, "invoice_unit_price": inv_price, "po_unit_price": po_price,
                 "variance_pct": price_var, "tolerance_pct": tolerances["price_variance_pct"],
                 "currency_converted": need_fx},
            ))
        if grn_status == "FAIL" and grn_received is not None:
            findings.append(make_finding(
                "GRN_QTY_SHORTAGE", "MEDIUM",
                f"Line {inv_lid}: invoice qty {inv_qty} exceeds GRN received qty {grn_received}.",
                {"invoice_line_id": inv_lid, "invoice_qty": inv_qty, "grn_qty_received": grn_received},
            ))
        elif grn_status == "FAIL":
            findings.append(make_finding(
                "MISSING_GRN", "MEDIUM",
                f"Line {inv_lid}: no GRN data available for 3-way match.",
                {"invoice_line_id": inv_lid, "po_line_id": po_lid},
            ))

        line_results.append({
            "invoice_line_id": inv_lid, "po_line_id": po_lid,
            "matched_by": pair["matched_by"],
            "invoice_qty": inv_qty, "po_qty": po_qty,
            "qty_variance_pct": qty_var, "qty_status": "PASS" if qty_ok else "FAIL",
            "invoice_unit_price": inv_price, "po_unit_price": po_price,
            "price_variance_pct": price_var, "price_status": "PASS" if price_ok else "FAIL",
            "grn_qty_received": grn_received, "grn_status": grn_status,
            "line_status": "MATCH" if line_ok else "MISMATCH",
        })

    if match_type == "2WAY":
        findings.append(make_finding(
            "NO_GRN", "MEDIUM",
            "No Goods Receipt Note available — only 2-way match performed. GRN required for auto-post.",
            {"match_type": "2WAY", "po_id": po_id},
        ))

    if any_mismatch:
        bad = sum(1 for r in line_results if r["line_status"] == "MISMATCH")
        overall = "MISMATCH" if bad == len(line_results) else "PARTIAL_MATCH"
    else:
        overall = "FULL_MATCH"

    result = {
        "match_type": match_type, "overall_status": overall,
        "po_id": po_id, "invoice_id": invoice_id,
        "currency_conversion": fx_detail, "line_results": line_results,
        "findings_count": len(findings), "tolerances_applied": tolerances,
    }
    return result, findings


def run_agent_e(args):
    bundle_dir = Path(args.bundle_dir).resolve()
    out_dir = Path(args.out_dir).resolve() if args.out_dir else bundle_dir
    manifest = get_manifest(bundle_dir)

    extraction_path = resolve_extraction(bundle_dir, args.extracted_invoice)
    if not extraction_path:
        raise FileNotFoundError("No extracted_invoice.json or mock_extraction.json found.")
    invoice = read_json(extraction_path)

    po = None
    if invoice.get("po_reference") or args.purchase_order or manifest.get("purchase_order_file"):
        po_path = resolve_purchase_order(bundle_dir, manifest, args.purchase_order)
        if po_path:
            po = read_json(po_path)

    grn_paths = resolve_grn_files(bundle_dir, manifest, args.grn)
    grn_list = [read_json(p) for p in grn_paths]

    policy_path = resolve_policy(bundle_dir, manifest, args.policy)
    policy = read_yaml(policy_path) if policy_path else {}
    tolerances = get_tolerances(policy)

    exchange_rates = None
    inv_cur = invoice.get("currency", "USD")
    po_cur = po.get("currency", "USD") if po else "USD"
    if inv_cur != po_cur:
        er_path = resolve_exchange_rates(bundle_dir, manifest, args.exchange_rates)
        if er_path:
            exchange_rates = read_json(er_path)

    match_result, new_findings = perform_matching(invoice, po, grn_list, tolerances, exchange_rates)

    write_json(out_dir / "match_result.json", match_result)
    update_context_packet(out_dir, match_result)
    append_findings(out_dir, new_findings)

    return {
        "result_path": str(out_dir / "match_result.json"),
        "context_path": str(out_dir / "context_packet.json"),
        "findings_path": str(out_dir / "findings.json"),
        "match_type": match_result["match_type"],
        "overall_status": match_result["overall_status"],
        "findings_count": match_result["findings_count"],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Agent E: PO/GRN/Invoice matching engine")
    parser.add_argument("--bundle-dir", required=True)
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--extracted-invoice", default=None)
    parser.add_argument("--purchase-order", default=None)
    parser.add_argument("--grn", default=None)
    parser.add_argument("--policy", default=None)
    parser.add_argument("--exchange-rates", default=None)
    print(json.dumps(run_agent_e(parser.parse_args()), indent=2))
