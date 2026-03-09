import argparse
import datetime
import json
import re
import shutil
import uuid
from pathlib import Path

import yaml



def write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=4, ensure_ascii=False) + "\n", encoding="utf-8")


def classify_file(filename: str) -> str:
    """Simple rule-based classification based on filename."""
    lower = filename.lower()
    if "invoice" in lower and lower.endswith(('.pdf', '.png', '.jpg')):
        return "invoice_document"
    elif "purchase_order" in lower or "po" in lower:
        return "purchase_order_data"
    elif "grn" in lower or "goods_receipt" in lower:
        return "goods_receipt_data"
    elif "vendor_master" in lower:
        return "vendor_master_record"
    elif "policy" in lower or "approval" in lower:
        return "approval_policy"
    elif "tax" in lower:
        return "tax_rules"
    else:
        return "unknown_file"


def _extract_json_fields(obj, prefix="$", depth=0, max_depth=2) -> dict:
    """Recursively extract scalar fields with JSON-path notation, up to max_depth."""
    fields = {}
    if depth > max_depth:
        return fields
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}"
            if isinstance(v, (str, int, float, bool)) or v is None:
                fields[path] = v
            elif isinstance(v, list):
                fields[f"{path}[]"] = f"array({len(v)} items)"
                if v and isinstance(v[0], dict) and depth < max_depth:
                    for sub_k, sub_v in v[0].items():
                        sub_path = f"{path}[0].{sub_k}"
                        if isinstance(sub_v, (str, int, float, bool)) or sub_v is None:
                            fields[sub_path] = sub_v
            elif isinstance(v, dict) and depth < max_depth:
                fields.update(_extract_json_fields(v, prefix=path, depth=depth + 1, max_depth=max_depth))
    return fields


def build_evidence_index_entry(file_path: Path, file_type: str, source: str) -> dict:
    """Build an evidence index entry describing key fields and their locations in a file."""
    entry = {"file_type": file_type, "source": source, "fields": {}}

    suffix = file_path.suffix.lower()
    if suffix == ".json":
        try:
            content = json.loads(file_path.read_text(encoding="utf-8"))
            entry["fields"] = _extract_json_fields(content)
        except Exception as e:
            entry["note"] = f"Could not parse JSON: {e}"
    elif suffix in (".yaml", ".yml"):
        try:
            content = yaml.safe_load(file_path.read_text(encoding="utf-8"))
            if isinstance(content, dict):
                entry["fields"] = _extract_json_fields(content)
        except Exception as e:
            entry["note"] = f"Could not parse YAML: {e}"
    elif suffix in (".pdf", ".png", ".jpg", ".jpeg"):
        entry["note"] = "Binary document — field extraction performed by Agent B (OCR)"
    else:
        entry["note"] = "Non-structured file — no field extraction"

    return entry


IGNORED_FILES = {".gitignore", ".DS_Store", "thumbs.db"}
IGNORED_SUFFIXES = {".pyc", ".pyo", ".log"}


def _should_ignore(filename: str) -> bool:
    return filename.lower() in IGNORED_FILES or Path(filename).suffix.lower() in IGNORED_SUFFIXES


def compute_risk_indicators(files: list, vendor_ids: list, run_dir: Path) -> list:
    """Derive early-warning risk flags from the file manifest and known vendor master."""
    indicators = []
    file_types = {f["type"] for f in files}

    if "purchase_order_data" not in file_types:
        indicators.append({
            "code": "MISSING_PO_REFERENCE",
            "severity": "HIGH",
            "detail": "No purchase order file found in bundle — invoice cannot be matched to a PO."
        })

    if "goods_receipt_data" not in file_types:
        indicators.append({
            "code": "NO_GRN_PRESENT",
            "severity": "MEDIUM",
            "detail": "No Goods Receipt Note found — 3-way match not possible; 2-way match only."
        })

    vendor_master_path = run_dir / "vendor_master.json"
    if vendor_ids and vendor_master_path.exists():
        try:
            master = json.loads(vendor_master_path.read_text(encoding="utf-8"))
            known_ids = {v.get("vendor_id") for v in master}
            unknown = [vid for vid in vendor_ids if vid not in known_ids]
            if unknown:
                indicators.append({
                    "code": "NEW_VENDOR_RISK",
                    "severity": "HIGH",
                    "detail": f"Vendor ID(s) not found in master: {unknown}. Requires vendor onboarding check."
                })
        except Exception:
            pass

    return indicators


def extract_metadata_candidates(file_path: Path) -> dict:
    """Scans text files (JSON/YAML) for potential ID references."""
    candidates = {
        "potential_vendor_ids": [],
        "potential_po_refs": []
    }

    if file_path.suffix.lower() == '.json':
        try:
            content = json.loads(file_path.read_text(encoding="utf-8"))
            str_content = str(content)

            # Regex patterns
            candidates["potential_vendor_ids"].extend(re.findall(r'V-\d{3,4}', str_content))
            candidates["potential_po_refs"].extend(re.findall(r'PO-\d{4}', str_content))

        except Exception as e:
            print(f"Warning: Could not parse {file_path.name} for metadata: {e}")

    return candidates


# --- Main Logic ---

def run_agent_a(args):
    # 1. Setup Paths
    bundle_dir = Path(args.bundle_dir).resolve()
    shared_dir = Path(args.shared_dir).resolve()

    # Determine where 'runs' folder should be
    if args.runs_dir:
        runs_root = Path(args.runs_dir).resolve()
    else:
        # Default: Project Root / runs
        # Assumes this script is in /agents, so parent.parent is Project Root
        runs_root = Path(__file__).resolve().parent.parent / "runs"

    # 2. Generate Run ID and Directory
    run_id = f"run_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    run_dir = runs_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    print(f"[Agent A] Starting Intake. Run ID: {run_id}")
    print(f"   -> Input: {bundle_dir}")
    print(f"   -> Output: {run_dir}")

    context_packet = {
        "run_id": run_id,
        "timestamp": datetime.datetime.now().isoformat(),
        "status": "intake_complete",
        "files": [],
        "evidence_index": {},
        "risk_indicators": [],
        "metadata_candidates": {
            "vendor_ids": [],
            "po_refs": []
        },
        "system_paths": {
            "input_bundle": str(bundle_dir),
            "run_directory": str(run_dir),
            "shared_config": str(shared_dir)
        }
    }

    # 3. Process Shared Files
    if shared_dir.exists():
        for item in shared_dir.iterdir():
            if item.is_file() and not _should_ignore(item.name):
                # Copy file
                dest_path = run_dir / item.name
                shutil.copy2(item, dest_path)

                # Metadata & Context
                file_type = classify_file(item.name)
                meta = extract_metadata_candidates(item)
                context_packet["metadata_candidates"]["vendor_ids"].extend(meta["potential_vendor_ids"])
                context_packet["metadata_candidates"]["po_refs"].extend(meta["potential_po_refs"])

                context_packet["files"].append({
                    "filename": item.name,
                    "type": file_type,
                    "source": "shared",
                    "path": str(dest_path)
                })
                context_packet["evidence_index"][item.name] = build_evidence_index_entry(item, file_type, "shared")
    else:
        print(f"   [Warning] Shared directory not found: {shared_dir}")

    # 4. Process Input Bundle
    if bundle_dir.exists():
        for item in bundle_dir.iterdir():
            if item.is_file() and not _should_ignore(item.name):
                # Copy file
                dest_path = run_dir / item.name
                shutil.copy2(item, dest_path)

                # Metadata & Context
                file_type = classify_file(item.name)
                meta = extract_metadata_candidates(item)
                context_packet["metadata_candidates"]["vendor_ids"].extend(meta["potential_vendor_ids"])
                context_packet["metadata_candidates"]["po_refs"].extend(meta["potential_po_refs"])

                context_packet["files"].append({
                    "filename": item.name,
                    "type": file_type,
                    "source": "input_bundle",
                    "path": str(dest_path)
                })
                context_packet["evidence_index"][item.name] = build_evidence_index_entry(item, file_type, "input_bundle")
    else:
        raise FileNotFoundError(f"Input bundle not found: {bundle_dir}")

    # 5. Deduplicate and Save
    context_packet["metadata_candidates"]["vendor_ids"] = list(set(context_packet["metadata_candidates"]["vendor_ids"]))
    context_packet["metadata_candidates"]["po_refs"] = list(set(context_packet["metadata_candidates"]["po_refs"]))
    context_packet["risk_indicators"] = compute_risk_indicators(
        context_packet["files"], context_packet["metadata_candidates"]["vendor_ids"], run_dir
    )

    out_path = run_dir / "context_packet.json"
    write_json(out_path, context_packet)

    print(f"[Agent A] Complete. Context saved to {out_path}")
    return {
        "run_id": run_id,
        "run_dir": str(run_dir),
        "context_path": str(out_path)
    }


if __name__ == "__main__":
    # Determine default paths relative to this script
    # Script is in /agents, so defaults are ../input_bundles/s01 and ../input_bundles/shared
    base_dir = Path(__file__).resolve().parent.parent
    default_bundle = base_dir / "input_bundles" / "s01"
    default_shared = base_dir / "input_bundles" / "shared"

    parser = argparse.ArgumentParser(description="Agent A: Intake & Context (Functional Style)")
    parser.add_argument("--bundle-dir", default=str(default_bundle), help="Path to the input invoice bundle")
    parser.add_argument("--shared-dir", default=str(default_shared), help="Path to shared config folder")
    parser.add_argument("--runs-dir", default=None, help="Root folder to create runs in (default: project_root/runs)")

    args = parser.parse_args()

    # Run the logic
    try:
        run_agent_a(args)
    except Exception as e:
        print(f"❌ Error: {e}")