import os
import json
import datetime
import shutil
import uuid
import re


class AgentA_Intake:
    def __init__(self, input_bundle_path, shared_folder_path):
        self.input_path = input_bundle_path
        self.shared_path = shared_folder_path

        # 1. Generate a unique Run ID (e.g., run_20260302_174500_a1b2c3d4)
        self.run_id = f"run_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"

        # 2. Define the run directory inside the project root's 'runs' folder
        # We use os.path.abspath(__file__) to find where THIS script is, then go up 2 levels
        # (agents -> project_root -> runs)
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.run_dir = os.path.join(project_root, "runs", self.run_id)

        # 3. Create the folder immediately
        os.makedirs(self.run_dir, exist_ok=True)

    def classify_file(self, filename):
        """Simple rule-based classification based on filename."""
        lower_name = filename.lower()
        if "invoice" in lower_name and lower_name.endswith(('.pdf', '.png', '.jpg')):
            return "invoice_document"
        elif "purchase_order" in lower_name or "po" in lower_name:
            return "purchase_order_data"
        elif "grn" in lower_name or "goods_receipt" in lower_name:
            return "goods_receipt_data"
        elif "vendor_master" in lower_name:
            return "vendor_master_record"
        elif "policy" in lower_name or "approval" in lower_name:
            return "approval_policy"
        elif "tax" in lower_name:
            return "tax_rules"
        else:
            return "unknown_file"

    def extract_metadata_candidates(self, file_path):
        """
        Scans text files (JSON/YAML) for potential ID references.
        """
        candidates = {
            "potential_vendor_ids": [],
            "potential_po_refs": []
        }

        # Simple text scan for JSON files only
        if file_path.endswith('.json'):
            try:
                with open(file_path, 'r') as f:
                    content = json.load(f)
                    str_content = str(content)

                    # Regex to find patterns like V-101 or PO-5001
                    vendor_matches = re.findall(r'V-\d{3,4}', str_content)
                    po_matches = re.findall(r'PO-\d{4}', str_content)

                    candidates["potential_vendor_ids"].extend(vendor_matches)
                    candidates["potential_po_refs"].extend(po_matches)
            except Exception as e:
                print(f"Warning: Could not parse {file_path} for metadata: {e}")

        return candidates

    def run(self):
        print(f"[Agent A] Starting intake for Run ID: {self.run_id}")

        context_packet = {
            "run_id": self.run_id,
            "timestamp": datetime.datetime.now().isoformat(),
            "status": "intake_complete",
            "files": [],
            "metadata_candidates": {
                "vendor_ids": [],
                "po_refs": []
            },
            "system_paths": {
                "input_bundle": self.input_path,
                "run_directory": self.run_dir,
                "shared_config": self.shared_path
            }
        }

        # --- Step 1: Process Shared Files ---
        print("   -> Loading Shared Configuration...")
        if os.path.exists(self.shared_path):
            for filename in os.listdir(self.shared_path):
                file_path = os.path.join(self.shared_path, filename)
                doc_type = self.classify_file(filename)

                # Snapshot file
                shutil.copy(file_path, os.path.join(self.run_dir, filename))

                # Extract metadata
                shared_meta = self.extract_metadata_candidates(file_path)
                context_packet["metadata_candidates"]["vendor_ids"].extend(shared_meta["potential_vendor_ids"])
                context_packet["metadata_candidates"]["po_refs"].extend(shared_meta["potential_po_refs"])

                context_packet["files"].append({
                    "filename": filename,
                    "type": doc_type,
                    "source": "shared",
                    "path": os.path.join(self.run_dir, filename)
                })
        else:
            print(f"   [Warning] Shared path not found: {self.shared_path}")

        # --- Step 2: Process Input Bundle (Invoice, PO, etc.) ---
        print(f"   -> Processing Input Bundle: {self.input_path}")
        if os.path.exists(self.input_path):
            for filename in os.listdir(self.input_path):
                file_path = os.path.join(self.input_path, filename)

                if os.path.isdir(file_path): continue

                doc_type = self.classify_file(filename)

                # Snapshot file
                shutil.copy(file_path, os.path.join(self.run_dir, filename))

                # Extract metadata
                meta = self.extract_metadata_candidates(file_path)
                context_packet["metadata_candidates"]["vendor_ids"].extend(meta["potential_vendor_ids"])
                context_packet["metadata_candidates"]["po_refs"].extend(meta["potential_po_refs"])

                context_packet["files"].append({
                    "filename": filename,
                    "type": doc_type,
                    "source": "input_bundle",
                    "path": os.path.join(self.run_dir, filename)
                })
        else:
            print(f"❌ Error: Input bundle path '{self.input_path}' does not exist.")
            return

        # Deduplicate candidates
        context_packet["metadata_candidates"]["vendor_ids"] = list(
            set(context_packet["metadata_candidates"]["vendor_ids"]))
        context_packet["metadata_candidates"]["po_refs"] = list(set(context_packet["metadata_candidates"]["po_refs"]))

        # --- Step 3: Save Context Packet ---
        output_path = os.path.join(self.run_dir, "context_packet.json")
        with open(output_path, 'w') as f:
            json.dump(context_packet, f, indent=4)

        print(f"[Agent A] Complete. Context Packet saved to: {output_path}")
        return output_path


# --- Execution Block (Standalone Testing) ---
if __name__ == "__main__":
    # This block only runs when you execute this script directly (e.g. Right Click -> Run)

    # 1. Dynamic Path Finding (Works in PyCharm, VS Code, Terminal)
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_script_dir)

    # 2. Define Test Paths
    # We default to 's01' as the standard clean test case
    TEST_BUNDLE_PATH = os.path.join(project_root, "input_bundles", "s01")
    SHARED_PATH = os.path.join(project_root, "input_bundles", "shared")

    print(f"--- Environment Check ---")
    print(f"Project Root: {project_root}")

    if os.path.exists(TEST_BUNDLE_PATH) and os.path.exists(SHARED_PATH):
        agent = AgentA_Intake(TEST_BUNDLE_PATH, SHARED_PATH)
        agent.run()
    else:
        print("\n⚠️  Setup Error: Input folders not found.")
        print(f"Checked: {TEST_BUNDLE_PATH}")