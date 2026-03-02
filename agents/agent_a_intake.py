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
        self.run_id = f"run_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        self.run_dir = os.path.join("runs", self.run_id)

        # Ensure run directory exists
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
        For PDFs/Images, we just return placeholders for Agent B (OCR) to fill later.
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
                    # Recursively search for keys like 'vendor_id' or 'po_number'
                    # This is a basic implementation for the prototype
                    str_content = str(content)

                    # Regex to find patterns like V-101, V-1001 or PO-5001
                    # Allow 3–4 digit vendor IDs to match values such as V-101 and V-2207
                    vendor_matches = re.findall(r'V-\d{3,4}', str_content)
                    po_matches = re.findall(r'PO-\d{4}', str_content)
                    
                    candidates["potential_vendor_ids"].extend(vendor_matches)
                    candidates["potential_po_refs"].extend(po_matches)
            except Exception as e:
                print(f"Warning: Could not parse {file_path} for metadata: {e}")

        return candidates

    def run(self):
        # Use ASCII-only logging to avoid encoding issues on some Windows terminals
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

        # 1. Process Shared Files (Vendor Master, Policies)
        print("   -> Loading Shared Configuration...")
        for filename in os.listdir(self.shared_path):
            file_path = os.path.join(self.shared_path, filename)
            doc_type = self.classify_file(filename)

            # Copy file to run directory for audit trail (snapshotting state)
            shutil.copy(file_path, os.path.join(self.run_dir, filename))

            # Extract metadata from shared JSON files as well (e.g. vendor_master.json)
            shared_meta = self.extract_metadata_candidates(file_path)
            context_packet["metadata_candidates"]["vendor_ids"].extend(shared_meta["potential_vendor_ids"])
            context_packet["metadata_candidates"]["po_refs"].extend(shared_meta["potential_po_refs"])

            context_packet["files"].append({
                "filename": filename,
                "type": doc_type,
                "source": "shared",
                "path": os.path.join(self.run_dir, filename)
            })

        # 2. Process Input Bundle (Invoice, PO, GRN)
        print(f"   -> Processing Input Bundle: {self.input_path}")
        if os.path.exists(self.input_path):
            for filename in os.listdir(self.input_path):
                file_path = os.path.join(self.input_path, filename)
                
                # Skip directories
                if os.path.isdir(file_path):
                    continue

                doc_type = self.classify_file(filename)
                
                # Copy file to run dir
                shutil.copy(file_path, os.path.join(self.run_dir, filename))

                # Extract basic metadata
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
        context_packet["metadata_candidates"]["vendor_ids"] = list(set(context_packet["metadata_candidates"]["vendor_ids"]))
        context_packet["metadata_candidates"]["po_refs"] = list(set(context_packet["metadata_candidates"]["po_refs"]))

        # 3. Save Context Packet
        output_path = os.path.join(self.run_dir, "context_packet.json")
        with open(output_path, 'w') as f:
            json.dump(context_packet, f, indent=4)

        print(f"[Agent A] Complete. Context Packet saved to: {output_path}")
        return output_path

# --- Execution Block (for testing) ---
if __name__ == "__main__":
    # Point this to one of your test scenarios
    # The available clean scenario folder is "input_bundles/s01" in this project.
    TEST_BUNDLE_PATH = "input_bundles/s01"
    SHARED_PATH = "input_bundles/shared"
    
    # Check if folders exist before running
    if os.path.exists(TEST_BUNDLE_PATH) and os.path.exists(SHARED_PATH):
        agent = AgentA_Intake(TEST_BUNDLE_PATH, SHARED_PATH)
        agent.run()
    else:
        print("⚠️ Please create the 'input_bundles/s01_clean' and 'input_bundles/shared' folders first.")