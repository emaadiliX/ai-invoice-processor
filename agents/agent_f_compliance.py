import os
import json
import yaml
import re
import datetime

class AgentF_Compliance:
    def __init__(self, run_id=None):
        # Find the project root
        # This trick finds where this script is, then foes up one level to the main folder

        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        self.project_root = os.path.dirname(current_script_dir)


        # Determine the RUN ID
        # If you don't give me a specific run, I'll find the lastest one automatically
        self.run_id = run_id
        if not self.run_id:
            self.run_id = self.get_lastest_run()

        self.run_dir = os.path.join(self.project_root, "runs", self.run_id)


        #Define PATHS TO ALL THE DATA WE NEED
        #We need the "Evidence" (what happened) and the "Rules" (what should happen)
        self.context_path = os.path.join(self.run_dir, "context_packet.json")
        self.extracted_invoice_path = os.path.join(self.run_dir, "extracted_invoice.json")
        self.vendor_resolution_path = os.path.join(self.run_dir, "vendor_resulution.json")

        self.tax_rules_path = os.path.join(self.run_dir, "tax_rules.yaml")
        self.vendor_master_path = os.path.join(self.run_dir, "vendor_master.json")
        self.policy_path = os.path.join(self.run_dir, "approval_policy.ymal")


    def get_lastest_run(self):
        """Helper to find the most recent run folder"""
        import glob

        #Look for any folder starting with 'run_'
        runs_path = os.path.join(self.project_root, "runs", "run_*")
        runs = glob.glob(runs_path)
        if not runs:
            raise FileNotFoundError("No runs found. Please run Agent A first.")
        return os.path.basename(max(runs, key=os.path.getctime))


















