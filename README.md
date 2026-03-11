# AI Invoice Processing System (IIPS)

An automated invoice processing pipeline that uses a chain of AI agents to handle invoices from receipt through to payment approval.

The system reads a PDF invoice, validates it, matches it against a Purchase Order and Goods Receipt Note, checks compliance and fraud signals, and produces a final routing decision — all without human intervention for clean invoices.

---

## Requirements

- Python 3.11+
- An OpenAI API key

---

## Setup

1. **Clone the repository**
   ```
   git clone https://github.com/emaadiliX/ai-invoice-processor.git
   ```

2. **Create and activate a virtual environment**
   ```
   python -m venv .venv
   .venv\Scripts\activate        # Windows
   source .venv/bin/activate     # Mac/Linux
   ```

3. **Install dependencies**
   ```
   pip install -r requirements.txt
   ```

4. **Set your OpenAI API key**

   Create a `.env` file in the `ai-invoice-processor/` directory:
   ```
   OPENAI_API_KEY=sk-...
   ```

---

## How to Run

### Run all 15 test scenarios
```
cd ai-invoice-processor
python demo.py
```
This runs every scenario and prints a PASS/FAIL table showing the expected vs actual decision.

### Run a single scenario
```
python run.py input_bundles/s01
```
Replace `s01` with any scenario folder name. Output is saved to `runs/`.

---

## Project Structure

```
ai-invoice-processor/
├── agents/                  # The 9 processing agents (A through I)
├── input_bundles/           # Test scenarios (s01–s15), each with invoice PDF + PO + GRN
│   └── shared/              # Shared config (vendor master, tax rules)
├── policy/                  # Approval policy configuration
├── runs/                    # Output from each pipeline run (auto-created)
├── demo.py                  # Runs all 15 scenarios and reports results
└── run.py                   # Runs a single scenario end-to-end
```

---

## The Pipeline

Each invoice bundle is processed through 9 agents in sequence:

| Agent | Role |
|---|---|
| A | Intake — organises files into a run directory |
| B | Extraction — reads the PDF using OpenAI |
| C | Vendor Resolution — matches vendor name to internal ID |
| D | Validation — checks invoice format and arithmetic |
| E | Matching — compares invoice against PO and GRN |
| F | Compliance — validates tax rates and policy rules |
| G | Anomaly Detection — checks for duplicates and fraud signals |
| H | Exception Triage — collects all findings and determines routing |
| I | Orchestrator — writes the final decision and audit log |

---

## Output Decisions

| Decision | Meaning |
|---|---|
| `AUTO_POST` | Clean invoice, posted automatically |
| `HOLD_FOR_APPROVAL` | Has issues, needs human review |
| `BLOCK` | Duplicate or fraud — payment stopped |
| `ESCALATE_TO_FINANCE_APPROVER` | Bank change on high-value invoice |
| `ROUTE_TO_DEPT_HEAD` | New vendor or no PO |
| `ROUTE_TO_MANUAL_REVIEW` | OCR confidence too low to trust |
