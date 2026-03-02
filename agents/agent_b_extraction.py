import os
import sys
import json
from datetime import datetime

import yaml
import pdfplumber
import jsonschema
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

CONFIDENCE_THRESHOLD = 0.80

SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "schemas", "extracted_invoice_schema.json"
)


def extract_text_from_pdf(pdf_path):
    """Read all pages of a PDF and return the combined text."""
    full_text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                full_text += page_text + "\n"
    return full_text


def call_openai_for_extraction(invoice_text):
    """Send invoice text to OpenAI and get structured extraction back."""
    client = OpenAI()

    system_prompt = """You are an invoice data extraction assistant.
You will receive raw text extracted from an invoice PDF.

Extract the following fields and return them as a single JSON object:

- invoice_id: The invoice number/ID
- invoice_date: In YYYY-MM-DD format
- due_date: In YYYY-MM-DD format, or null if not found
- vendor_name: The vendor/supplier company name (from the FROM section)
- vendor_id: Always set to null (resolved by a separate system)
- po_reference: Purchase order reference number, or null if not found
- currency: 3-letter ISO code (USD, EUR, GBP, etc.)
- line_items: Array of objects, each with:
    - line_id (integer, starting from 1)
    - description (string)
    - quantity (number)
    - unit_price (number, no currency symbols)
    - total (number, no currency symbols)
- subtotal: Number without currency symbols
- tax_amount: Number without currency symbols, or null if unreadable
- total_amount: Total due as a number, or null if unreadable
- confidence_scores: Object with these keys, each a float from 0.0 to 1.0:
    invoice_id, invoice_date, due_date, vendor_name, po_reference,
    currency, line_item_description, line_item_quantity, line_item_unit_price,
    subtotal, tax_amount, total_amount
  Scoring guide:
    0.95 = clearly readable
    0.70-0.90 = somewhat uncertain or partially visible
    below 0.50 = garbled, illegible, or contains question marks
- extraction_notes: Brief note about extraction quality or issues found

Rules:
- Convert dates like "12 February 2024" to "2024-02-12"
- Strip currency symbols and commas from all numbers
- If text contains ??? or is clearly garbled, set that field to null with low confidence
- vendor_id must always be null"""

    user_prompt = f"Extract structured data from this invoice:\n\n{invoice_text}"

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        response_format={"type": "json_object"},
        temperature=0.0,
    )

    result_text = response.choices[0].message.content
    if result_text is None:
        print("ERROR: OpenAI returned an empty response.")
        sys.exit(1)

    extracted = json.loads(result_text)
    return extracted


def flag_low_confidence_fields(extracted_data):
    """Check confidence scores and build the low_confidence_fields list."""
    scores = extracted_data.get("confidence_scores", {})
    low_fields = []

    for field_name, score in scores.items():
        if score < CONFIDENCE_THRESHOLD:
            low_fields.append(field_name)

    extracted_data["low_confidence_fields"] = low_fields
    return extracted_data


def validate_output(extracted_data):
    """Validate the extracted data against the JSON schema."""
    with open(SCHEMA_PATH, "r") as f:
        schema = json.load(f)

    try:
        jsonschema.validate(instance=extracted_data, schema=schema)
        print("Output passed schema validation.")
        return True
    except jsonschema.ValidationError as e:
        print(f"WARNING: Schema validation failed: {e.message}")
        return False


def check_for_mock_extraction(bundle_path):
    """If a mock_extraction.json exists in the bundle, use it instead of calling OpenAI."""
    mock_path = os.path.join(bundle_path, "mock_extraction.json")
    if os.path.exists(mock_path):
        print(f"Found mock extraction at {mock_path}, using it directly.")
        with open(mock_path, "r") as f:
            return json.load(f)
    return None


def generate_run_id(scenario_id):
    """Create a unique run ID from the scenario name and current timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{scenario_id}_{timestamp}"


def run_extraction(bundle_path):
    """Run the full extraction pipeline for a single invoice bundle."""

    manifest_path = os.path.join(bundle_path, "manifest.yaml")
    with open(manifest_path, "r") as f:
        manifest = yaml.safe_load(f)

    scenario_id = manifest["scenario_id"]
    invoice_filename = manifest["invoice_file"]
    invoice_path = os.path.join(bundle_path, invoice_filename)

    print(f"--- Agent B: Invoice Extraction ---")
    print(f"Scenario: {scenario_id}")
    print(f"Invoice:  {invoice_path}")
    print()

    extracted = check_for_mock_extraction(bundle_path)

    if extracted is not None:
        print("Using mock extraction data (skipped OpenAI call).")
        extracted = flag_low_confidence_fields(extracted)
    else:
        print("Extracting text from PDF...")
        invoice_text = extract_text_from_pdf(invoice_path)

        if not invoice_text.strip():
            print("ERROR: Could not extract any text from the PDF.")
            sys.exit(1)

        print(f"Extracted {len(invoice_text)} characters of text.")

        print("Sending to OpenAI for field extraction...")
        extracted = call_openai_for_extraction(invoice_text)
        print("Received structured data from OpenAI.")

        extracted = flag_low_confidence_fields(extracted)

    validate_output(extracted)

    run_id = generate_run_id(scenario_id)
    project_root = os.path.join(os.path.dirname(__file__), "..")
    run_dir = os.path.join(project_root, "runs", run_id)
    os.makedirs(run_dir, exist_ok=True)

    output_path = os.path.join(run_dir, "extracted_invoice.json")
    with open(output_path, "w") as f:
        json.dump(extracted, f, indent=2)

    print()
    print(f"Extraction complete!")
    print(f"Run ID:  {run_id}")
    print(f"Output:  {output_path}")

    if extracted.get("low_confidence_fields"):
        print(f"Low confidence fields: {extracted['low_confidence_fields']}")

    return output_path


def main():
    if len(sys.argv) < 2:
        print("Usage: python agent_b_extraction.py <path_to_input_bundle>")
        print("Example: python agent_b_extraction.py input_bundles/s01")
        sys.exit(1)

    bundle_path = sys.argv[1]

    if not os.path.isdir(bundle_path):
        print(f"ERROR: Bundle path not found: {bundle_path}")
        sys.exit(1)

    run_extraction(bundle_path)


if __name__ == "__main__":
    main()
