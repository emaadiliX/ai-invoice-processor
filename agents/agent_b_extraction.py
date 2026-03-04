import os
import sys
import json
import csv
import base64
from datetime import datetime

import yaml
import pdfplumber
import jsonschema
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

CONFIDENCE_THRESHOLD = 0.80
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".tiff", ".bmp"}

SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "schemas", "extracted_invoice_schema.json"
)


def get_file_type(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".pdf":
        return "pdf"
    if ext in IMAGE_EXTENSIONS:
        return "image"
    return "unknown"


def extract_text_from_pdf(pdf_path):
    full_text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                full_text += page_text + "\n"
    return full_text


EXTRACTION_SYSTEM_PROMPT = """You are an invoice data extraction assistant.
You will receive raw text extracted from an invoice document.

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


def call_openai_for_extraction(invoice_text):
    client = OpenAI()

    user_prompt = f"Extract structured data from this invoice:\n\n{invoice_text}"

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
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


def call_openai_for_image_extraction(image_path):
    client = OpenAI()

    with open(image_path, "rb") as f:
        image_data = base64.b64encode(f.read()).decode("utf-8")

    ext = os.path.splitext(image_path)[1].lower()
    mime_types = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".tiff": "image/tiff",
        ".bmp": "image/bmp",
    }
    mime_type = mime_types.get(ext, "image/png")

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Extract structured data from this invoice image:"},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:{mime_type};base64,{image_data}"
                        },
                    },
                ],
            },
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
    scores = extracted_data.get("confidence_scores", {})
    low_fields = []

    for field_name, score in scores.items():
        if score < CONFIDENCE_THRESHOLD:
            low_fields.append(field_name)

    extracted_data["low_confidence_fields"] = low_fields
    return extracted_data


def validate_output(extracted_data):
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
    mock_path = os.path.join(bundle_path, "mock_extraction.json")
    if os.path.exists(mock_path):
        print(f"Found mock extraction at {mock_path}, using it directly.")
        with open(mock_path, "r") as f:
            return json.load(f)
    return None


def write_line_items_csv(extracted_data, csv_path):
    scores = extracted_data.get("confidence_scores", {})
    line_confidence = round(
        sum(scores.get(k, 0) for k in ["line_item_description", "line_item_quantity", "line_item_unit_price"]) / 3, 2
    )
    po_ref = extracted_data.get("po_reference")

    fieldnames = ["line_no", "description", "quantity", "unit_price", "total", "po_line_ref", "confidence"]

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for item in extracted_data.get("line_items", []):
            writer.writerow({
                "line_no": item.get("line_id"),
                "description": item.get("description"),
                "quantity": item.get("quantity"),
                "unit_price": item.get("unit_price"),
                "total": item.get("total"),
                "po_line_ref": po_ref,
                "confidence": line_confidence,
            })


def generate_run_id(scenario_id):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{scenario_id}_{timestamp}"


def run_extraction(bundle_path, run_dir=None):
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
        file_type = get_file_type(invoice_path)

        if file_type == "pdf":
            print("Extracting text from PDF...")
            invoice_text = extract_text_from_pdf(invoice_path)

            if not invoice_text.strip():
                print("ERROR: Could not extract any text from the PDF.")
                sys.exit(1)

            print(f"Extracted {len(invoice_text)} characters of text.")
            print("Sending to OpenAI for field extraction...")
            extracted = call_openai_for_extraction(invoice_text)

        elif file_type == "image":
            print("Detected image invoice, using Vision API...")
            extracted = call_openai_for_image_extraction(invoice_path)

        else:
            print(f"ERROR: Unsupported file type: {invoice_path}")
            sys.exit(1)

        print("Received structured data from OpenAI.")
        extracted = flag_low_confidence_fields(extracted)

    is_valid = validate_output(extracted)
    if not is_valid:
        print("WARNING: Continuing with extraction despite schema validation issues.")

    if run_dir is None:
        run_id = generate_run_id(scenario_id)
        project_root = os.path.join(os.path.dirname(__file__), "..")
        run_dir = os.path.join(project_root, "runs", run_id)
        os.makedirs(run_dir, exist_ok=True)
    else:
        run_id = os.path.basename(run_dir)

    output_path = os.path.join(run_dir, "extracted_invoice.json")
    with open(output_path, "w") as f:
        json.dump(extracted, f, indent=2)

    csv_path = os.path.join(run_dir, "line_items.csv")
    write_line_items_csv(extracted, csv_path)

    print()
    print(f"Extraction complete!")
    print(f"Run ID:  {run_id}")
    print(f"Output:  {output_path}")

    if extracted.get("low_confidence_fields"):
        print(f"Low confidence fields: {extracted['low_confidence_fields']}")

    return {
        "result_path": output_path,
        "context_path": None,
        "finding_path": None,
        "scenario_id": scenario_id,
        "low_confidence_fields": extracted.get("low_confidence_fields", []),
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Agent B: OCR & Extraction")
    parser.add_argument("--bundle-dir", required=True, help="Path to the input bundle directory")
    parser.add_argument("--run-dir", default=None, help="Optional path to an existing run directory")
    args = parser.parse_args()

    if not os.path.isdir(bundle_dir):
        print(f"Bundle not found: {args.bundle_dir}")
        sys.exit(1)

    result = run_extraction(args.bundle_dir, run_dir=args.run_dir)
    print(json.dumps(result, indent=2))
