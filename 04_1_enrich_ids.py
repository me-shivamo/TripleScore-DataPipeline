"""
Standalone ID and source enrichment module.

Usage:
    python 04_1_enrich_ids.py [--input-dir DIR] [--output-dir DIR] [--file FILE]

Reads 04_Classified-Output/*.json files, adds 'id' (first key) and 'source'
(last key) to every question, and saves the enriched results to
04_1_Enriched-Output/*.json.

ID format   : {q_number:02d}{subject_abbr}{shift}{month}{year}
              e.g. 01M1Jan2026
Source format: {month} {year} Shift {shift}
              e.g. Jan 2026 Shift 1
"""

import json
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

SUBJECT_ABBR = {
    "Physics":   "P",
    "Chemistry": "C",
    "Maths":     "M",
}

MONTHS = r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec"


def parse_pdf_meta(stem: str):
    """Extract (year, month, shift) from a PDF/JSON filename stem.

    Rules:
    - year  : first 4-digit sequence matching 20xx
    - month : first occurrence of a month abbreviation
    - shift : digit after 'Shift_'; defaults to 2 if 'Shift' is absent
    """
    year_match = re.search(r"20\d{2}", stem)
    year = year_match.group() if year_match else "Unknown"

    month_match = re.search(MONTHS, stem)
    month = month_match.group() if month_match else "Unknown"

    shift_match = re.search(r"Shift_(\d+)", stem, re.IGNORECASE)
    shift = shift_match.group(1) if shift_match else "2"

    return year, month, shift


def enrich_file(input_file: Path, output_dir: Path):
    """Add 'id' and 'source' to every question in a single JSON file."""
    questions = json.loads(input_file.read_text(encoding="utf-8"))
    year, month, shift = parse_pdf_meta(input_file.stem)
    source = f"{month} {year} Shift {shift}"

    print(f"\nProcessing {input_file.name}: {len(questions)} question(s)", flush=True)
    print(f"  Parsed meta -> year={year}, month={month}, shift={shift}", flush=True)

    enriched = []
    for idx, q in enumerate(questions):
        subject = q.get("subject", "")
        abbr = SUBJECT_ABBR.get(subject, "X")
        question_id = f"{idx + 1:02d}{abbr}{shift}{month}{year}"

        # Build new dict with 'id' first and 'source' last
        new_q = {"id": question_id}
        new_q.update(q)
        new_q["source"] = source
        enriched.append(new_q)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / input_file.name
    output_file.write_text(
        json.dumps(enriched, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"  Saved to {output_file}", flush=True)
    return output_file


def enrich_all(input_dir=None, output_dir=None, single_file=None):
    """Enrich all JSON files in input_dir, or a single file."""
    if input_dir is None:
        input_dir = BASE_DIR / "04_Classified-Output"
    else:
        input_dir = Path(input_dir)

    if output_dir is None:
        output_dir = BASE_DIR / "04_1_Enriched-Output"
    else:
        output_dir = Path(output_dir)

    if single_file:
        json_files = [Path(single_file)]
        if not json_files[0].exists():
            print(f"File not found: {json_files[0]}", flush=True)
            return
    else:
        json_files = sorted(input_dir.glob("*.json"))
        if not json_files:
            print(f"No JSON files found in {input_dir}", flush=True)
            return

    print(f"Found {len(json_files)} file(s) to enrich.", flush=True)

    for json_file in json_files:
        enrich_file(json_file, output_dir)

    print("\nEnrichment complete.", flush=True)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Add question IDs and source to classified JSON.")
    parser.add_argument("--input-dir",  default=None, help="Input directory (default: 04_Classified-Output/)")
    parser.add_argument("--output-dir", default=None, help="Output directory (default: 04_1_Enriched-Output/)")
    parser.add_argument("--file",       default=None, help="Process a single JSON file instead of the whole directory")
    args = parser.parse_args()

    enrich_all(args.input_dir, args.output_dir, args.file)
