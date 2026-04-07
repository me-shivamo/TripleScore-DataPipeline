"""
Import embedded questions from 05_Embedded-Output/ into Supabase (PostgreSQL).

- Skips questions already in the database (safe to re-run)
- Reads DATABASE_URL from TripleScore-Backend/.env
- Converts flat option lists → labelled objects  e.g. ["5","3"] → [{"label":"A","text":"5"},...]
- Maps subject names  Maths→MATH, Physics→PHYSICS, Chemistry→CHEMISTRY
- Maps question types  multiple_choice→MCQ, integer→INTEGER

Usage:
    python 06_import_to_supabase.py
    python 06_import_to_supabase.py --file 05_Embedded-Output/some_paper.json
    python 06_import_to_supabase.py --input-dir 05_Embedded-Output/
"""

import argparse
import json
import os
import sys
import uuid
from pathlib import Path

# ── Resolve paths ────────────────────────────────────────────────────────────
PIPELINE_DIR = Path(__file__).resolve().parent
BACKEND_DIR  = PIPELINE_DIR.parent / "TripleScore-Backend"
DEFAULT_INPUT_DIR = PIPELINE_DIR / "05_Embedded-Output"

# ── Bootstrap Django from the backend ────────────────────────────────────────
sys.path.insert(0, str(BACKEND_DIR))

# Load DATABASE_URL from backend .env if not already set
env_file = BACKEND_DIR / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django
django.setup()

from apps.questions.models import Question  # noqa: E402  (after django.setup)

# ── Mappings ─────────────────────────────────────────────────────────────────
SUBJECT_MAP = {
    "maths":     "MATH",
    "math":      "MATH",
    "mathematics": "MATH",
    "physics":   "PHYSICS",
    "chemistry": "CHEMISTRY",
}

TYPE_MAP = {
    "multiple_choice": "MCQ",
    "mcq":             "MCQ",
    "integer":         "INTEGER",
    "integer_type":    "INTEGER",
}

LABELS = ["A", "B", "C", "D", "E"]


# ── Core import function ──────────────────────────────────────────────────────
def import_file(path: Path) -> tuple[int, int]:
    """Import a single embedded JSON file. Returns (created, skipped)."""
    data = json.loads(path.read_text())
    created = skipped = 0

    for i, q in enumerate(data):
        # Stable ID derived from filename + index
        stem = path.stem[:28]          # keep ID within 36 chars
        qid  = f"{stem}_{i + 1}"

        if Question.objects.filter(id=qid).exists():
            skipped += 1
            continue

        # Subject
        subject_raw = q.get("subject", "").strip().lower()
        subject = SUBJECT_MAP.get(subject_raw, subject_raw.upper())

        # Question type
        type_raw  = q.get("type", "").strip().lower()
        qtype     = TYPE_MAP.get(type_raw, "MCQ")

        # Options: convert flat list → labelled objects if needed
        raw_opts = q.get("options", [])
        if raw_opts and isinstance(raw_opts[0], str):
            options = [{"label": LABELS[j], "text": opt}
                       for j, opt in enumerate(raw_opts)]
        else:
            options = raw_opts          # already labelled

        # Correct option: match answer text → label, fallback to raw (truncated)
        correct_text   = str(q.get("correct_answer", "")).strip()
        correct_option = correct_text
        for opt in options:
            if opt.get("text", "").strip() == correct_text:
                correct_option = opt["label"]
                break
        correct_option = correct_option[:32]   # column is varchar(32)

        Question.objects.create(
            id             = qid,
            subject        = subject,
            chapter        = q.get("chapter", ""),
            topic          = q.get("topic", ""),
            content        = q.get("question", ""),
            question_type  = qtype,
            options        = options,
            correct_option = correct_option,
            explanation    = q.get("explanation", ""),
            source         = q.get("source", ""),
            embedding      = q.get("embedding"),
        )
        created += 1

    return created, skipped


# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Import embedded questions into Supabase."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--file",
        metavar="PATH",
        help="Import a single JSON file",
    )
    group.add_argument(
        "--input-dir",
        metavar="DIR",
        default=str(DEFAULT_INPUT_DIR),
        help=f"Directory of embedded JSON files (default: {DEFAULT_INPUT_DIR})",
    )
    args = parser.parse_args()

    if args.file:
        files = [Path(args.file)]
    else:
        input_dir = Path(args.input_dir)
        files = sorted(
            f for f in input_dir.glob("*.json") if f.name != "index.json"
        )

    if not files:
        print("No JSON files found.")
        sys.exit(1)

    total_created = total_skipped = 0

    for f in files:
        print(f"Importing {f.name} ...", end=" ", flush=True)
        created, skipped = import_file(f)
        print(f"{created} created, {skipped} skipped")
        total_created += created
        total_skipped += skipped

    print(f"\nDone — {total_created} created, {total_skipped} skipped")
    print(f"Total questions in database: {Question.objects.count()}")


if __name__ == "__main__":
    main()
