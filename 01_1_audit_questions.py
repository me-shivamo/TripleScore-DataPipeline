"""
Standalone question audit module.

Usage:
    python 01_1_audit_questions.py <markdown_file>

Reads a 01_Datalab-Output markdown file, detects all question numbers using
the same regex pattern as 03_structure_gemini.py, and reports which questions
are missing from the range 1 to 75.
"""

import re
import sys
from pathlib import Path

QUESTION_START = re.compile(
    r"^(?:-\s*)?(?:\*\*)?(\d{1,3})[.\*]",
    re.MULTILINE,
)


def audit_questions(md_path):
    """Check which questions from 1-75 are missing in the markdown file.

    Args:
        md_path: Path to the extracted markdown file.

    Returns:
        Tuple of (found_nums, missing) where both are sorted lists of ints.
    """
    md_path = Path(md_path)
    if not md_path.exists():
        raise FileNotFoundError(f"Markdown file not found: {md_path}")

    markdown_text = md_path.read_text(encoding="utf-8")
    found_nums = sorted({int(m.group(1)) for m in QUESTION_START.finditer(markdown_text)})
    missing = sorted(set(range(1, 76)) - set(found_nums))

    print(f"Questions found ({len(found_nums)}): {found_nums}", flush=True)
    if missing:
        print(f"Missing from 1-75 ({len(missing)}): {missing}", flush=True)
    else:
        print("All 75 questions accounted for.", flush=True)

    return found_nums, missing


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python 01_1_audit_questions.py <markdown_file>")
        sys.exit(1)

    audit_questions(sys.argv[1])
