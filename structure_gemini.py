"""
Standalone Gemini structuring module.

Usage:
    python structure_gemini.py <markdown_file> [--output-dir DIR]

Reads a Cloudinary-Output markdown file, splits it into individual question
blocks, sends each to Gemini 2.5 Flash-Lite (via OpenRouter) for structured
extraction, and saves the result as a JSON array.
"""

import asyncio
import json
import os
import re
from pathlib import Path


def load_env_file(env_path):
    if not env_path.exists():
        return

    with env_path.open() as env_file:
        for raw_line in env_file:
            line = raw_line.strip()

            if not line or line.startswith("#"):
                continue

            if line.startswith("export "):
                line = line[len("export "):].strip()

            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("'\""))


BASE_DIR = Path(__file__).resolve().parent
load_env_file(BASE_DIR / ".env")

QUESTION_START = re.compile(
    r"^(?:-\s*)?(?:\*\*)?(\d{1,3})[\.\*]",
    re.MULTILINE,
)

SYSTEM_PROMPT = """You are a JEE exam question parser. Extract structured data from this markdown block.

Rules:
- "question": Full question text. Preserve LaTeX ($...$ and $$...$$). Remove bold markers (**) and question number prefix.
- "question_image": Cloudinary URL of the question diagram (first image before "Ans." line), or empty string if none.
- "options": Array of option strings. Preserve LaTeX. Strip "(1)", "(2)" etc. prefixes. Always 4 elements for MCQ.
- "type": "multiple_choice" (4 options, single answer), "numerical" (blanks/asks for number), "multiple_select" (multiple correct), "open_text" (otherwise).
- "correct_answer": The actual answer value (e.g. "$mg/4$", "120.15", "1000"), NOT the option number.
- "explanation_image": Cloudinary URL of solution diagram (image after "Ans." line), or empty string if none.
- "explanation": Full solution text exact as in markdown + simple short explanation. Preserve LaTeX. Remove "Sol." prefix and "Correct option (N)" suffix.
- "subject": Classify as "Physics", "Chemistry", or "Maths" based on question content."""

RESPONSE_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "question_extraction",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "question": {"type": "string"},
                "question_image": {"type": "string"},
                "options": {"type": "array", "items": {"type": "string"}},
                "type": {
                    "type": "string",
                    "enum": [
                        "multiple_choice",
                        "multiple_select",
                        "numerical",
                        "open_text",
                    ],
                },
                "correct_answer": {"type": "string"},
                "explanation_image": {"type": "string"},
                "explanation": {"type": "string"},
                "subject": {
                    "type": "string",
                    "enum": ["Physics", "Chemistry", "Maths"],
                },
            },
            "required": [
                "question",
                "question_image",
                "options",
                "type",
                "correct_answer",
                "explanation_image",
                "explanation",
                "subject",
            ],
            "additionalProperties": False,
        },
    },
}

MAX_RETRIES = 3
RETRY_BASE_DELAY = 2


def split_into_question_blocks(markdown_text):
    """Split markdown into (question_number, block_text) tuples."""
    matches = list(QUESTION_START.finditer(markdown_text))
    if not matches:
        return []

    blocks = []
    for i, match in enumerate(matches):
        start = match.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(markdown_text)
        block_text = markdown_text[start:end].strip()
        question_num = int(match.group(1))
        blocks.append((question_num, block_text))

    return blocks


async def extract_question(client, block_text, question_num):
    """Send a single question block to Gemini via OpenRouter and return structured JSON."""
    for attempt in range(MAX_RETRIES):
        try:
            response = await client.chat.completions.create(
                model="google/gemini-2.5-flash-lite",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": block_text},
                ],
                response_format=RESPONSE_SCHEMA,
                temperature=0.5,
            )
            content = response.choices[0].message.content
            return json.loads(content)
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                print(f"  Q{question_num}: FAILED after {MAX_RETRIES} attempts: {e}", flush=True)
                return {"error": str(e), "question_number": question_num}
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            print(f"  Q{question_num}: retry {attempt + 1} in {delay}s ({e})", flush=True)
            await asyncio.sleep(delay)


async def structure_markdown(md_path, output_dir=None):
    """Parse Cloudinary-Output markdown into structured JSON using Gemini.

    Args:
        md_path: Path to the Cloudinary-Output markdown file.
        output_dir: Where to save the JSON. Defaults to Structured-Output/.

    Returns:
        Path to the output JSON file.
    """
    import openai

    md_path = Path(md_path)
    if not md_path.exists():
        raise FileNotFoundError(f"Markdown file not found: {md_path}")

    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not set. Add it to .env or environment.")

    if output_dir is None:
        output_dir = BASE_DIR / "Structured-Output"
    else:
        output_dir = Path(output_dir)

    markdown_text = md_path.read_text(encoding="utf-8")
    blocks = split_into_question_blocks(markdown_text)

    if not blocks:
        raise RuntimeError(f"No question blocks found in {md_path.name}")

    print(f"Found {len(blocks)} question(s). Processing sequentially...", flush=True)

    client = openai.AsyncOpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
    )

    results = []
    for question_num, block_text in blocks:
        print(f"  Processing Q{question_num}...", flush=True)
        result = await extract_question(client, block_text, question_num)
        results.append(result)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{md_path.stem}.json"
    output_file.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    success_count = sum(1 for r in results if "error" not in r)
    print(f"Structured {success_count}/{len(blocks)} question(s).", flush=True)
    print(f"Saved to {output_file}", flush=True)

    return output_file


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Structure markdown into JSON via Gemini.")
    parser.add_argument("markdown", help="Path to the Cloudinary-Output markdown file")
    parser.add_argument("--output-dir", default=None, help="Output directory (default: Structured-Output/)")
    args = parser.parse_args()

    asyncio.run(structure_markdown(args.markdown, args.output_dir))
