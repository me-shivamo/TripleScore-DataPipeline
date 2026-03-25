"""
Standalone Gemini structuring module.

Usage:
    python 03_structure_gemini.py <markdown_file> [--output-dir DIR]

Reads a 02_DO-Spaces-Output markdown file, splits it into individual question
blocks, sends each one to Gemini 2.5 Flash-Lite (via OpenRouter) for structured
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

SYSTEM_PROMPT_SINGLE = """You are a JEE exam question parser. Extract structured data from this markdown block.

Rules:
- "question": Full question text. Preserve the exact markdown text in string. Remove question number prefix. If the question has diagram(s)/image(s), keep them inline as markdown image syntax ![description](url) at their original position in the text. Remove duplicate image description text that appears as a standalone paragraph after the image link.
- "options": Array of option strings. Preserve the exact markdown text. Remove option numbers "(1)" or "(A)" etc... prefixes. Always 4 elements for MCQ.
- "type": "multiple_choice", "numerical", "multiple_select", "open_text".
- "correct_answer": The actual answer value, NOT include the option number.
- "explanation": Full solution text exact as in markdown (make sure that it will proper render on website). Preserve markdown text in string. Remove "Sol." prefix and "Correct option (N)" suffix. If the solution has diagram(s)/image(s), keep them inline as markdown image syntax ![description](url) at their original position. Remove duplicate image description text that appears as a standalone paragraph after the image link.
- "subject": Classify as "Physics", "Chemistry", or "Maths" based on question content."""

RESPONSE_SCHEMA_SINGLE = {
    "type": "json_schema",
    "json_schema": {
        "name": "question_extraction",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "question": {"type": "string"},
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
                "explanation": {"type": "string"},
                "subject": {
                    "type": "string",
                    "enum": ["Physics", "Chemistry", "Maths"],
                },
            },
            "propertyOrdering": [
                "question",
                "options",
                "type",
                "correct_answer",
                "explanation",
                "subject",
            ],
            "required": [
                "question",
                "options",
                "type",
                "correct_answer",
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


async def extract_question_single(client, block_text, question_num):
    """Send a single question block to Gemini and return structured JSON."""
    for attempt in range(MAX_RETRIES):
        try:
            response = await client.chat.completions.create(
                model="google/gemini-2.5-flash-lite",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT_SINGLE},
                    {"role": "user", "content": block_text},
                ],
                response_format=RESPONSE_SCHEMA_SINGLE,
                temperature=1,
                max_tokens=2500,
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
    """Parse 02_DO-Spaces-Output markdown into structured JSON using Gemini.

    Args:
        md_path: Path to the 02_DO-Spaces-Output markdown file.
        output_dir: Where to save the JSON. Defaults to 03_Structured-Output/.

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
        output_dir = BASE_DIR / "03_Structured-Output"
    else:
        output_dir = Path(output_dir)

    markdown_text = md_path.read_text(encoding="utf-8")
    blocks = split_into_question_blocks(markdown_text)

    if not blocks:
        raise RuntimeError(f"No question blocks found in {md_path.name}")

    print(f"Found {len(blocks)} question(s). Processing one by one...", flush=True)

    client = openai.AsyncOpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
        timeout=120.0,
    )

    results = []
    success_count = 0
    fail_count = 0
    total = len(blocks)

    for q_num, q_text in blocks:
        print(f"  Processing Q{q_num}...", flush=True)
        result = await extract_question_single(client, q_text, q_num)
        results.append(result)
        if "error" in result:
            fail_count += 1
        else:
            success_count += 1

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{md_path.stem}.json"
    output_file.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"\n--- Structuring Summary ---", flush=True)
    print(f"Total questions: {total}", flush=True)
    print(f"Successful: {success_count}", flush=True)
    print(f"Failed: {fail_count}", flush=True)
    print(f"Saved to {output_file}", flush=True)

    return output_file


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Structure markdown into JSON via Gemini.")
    parser.add_argument("markdown", help="Path to the 02_DO-Spaces-Output markdown file")
    parser.add_argument("--output-dir", default=None, help="Output directory (default: 03_Structured-Output/)")
    args = parser.parse_args()

    asyncio.run(structure_markdown(args.markdown, args.output_dir))
