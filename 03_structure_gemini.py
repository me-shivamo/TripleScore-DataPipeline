"""
Standalone Gemini structuring module.

Usage:
    python structure_gemini.py <markdown_file> [--output-dir DIR]

Reads a DO-Spaces-Output markdown file, splits it into individual question
blocks, sends pairs of 2 to Gemini 2.5 Flash-Lite (via OpenRouter) for structured
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
- "question": Full question text. Preserve LaTeX ($...$ and $$...$$). Remove bold markers (**) and question number prefix. If the question has diagram(s)/image(s), keep them inline as markdown image syntax ![description](url) at their original position in the text. Remove duplicate image description text that appears as a standalone paragraph after the image link.
- "options": Array of option strings. Preserve LaTeX. Strip "(1)", "(2)" etc. prefixes. Always 4 elements for MCQ.
- "type": "multiple_choice" (4 options, single answer), "numerical" (blanks/asks for number), "multiple_select" (multiple correct), "open_text" (otherwise).
- "correct_answer": The actual answer value (e.g. "$mg/4$", "120.15", "1000"), NOT the option number.
- "explanation": Full solution text exact as in markdown + simple short explanation. Preserve LaTeX. Remove "Sol." prefix and "Correct option (N)" suffix. If the solution has diagram(s)/image(s), keep them inline as markdown image syntax ![description](url) at their original position. Remove duplicate image description text that appears as a standalone paragraph after the image link.
- "subject": Classify as "Physics", "Chemistry", or "Maths" based on question content."""

SYSTEM_PROMPT_BATCH = """You are a JEE exam question parser. You will receive TWO question blocks separated by "---QUESTION SEPARATOR---". Extract structured data from EACH block and return a JSON array of exactly 2 objects.

Rules for each object:
- "question": Full question text. Preserve LaTeX ($...$ and $$...$$). Remove bold markers (**) and question number prefix. If the question has diagram(s)/image(s), keep them inline as markdown image syntax ![description](url) at their original position in the text. Remove duplicate image description text that appears as a standalone paragraph after the image link.
- "options": Array of option strings. Preserve LaTeX. Strip "(1)", "(2)" etc. prefixes. Always 4 elements for MCQ.
- "type": "multiple_choice" (4 options, single answer), "numerical" (blanks/asks for number), "multiple_select" (multiple correct), "open_text" (otherwise).
- "correct_answer": The actual answer value (e.g. "$mg/4$", "120.15", "1000"), NOT the option number.
- "explanation": Full solution text exact as in markdown + simple short explanation. Preserve LaTeX. Remove "Sol." prefix and "Correct option (N)" suffix. If the solution has diagram(s)/image(s), keep them inline as markdown image syntax ![description](url) at their original position. Remove duplicate image description text that appears as a standalone paragraph after the image link.
- "subject": Classify as "Physics", "Chemistry", or "Maths" based on question content.

Return a JSON array of exactly 2 structured objects, one per question, in the same order as provided."""

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

RESPONSE_SCHEMA_BATCH = {
    "type": "json_schema",
    "json_schema": {
        "name": "question_extraction_batch",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "questions": {
                    "type": "array",
                    "items": {
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
            },
            "required": ["questions"],
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


async def extract_question_batch(client, block1, block2):
    """Send 2 question blocks in one API call. Returns list of 2 results or None on failure."""
    q1_num, q1_text = block1
    q2_num, q2_text = block2
    combined = f"{q1_text}\n\n---QUESTION SEPARATOR---\n\n{q2_text}"

    for attempt in range(MAX_RETRIES):
        try:
            response = await client.chat.completions.create(
                model="google/gemini-2.5-flash-lite",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT_BATCH},
                    {"role": "user", "content": combined},
                ],
                response_format=RESPONSE_SCHEMA_BATCH,
                temperature=0.5,
            )
            content = response.choices[0].message.content
            parsed = json.loads(content)
            questions = parsed.get("questions", parsed) if isinstance(parsed, dict) else parsed
            if isinstance(questions, list) and len(questions) == 2:
                return questions
            print(f"  Batch Q{q1_num}+Q{q2_num}: unexpected response length {len(questions) if isinstance(questions, list) else 'N/A'}, falling back", flush=True)
            return None
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                print(f"  Batch Q{q1_num}+Q{q2_num}: FAILED after {MAX_RETRIES} attempts: {e}, falling back to individual", flush=True)
                return None
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            print(f"  Batch Q{q1_num}+Q{q2_num}: retry {attempt + 1} in {delay}s ({e})", flush=True)
            await asyncio.sleep(delay)


async def structure_markdown(md_path, output_dir=None):
    """Parse DO-Spaces-Output markdown into structured JSON using Gemini.

    Args:
        md_path: Path to the DO-Spaces-Output markdown file.
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

    print(f"Found {len(blocks)} question(s). Processing in batches of 2...", flush=True)

    client = openai.AsyncOpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
        timeout=120.0,
    )

    results = []
    batch_success = 0
    batch_fail = 0
    individual_success = 0
    individual_fail = 0
    total = len(blocks)

    i = 0
    while i < len(blocks):
        if i + 1 < len(blocks):
            # Try batch of 2
            q1_num = blocks[i][0]
            q2_num = blocks[i + 1][0]
            print(f"  Processing batch Q{q1_num}+Q{q2_num}...", flush=True)
            batch_result = await extract_question_batch(client, blocks[i], blocks[i + 1])

            if batch_result is not None:
                results.extend(batch_result)
                batch_success += 1
                print(f"  Batch Q{q1_num}+Q{q2_num}: OK", flush=True)
                i += 2
                continue

            # Fallback: process individually
            batch_fail += 1
            print(f"  Falling back to individual for Q{q1_num} and Q{q2_num}...", flush=True)
            for j in range(2):
                q_num, q_text = blocks[i + j]
                print(f"  Processing Q{q_num} individually...", flush=True)
                result = await extract_question_single(client, q_text, q_num)
                results.append(result)
                if "error" in result:
                    individual_fail += 1
                else:
                    individual_success += 1
            i += 2
        else:
            # Odd one out - process individually
            q_num, q_text = blocks[i]
            print(f"  Processing Q{q_num} individually (odd)...", flush=True)
            result = await extract_question_single(client, q_text, q_num)
            results.append(result)
            if "error" in result:
                individual_fail += 1
            else:
                individual_success += 1
            i += 1

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{md_path.stem}.json"
    output_file.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    success_count = sum(1 for r in results if "error" not in r)
    print(f"\n--- Structuring Summary ---", flush=True)
    print(f"Total questions: {total}", flush=True)
    print(f"Successful: {success_count}", flush=True)
    print(f"Failed: {total - success_count}", flush=True)
    print(f"Batch calls succeeded: {batch_success}", flush=True)
    print(f"Batch calls failed (fell back): {batch_fail}", flush=True)
    print(f"Individual calls: success={individual_success}, fail={individual_fail}", flush=True)
    print(f"Saved to {output_file}", flush=True)

    return output_file


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Structure markdown into JSON via Gemini.")
    parser.add_argument("markdown", help="Path to the DO-Spaces-Output markdown file")
    parser.add_argument("--output-dir", default=None, help="Output directory (default: Structured-Output/)")
    args = parser.parse_args()

    asyncio.run(structure_markdown(args.markdown, args.output_dir))
