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
import sqlite3
from datetime import datetime, timezone
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

# Pricing for google/gemini-2.5-flash-lite via OpenRouter (USD per 1M tokens)
# Verify latest rates at: https://openrouter.ai/google/gemini-2.5-flash-lite
MODEL_PRICING = {
    "input_per_1m":  0.10,   # $ per 1M input tokens
    "output_per_1m": 0.40,   # $ per 1M output tokens
}

DB_PATH = Path(__file__).resolve().parent / "token_usage.db"

QUESTION_START = re.compile(
    r"^(?:-\s*)?(?:\*\*)?(\d{1,3})[\.\*]",
    re.MULTILINE,
)

SYSTEM_PROMPT_SINGLE = """You are a JEE exam question parser. Extract structured data from this markdown block.

CRITICAL: Copy all text character-for-character from the source. Do NOT rephrase, reformat, reorder, or alter any content — including LaTeX, symbols, whitespace, or markdown syntax. The ONLY permitted modifications are:
1. Remove leading question number prefix (e.g. "1.", "**2.**").
2. Remove option label prefixes (e.g. "(1)", "(A)", "A.").
3. Remove "Sol." prefix and "Correct option (N)" suffix from explanation.
4. Remove duplicate image alt-text that appears as a standalone paragraph immediately after its ![desc](url).

Rules:
- "question": Question text after removing prefix. Keep all images as ![desc](url) inline.
- "options": 4-element array for MCQ; empty array for numerical/open_text. Keep exact text after removing label prefix.
- "type": "multiple_choice", "numerical", "multiple_select", or "open_text".
- "correct_answer": Exact answer value, no option number.
- "explanation": Full solution text after removing Sol. prefix and Correct option suffix. Keep all images inline.
- "subject": "Physics", "Chemistry", or "Maths"."""

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


def setup_database():
    """Create SQLite DB and tables if they don't already exist."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS structuring_runs (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            pdf_name            TEXT    NOT NULL,
            run_timestamp       TEXT    NOT NULL,
            total_questions     INTEGER NOT NULL,
            successful          INTEGER NOT NULL,
            failed              INTEGER NOT NULL,
            total_input_tokens  INTEGER NOT NULL,
            total_output_tokens INTEGER NOT NULL,
            total_tokens        INTEGER NOT NULL,
            total_input_cost    REAL    NOT NULL,
            total_output_cost   REAL    NOT NULL,
            total_cost          REAL    NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS structuring_question_stats (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL REFERENCES structuring_runs(id),
            question_number INTEGER NOT NULL,
            subject         TEXT,
            input_tokens    INTEGER NOT NULL,
            output_tokens   INTEGER NOT NULL,
            total_tokens    INTEGER NOT NULL,
            input_cost      REAL    NOT NULL,
            output_cost     REAL    NOT NULL,
            total_cost      REAL    NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def compute_cost(input_tokens, output_tokens):
    """Return (input_cost, output_cost, total_cost) in USD."""
    input_cost  = input_tokens  * MODEL_PRICING["input_per_1m"]  / 1_000_000
    output_cost = output_tokens * MODEL_PRICING["output_per_1m"] / 1_000_000
    return input_cost, output_cost, input_cost + output_cost


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
    """Send a single question block to Gemini and return (structured JSON, usage_dict)."""
    _no_usage = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}

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
            usage = response.usage
            usage_dict = {
                "input_tokens":  usage.prompt_tokens,
                "output_tokens": usage.completion_tokens,
                "total_tokens":  usage.total_tokens,
            }
            return json.loads(content), usage_dict
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                print(f"  Q{question_num}: FAILED after {MAX_RETRIES} attempts: {e}", flush=True)
                return {"error": str(e), "question_number": question_num}, _no_usage
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

    setup_database()

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
    per_question_stats = []

    for q_num, q_text in blocks:
        print(f"  Processing Q{q_num}...", flush=True)
        result, usage = await extract_question_single(client, q_text, q_num)
        results.append(result)

        in_cost, out_cost, total_cost = compute_cost(usage["input_tokens"], usage["output_tokens"])

        if "error" in result:
            fail_count += 1
        else:
            success_count += 1
            print(
                f"    -> {result.get('subject', '?')}"
                f"  |  tokens: {usage['input_tokens']} in + {usage['output_tokens']} out = {usage['total_tokens']}"
                f"  |  cost: ${in_cost:.6f} + ${out_cost:.6f} = ${total_cost:.6f}",
                flush=True,
            )

        per_question_stats.append({
            "question_number": q_num,
            "subject": result.get("subject"),
            **usage,
            "input_cost":  in_cost,
            "output_cost": out_cost,
            "total_cost":  total_cost,
        })

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{md_path.stem}.json"
    output_file.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Aggregate totals
    total  = len(blocks)
    t_in   = sum(s["input_tokens"]  for s in per_question_stats)
    t_out  = sum(s["output_tokens"] for s in per_question_stats)
    t_tok  = sum(s["total_tokens"]  for s in per_question_stats)
    t_in_c = sum(s["input_cost"]    for s in per_question_stats)
    t_ou_c = sum(s["output_cost"]   for s in per_question_stats)
    t_cost = sum(s["total_cost"]    for s in per_question_stats)

    print(f"\n--- Structuring Summary for {md_path.name} ---", flush=True)
    print(f"Total questions : {total}  |  Successful: {success_count}  |  Failed: {fail_count}", flush=True)
    print(f"Total tokens    : {t_in} in + {t_out} out = {t_tok}", flush=True)
    print(f"Total cost      : ${t_in_c:.6f} + ${t_ou_c:.6f} = ${t_cost:.6f}", flush=True)
    print(f"Saved to {output_file}", flush=True)

    # Persist to SQLite
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO structuring_runs
            (pdf_name, run_timestamp, total_questions, successful, failed,
             total_input_tokens, total_output_tokens, total_tokens,
             total_input_cost, total_output_cost, total_cost)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        md_path.name,
        datetime.now(timezone.utc).isoformat(),
        total, success_count, fail_count,
        t_in, t_out, t_tok,
        t_in_c, t_ou_c, t_cost,
    ))
    run_id = cur.lastrowid
    cur.executemany("""
        INSERT INTO structuring_question_stats
            (run_id, question_number, subject,
             input_tokens, output_tokens, total_tokens,
             input_cost, output_cost, total_cost)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (
            run_id,
            s["question_number"], s["subject"],
            s["input_tokens"], s["output_tokens"], s["total_tokens"],
            s["input_cost"], s["output_cost"], s["total_cost"],
        )
        for s in per_question_stats
    ])
    conn.commit()
    conn.close()
    print(f"Token usage saved to {DB_PATH}", flush=True)

    return output_file


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Structure markdown into JSON via Gemini.")
    parser.add_argument("markdown", help="Path to the 02_DO-Spaces-Output markdown file")
    parser.add_argument("--output-dir", default=None, help="Output directory (default: 03_Structured-Output/)")
    args = parser.parse_args()

    asyncio.run(structure_markdown(args.markdown, args.output_dir))
