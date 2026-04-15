"""
Standalone topic/chapter classification module.

Usage:
    python 04_classify_topic_chapter.py [--input-dir DIR] [--output-dir DIR]

Reads 03_Structured-Output/*.json files, sends each question individually to Gemini (via
OpenRouter) to classify topic and chapter, and saves the enriched results
to 04_Classified-Output/*.json.
"""

import asyncio
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# Pricing for google/gemini-2.5-flash-lite via OpenRouter (USD per 1M tokens)
# Verify latest rates at: https://openrouter.ai/google/gemini-2.5-flash-lite
MODEL_PRICING = {
    "input_per_1m":  0.10,   # $ per 1M input tokens
    "output_per_1m": 0.40,   # $ per 1M output tokens
}

DB_PATH = Path(__file__).resolve().parent / "token_usage.db"


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


def setup_database():
    """Create SQLite DB and tables if they don't already exist."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pdf_runs (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            pdf_name            TEXT    NOT NULL,
            run_timestamp       TEXT    NOT NULL,
            total_questions     INTEGER NOT NULL,
            classified          INTEGER NOT NULL,
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
        CREATE TABLE IF NOT EXISTS question_stats (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL REFERENCES pdf_runs(id),
            question_number INTEGER NOT NULL,
            subject         TEXT,
            topic           TEXT,
            chapter         TEXT,
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

SYSTEM_PROMPT = "Here is the given question of JEE paper with the subject, find which topic this question is belongs and the from which chapter"

RESPONSE_SCHEMA_PHYSICS = {
    "type": "json_schema",
    "json_schema": {
        "name": "topic_chapter_classification_physics",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "topic": {
                    "type": "string",
                    "enum": [
                        "Units and Measurements",
                        "Mechanics",
                        "Properties of Matter",
                        "Thermodynamics",
                        "Oscillations and Waves",
                        "Electrodynamics",
                        "Optics",
                        "Modern Physics",
                    ],
                },
                "chapter": {
                    "type": "string",
                    "enum": [
                        "Units and Measurements",
                        "Kinematics",
                        "Laws of Motion",
                        "Work Energy and Power",
                        "Centre of Mass and Rotational Motion",
                        "Gravitation",
                        "Mechanical Properties of Solids",
                        "Mechanical Properties of Fluids",
                        "Thermal Properties of Matter",
                        "Thermodynamics",
                        "Kinetic Theory of Gases",
                        "Oscillations",
                        "Waves",
                        "Electrostatics",
                        "Capacitance",
                        "Current Electricity",
                        "Magnetic Effects of Current",
                        "Magnetism and Matter",
                        "Electromagnetic Induction",
                        "Alternating Current",
                        "Electromagnetic Waves",
                        "Ray Optics and Optical Instruments",
                        "Wave Optics",
                        "Dual Nature of Matter and Radiation",
                        "Atoms",
                        "Nuclei",
                        "Semiconductors",
                        "Experimental Physics",
                    ],
                },
            },
            "propertyOrdering": ["topic", "chapter"],
            "required": ["topic", "chapter"],
            "additionalProperties": False,
        },
    },
}

RESPONSE_SCHEMA_CHEMISTRY = {
    "type": "json_schema",
    "json_schema": {
        "name": "topic_chapter_classification_chemistry",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "topic": {
                    "type": "string",
                    "enum": [
                        "Physical Chemistry",
                        "Inorganic Chemistry",
                        "Organic Chemistry",
                    ],
                },
                "chapter": {
                    "type": "string",
                    "enum": [
                        "Some Basic Concepts of Chemistry",
                        "Structure of Atom",
                        "States of Matter",
                        "Thermodynamics",
                        "Equilibrium",
                        "Redox Reactions",
                        "Solid State",
                        "Solutions",
                        "Electrochemistry",
                        "Chemical Kinetics",
                        "Surface Chemistry",
                        "Classification of Elements and Periodicity",
                        "Chemical Bonding and Molecular Structure",
                        "Hydrogen",
                        "s-Block Elements",
                        "p-Block Elements",
                        "d and f Block Elements",
                        "Coordination Compounds",
                        "Metallurgy",
                        "Environmental Chemistry",
                        "Basic Principles of Organic Chemistry",
                        "Hydrocarbons",
                        "Haloalkanes and Haloarenes",
                        "Alcohols Phenols and Ethers",
                        "Aldehydes Ketones and Carboxylic Acids",
                        "Amines",
                        "Biomolecules",
                        "Polymers",
                        "Chemistry in Everyday Life",
                        "Practical Organic Chemistry",
                    ],
                },
            },
            "propertyOrdering": ["topic", "chapter"],
            "required": ["topic", "chapter"],
            "additionalProperties": False,
        },
    },
}

RESPONSE_SCHEMA_MATHS = {
    "type": "json_schema",
    "json_schema": {
        "name": "topic_chapter_classification_maths",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "topic": {
                    "type": "string",
                    "enum": [
                        "Algebra",
                        "Calculus",
                        "Coordinate Geometry",
                        "3D Geometry",
                        "Vector Algebra",
                        "Trigonometry",
                        "Mathematical Reasoning",
                        "Statistics and Probability",
                    ],
                },
                "chapter": {
                    "type": "string",
                    "enum": [
                        "Sets Relations and Functions",
                        "Complex Numbers",
                        "Quadratic Equations",
                        "Sequences and Series",
                        "Permutations and Combinations",
                        "Binomial Theorem",
                        "Matrices",
                        "Determinants",
                        "Limits",
                        "Continuity and Differentiability",
                        "Application of Derivatives",
                        "Indefinite Integration",
                        "Definite Integration",
                        "Differential Equations",
                        "Straight Lines",
                        "Circles",
                        "Parabola",
                        "Ellipse",
                        "Hyperbola",
                        "Three Dimensional Geometry",
                        "Vector Algebra",
                        "Trigonometric Ratios and Identities",
                        "Trigonometric Equations",
                        "Inverse Trigonometric Functions",
                        "Mathematical Reasoning",
                        "Statistics",
                        "Probability",
                    ],
                },
            },
            "propertyOrdering": ["topic", "chapter"],
            "required": ["topic", "chapter"],
            "additionalProperties": False,
        },
    },
}

RESPONSE_SCHEMA_BY_SUBJECT = {
    "Physics": RESPONSE_SCHEMA_PHYSICS,
    "Chemistry": RESPONSE_SCHEMA_CHEMISTRY,
    "Maths": RESPONSE_SCHEMA_MATHS,
}

MAX_RETRIES = 3
RETRY_BASE_DELAY = 2
API_DELAY = 0.5


def build_question_text(question):
    return (
        f"Question: {question.get('question', '')}\n"
        f"Subject: {question.get('subject', 'Unknown')}"
    )


async def classify_question_single(client, question, idx, schema):
    """Send a single question to Gemini to get topic and chapter.

    Returns:
        (classification_dict, usage_dict) where usage_dict has keys:
        input_tokens, output_tokens, total_tokens.
        On hard failure, usage_dict values are all 0.
    """
    user_content = build_question_text(question)
    _no_usage = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}

    for attempt in range(MAX_RETRIES):
        try:
            response = await client.chat.completions.create(
                model="google/gemini-2.5-flash-lite",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                ],
                response_format=schema,
                temperature=0.2,
            )
            classification = json.loads(response.choices[0].message.content)
            usage = response.usage
            usage_dict = {
                "input_tokens":  usage.prompt_tokens,
                "output_tokens": usage.completion_tokens,
                "total_tokens":  usage.total_tokens,
            }
            return classification, usage_dict
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                print(f"  Q{idx + 1}: FAILED after {MAX_RETRIES} attempts: {e}", flush=True)
                return {"topic": "Unknown", "chapter": "Unknown"}, _no_usage
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            print(f"  Q{idx + 1}: retry {attempt + 1} in {delay}s ({e})", flush=True)
            await asyncio.sleep(delay)


async def classify_file(client, input_file, output_dir):
    """Classify all questions in a single JSON file one by one."""
    questions = json.loads(input_file.read_text(encoding="utf-8"))
    print(f"\nProcessing {input_file.name}: {len(questions)} question(s)", flush=True)

    classifiable = [(idx, q) for idx, q in enumerate(questions) if "error" not in q]

    per_question_stats = []

    for idx, q in classifiable:
        schema = RESPONSE_SCHEMA_BY_SUBJECT.get(q.get("subject"), RESPONSE_SCHEMA_PHYSICS)
        print(f"  Classifying Q{idx + 1} ({q.get('subject', '?')})...", flush=True)

        result, usage = await classify_question_single(client, q, idx, schema)
        questions[idx]["topic"]    = result["topic"]
        questions[idx]["chapter"]  = result["chapter"]

        in_cost, out_cost, total_cost = compute_cost(usage["input_tokens"], usage["output_tokens"])

        print(
            f"    -> {result['topic']} / {result['chapter']}"
            f"  |  tokens: {usage['input_tokens']} in + {usage['output_tokens']} out = {usage['total_tokens']}"
            f"  |  cost: ${in_cost:.6f} + ${out_cost:.6f} = ${total_cost:.6f}",
            flush=True,
        )

        per_question_stats.append({
            "question_number": idx + 1,
            "subject":         q.get("subject"),
            "topic":           result["topic"],
            "chapter":         result["chapter"],
            **usage,
            "input_cost":  in_cost,
            "output_cost": out_cost,
            "total_cost":  total_cost,
        })

        await asyncio.sleep(API_DELAY)

    # --- Save enriched JSON ---
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / input_file.name
    output_file.write_text(
        json.dumps(questions, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # --- Aggregate totals ---
    classified = sum(1 for q in questions if q.get("topic") not in (None, "Unknown"))
    total      = len(questions)
    t_in   = sum(s["input_tokens"]  for s in per_question_stats)
    t_out  = sum(s["output_tokens"] for s in per_question_stats)
    t_tok  = sum(s["total_tokens"]  for s in per_question_stats)
    t_in_c = sum(s["input_cost"]    for s in per_question_stats)
    t_ou_c = sum(s["output_cost"]   for s in per_question_stats)
    t_cost = sum(s["total_cost"]    for s in per_question_stats)

    print(f"\n--- Classification Summary for {input_file.name} ---", flush=True)
    print(f"Total questions : {total}  |  Classified: {classified}  |  Failed: {total - classified}", flush=True)
    print(f"Total tokens    : {t_in} in + {t_out} out = {t_tok}", flush=True)
    print(f"Total cost      : ${t_in_c:.6f} + ${t_ou_c:.6f} = ${t_cost:.6f}", flush=True)
    print(f"Saved to {output_file}", flush=True)

    # --- Persist to SQLite ---
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO pdf_runs
            (pdf_name, run_timestamp, total_questions, classified, failed,
             total_input_tokens, total_output_tokens, total_tokens,
             total_input_cost, total_output_cost, total_cost)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        input_file.name,
        datetime.now(timezone.utc).isoformat(),
        total, classified, total - classified,
        t_in, t_out, t_tok,
        t_in_c, t_ou_c, t_cost,
    ))
    run_id = cur.lastrowid
    cur.executemany("""
        INSERT INTO question_stats
            (run_id, question_number, subject, topic, chapter,
             input_tokens, output_tokens, total_tokens,
             input_cost, output_cost, total_cost)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (
            run_id,
            s["question_number"], s["subject"], s["topic"], s["chapter"],
            s["input_tokens"], s["output_tokens"], s["total_tokens"],
            s["input_cost"], s["output_cost"], s["total_cost"],
        )
        for s in per_question_stats
    ])
    conn.commit()
    conn.close()
    print(f"Token usage saved to {DB_PATH}", flush=True)

    return output_file


async def classify_all(input_dir=None, output_dir=None, single_file=None):
    """Classify topic/chapter for all questions in input_dir, or a single file."""
    import openai
    setup_database()

    if input_dir is None:
        input_dir = BASE_DIR / "03_Structured-Output"
    else:
        input_dir = Path(input_dir)

    if output_dir is None:
        output_dir = BASE_DIR / "04_Classified-Output"
    else:
        output_dir = Path(output_dir)

    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not set. Add it to .env or environment.")

    if single_file:
        json_files = [Path(single_file)]
        if not json_files[0].exists():
            print(f"File not found: {json_files[0]}", flush=True)
            return
    else:
        all_files = sorted(input_dir.glob("*.json"))
        if not all_files:
            print(f"No JSON files found in {input_dir}", flush=True)
            return
        json_files = [f for f in all_files if not (output_dir / f.name).exists()]
        skipped = len(all_files) - len(json_files)
        if skipped:
            print(f"Skipping {skipped} already-classified file(s).", flush=True)
        if not json_files:
            print("All files already classified. Nothing to do.", flush=True)
            return

    print(f"Found {len(json_files)} file(s) to classify.", flush=True)

    client = openai.AsyncOpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
        timeout=120.0,
    )

    for json_file in json_files:
        await classify_file(client, json_file, output_dir)

    print("\nClassification complete.", flush=True)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Classify questions by topic and chapter.")
    parser.add_argument("--input-dir", default=None, help="Input directory (default: 03_Structured-Output/)")
    parser.add_argument("--output-dir", default=None, help="Output directory (default: 04_Classified-Output/)")
    parser.add_argument("--file", default=None, help="Process a single JSON file instead of the whole directory")
    args = parser.parse_args()

    asyncio.run(classify_all(args.input_dir, args.output_dir, args.file))
