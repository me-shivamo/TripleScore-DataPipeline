"""
Standalone topic/chapter classification module.

Usage:
    python classify_topic_chapter.py [--input-dir DIR] [--output-dir DIR]

Reads Structured-Output/*.json files, sends pairs of 2 questions to Gemini (via
OpenRouter) to classify topic and chapter, and saves the enriched results
to Classified-Output/*.json.
"""

import asyncio
import json
import os
import time
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

SYSTEM_PROMPT = """You are a JEE exam question classifier. Given a question and its subject, return the topic and chapter.

Pick from ONLY these values:

## Physics
Topics and their chapters:
- Mechanics: Kinematics, Laws of Motion, Work Energy Power, Rotational Motion, Gravitation, Properties of Solids and Liquids, Center of Mass and Momentum
- Thermodynamics: Kinetic Theory of Gases, Laws of Thermodynamics, Heat Transfer
- Electromagnetism: Electrostatics, Current Electricity, Magnetic Effects of Current, Electromagnetic Induction, Alternating Current, Electromagnetic Waves
- Optics: Ray Optics, Wave Optics
- Modern Physics: Dual Nature of Matter and Radiation, Atoms and Nuclei, Semiconductor Electronics
- Waves: Mechanical Waves, Sound Waves, Oscillations
- Units and Measurements: Units Dimensions and Errors

## Chemistry
Topics and their chapters:
- Physical Chemistry: Mole Concept and Stoichiometry, Atomic Structure, Chemical Bonding, States of Matter, Chemical Thermodynamics, Chemical Equilibrium, Ionic Equilibrium, Electrochemistry, Chemical Kinetics, Surface Chemistry, Solutions
- Organic Chemistry: General Organic Chemistry, Hydrocarbons, Haloalkanes and Haloarenes, Alcohols Phenols Ethers, Aldehydes Ketones Carboxylic Acids, Amines, Biomolecules, Polymers, Chemistry in Everyday Life, Environmental Chemistry
- Inorganic Chemistry: Periodic Table and Periodicity, s-Block Elements, p-Block Elements, d and f Block Elements, Coordination Compounds, Metallurgy, Hydrogen, Qualitative Analysis

## Maths
Topics and their chapters:
- Algebra: Sets Relations and Functions, Complex Numbers, Quadratic Equations, Sequences and Series, Binomial Theorem, Permutations and Combinations, Matrices and Determinants, Mathematical Reasoning
- Calculus: Limits and Continuity, Differentiation, Applications of Derivatives, Indefinite Integration, Definite Integration, Applications of Integrals, Differential Equations
- Coordinate Geometry: Straight Lines, Circles, Conic Sections (Parabola Ellipse Hyperbola), Three Dimensional Geometry
- Trigonometry: Trigonometric Functions, Inverse Trigonometric Functions, Trigonometric Equations
- Probability and Statistics: Probability, Statistics
- Vectors: Vector Algebra
- Linear Programming: Linear Programming

Return ONLY valid topic and chapter values from the lists above."""

SYSTEM_PROMPT_BATCH = """You are a JEE exam question classifier. You will receive TWO questions. For each, return the topic and chapter.

Pick from ONLY these values:

## Physics
Topics and their chapters:
- Mechanics: Kinematics, Laws of Motion, Work Energy Power, Rotational Motion, Gravitation, Properties of Solids and Liquids, Center of Mass and Momentum
- Thermodynamics: Kinetic Theory of Gases, Laws of Thermodynamics, Heat Transfer
- Electromagnetism: Electrostatics, Current Electricity, Magnetic Effects of Current, Electromagnetic Induction, Alternating Current, Electromagnetic Waves
- Optics: Ray Optics, Wave Optics
- Modern Physics: Dual Nature of Matter and Radiation, Atoms and Nuclei, Semiconductor Electronics
- Waves: Mechanical Waves, Sound Waves, Oscillations
- Units and Measurements: Units Dimensions and Errors

## Chemistry
Topics and their chapters:
- Physical Chemistry: Mole Concept and Stoichiometry, Atomic Structure, Chemical Bonding, States of Matter, Chemical Thermodynamics, Chemical Equilibrium, Ionic Equilibrium, Electrochemistry, Chemical Kinetics, Surface Chemistry, Solutions
- Organic Chemistry: General Organic Chemistry, Hydrocarbons, Haloalkanes and Haloarenes, Alcohols Phenols Ethers, Aldehydes Ketones Carboxylic Acids, Amines, Biomolecules, Polymers, Chemistry in Everyday Life, Environmental Chemistry
- Inorganic Chemistry: Periodic Table and Periodicity, s-Block Elements, p-Block Elements, d and f Block Elements, Coordination Compounds, Metallurgy, Hydrogen, Qualitative Analysis

## Maths
Topics and their chapters:
- Algebra: Sets Relations and Functions, Complex Numbers, Quadratic Equations, Sequences and Series, Binomial Theorem, Permutations and Combinations, Matrices and Determinants, Mathematical Reasoning
- Calculus: Limits and Continuity, Differentiation, Applications of Derivatives, Indefinite Integration, Definite Integration, Applications of Integrals, Differential Equations
- Coordinate Geometry: Straight Lines, Circles, Conic Sections (Parabola Ellipse Hyperbola), Three Dimensional Geometry
- Trigonometry: Trigonometric Functions, Inverse Trigonometric Functions, Trigonometric Equations
- Probability and Statistics: Probability, Statistics
- Vectors: Vector Algebra
- Linear Programming: Linear Programming

Return a JSON object with a "classifications" array of exactly 2 objects, each with "topic" and "chapter" fields. Return ONLY valid values from the lists above."""

RESPONSE_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "topic_chapter_classification",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string"},
                "chapter": {"type": "string"},
            },
            "required": ["topic", "chapter"],
            "additionalProperties": False,
        },
    },
}

RESPONSE_SCHEMA_BATCH = {
    "type": "json_schema",
    "json_schema": {
        "name": "topic_chapter_classification_batch",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "classifications": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "topic": {"type": "string"},
                            "chapter": {"type": "string"},
                        },
                        "required": ["topic", "chapter"],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["classifications"],
            "additionalProperties": False,
        },
    },
}

MAX_RETRIES = 3
RETRY_BASE_DELAY = 2
API_DELAY = 0.5


def build_question_text(question):
    return (
        f"Subject: {question.get('subject', 'Unknown')}\n"
        f"Question: {question.get('question', '')}\n"
        f"Options: {', '.join(question.get('options', []))}"
    )


async def classify_question_single(client, question, idx):
    """Send a single question to Gemini to get topic and chapter."""
    user_content = build_question_text(question)

    for attempt in range(MAX_RETRIES):
        try:
            response = await client.chat.completions.create(
                model="google/gemini-2.5-flash-lite",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                ],
                response_format=RESPONSE_SCHEMA,
                temperature=0.3,
            )
            content = response.choices[0].message.content
            return json.loads(content)
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                print(f"  Q{idx + 1}: FAILED after {MAX_RETRIES} attempts: {e}", flush=True)
                return {"topic": "Unknown", "chapter": "Unknown"}
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            print(f"  Q{idx + 1}: retry {attempt + 1} in {delay}s ({e})", flush=True)
            await asyncio.sleep(delay)


async def classify_question_batch(client, q1, q2, idx1, idx2):
    """Send 2 questions in one API call. Returns list of 2 results or None on failure."""
    user_content = (
        f"Question 1:\n{build_question_text(q1)}\n\n"
        f"---\n\n"
        f"Question 2:\n{build_question_text(q2)}"
    )

    for attempt in range(MAX_RETRIES):
        try:
            response = await client.chat.completions.create(
                model="google/gemini-2.5-flash-lite",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT_BATCH},
                    {"role": "user", "content": user_content},
                ],
                response_format=RESPONSE_SCHEMA_BATCH,
                temperature=0.3,
            )
            content = response.choices[0].message.content
            parsed = json.loads(content)
            classifications = parsed.get("classifications", [])
            if len(classifications) == 2:
                return classifications
            print(f"  Batch Q{idx1+1}+Q{idx2+1}: unexpected length {len(classifications)}, falling back", flush=True)
            return None
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                print(f"  Batch Q{idx1+1}+Q{idx2+1}: FAILED after {MAX_RETRIES} attempts: {e}, falling back", flush=True)
                return None
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            print(f"  Batch Q{idx1+1}+Q{idx2+1}: retry {attempt + 1} in {delay}s ({e})", flush=True)
            await asyncio.sleep(delay)


async def classify_file(client, input_file, output_dir):
    """Classify all questions in a single JSON file using batches of 2."""
    questions = json.loads(input_file.read_text(encoding="utf-8"))
    print(f"\nProcessing {input_file.name}: {len(questions)} question(s)", flush=True)

    # Build list of classifiable indices
    classifiable = [(idx, q) for idx, q in enumerate(questions) if "error" not in q]

    batch_success = 0
    batch_fail = 0
    individual_success = 0
    individual_fail = 0

    i = 0
    while i < len(classifiable):
        if i + 1 < len(classifiable):
            idx1, q1 = classifiable[i]
            idx2, q2 = classifiable[i + 1]
            print(f"  Classifying batch Q{idx1+1}+Q{idx2+1}...", flush=True)

            batch_result = await classify_question_batch(client, q1, q2, idx1, idx2)

            if batch_result is not None:
                questions[idx1]["topic"] = batch_result[0]["topic"]
                questions[idx1]["chapter"] = batch_result[0]["chapter"]
                questions[idx2]["topic"] = batch_result[1]["topic"]
                questions[idx2]["chapter"] = batch_result[1]["chapter"]
                batch_success += 1
                i += 2
                await asyncio.sleep(API_DELAY)
                continue

            # Fallback to individual
            batch_fail += 1
            for j in range(2):
                idx, q = classifiable[i + j]
                print(f"  Classifying Q{idx+1} individually...", flush=True)
                result = await classify_question_single(client, q, idx)
                questions[idx]["topic"] = result["topic"]
                questions[idx]["chapter"] = result["chapter"]
                if result["topic"] == "Unknown":
                    individual_fail += 1
                else:
                    individual_success += 1
                await asyncio.sleep(API_DELAY)
            i += 2
        else:
            # Odd one out
            idx, q = classifiable[i]
            print(f"  Classifying Q{idx+1} individually (odd)...", flush=True)
            result = await classify_question_single(client, q, idx)
            questions[idx]["topic"] = result["topic"]
            questions[idx]["chapter"] = result["chapter"]
            if result["topic"] == "Unknown":
                individual_fail += 1
            else:
                individual_success += 1
            i += 1
            await asyncio.sleep(API_DELAY)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / input_file.name
    output_file.write_text(
        json.dumps(questions, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    classified = sum(1 for q in questions if q.get("topic") not in (None, "Unknown"))
    total = len(questions)
    print(f"\n--- Classification Summary for {input_file.name} ---", flush=True)
    print(f"Total questions: {total}", flush=True)
    print(f"Classified: {classified}", flush=True)
    print(f"Failed: {total - classified}", flush=True)
    print(f"Batch calls succeeded: {batch_success}", flush=True)
    print(f"Batch calls failed (fell back): {batch_fail}", flush=True)
    print(f"Individual calls: success={individual_success}, fail={individual_fail}", flush=True)
    print(f"Saved to {output_file}", flush=True)
    return output_file


async def classify_all(input_dir=None, output_dir=None):
    """Classify topic/chapter for all questions in input_dir."""
    import openai

    if input_dir is None:
        input_dir = BASE_DIR / "Structured-Output"
    else:
        input_dir = Path(input_dir)

    if output_dir is None:
        output_dir = BASE_DIR / "Classified-Output"
    else:
        output_dir = Path(output_dir)

    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not set. Add it to .env or environment.")

    json_files = sorted(input_dir.glob("*.json"))
    if not json_files:
        print(f"No JSON files found in {input_dir}", flush=True)
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
    parser.add_argument("--input-dir", default=None, help="Input directory (default: Structured-Output/)")
    parser.add_argument("--output-dir", default=None, help="Output directory (default: Classified-Output/)")
    args = parser.parse_args()

    asyncio.run(classify_all(args.input_dir, args.output_dir))
