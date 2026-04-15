"""
Standalone embedding module.

Usage:
    python 05_embed_questions.py [--input-dir DIR] [--output-dir DIR]

Reads 04_Classified-Output/*.json files, generates embeddings for each question
using Google's text-embedding-004 model, and saves to 05_Embedded-Output/*.json
with an index.json for fast lookup.
"""

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

API_DELAY = 0.1  # gemini-embedding-001 has 1500 req/min free tier


def build_embedding_text(question):
    """Build the embedding input string from subject, chapter, topic, question, and options."""
    options_str = ", ".join(question.get("options", []))
    return (
        f"Subject: {question.get('subject', '')}\n"
        f"Chapter: {question.get('chapter', '')}\n"
        f"Topic: {question.get('topic', '')}\n"
        f"Question: {question.get('question', '')}\n"
        f"Options: {options_str}"
    )


def embed_file(input_file, output_dir, genai):
    """Generate embeddings for all questions in a single JSON file."""
    questions = json.loads(input_file.read_text(encoding="utf-8"))
    print(f"\nEmbedding {input_file.name}: {len(questions)} question(s)", flush=True)

    failed = []
    for idx, question in enumerate(questions):
        if "error" in question:
            failed.append(idx)
            continue

        try:
            text = build_embedding_text(question)
            result = genai.embed_content(
                model="models/gemini-embedding-2-preview",
                content=text,
                task_type="RETRIEVAL_DOCUMENT",
                title=question.get("title", ""),
            )
            question["embedding"] = result["embedding"]
        except Exception as e:
            print(f"  Q{idx + 1}: embedding failed: {e}", flush=True)
            print(f"  Q{idx + 1}: logging error and continuing with remaining questions...", flush=True)
            failed.append(idx)

        if (idx + 1) % 10 == 0 or (idx + 1) == len(questions):
            embedded_so_far = sum(1 for q in questions[:idx+1] if "embedding" in q)
            print(f"  Progress: {idx + 1}/{len(questions)} processed, {embedded_so_far} embedded, {len(failed)} failed", flush=True)

        time.sleep(API_DELAY)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / input_file.name
    output_file.write_text(
        json.dumps(questions, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    embedded_count = sum(1 for q in questions if "embedding" in q)
    print(f"Embedded {embedded_count}/{len(questions)} question(s). Saved to {output_file}", flush=True)

    if failed:
        print(f"  Failed indices: {failed}", flush=True)

    return questions


def build_index(all_questions, output_dir):
    """Build index.json mapping question IDs to embeddings."""
    index = {}
    for file_name, questions in all_questions.items():
        for idx, q in enumerate(questions):
            if "embedding" in q:
                key = f"{file_name}_{idx}"
                index[key] = q["embedding"]

    index_file = output_dir / "index.json"
    index_file.write_text(json.dumps(index), encoding="utf-8")
    print(f"\nIndex saved to {index_file} ({len(index)} entries)", flush=True)


def embed_all(input_dir=None, output_dir=None, single_file=None):
    """Generate embeddings for all classified questions, or a single file."""
    import google.generativeai as genai

    if input_dir is None:
        input_dir = BASE_DIR / "04_1_Enriched-Output"
    else:
        input_dir = Path(input_dir)

    if output_dir is None:
        output_dir = BASE_DIR / "05_Embedded-Output"
    else:
        output_dir = Path(output_dir)

    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY not set. Add it to .env or environment.")

    genai.configure(api_key=api_key)

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
            print(f"Skipping {skipped} already-embedded file(s).", flush=True)

    if json_files:
        print(f"Found {len(json_files)} file(s) to embed.", flush=True)
        for json_file in json_files:
            embed_file(json_file, output_dir, genai)
    else:
        print("All files already embedded. Nothing to do.", flush=True)

    # Rebuild index from ALL output files (both old and newly embedded)
    all_questions = {}
    for out_file in sorted(output_dir.glob("*.json")):
        if out_file.name == "index.json":
            continue
        all_questions[out_file.stem] = json.loads(out_file.read_text(encoding="utf-8"))

    build_index(all_questions, output_dir)
    print("\nEmbedding complete.", flush=True)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate embeddings for classified questions.")
    parser.add_argument("--input-dir", default=None, help="Input directory (default: 04_Classified-Output/)")
    parser.add_argument("--output-dir", default=None, help="Output directory (default: 05_Embedded-Output/)")
    args = parser.parse_args()

    embed_all(args.input_dir, args.output_dir)
