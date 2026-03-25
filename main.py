"""
Main pipeline orchestrator.

Usage:
    python main.py [PDF_PATH] [--debug]

Runs the full pipeline: PDF extraction -> DigitalOcean Spaces upload.
Configure the pipeline steps below by toggling the flags.
"""

import asyncio
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# ============================================================
# PIPELINE CONFIGURATION - Change these to control the pipeline
# ============================================================

# Input PDF path
PDF_PATH = str(BASE_DIR / "PDFs" / "JEE_Mains_2026_Jan_28.pdf")

# Output directory for extracted markdown and images
OUTPUT_DIR = str(BASE_DIR / "01_Datalab-Output")

# Page range to extract (e.g. "3-5", "4", None for full PDF)
PAGE_RANGE = ""  # Empty string = extract ALL pages

# Pipeline steps - toggle these to run specific steps
RUN_EXTRACTION = True       # Step 1: Extract PDF to markdown + images via Datalab
RUN_SPACES_UPLOAD = True      # Step 2: Upload images to DigitalOcean Spaces and update markdown
RUN_GEMINI_STRUCTURING = True  # Step 3: Structure markdown to JSON via Gemini
RUN_CLASSIFICATION = True       # Step 4: Classify topic and chapter via Gemini
RUN_ENRICHMENT = True           # Step 4.1: Add question IDs and source
RUN_EMBEDDING = True            # Step 5: Generate embeddings via Google Gemini API

# DigitalOcean Spaces folder name (images will be uploaded to this folder)
# Set to None to auto-generate from PDF name: "TripleScore/{pdf_stem}"
SPACES_FOLDER = None

# Debug mode - process only a single page (useful for testing)
DEBUG_MODE = False

# Datalab polling settings
POLL_INTERVAL_SECONDS = 3
MAX_POLLS = 600

# ============================================================


async def run_pipeline():
    pdf_path = PDF_PATH
    output_dir = OUTPUT_DIR
    page_range = PAGE_RANGE
    debug = DEBUG_MODE

    # CLI args override the config above
    import argparse
    parser = argparse.ArgumentParser(description="Run the full data pipeline.")
    parser.add_argument("pdf", nargs="?", default=None, help="Path to input PDF")
    parser.add_argument("--debug", action="store_true", help="Process only a single page")
    args = parser.parse_args()

    if args.pdf:
        pdf_path = args.pdf
    if args.debug:
        debug = True

    md_path = None

    # --- Step 1: PDF Extraction ---
    if RUN_EXTRACTION:
        import importlib
        extract = importlib.import_module("01_extract_pdf").extract

        page_range_value = page_range or os.getenv("PAGE_RANGE") or None
        # Ensure empty strings become None (extract full PDF)
        if page_range_value and not page_range_value.strip():
            page_range_value = None

        print("=" * 50, flush=True)
        print("STEP 1: PDF Extraction (Datalab)", flush=True)
        print("=" * 50, flush=True)

        md_path = await extract(
            pdf_path=pdf_path,
            output_dir=output_dir,
            page_range=page_range_value,
            debug=debug,
            poll_interval=POLL_INTERVAL_SECONDS,
            max_polls=MAX_POLLS,
        )
    else:
        # If skipping extraction, look for existing markdown
        pdf_stem = Path(pdf_path).stem
        candidate = Path(output_dir) / f"{pdf_stem}.md"
        if candidate.exists():
            md_path = candidate
            print(f"Skipping extraction. Using existing: {candidate.name}", flush=True)
        else:
            print(f"Skipping extraction. No markdown found at: {candidate}", flush=True)

    # --- Step 2: DigitalOcean Spaces Upload ---
    spaces_md_path = None
    if RUN_SPACES_UPLOAD and md_path:
        import importlib
        upload_and_rewrite = importlib.import_module("02_upload_digitalocean").upload_and_rewrite

        print("\n" + "=" * 50, flush=True)
        print("STEP 2: DigitalOcean Spaces Upload", flush=True)
        print("=" * 50, flush=True)

        _url_map, spaces_md_path = await upload_and_rewrite(
            md_path=md_path,
            spaces_folder=SPACES_FOLDER,
        )
    elif RUN_SPACES_UPLOAD and not md_path:
        print("Skipping Spaces upload: no markdown file available.", flush=True)

    # If skipping Step 2, look for existing 02_DO-Spaces-Output markdown
    if not spaces_md_path and md_path:
        candidate = BASE_DIR / "02_DO-Spaces-Output" / Path(md_path).name
        if candidate.exists():
            spaces_md_path = candidate
            print(f"Using existing Spaces output: {candidate.name}", flush=True)

    # --- Step 3: Gemini Structuring ---
    if RUN_GEMINI_STRUCTURING and spaces_md_path:
        import importlib
        structure_markdown = importlib.import_module("03_structure_gemini").structure_markdown

        print("\n" + "=" * 50, flush=True)
        print("STEP 3: Gemini Structuring", flush=True)
        print("=" * 50, flush=True)

        await structure_markdown(md_path=spaces_md_path)
    elif RUN_GEMINI_STRUCTURING and not spaces_md_path:
        print("Skipping Gemini structuring: no Spaces markdown available.", flush=True)

    # --- Step 4: Topic/Chapter Classification ---
    if RUN_CLASSIFICATION:
        import importlib
        classify_all = importlib.import_module("04_classify_topic_chapter").classify_all

        print("\n" + "=" * 50, flush=True)
        print("STEP 4: Topic/Chapter Classification", flush=True)
        print("=" * 50, flush=True)

        await classify_all()
    else:
        print("Skipping classification.", flush=True)

    # --- Step 4.1: ID and Source Enrichment ---
    if RUN_ENRICHMENT:
        import importlib
        enrich_all = importlib.import_module("04_1_enrich_ids").enrich_all

        print("\n" + "=" * 50, flush=True)
        print("STEP 4.1: ID and Source Enrichment", flush=True)
        print("=" * 50, flush=True)

        enrich_all()
    else:
        print("Skipping enrichment.", flush=True)

    # --- Step 5: Embedding Generation ---
    if RUN_EMBEDDING:
        import importlib
        embed_all = importlib.import_module("05_embed_questions").embed_all

        print("\n" + "=" * 50, flush=True)
        print("STEP 5: Embedding Generation", flush=True)
        print("=" * 50, flush=True)

        embed_all()
    else:
        print("Skipping embedding.", flush=True)

    print("\n" + "=" * 50, flush=True)
    print("Pipeline complete.", flush=True)
    print("=" * 50, flush=True)


if __name__ == "__main__":
    asyncio.run(run_pipeline())
