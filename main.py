"""
Main pipeline orchestrator.

Usage:
    python main.py [PDF_PATH] [--debug]

Runs the full pipeline: PDF extraction -> Cloudinary upload.
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
OUTPUT_DIR = str(BASE_DIR / "Datalab-Output")

# Page range to extract (e.g. "3-5", "4", None for full PDF)
PAGE_RANGE = None  # Set to None to use the value from .env

# Pipeline steps - toggle these to run specific steps
RUN_EXTRACTION = True       # Step 1: Extract PDF to markdown + images via Datalab
RUN_CLOUDINARY_UPLOAD = True  # Step 2: Upload images to Cloudinary and update markdown
RUN_GEMINI_STRUCTURING = True  # Step 3: Structure markdown to JSON via Gemini

# Cloudinary folder name (images will be uploaded to this folder)
# Set to None to auto-generate from PDF name: "TripleScore/{pdf_stem}"
CLOUDINARY_FOLDER = None

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
        from extract_pdf import extract

        page_range_value = page_range or os.getenv("PAGE_RANGE") or None

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

    # --- Step 2: Cloudinary Upload ---
    cloudinary_md_path = None
    if RUN_CLOUDINARY_UPLOAD and md_path:
        from upload_cloudinary import upload_and_rewrite

        print("\n" + "=" * 50, flush=True)
        print("STEP 2: Cloudinary Upload", flush=True)
        print("=" * 50, flush=True)

        _url_map, cloudinary_md_path = await upload_and_rewrite(
            md_path=md_path,
            cloudinary_folder=CLOUDINARY_FOLDER,
        )
    elif RUN_CLOUDINARY_UPLOAD and not md_path:
        print("Skipping Cloudinary upload: no markdown file available.", flush=True)

    # If skipping Step 2, look for existing Cloudinary-Output markdown
    if not cloudinary_md_path and md_path:
        candidate = BASE_DIR / "Cloudinary-Output" / Path(md_path).name
        if candidate.exists():
            cloudinary_md_path = candidate
            print(f"Using existing Cloudinary output: {candidate.name}", flush=True)

    # --- Step 3: Gemini Structuring ---
    if RUN_GEMINI_STRUCTURING and cloudinary_md_path:
        from structure_gemini import structure_markdown

        print("\n" + "=" * 50, flush=True)
        print("STEP 3: Gemini Structuring", flush=True)
        print("=" * 50, flush=True)

        await structure_markdown(md_path=cloudinary_md_path)
    elif RUN_GEMINI_STRUCTURING and not cloudinary_md_path:
        print("Skipping Gemini structuring: no Cloudinary markdown available.", flush=True)

    print("\n" + "=" * 50, flush=True)
    print("Pipeline complete.", flush=True)
    print("=" * 50, flush=True)


if __name__ == "__main__":
    asyncio.run(run_pipeline())
