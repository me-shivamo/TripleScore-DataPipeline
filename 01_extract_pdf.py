"""
Standalone Datalab PDF extraction module.

Usage:
    python 01_extract_pdf.py [PDF_PATH] [--debug] [--start-page N] [--end-page N] [--chunk-size N]

Extracts a PDF to markdown + images using the Datalab API.
Output is saved to 01_Datalab-Output/{pdf_stem}.md and 01_Datalab-Output/images/
"""

import argparse
import asyncio
import base64
import os
import re
import time
from pathlib import Path

from pypdf import PdfReader


def configure_ssl_certificates():
    if os.getenv("SSL_CERT_FILE"):
        return

    ca_bundle = None

    try:
        import certifi

        ca_bundle = certifi.where()
    except ImportError:
        try:
            from pip._vendor import certifi as pip_certifi

            ca_bundle = pip_certifi.where()
        except Exception:
            return

    os.environ.setdefault("SSL_CERT_FILE", ca_bundle)
    os.environ.setdefault("REQUESTS_CA_BUNDLE", ca_bundle)


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
configure_ssl_certificates()

from datalab_sdk import AsyncDatalabClient, ConvertOptions
from datalab_sdk.exceptions import DatalabAPIError, DatalabTimeoutError

IMAGE_LINK_PATTERN = re.compile(r"!\[[^\]]*]\(([^)]+)\)")


def get_pdf_page_count(pdf_path: Path) -> int:
    """Return the total number of pages in a PDF file."""
    reader = PdfReader(str(pdf_path))
    return len(reader.pages)


def resolve_page_bounds(start_page, end_page, total_pages: int) -> tuple:
    """Resolve start/end page values to a 0-based inclusive (start, end) tuple.
    None for start_page defaults to 0; None for end_page defaults to total_pages - 1."""
    start = int(start_page) if start_page is not None else 0
    end = int(end_page) if end_page is not None else total_pages - 1

    start = max(0, start)
    end = min(end, total_pages - 1)

    if start > end:
        raise ValueError(
            f"Invalid page bounds: start ({start}) > end ({end})"
        )

    return start, end


class ProgressDatalabClient(AsyncDatalabClient):
    async def _poll_result(
        self, check_url: str, max_polls: int = 300, poll_interval: int = 1
    ):
        full_url = (
            check_url
            if check_url.startswith("http")
            else f"{self.base_url}/{check_url.lstrip('/')}"
        )
        started_at = time.monotonic()

        print("Conversion started. Waiting for Datalab updates...", flush=True)

        for poll_number in range(1, max_polls + 1):
            data = await self._poll_get_with_retry(full_url)

            status = data.get("status", "unknown")
            elapsed_seconds = int(time.monotonic() - started_at)
            details = [
                f"status={status}",
                f"elapsed={elapsed_seconds}s",
                f"poll={poll_number}/{max_polls}",
            ]

            page_count = data.get("page_count")
            runtime = data.get("runtime")
            if page_count is not None:
                details.append(f"pages={page_count}")
            if runtime is not None:
                details.append(f"runtime={runtime}s")

            print(f"Conversion update: {', '.join(details)}", flush=True)

            if status == "complete":
                return data

            if not data.get("success", True) and status != "processing":
                raise DatalabAPIError(
                    f"Processing failed: {data.get('error', 'Unknown error')}"
                )

            print(f"  Polling again in 30s... (poll {poll_number}/{max_polls})", flush=True)
            await asyncio.sleep(30)

        raise DatalabTimeoutError(
            f"Polling timed out after {max_polls * poll_interval} seconds"
        )


def save_images(images, images_dir):
    images_dir.mkdir(parents=True, exist_ok=True)

    for image_name, image_data in (images or {}).items():
        image_path = images_dir / image_name
        with image_path.open("wb") as image_file:
            image_file.write(base64.b64decode(image_data))


def move_existing_images_to_output_dir(base_dir, images_dir):
    images_dir.mkdir(parents=True, exist_ok=True)

    for image_path in base_dir.glob("*_img.*"):
        target_path = images_dir / image_path.name
        if image_path == target_path:
            continue
        image_path.replace(target_path)


def rewrite_markdown_image_paths(markdown_text):
    def replace_image_path(match):
        image_ref = match.group(1).strip()
        image_name = Path(image_ref).name
        return match.group(0).replace(image_ref, f"images/{image_name}")

    return IMAGE_LINK_PATTERN.sub(replace_image_path, markdown_text)


async def extract(
    pdf_path,
    output_dir,
    start_page=None,
    end_page=None,
    debug=False,
    poll_interval=3,
    max_polls=600,
    chunk_size=0,
    min_quality_score=4.0,
    max_chunk_retries=2,
    parse_mode="balanced",
):
    """Extract PDF to markdown + images. Returns the output markdown file path."""
    api_key = os.getenv("DATALAB_API_KEY")
    if not api_key:
        raise RuntimeError(
            "Missing DATALAB_API_KEY. Set it in the .env file "
            "in this directory before running the script."
        )

    pdf_path = Path(pdf_path).resolve()
    output_dir = Path(output_dir)
    images_dir = output_dir / "images"
    output_stem = pdf_path.stem

    if not pdf_path.exists():
        raise FileNotFoundError(f"Input PDF not found: {pdf_path}")

    move_existing_images_to_output_dir(BASE_DIR, images_dir)

    total_pages = get_pdf_page_count(pdf_path)
    range_start, range_end = resolve_page_bounds(start_page, end_page, total_pages)
    effective_chunk = chunk_size if chunk_size > 0 else (range_end - range_start + 1)

    total_chunks = -(-( range_end - range_start + 1) // effective_chunk)  # ceiling division
    print(
        f"PDF has {total_pages} pages. "
        f"Extracting pages {range_start}-{range_end} "
        f"in {total_chunks} chunk(s) of {effective_chunk} page(s) each. "
        f"Quality threshold: {min_quality_score}, max retries per chunk: {max_chunk_retries}.",
        flush=True,
    )

    all_markdown_parts = []
    all_images = {}
    chunk_number = 0

    async with ProgressDatalabClient(api_key=api_key) as client:
        chunk_start = range_start
        while chunk_start <= range_end:
            chunk_end = min(chunk_start + effective_chunk - 1, range_end)
            chunk_range_str = f"{chunk_start}-{chunk_end}"
            chunk_number += 1

            best_result = None
            best_score = -1.0
            accepted = False

            for attempt in range(1, max_chunk_retries + 2):  # 1 initial + max_chunk_retries
                print(
                    f"\n[Chunk {chunk_number}/{total_chunks}] "
                    f"Pages {chunk_range_str} — attempt {attempt}/{max_chunk_retries + 1}",
                    flush=True,
                )

                options = ConvertOptions(
                    output_format="markdown",
                    mode=parse_mode,
                    page_range=chunk_range_str,
                    skip_cache=True,
                )
                result = await client.convert(
                    pdf_path,
                    options=options,
                    max_polls=max_polls,
                    poll_interval=poll_interval,
                )

                score = result.parse_quality_score
                score_display = f"{score:.2f}" if score is not None else "N/A"
                print(
                    f"[Chunk {chunk_number}/{total_chunks}] "
                    f"Pages {chunk_range_str} — "
                    f"quality_score={score_display}, "
                    f"pages={result.page_count}, "
                    f"runtime={result.runtime}s",
                    flush=True,
                )

                # Track best result across attempts
                if score is not None and score > best_score:
                    best_score = score
                    best_result = result
                elif score is None and best_result is None:
                    best_result = result

                if score is None or score > min_quality_score:
                    print(
                        f"[Chunk {chunk_number}/{total_chunks}] "
                        f"Pages {chunk_range_str} — quality accepted.",
                        flush=True,
                    )
                    accepted = True
                    break
                else:
                    if attempt <= max_chunk_retries:
                        print(
                            f"[Chunk {chunk_number}/{total_chunks}] "
                            f"Pages {chunk_range_str} — quality score {score_display} <= {min_quality_score}. "
                            f"Retrying ({attempt}/{max_chunk_retries})...",
                            flush=True,
                        )

            if not accepted:
                print(
                    f"\n[ERROR] Pages {chunk_range_str} (chunk {chunk_number}/{total_chunks}) "
                    f"failed quality check after {max_chunk_retries + 1} attempts. "
                    f"Best score achieved: {best_score:.2f} (threshold: {min_quality_score}). "
                    f"Stopping extraction.",
                    flush=True,
                )
                if all_markdown_parts:
                    print(
                        f"Saving partial output from {len(all_markdown_parts)} successful chunk(s) "
                        f"(pages {range_start}-{chunk_start - 1}).",
                        flush=True,
                    )
                break

            # Use the accepted (or best) result
            final_result = result if accepted else best_result
            chunk_md = rewrite_markdown_image_paths(final_result.markdown or "")
            if not chunk_md.strip():
                print(
                    f"[Chunk {chunk_number}/{total_chunks}] "
                    f"Pages {chunk_range_str} returned empty markdown. Stopping.",
                    flush=True,
                )
                break

            all_markdown_parts.append(chunk_md)
            if final_result.images:
                all_images.update(final_result.images)

            chunk_start += effective_chunk

    if not all_markdown_parts:
        raise RuntimeError("Datalab returned no markdown content for any chunk.")

    markdown_text = "\n\n".join(all_markdown_parts)

    # Save output locally
    output_dir.mkdir(parents=True, exist_ok=True)
    save_images(all_images, images_dir)

    # Save full markdown
    output_file = "debug_output.md" if debug else f"{output_stem}.md"
    md_path = output_dir / output_file
    with open(md_path, "w", encoding="utf-8") as output_handle:
        output_handle.write(markdown_text)

    print("\nExtraction complete.", flush=True)
    print(f"Markdown saved to {output_file} and images are in {images_dir.name}/", flush=True)

    return md_path


def parse_args():
    parser = argparse.ArgumentParser(description="Extract PDF to markdown using Datalab.")
    parser.add_argument(
        "pdf",
        nargs="?",
        default=str(BASE_DIR / "PDFs" / "JEE_Mains_2026_Jan_28.pdf"),
        help="Path to the input PDF file",
    )
    parser.add_argument("--debug", action="store_true", help="Debug mode")

    env_start = os.getenv("START_PAGE", "").strip()
    parser.add_argument("--start-page", type=int, default=int(env_start) if env_start else None,
                        help="First page to extract, 1-based (reads START_PAGE env var)")

    env_end = os.getenv("END_PAGE", "").strip()
    parser.add_argument("--end-page", type=int, default=int(env_end) if env_end else None,
                        help="Last page to extract, 1-based (reads END_PAGE env var)")

    env_chunk = os.getenv("CHUNK_SIZE", "").strip()
    parser.add_argument("--chunk-size", type=int, default=int(env_chunk) if env_chunk else 0,
                        help="Pages per API call. 0 or empty = full range in one call (reads CHUNK_SIZE env var)")

    env_quality = os.getenv("MIN_QUALITY_SCORE", "").strip()
    parser.add_argument("--min-quality-score", type=float,
                        default=float(env_quality) if env_quality else 4.0,
                        help="Minimum parse quality score to accept a chunk (reads MIN_QUALITY_SCORE env var)")

    env_retries = os.getenv("MAX_CHUNK_RETRIES", "").strip()
    parser.add_argument("--max-chunk-retries", type=int,
                        default=int(env_retries) if env_retries else 2,
                        help="Max retries per chunk on quality failure (reads MAX_CHUNK_RETRIES env var)")

    env_mode = os.getenv("PARSE_MODE", "").strip()
    parser.add_argument("--parse-mode", default=env_mode if env_mode else "balanced",
                        choices=["fast", "balanced", "accurate"],
                        help="Datalab parsing mode: fast, balanced, or accurate (reads PARSE_MODE env var)")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(
        extract(
            pdf_path=args.pdf,
            output_dir=BASE_DIR / "01_Datalab-Output",
            start_page=args.start_page,
            end_page=args.end_page,
            debug=args.debug,
            chunk_size=args.chunk_size,
            min_quality_score=args.min_quality_score,
            max_chunk_retries=args.max_chunk_retries,
            parse_mode=args.parse_mode,
        )
    )
