"""
Standalone Datalab PDF extraction module.

Usage:
    python 01_extract_pdf.py [PDF_PATH] [--debug] [--page-range PAGE_RANGE]

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


def parse_page_range(page_range, total_pages: int) -> tuple:
    """Parse a page_range string like '3-20' into a (start, end) 1-based inclusive tuple.
    If page_range is None, returns (1, total_pages)."""
    if not page_range or not str(page_range).strip():
        return 1, total_pages

    page_range = str(page_range).strip()
    if "-" in page_range:
        parts = page_range.split("-", 1)
        start = int(parts[0].strip())
        end = int(parts[1].strip())
    else:
        start = int(page_range)
        end = int(page_range)

    start = max(1, start)
    end = min(end, total_pages)

    if start > end:
        raise ValueError(
            f"Invalid page_range '{page_range}': start ({start}) > end ({end})"
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
    page_range=None,
    debug=False,
    poll_interval=3,
    max_polls=600,
    chunk_size=0,
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
    range_start, range_end = parse_page_range(page_range, total_pages)
    effective_chunk = chunk_size if chunk_size > 0 else (range_end - range_start + 1)
    print(f"PDF has {total_pages} pages. Extracting pages {range_start}-{range_end} in chunks of {effective_chunk}.", flush=True)

    all_markdown_parts = []
    all_images = {}

    async with ProgressDatalabClient(api_key=api_key) as client:
        chunk_start = range_start
        while chunk_start <= range_end:
            chunk_end = min(chunk_start + effective_chunk - 1, range_end)
            chunk_range_str = f"{chunk_start}-{chunk_end}"

            print(f"Submitting {pdf_path.name} pages {chunk_range_str}...", flush=True)
            options = ConvertOptions(
                output_format="markdown",
                mode="balanced",
                page_range=chunk_range_str,
            )
            result = await client.convert(
                pdf_path,
                options=options,
                max_polls=max_polls,
                poll_interval=poll_interval,
            )

            chunk_md = rewrite_markdown_image_paths(result.markdown or "")
            if not chunk_md.strip():
                print(f"Pages {chunk_range_str} returned empty markdown. Stopping.", flush=True)
                break

            all_markdown_parts.append(chunk_md)
            if result.images:
                all_images.update(result.images)

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

    print("Extraction complete.", flush=True)
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
    parser.add_argument("--debug", action="store_true", help="Process only a single page")
    parser.add_argument("--page-range", default=None, help="Page range (e.g. '3-5')")
    env_chunk = os.getenv("CHUNK_SIZE", "").strip()
    default_chunk = int(env_chunk) if env_chunk else 0
    parser.add_argument("--chunk-size", type=int, default=default_chunk,
                        help="Pages per API call. 0 or empty = full PDF in one call (reads CHUNK_SIZE env var)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    page_range = args.page_range or os.getenv("PAGE_RANGE") or None
    asyncio.run(
        extract(
            pdf_path=args.pdf,
            output_dir=BASE_DIR / "01_Datalab-Output",
            page_range=page_range,
            debug=args.debug,
            chunk_size=args.chunk_size,
        )
    )
