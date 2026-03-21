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

    options = ConvertOptions(
        output_format="markdown",
        mode="balanced",
        page_range=page_range,
    )

    print(f"Submitting {pdf_path.name} for conversion...", flush=True)
    async with ProgressDatalabClient(api_key=api_key) as client:
        result = await client.convert(
            pdf_path,
            options=options,
            max_polls=max_polls,
            poll_interval=poll_interval,
        )

    markdown_text = rewrite_markdown_image_paths(result.markdown or "")
    if not markdown_text:
        raise RuntimeError("Datalab returned no markdown content.")

    result.markdown = markdown_text

    # Save output locally
    output_dir.mkdir(parents=True, exist_ok=True)
    result.save_output(output_dir / output_stem, save_images=False)
    save_images(result.images, images_dir)

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
        )
    )
