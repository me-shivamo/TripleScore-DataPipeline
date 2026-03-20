import asyncio
import base64
import json
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

# --- CONFIGURATION ---
DEBUG_MODE = False  # Set to False to process the entire document
INPUT_FILE = BASE_DIR / "JEE_Mains_2026_Jan_28.pdf"
OUTPUT_STEM = "jee_mains_test"
IMAGES_DIR = BASE_DIR / "images"
API_KEY = os.getenv("DATALAB_API_KEY")
POLL_INTERVAL_SECONDS = 3
MAX_POLLS = 600
DEBUG_PAGE_RANGE = "4"
PAGE_BREAK_PATTERN = re.compile(r"(?:^|\n\n)\{(\d+)\}-{20,}\n\n")
IMAGE_LINK_PATTERN = re.compile(r"!\[[^\]]*]\(([^)]+)\)")
# ---------------------

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

            await asyncio.sleep(poll_interval)

        raise DatalabTimeoutError(
            f"Polling timed out after {max_polls * poll_interval} seconds"
        )


def save_images(images):
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    for image_name, image_data in (images or {}).items():
        image_path = IMAGES_DIR / image_name
        with image_path.open("wb") as image_file:
            image_file.write(base64.b64decode(image_data))


def move_existing_images_to_output_dir():
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    for image_path in BASE_DIR.glob("*_img.*"):
        target_path = IMAGES_DIR / image_path.name
        if image_path == target_path:
            continue
        image_path.replace(target_path)


def rewrite_markdown_image_paths(markdown_text):
    def replace_image_path(match):
        image_ref = match.group(1).strip()
        image_name = Path(image_ref).name
        return match.group(0).replace(image_ref, f"images/{image_name}")

    return IMAGE_LINK_PATTERN.sub(replace_image_path, markdown_text)


def split_markdown_into_pages(markdown_text):
    markdown_text = markdown_text.strip()
    if not markdown_text:
        return []

    matches = list(PAGE_BREAK_PATTERN.finditer(markdown_text))
    if not matches:
        return [(1, markdown_text)]

    pages = []
    for index, match in enumerate(matches):
        page_number = int(match.group(1))
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(markdown_text)
        page_markdown = markdown_text[start:end].strip()
        pages.append((page_number, page_markdown))

    return pages


def build_page_records(markdown_text):
    page_records = []

    for page_number, page_markdown in split_markdown_into_pages(markdown_text):
        images = []
        seen_images = set()

        for match in IMAGE_LINK_PATTERN.finditer(page_markdown):
            image_name = Path(match.group(1)).name
            if image_name in seen_images:
                continue
            seen_images.add(image_name)
            images.append(
                {
                    "name": image_name,
                    "local_path": str(IMAGES_DIR / image_name),
                }
            )

        page_records.append(
            {
                "page_number": page_number,
                "markdown": page_markdown,
                "images": images,
            }
        )

    return page_records


async def extract_page_markdown():
    if not API_KEY:
        raise RuntimeError(
            "Missing DATALAB_API_KEY. Set it in the .env file "
            "in this directory before running the script."
        )

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input PDF not found: {INPUT_FILE}")

    move_existing_images_to_output_dir()

    page_range = DEBUG_PAGE_RANGE if DEBUG_MODE else None

    # 1. Convert with paginated markdown so each page can be split cleanly
    options = ConvertOptions(
        output_format="markdown",
        mode="balanced",
        paginate=True,
        page_range=page_range,
    )

    print(f"Submitting {INPUT_FILE.name} for conversion...", flush=True)
    async with ProgressDatalabClient(api_key=API_KEY) as client:
        result = await client.convert(
            INPUT_FILE,
            options=options,
            max_polls=MAX_POLLS,
            poll_interval=POLL_INTERVAL_SECONDS,
        )

    markdown_text = rewrite_markdown_image_paths(result.markdown or "")
    if not markdown_text:
        raise RuntimeError("Datalab returned no markdown content.")

    result.markdown = markdown_text

    # 2. Save output locally
    result.save_output(BASE_DIR / OUTPUT_STEM, save_images=False)
    save_images(result.images)

    page_records = build_page_records(markdown_text)

    # 3. Save page-wise markdown JSON
    output_file = "debug_pages.json" if DEBUG_MODE else "full_pages.json"
    with open(BASE_DIR / output_file, "w", encoding="utf-8") as output_handle:
        json.dump(page_records, output_handle, indent=4, ensure_ascii=False)

    print(f"Extraction complete. Processed {len(page_records)} pages.", flush=True)
    print(
        f"Data saved to {output_file} and images are in {IMAGES_DIR.name}/",
        flush=True,
    )

if __name__ == "__main__":
    asyncio.run(extract_page_markdown())
