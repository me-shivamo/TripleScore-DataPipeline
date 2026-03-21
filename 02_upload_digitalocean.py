"""
Standalone DigitalOcean Spaces upload module.

Usage:
    python 02_upload_digitalocean.py <markdown_file> [--folder FOLDER]

Uploads all images referenced in the markdown file to DigitalOcean Spaces,
then rewrites the markdown with CDN URLs.
"""

import asyncio
import os
import re
from pathlib import Path

import boto3


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

IMAGE_LINK_PATTERN = re.compile(r"!\[[^\]]*]\(([^)]+)\)")


def get_spaces_client():
    """Create and return a boto3 client for DigitalOcean Spaces."""
    region = os.getenv("DO_SPACES_REGION")
    access_key = os.getenv("DO_SPACES_ACCESS_KEY")
    secret_key = os.getenv("DO_SPACES_SECRET_KEY")

    if not all([region, access_key, secret_key]):
        return None

    return boto3.client(
        "s3",
        region_name=region,
        endpoint_url=f"https://{region}.digitaloceanspaces.com",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


async def upload_images(images_dir, folder="TripleScore"):
    """Upload all images in a directory to DigitalOcean Spaces.

    Args:
        images_dir: Path to the directory containing images.
        folder: Folder path within the Space.

    Returns:
        dict mapping local filename -> CDN URL.
    """
    url_map = {}
    images_dir = Path(images_dir)

    if not images_dir.exists():
        print(f"Images directory not found: {images_dir}", flush=True)
        return url_map

    image_files = [f for f in images_dir.iterdir() if f.is_file()]
    if not image_files:
        print("No images found to upload.", flush=True)
        return url_map

    client = get_spaces_client()
    if client is None:
        print("DigitalOcean Spaces credentials not configured. Skipping upload.", flush=True)
        return url_map

    space_name = os.getenv("DO_SPACES_NAME")
    cdn_endpoint = os.getenv("DO_SPACES_CDN_ENDPOINT", "").rstrip("/")

    print(f"Uploading {len(image_files)} image(s) to DigitalOcean Spaces...", flush=True)

    for image_path in image_files:
        try:
            object_key = f"{folder}/{image_path.name}"

            # Determine content type
            ext = image_path.suffix.lower()
            content_types = {
                ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                ".png": "image/png", ".gif": "image/gif",
                ".webp": "image/webp", ".svg": "image/svg+xml",
            }
            content_type = content_types.get(ext, "application/octet-stream")

            await asyncio.to_thread(
                client.upload_file,
                str(image_path),
                space_name,
                object_key,
                ExtraArgs={"ACL": "public-read", "ContentType": content_type},
            )

            cdn_url = f"{cdn_endpoint}/{object_key}"
            url_map[image_path.name] = cdn_url
            print(f"  Uploaded: {image_path.name}", flush=True)
        except Exception as e:
            print(f"  Failed to upload {image_path.name}: {e}", flush=True)

    print(f"Uploaded {len(url_map)}/{len(image_files)} image(s).", flush=True)
    return url_map


def rewrite_markdown_with_cdn_urls(markdown_text, url_map):
    """Replace local image paths with CDN URLs in markdown text."""
    if not url_map:
        return markdown_text

    def replace_with_cdn(match):
        image_ref = match.group(1).strip()
        image_name = Path(image_ref).name
        if image_name in url_map:
            return match.group(0).replace(image_ref, url_map[image_name])
        return match.group(0)

    return IMAGE_LINK_PATTERN.sub(replace_with_cdn, markdown_text)


async def upload_and_rewrite(md_path, spaces_folder=None, output_dir=None):
    """Upload images referenced in a markdown file to DigitalOcean Spaces and save output.

    Args:
        md_path: Path to the source markdown file.
        spaces_folder: Folder name in the Space. Defaults to "TripleScore/{md_stem}".
        output_dir: Directory to save the updated markdown. Defaults to 02_DO-Spaces-Output/.

    Returns:
        dict mapping local filename -> CDN URL.
    """
    md_path = Path(md_path)
    if not md_path.exists():
        raise FileNotFoundError(f"Markdown file not found: {md_path}")

    images_dir = md_path.parent / "images"
    if spaces_folder is None:
        spaces_folder = f"TripleScore/{md_path.stem}"

    if output_dir is None:
        output_dir = BASE_DIR / "02_DO-Spaces-Output"
    else:
        output_dir = Path(output_dir)

    # Upload images
    url_map = await upload_images(images_dir, spaces_folder)

    # Save updated markdown to output dir
    markdown_text = md_path.read_text(encoding="utf-8")
    if url_map:
        markdown_text = rewrite_markdown_with_cdn_urls(markdown_text, url_map)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / md_path.name
    output_file.write_text(markdown_text, encoding="utf-8")
    print(f"Saved updated markdown to {output_file}", flush=True)

    return url_map, output_file


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Upload images to DigitalOcean Spaces and update markdown.")
    parser.add_argument("markdown", help="Path to the markdown file")
    parser.add_argument("--folder", default=None, help="Spaces folder (default: TripleScore/<md_stem>)")
    parser.add_argument("--output-dir", default=None, help="Output directory (default: 02_DO-Spaces-Output/)")
    args = parser.parse_args()

    asyncio.run(upload_and_rewrite(args.markdown, args.folder, args.output_dir))
