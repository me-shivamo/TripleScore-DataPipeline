"""
Standalone Cloudinary upload module.

Usage:
    python upload_cloudinary.py <markdown_file> [--folder FOLDER]

Uploads all images referenced in the markdown file to Cloudinary,
then rewrites the markdown with Cloudinary URLs.
"""

import asyncio
import os
import re
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

IMAGE_LINK_PATTERN = re.compile(r"!\[[^\]]*]\(([^)]+)\)")


def configure_cloudinary():
    """Configure Cloudinary SDK. Returns True if successful."""
    cloud_name = os.getenv("CLOUDINARY_CLOUD_NAME")
    api_key = os.getenv("CLOUDINARY_API_KEY")
    api_secret = os.getenv("CLOUDINARY_API_SECRET")

    if not all([cloud_name, api_key, api_secret]):
        return False

    import cloudinary

    cloudinary.config(
        cloud_name=cloud_name,
        api_key=api_key,
        api_secret=api_secret,
        secure=True,
    )
    return True


async def upload_images(images_dir, cloudinary_folder="TripleScore"):
    """Upload all images in a directory to Cloudinary.

    Args:
        images_dir: Path to the directory containing images.
        cloudinary_folder: Cloudinary folder to upload into.

    Returns:
        dict mapping local filename -> Cloudinary secure URL.
    """
    import cloudinary.uploader

    url_map = {}
    images_dir = Path(images_dir)

    if not images_dir.exists():
        print(f"Images directory not found: {images_dir}", flush=True)
        return url_map

    image_files = [f for f in images_dir.iterdir() if f.is_file()]
    if not image_files:
        print("No images found to upload.", flush=True)
        return url_map

    print(f"Uploading {len(image_files)} image(s) to Cloudinary...", flush=True)

    for image_path in image_files:
        try:
            public_id = f"{cloudinary_folder}/{image_path.stem}"
            response = await asyncio.to_thread(
                cloudinary.uploader.upload,
                str(image_path),
                public_id=public_id,
                overwrite=True,
                resource_type="image",
            )
            secure_url = response.get("secure_url")
            if secure_url:
                url_map[image_path.name] = secure_url
                print(f"  Uploaded: {image_path.name}", flush=True)
            else:
                print(f"  Warning: No URL returned for {image_path.name}", flush=True)
        except Exception as e:
            print(f"  Failed to upload {image_path.name}: {e}", flush=True)

    print(f"Uploaded {len(url_map)}/{len(image_files)} image(s).", flush=True)
    return url_map


def rewrite_markdown_with_cloudinary_urls(markdown_text, url_map):
    """Replace local image paths with Cloudinary URLs in markdown text."""
    if not url_map:
        return markdown_text

    def replace_with_cloudinary(match):
        image_ref = match.group(1).strip()
        image_name = Path(image_ref).name
        if image_name in url_map:
            return match.group(0).replace(image_ref, url_map[image_name])
        return match.group(0)

    return IMAGE_LINK_PATTERN.sub(replace_with_cloudinary, markdown_text)


async def upload_and_rewrite(md_path, cloudinary_folder=None):
    """Upload images referenced in a markdown file to Cloudinary and rewrite the file.

    Args:
        md_path: Path to the markdown file.
        cloudinary_folder: Cloudinary folder name. Defaults to "TripleScore/{md_stem}".

    Returns:
        dict mapping local filename -> Cloudinary secure URL.
    """
    md_path = Path(md_path)
    if not md_path.exists():
        raise FileNotFoundError(f"Markdown file not found: {md_path}")

    images_dir = md_path.parent / "images"
    if cloudinary_folder is None:
        cloudinary_folder = f"TripleScore/{md_path.stem}"

    if not configure_cloudinary():
        print("Cloudinary credentials not configured. Skipping upload.", flush=True)
        return {}

    # Upload images
    url_map = await upload_images(images_dir, cloudinary_folder)

    # Rewrite markdown file
    if url_map:
        markdown_text = md_path.read_text(encoding="utf-8")
        updated_text = rewrite_markdown_with_cloudinary_urls(markdown_text, url_map)
        md_path.write_text(updated_text, encoding="utf-8")
        print(f"Updated {md_path.name} with Cloudinary URLs.", flush=True)

    return url_map


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Upload images to Cloudinary and update markdown.")
    parser.add_argument("markdown", help="Path to the markdown file")
    parser.add_argument("--folder", default=None, help="Cloudinary folder (default: TripleScore/<md_stem>)")
    args = parser.parse_args()

    asyncio.run(upload_and_rewrite(args.markdown, args.folder))
