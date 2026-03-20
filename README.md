# TripleScore Data Pipeline

PDF to Markdown extraction pipeline with Cloudinary image hosting.

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Add your credentials to `.env`:

```
DATALAB_API_KEY=your_datalab_api_key
PAGE_RANGE=3-5

CLOUDINARY_CLOUD_NAME=your_cloud_name
CLOUDINARY_API_KEY=your_api_key
CLOUDINARY_API_SECRET=your_api_secret
```

## Usage

### Full Pipeline

Runs extraction + Cloudinary upload in sequence:

```bash
python main.py
python main.py PDFs/some_paper.pdf
python main.py --debug
```

You can toggle pipeline steps by editing the config variables at the top of `main.py`:

```python
RUN_EXTRACTION = True         # Toggle Step 1: PDF extraction
RUN_CLOUDINARY_UPLOAD = True  # Toggle Step 2: Cloudinary upload
PAGE_RANGE = None             # e.g. "3-5", or None to use .env value
CLOUDINARY_FOLDER = None      # Auto-generates as "TripleScore/{pdf_stem}"
DEBUG_MODE = False             # Process only a single page
```

### Standalone: PDF Extraction Only

```bash
python extract_pdf.py
python extract_pdf.py PDFs/some_paper.pdf
python extract_pdf.py PDFs/some_paper.pdf --page-range 1-3
python extract_pdf.py --debug
```

Output is saved to `Datalab-Output/{pdf_stem}.md` and `Datalab-Output/images/`.

### Standalone: Cloudinary Upload Only

Upload images from an already-extracted markdown file:

```bash
python upload_cloudinary.py Datalab-Output/JEE_Mains_2026_Jan_28.md
python upload_cloudinary.py Datalab-Output/JEE_Mains_2026_Jan_28.md --folder MyFolder/subfolder
python upload_cloudinary.py Datalab-Output/JEE_Mains_2026_Jan_28.md --output-dir custom_output/
```

This uploads all images referenced in the markdown to Cloudinary and saves the updated markdown (with Cloudinary URLs) to `Cloudinary-Output/`. The original file in `Datalab-Output/` stays unchanged.

## Project Structure

```
├── main.py                 # Pipeline orchestrator (configurable)
├── extract_pdf.py          # Standalone PDF extraction (Datalab)
├── upload_cloudinary.py    # Standalone Cloudinary upload
├── requirements.txt        # Python dependencies
├── .env                    # API keys and config (not committed)
├── PDFs/                   # Input PDF files
├── Datalab-Output/         # Extraction output
│   ├── *.md                # Extracted markdown (local image paths)
│   ├── *.metadata.json     # Extraction metadata
│   └── images/             # Extracted images
└── Cloudinary-Output/      # Cloudinary-updated output
    └── *.md                # Markdown with Cloudinary image URLs
```
