# TripleScore Data Pipeline

PDF to structured, embedded question bank pipeline for JEE exam papers.

## Pipeline Overview

```
PDF
 └─ Step 1:   01_extract_pdf.py          → 01_Datalab-Output/*.md + images/
 └─ Step 1.1: 01_1_audit_questions.py    → console report (missing questions)
 └─ Step 2:   02_upload_digitalocean.py  → 02_DO-Spaces-Output/*.md (CDN URLs)
 └─ Step 3:   03_structure_gemini.py     → 03_Structured-Output/*.json
 └─ Step 4:   04_classify_topic_chapter.py → 04_Classified-Output/*.json
 └─ Step 4.1: 04_1_enrich_ids.py        → 04_1_Enriched-Output/*.json
 └─ Step 5:   05_embed_questions.py      → 05_Embedded-Output/*.json + index.json
 └─ Step 6:   06_import_to_supabase.py  → Supabase questions table
```

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Add your credentials and configuration to `.env`:

```
DATALAB_API_KEY=your_datalab_api_key
GOOGLE_API_KEY=your_google_api_key

# DigitalOcean Spaces
DO_SPACES_KEY=your_spaces_key
DO_SPACES_SECRET=your_spaces_secret
DO_SPACES_BUCKET=your_bucket_name
DO_SPACES_REGION=your_region

# --- PDF Extraction Page Controls ---
START_PAGE=          # First page to extract (1-based). Leave empty for page 1.
END_PAGE=            # Last page to extract (1-based). Leave empty for last page.
CHUNK_SIZE=5         # Pages per API call. Leave empty to send full range in one call.
MIN_QUALITY_SCORE=4.0   # Chunks scoring at or below this threshold will be retried (0-10 scale).
MAX_CHUNK_RETRIES=2     # Number of retries per chunk on quality failure.
PARSE_MODE=balanced     # Datalab parsing mode: fast, balanced, or accurate.
```

## Usage

### Full Pipeline

Runs all steps in sequence (extraction → audit → upload → structuring → classification → enrichment → embedding):

```bash
python main.py
python main.py PDFs/some_paper.pdf
python main.py --debug
```

Toggle individual steps and set page bounds by editing the config block at the top of `main.py`:

```python
RUN_EXTRACTION = True       # Step 1:   Extract PDF to markdown + images via Datalab
RUN_QUESTION_AUDIT = True   # Step 1.1: Audit extracted markdown for missing questions (1-75)
RUN_SPACES_UPLOAD = True    # Step 2:   Upload images to DigitalOcean Spaces
RUN_GEMINI_STRUCTURING = True  # Step 3: Structure markdown to JSON via Gemini
RUN_CLASSIFICATION = True   # Step 4:   Classify topic and chapter
RUN_ENRICHMENT = True       # Step 4.1: Add question title and source
RUN_EMBEDDING = True        # Step 5:   Generate embeddings
START_PAGE = None           # First page to extract (None = page 1)
END_PAGE = None             # Last page to extract (None = last page)
DEBUG_MODE = False          # Debug mode (single page)
```

---

### Step 1 — PDF Extraction

```bash
python 01_extract_pdf.py
python 01_extract_pdf.py PDFs/some_paper.pdf
python 01_extract_pdf.py PDFs/some_paper.pdf --start-page 3 --end-page 20
python 01_extract_pdf.py PDFs/some_paper.pdf --chunk-size 1
python 01_extract_pdf.py PDFs/some_paper.pdf --parse-mode accurate
python 01_extract_pdf.py --debug
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `pdf` | positional | `PDFs/JEE_Mains_2026_Jan_28.pdf` | Path to the input PDF file |
| `--start-page N` | int | `START_PAGE` env / `1` | First page to extract (1-based) |
| `--end-page N` | int | `END_PAGE` env / last page | Last page to extract (1-based) |
| `--chunk-size N` | int | `CHUNK_SIZE` env / full range | Pages per API call. `0` = send full range in one call |
| `--min-quality-score F` | float | `MIN_QUALITY_SCORE` env / `4.0` | Minimum parse quality score to accept a chunk (0–10) |
| `--max-chunk-retries N` | int | `MAX_CHUNK_RETRIES` env / `2` | Max retries per chunk when quality check fails |
| `--parse-mode MODE` | str | `PARSE_MODE` env / `balanced` | Datalab parsing mode: `fast`, `balanced`, or `accurate` |
| `--debug` | flag | `False` | Debug mode — saves output as `debug_output.md` |

Output: `01_Datalab-Output/{pdf_stem}.md` and `01_Datalab-Output/images/`

---

### Step 1.1 — Question Audit

Check which questions (1–75) are present or missing in an extracted markdown file:

```bash
python 01_1_audit_questions.py 01_Datalab-Output/some_paper.md
```

Example output:
```
Questions found (73): [1, 2, 3, ...]
Missing from 1-75 (2): [34, 61]
```

---

### Step 2 — DigitalOcean Spaces Upload

Upload images from an extracted markdown file to DigitalOcean Spaces:

```bash
python 02_upload_digitalocean.py 01_Datalab-Output/some_paper.md
```

Output: `02_DO-Spaces-Output/{pdf_stem}.md` (markdown with CDN image URLs; original file unchanged)

---

### Step 3 — Gemini Structuring

Structure markdown into a JSON array of questions:

```bash
python 03_structure_gemini.py
python 03_structure_gemini.py 02_DO-Spaces-Output/some_paper.md
```

Output: `03_Structured-Output/{pdf_stem}.json`

---

### Step 4 — Topic/Chapter Classification

Classify each question by subject, topic, and chapter:

```bash
# Process all JSON files in 03_Structured-Output/
python 04_classify_topic_chapter.py

# Process a single file
python 04_classify_topic_chapter.py --file 03_Structured-Output/some_paper.json

# Custom directories
python 04_classify_topic_chapter.py --input-dir my_structured/ --output-dir my_classified/
```

| Flag | Default | Description |
|---|---|---|
| `--file PATH` | `None` | Process a single JSON file instead of the whole input directory |
| `--input-dir DIR` | `03_Structured-Output/` | Directory containing structured JSON files |
| `--output-dir DIR` | `04_Classified-Output/` | Directory to save classified JSON files |

Output: `04_Classified-Output/{pdf_stem}.json` — each question enriched with `topic` and `chapter`

---

### Step 4.1 — ID and Source Enrichment

Add `title` (first key) and `source` (last key) to every question:

```bash
# Process all JSON files in 04_Classified-Output/
python 04_1_enrich_ids.py

# Process a single file
python 04_1_enrich_ids.py --file 04_Classified-Output/some_paper.json

# Custom directories
python 04_1_enrich_ids.py --input-dir my_classified/ --output-dir my_enriched/
```

| Flag | Default | Description |
|---|---|---|
| `--file PATH` | `None` | Process a single JSON file |
| `--input-dir DIR` | `04_Classified-Output/` | Input directory |
| `--output-dir DIR` | `04_1_Enriched-Output/` | Output directory |

Title format: `JEE Mains {year} {month} {day} - {shift} - {subject} Q{number}`
Source format: `{month} {year} Shift {shift}`

Output: `04_1_Enriched-Output/{pdf_stem}.json`

---

### Step 5 — Embedding Generation

Generate embeddings using Google's `gemini-embedding-2-preview` model:

```bash
# Process all JSON files in 04_1_Enriched-Output/
python 05_embed_questions.py

# Custom directories
python 05_embed_questions.py --input-dir my_enriched/ --output-dir my_embedded/
```

| Flag | Default | Description |
|---|---|---|
| `--input-dir DIR` | `04_1_Enriched-Output/` | Input directory |
| `--output-dir DIR` | `05_Embedded-Output/` | Output directory |

Requires `GOOGLE_API_KEY` in `.env`.

Output: `05_Embedded-Output/{pdf_stem}.json` — each question with an `embedding` field, plus `05_Embedded-Output/index.json` for fast lookup.

---

---

### Step 6 — Import to Supabase

Upload embedded questions from `05_Embedded-Output/` into the Supabase (PostgreSQL) database.

Safe to re-run at any time — questions already in the database are skipped automatically.

```bash
# Import all files in 05_Embedded-Output/  (most common)
python 06_import_to_supabase.py

# Import a single file
python 06_import_to_supabase.py --file 05_Embedded-Output/some_paper.json

# Import from a custom directory
python 06_import_to_supabase.py --input-dir my_embedded/
```

| Flag | Default | Description |
|---|---|---|
| `--file PATH` | `None` | Import a single JSON file instead of the whole directory |
| `--input-dir DIR` | `05_Embedded-Output/` | Directory of embedded JSON files to import |

**Requirements:** `DATABASE_URL` must be set in `TripleScore-Backend/.env` pointing to your Supabase instance.

Example output:
```
Importing JEE_Mains_2025_Shift_2_Question_Paper_Apr_03.json ... 75 created, 0 skipped

Done — 75 created, 0 skipped
Total questions in database: 75
```

---

## Project Structure

```
├── main.py                        # Pipeline orchestrator (configurable)
├── 01_extract_pdf.py              # Step 1:   PDF extraction via Datalab
├── 01_1_audit_questions.py        # Step 1.1: Audit extracted markdown for missing questions
├── 02_upload_digitalocean.py      # Step 2:   Upload images to DigitalOcean Spaces
├── 03_structure_gemini.py         # Step 3:   Structure markdown to JSON via Gemini
├── 04_classify_topic_chapter.py   # Step 4:   Topic/chapter classification via Gemini
├── 04_1_enrich_ids.py             # Step 4.1: Add question title and source
├── 05_embed_questions.py          # Step 5:   Generate embeddings via Gemini
├── 06_import_to_supabase.py       # Step 6:   Import embedded questions into Supabase
├── requirements.txt               # Python dependencies
├── .env                           # API keys and config (not committed)
├── PDFs/                          # Input PDF files
├── 01_Datalab-Output/             # Step 1 output
│   ├── *.md                       # Extracted markdown (local image paths)
│   └── images/                    # Extracted images
├── 02_DO-Spaces-Output/           # Step 2 output — markdown with CDN image URLs
├── 03_Structured-Output/          # Step 3 output — structured JSON
├── 04_Classified-Output/          # Step 4 output — classified JSON
├── 04_1_Enriched-Output/          # Step 4.1 output — enriched JSON with title/source
└── 05_Embedded-Output/            # Step 5 output — JSON with embeddings + index.json
```
