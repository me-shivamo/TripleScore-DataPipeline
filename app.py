"""
TripleScore Pipeline GUI
~~~~~~~~~~~~~~~~~~~~~~~~
Modern light-theme tkinter app wrapping all pipeline steps.
Run with: python app.py
Requires: sudo apt install python3-tk  (one-time, WSL2/Linux)
"""

# ===== IMPORTS =====
import asyncio
import collections
import contextlib
import ctypes
import importlib
import io
import json
import queue
import sys
import threading
import traceback
import tkinter as tk
import tkinter.ttk as ttk
import tkinter.scrolledtext as scrolledtext
import tkinter.filedialog as filedialog
from enum import Enum
from pathlib import Path

# ===== BASE DIR & SYS.PATH =====
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

ENV_PATH   = BASE_DIR / ".env"
PDFS_DIR   = BASE_DIR / "PDFs"
PROCESSED_DIR = PDFS_DIR / "Processed"

# ===== MODERN LIGHT COLOUR PALETTE =====
C = {
    # Surfaces
    "root_bg":      "#F0F2F5",   # outer window background
    "surface":      "#FFFFFF",   # card / panel white
    "surface_alt":  "#F8F9FB",   # slightly off-white rows
    "border":       "#E2E8F0",   # subtle border
    "border_focus": "#6366F1",   # indigo focus ring

    # Sidebar / header
    "sidebar_bg":   "#1E293B",   # slate-900 sidebar
    "sidebar_text": "#F1F5F9",   # slate-100
    "sidebar_dim":  "#94A3B8",   # slate-400
    "sidebar_hover":"#334155",   # slate-700 hover

    # Typography
    "text_primary":  "#0F172A",  # slate-900
    "text_secondary":"#475569",  # slate-600
    "text_muted":    "#94A3B8",  # slate-400

    # Brand / accent
    "indigo":       "#6366F1",   # primary action
    "indigo_hover": "#4F46E5",
    "indigo_light": "#EEF2FF",   # indigo-50 badge bg

    # Status badges
    "idle_bg":    "#F1F5F9",  "idle_fg":    "#64748B",
    "run_bg":     "#FEF3C7",  "run_fg":     "#B45309",
    "done_bg":    "#D1FAE5",  "done_fg":    "#065F46",
    "error_bg":   "#FEE2E2",  "error_fg":   "#991B1B",
    "skip_bg":    "#F1F5F9",  "skip_fg":    "#94A3B8",

    # Step number pills
    "pill_bg":    "#6366F1",  "pill_fg":    "#FFFFFF",
    "pill_dis_bg":"#CBD5E1",  "pill_dis_fg":"#FFFFFF",

    # Buttons
    "btn_primary_bg":  "#6366F1",  "btn_primary_fg":  "#FFFFFF",
    "btn_primary_hov": "#4F46E5",
    "btn_ghost_bg":    "#FFFFFF",  "btn_ghost_fg":    "#475569",
    "btn_ghost_brd":   "#E2E8F0",
    "btn_run_bg":      "#6366F1",  "btn_run_fg":      "#FFFFFF",
    "btn_danger_bg":   "#EF4444",  "btn_danger_fg":   "#FFFFFF",

    # Log pane
    "log_bg":   "#FAFAFA",   "log_fg":   "#1E293B",
    "log_err":  "#DC2626",   "log_ok":   "#16A34A",
    "log_hdr":  "#4F46E5",   "log_warn": "#D97706",
    "log_line_alt": "#F8FAFC",

    # Entry / input
    "input_bg": "#FFFFFF",   "input_fg":  "#0F172A",
    "input_brd":"#CBD5E1",
}

FONT_FAMILY = "Segoe UI" if sys.platform == "win32" else "Helvetica"


# ===== STEP DEFINITIONS =====
STEP_DEFS = [
    {
        "id": "step1",
        "num": "1",
        "label": "PDF Extraction",
        "desc": "Datalab API  ·  PDF → Markdown + images",
        "async": True,
        "default_enabled": True,
        "settings": [
            ("start_page",        "Start Page",         "entry",    ""),
            ("end_page",          "End Page",            "entry",    ""),
            ("chunk_size",        "Chunk Size",          "spinbox",  "6"),
            ("min_quality_score", "Min Quality Score",   "entry",    "4.0"),
            ("max_chunk_retries", "Max Chunk Retries",   "spinbox",  "2"),
            ("parse_mode",        "Parse Mode",          "combobox", "balanced"),
        ],
    },
    {
        "id": "step1_1",
        "num": "1.1",
        "label": "Question Audit",
        "desc": "Verifies questions 1–75 are present in the extracted markdown",
        "async": False,
        "default_enabled": True,
        "settings": [
            ("step1_1_input_md", "Input MD path  (empty = auto from Step 1)", "browse_file", ""),
        ],
    },
    {
        "id": "step2",
        "num": "2",
        "label": "CDN Upload",
        "desc": "DigitalOcean Spaces  ·  Upload images and rewrite URLs",
        "async": True,
        "default_enabled": True,
        "settings": [
            ("spaces_folder", "Spaces Folder  (empty = auto)", "entry", ""),
        ],
    },
    {
        "id": "step3",
        "num": "3",
        "label": "Gemini Structuring",
        "desc": "OpenRouter / Gemini  ·  Markdown → structured JSON",
        "async": True,
        "default_enabled": True,
        "settings": [
            ("step3_input_md", "Input MD path  (empty = auto from Step 2)", "browse_file", ""),
        ],
    },
    {
        "id": "step4",
        "num": "4",
        "label": "Classification",
        "desc": "Gemini  ·  Classify topic & chapter per question",
        "async": True,
        "default_enabled": True,
        "settings": [
            ("step4_input_dir", "Input Dir  (empty = auto from Step 3)", "browse_dir", ""),
        ],
    },
    {
        "id": "step4_1",
        "num": "4.1",
        "label": "ID Enrichment",
        "desc": "Adds title, source and ID metadata from the PDF filename",
        "async": False,
        "default_enabled": True,
        "settings": [
            ("step4_1_input_dir", "Input Dir  (empty = auto from Step 4)", "browse_dir", ""),
        ],
    },
    {
        "id": "step5",
        "num": "5",
        "label": "Embedding",
        "desc": "Google Gemini  ·  Generate vector embeddings for search",
        "async": False,
        "default_enabled": True,
        "settings": [
            ("step5_input_dir", "Input Dir  (empty = auto from Step 4.1)", "browse_dir", ""),
        ],
    },
    {
        "id": "step6",
        "num": "6",
        "label": "Import to Supabase",
        "desc": "Django ORM  ·  Insert questions into PostgreSQL",
        "async": False,
        "default_enabled": False,
        "settings": [
            ("step6_input_dir",   "Input Dir  (empty = 05_Embedded-Output/)", "browse_dir",  ""),
            ("step6_single_file", "Single File  (empty = all files)",          "browse_file", ""),
        ],
    },
]

STEP_BY_ID = {s["id"]: s for s in STEP_DEFS}


# ===== STATE ENUM =====
class StepState(Enum):
    IDLE    = "Idle"
    RUNNING = "Running"
    DONE    = "Done"
    ERROR   = "Error"
    SKIPPED = "Skipped"

# badge (bg, fg) per state
BADGE = {
    StepState.IDLE:    (C["idle_bg"],  C["idle_fg"]),
    StepState.RUNNING: (C["run_bg"],   C["run_fg"]),
    StepState.DONE:    (C["done_bg"],  C["done_fg"]),
    StepState.ERROR:   (C["error_bg"], C["error_fg"]),
    StepState.SKIPPED: (C["skip_bg"],  C["skip_fg"]),
}


# ===== QUEUE WRITER =====
class QueueWriter(io.TextIOBase):
    def __init__(self, q: queue.Queue):
        self._q = q

    def write(self, text: str) -> int:
        if text:
            self._q.put(text)
        return len(text)

    def flush(self):
        pass


# ===== ENV HELPERS =====
def _parse_env_file() -> dict:
    env = {}
    if not ENV_PATH.exists():
        return env
    with ENV_PATH.open() as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            key, _, val = line.partition("=")
            env[key.strip()] = val.strip().strip("'\"")
    return env


def _write_env_key(key: str, value: str):
    SAFE_KEYS = {
        "START_PAGE", "END_PAGE", "CHUNK_SIZE", "MIN_QUALITY_SCORE",
        "MAX_CHUNK_RETRIES", "PARSE_MODE", "SPACES_FOLDER",
        "POLL_INTERVAL_SECONDS", "MAX_POLLS",
    }
    if key not in SAFE_KEYS or not ENV_PATH.exists():
        return
    lines = ENV_PATH.read_text().splitlines(keepends=True)
    found = False
    new_lines = []
    for line in lines:
        stripped = line.strip()
        if "=" in stripped and not stripped.startswith("#"):
            k = stripped.split("=", 1)[0].strip()
            if k == key:
                new_lines.append(f"{key}={value}\n")
                found = True
                continue
        new_lines.append(line)
    if not found:
        new_lines.append(f"{key}={value}\n")
    ENV_PATH.write_text("".join(new_lines))


# ===== TTK STYLE SETUP =====
def _apply_ttk_style():
    style = ttk.Style()
    try:
        style.theme_use("clam")
    except Exception:
        pass

    style.configure("TScrollbar",
                    troughcolor=C["border"], background=C["text_muted"],
                    borderwidth=0, arrowsize=12)

    style.configure("TCombobox",
                    fieldbackground=C["input_bg"], background=C["input_bg"],
                    foreground=C["text_primary"],
                    selectbackground=C["indigo_light"],
                    selectforeground=C["text_primary"],
                    relief="flat", borderwidth=1,
                    bordercolor=C["input_brd"])
    style.map("TCombobox",
              fieldbackground=[("readonly", C["input_bg"])],
              foreground=[("readonly", C["text_primary"])])

    style.configure("Sash", sashthickness=6, sashrelief="flat",
                    background=C["border"])

    style.configure("Vertical.TScrollbar",
                    background=C["text_muted"], troughcolor=C["surface_alt"],
                    borderwidth=0, arrowsize=0, relief="flat")
    style.map("Vertical.TScrollbar",
              background=[("active", C["text_secondary"])])


# ===== HELPER — rounded pill label using Canvas =====
def _pill_label(parent, text, bg, fg, font, padx=10, pady=3, radius=8):
    """Returns a Canvas that draws a pill-shaped badge."""
    tmp = tk.Label(parent, text=text, font=font)
    tmp.update_idletasks()
    tw = tmp.winfo_reqwidth() + padx * 2
    th = tmp.winfo_reqheight() + pady * 2
    tmp.destroy()

    cv = tk.Canvas(parent, width=tw, height=th,
                   bg=parent.cget("bg"), highlightthickness=0, bd=0)
    r = radius
    cv.create_arc(0, 0, 2*r, 2*r, start=90, extent=90, fill=bg, outline=bg)
    cv.create_arc(tw-2*r, 0, tw, 2*r, start=0, extent=90, fill=bg, outline=bg)
    cv.create_arc(0, th-2*r, 2*r, th, start=180, extent=90, fill=bg, outline=bg)
    cv.create_arc(tw-2*r, th-2*r, tw, th, start=270, extent=90, fill=bg, outline=bg)
    cv.create_rectangle(r, 0, tw-r, th, fill=bg, outline=bg)
    cv.create_rectangle(0, r, tw, th-r, fill=bg, outline=bg)
    cv.create_text(tw//2, th//2, text=text, fill=fg, font=font, anchor="center")
    return cv


# ===== PIPELINE APP =====
class PipelineApp:

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("TripleScore  ·  Data Pipeline")
        self.root.geometry("1380x900")
        self.root.minsize(1000, 680)
        self.root.configure(bg=C["root_bg"])

        _apply_ttk_style()

        self._pipeline_md_path = None
        self._pipeline_spaces_md_path = None
        self._active_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

        self.step_widgets: dict = {}
        self.step_states: dict[str, StepState] = {}
        self.step_enabled: dict[str, tk.BooleanVar] = {}

        self.cfg: dict[str, tk.StringVar] = {}
        self.debug_var = tk.BooleanVar(value=False)
        self.step_cfg: dict[str, tk.StringVar] = {}

        # PDF picker state
        self._pdf_list: list[Path] = []          # all PDFs found (excluding Processed/)
        self._pdf_listbox: tk.Listbox | None = None
        self._pdf_count_var = tk.StringVar(value="")

        self.log_queue: queue.Queue = queue.Queue()

        self._active_scroll_canvas = None
        self._build_layout()
        # Global scroll handler: routes to whichever canvas the mouse is over
        self.root.bind_all("<MouseWheel>", self._on_global_scroll)
        self.root.bind_all("<Button-4>",   lambda e: self._on_global_scroll_linux(-1))
        self.root.bind_all("<Button-5>",   lambda e: self._on_global_scroll_linux(1))
        self._load_config_from_env()
        self._refresh_pdf_list()
        self._detect_existing_outputs()
        self._refresh_io_labels()
        self._poll_log_queue()

    # ------------------------------------------------------------------ #
    #  SCROLL ROUTING                                                      #
    # ------------------------------------------------------------------ #

    def _set_active_scroll_canvas(self, canvas):
        self._active_scroll_canvas = canvas

    def _on_global_scroll(self, event):
        if self._active_scroll_canvas:
            self._active_scroll_canvas.yview_scroll(
                int(-1 * (event.delta / 120)), "units")

    def _on_global_scroll_linux(self, direction: int):
        if self._active_scroll_canvas:
            self._active_scroll_canvas.yview_scroll(direction, "units")

    # ------------------------------------------------------------------ #
    #  TOP-LEVEL LAYOUT                                                    #
    # ------------------------------------------------------------------ #

    def _build_layout(self):
        # Top header bar
        self._build_topbar()

        # Body: sidebar + content pane
        body = tk.Frame(self.root, bg=C["root_bg"])
        body.pack(fill="both", expand=True)

        # Left sidebar (fixed ~300px)
        self._sidebar = tk.Frame(body, bg=C["sidebar_bg"], width=300)
        self._sidebar.pack(side="left", fill="y")
        self._sidebar.pack_propagate(False)
        self._build_sidebar()

        # Right content area split: steps list | log
        content = tk.Frame(body, bg=C["root_bg"])
        content.pack(side="left", fill="both", expand=True)

        paned = tk.PanedWindow(content, orient="horizontal",
                               bg=C["border"], sashwidth=5,
                               sashrelief="flat", showhandle=False)
        paned.pack(fill="both", expand=True, padx=0, pady=0)

        # Steps pane (scrollable)
        steps_outer = tk.Frame(paned, bg=C["root_bg"])
        paned.add(steps_outer, minsize=420, width=560)
        self._build_steps_pane(steps_outer)

        # Tools pane
        tools_outer = tk.Frame(paned, bg=C["root_bg"])
        paned.add(tools_outer, minsize=280, width=340)
        self._build_tools_pane(tools_outer)

        # Log pane
        log_outer = tk.Frame(paned, bg=C["root_bg"])
        paned.add(log_outer, minsize=320)
        self._build_log_pane(log_outer)

    # ------------------------------------------------------------------ #
    #  TOP BAR                                                             #
    # ------------------------------------------------------------------ #

    def _build_topbar(self):
        bar = tk.Frame(self.root, bg=C["surface"], height=56)
        bar.pack(side="top", fill="x")
        bar.pack_propagate(False)

        # Bottom border
        tk.Frame(bar, bg=C["border"], height=1).pack(side="bottom", fill="x")

        # Logo / title
        title_frame = tk.Frame(bar, bg=C["surface"])
        title_frame.pack(side="left", padx=20)

        tk.Label(title_frame,
                 text="TripleScore",
                 font=(FONT_FAMILY, 14, "bold"),
                 bg=C["surface"], fg=C["indigo"]).pack(side="left")
        tk.Label(title_frame,
                 text="  Data Pipeline",
                 font=(FONT_FAMILY, 14),
                 bg=C["surface"], fg=C["text_primary"]).pack(side="left")

        # Right side: debug + run all
        right = tk.Frame(bar, bg=C["surface"])
        right.pack(side="right", padx=20)

        # Debug toggle
        debug_frame = tk.Frame(right, bg=C["surface"],
                               highlightbackground=C["border"], highlightthickness=1)
        debug_frame.pack(side="left", padx=(0, 12))
        tk.Checkbutton(
            debug_frame, text="  Debug mode",
            variable=self.debug_var,
            bg=C["surface"], fg=C["text_secondary"],
            selectcolor=C["indigo_light"],
            activebackground=C["surface"],
            activeforeground=C["text_primary"],
            font=(FONT_FAMILY, 9),
            cursor="hand2",
            relief="flat",
        ).pack(padx=10, pady=6)

        self.stop_btn = tk.Button(
            right,
            text="  ■   Stop  ",
            font=(FONT_FAMILY, 10, "bold"),
            bg=C["btn_danger_bg"], fg=C["btn_danger_fg"],
            activebackground="#DC2626",
            activeforeground="#FFFFFF",
            relief="flat", cursor="hand2",
            padx=4, pady=8,
            command=self._stop_pipeline,
            state="disabled",
        )
        self.stop_btn.pack(side="left", padx=(0, 8))

        self.run_all_btn = tk.Button(
            right,
            text="  ▶   Run All Pipeline  ",
            font=(FONT_FAMILY, 10, "bold"),
            bg=C["btn_primary_bg"], fg=C["btn_primary_fg"],
            activebackground=C["btn_primary_hov"],
            activeforeground=C["btn_primary_fg"],
            relief="flat", cursor="hand2",
            padx=4, pady=8,
            command=self._run_all,
        )
        self.run_all_btn.pack(side="left")

    # ------------------------------------------------------------------ #
    #  SIDEBAR                                                             #
    # ------------------------------------------------------------------ #

    def _build_sidebar(self):
        sb = self._sidebar

        # Title
        tk.Label(sb, text="Configuration",
                 font=(FONT_FAMILY, 11, "bold"),
                 bg=C["sidebar_bg"], fg=C["sidebar_text"]
                 ).pack(anchor="w", padx=20, pady=(20, 4))
        tk.Frame(sb, bg=C["sidebar_hover"], height=1).pack(fill="x", padx=20, pady=(0, 12))

        # ── PDF Picker ─────────────────────────────────────────────────
        pdf_hdr = tk.Frame(sb, bg=C["sidebar_bg"])
        pdf_hdr.pack(fill="x", padx=18, pady=(0, 4))
        tk.Label(pdf_hdr, text="Select PDF",
                 font=(FONT_FAMILY, 8),
                 bg=C["sidebar_bg"], fg=C["sidebar_dim"]
                 ).pack(side="left")
        # Legend
        tk.Label(pdf_hdr, text="✓ done",
                 font=(FONT_FAMILY, 7),
                 bg=C["sidebar_bg"], fg="#4ADE80"
                 ).pack(side="right")
        tk.Label(pdf_hdr, text="· ",
                 font=(FONT_FAMILY, 7),
                 bg=C["sidebar_bg"], fg=C["sidebar_dim"]
                 ).pack(side="right")
        tk.Label(pdf_hdr, text="pending",
                 font=(FONT_FAMILY, 7),
                 bg=C["sidebar_bg"], fg=C["sidebar_text"]
                 ).pack(side="right")

        picker_frame = tk.Frame(sb, bg=C["sidebar_bg"])
        picker_frame.pack(fill="x", padx=16, pady=(0, 4))

        # Listbox + scrollbar
        lb_frame = tk.Frame(picker_frame, bg=C["sidebar_hover"],
                            highlightbackground=C["sidebar_hover"], highlightthickness=1)
        lb_frame.pack(fill="x")

        self._pdf_listbox = tk.Listbox(
            lb_frame,
            height=8,
            bg=C["sidebar_hover"], fg=C["sidebar_text"],
            selectbackground=C["indigo"], selectforeground="#FFFFFF",
            activestyle="none",
            font=(FONT_FAMILY, 9),
            relief="flat", borderwidth=0,
            highlightthickness=0,
            cursor="hand2",
        )
        lb_vsb = ttk.Scrollbar(lb_frame, orient="vertical",
                               command=self._pdf_listbox.yview,
                               style="Vertical.TScrollbar")
        self._pdf_listbox.configure(yscrollcommand=lb_vsb.set)
        lb_vsb.pack(side="right", fill="y")
        self._pdf_listbox.pack(side="left", fill="both", expand=True, padx=4, pady=4)
        self._pdf_listbox.bind("<<ListboxSelect>>", self._on_pdf_select)

        # Count + Refresh row
        lb_ctrl = tk.Frame(picker_frame, bg=C["sidebar_bg"])
        lb_ctrl.pack(fill="x", pady=(4, 0))
        tk.Label(lb_ctrl, textvariable=self._pdf_count_var,
                 font=(FONT_FAMILY, 8), bg=C["sidebar_bg"], fg=C["sidebar_dim"]
                 ).pack(side="left")
        self._sidebar_btn(lb_ctrl, "⟳  Refresh", self._refresh_pdf_list
                          ).pack(side="right")

        # Hidden StringVar still used internally by _read_config
        self.cfg["pdf_path"] = tk.StringVar()

        # Separator
        tk.Frame(sb, bg=C["sidebar_hover"], height=1).pack(fill="x", padx=16, pady=(8, 8))

        # ── Other global config fields ──────────────────────────────────
        fields = [
            ("output_dir",   "Step 1 Output Dir",    "browse_dir"),
            ("poll_interval","Poll Interval (sec)",  "entry"),
            ("max_polls",    "Max Polls",             "entry"),
        ]

        for key, label, wtype in fields:
            self.cfg[key] = tk.StringVar()
            self._sidebar_field(sb, key, label, wtype)

        # .env buttons
        btn_row = tk.Frame(sb, bg=C["sidebar_bg"])
        btn_row.pack(fill="x", padx=16, pady=(8, 0))

        self._sidebar_btn(btn_row, "Load .env", self._load_config_from_env
                          ).pack(side="left", padx=(0, 6))
        self._sidebar_btn(btn_row, "Save .env", self._save_config_to_env
                          ).pack(side="left")

        # Divider
        tk.Frame(sb, bg=C["sidebar_hover"], height=1).pack(fill="x", padx=20, pady=16)

        # Pipeline summary: step count pills
        tk.Label(sb, text="Steps overview",
                 font=(FONT_FAMILY, 10, "bold"),
                 bg=C["sidebar_bg"], fg=C["sidebar_text"]
                 ).pack(anchor="w", padx=20, pady=(0, 8))

        self._summary_frame = tk.Frame(sb, bg=C["sidebar_bg"])
        self._summary_frame.pack(fill="x", padx=16, pady=(0, 8))
        self._build_summary_pills()

        # Status label at bottom of sidebar
        tk.Frame(sb, bg=C["sidebar_hover"], height=1).pack(fill="x", padx=20, pady=(8, 0), side="bottom")
        self.status_var = tk.StringVar(value="Ready")
        tk.Label(sb, textvariable=self.status_var,
                 font=(FONT_FAMILY, 8, "italic"),
                 bg=C["sidebar_bg"], fg=C["sidebar_dim"],
                 wraplength=260, justify="left"
                 ).pack(anchor="w", padx=20, pady=8, side="bottom")

    def _sidebar_field(self, parent, key, label, wtype):
        wrapper = tk.Frame(parent, bg=C["sidebar_bg"])
        wrapper.pack(fill="x", padx=16, pady=3)

        tk.Label(wrapper, text=label,
                 font=(FONT_FAMILY, 8),
                 bg=C["sidebar_bg"], fg=C["sidebar_dim"]
                 ).pack(anchor="w", padx=2, pady=(0, 2))

        row = tk.Frame(wrapper, bg=C["sidebar_bg"])
        row.pack(fill="x")

        e = tk.Entry(row,
                     textvariable=self.cfg[key],
                     bg=C["sidebar_hover"], fg=C["sidebar_text"],
                     insertbackground=C["sidebar_text"],
                     relief="flat", font=(FONT_FAMILY, 9),
                     highlightbackground=C["sidebar_hover"],
                     highlightthickness=1)
        e.pack(side="left", fill="x", expand=True, ipady=5)
        e.bind("<FocusIn>",  lambda ev, w=e: w.config(highlightbackground=C["indigo"]))
        e.bind("<FocusOut>", lambda ev, w=e: w.config(highlightbackground=C["sidebar_hover"]))

        if wtype in ("browse_file", "browse_dir"):
            v = self.cfg[key]
            cmd = self._browse_file if wtype == "browse_file" else self._browse_dir
            tk.Button(row, text="…",
                      font=(FONT_FAMILY, 9), width=3,
                      bg=C["sidebar_hover"], fg=C["sidebar_text"],
                      activebackground=C["indigo"], activeforeground="#fff",
                      relief="flat", cursor="hand2",
                      command=lambda v=v, c=cmd: c(v)
                      ).pack(side="left", padx=(2, 0), ipady=5)

    def _sidebar_btn(self, parent, text, cmd):
        return tk.Button(parent, text=text,
                         font=(FONT_FAMILY, 8),
                         bg=C["sidebar_hover"], fg=C["sidebar_text"],
                         activebackground=C["indigo"], activeforeground="#fff",
                         relief="flat", cursor="hand2",
                         padx=10, pady=5,
                         command=cmd)

    # ------------------------------------------------------------------ #
    #  PDF PICKER HELPERS                                                  #
    # ------------------------------------------------------------------ #

    def _is_pdf_fully_done(self, pdf_path: Path) -> bool:
        """Return True if all pipeline outputs exist for this PDF."""
        stem = pdf_path.stem
        return all(p.exists() for p in [
            BASE_DIR / "01_Datalab-Output"   / f"{stem}.md",
            BASE_DIR / "02_DO-Spaces-Output" / f"{stem}.md",
            BASE_DIR / "03_Structured-Output" / f"{stem}.json",
            BASE_DIR / "04_Classified-Output" / f"{stem}.json",
            BASE_DIR / "04_1_Enriched-Output" / f"{stem}.json",
            BASE_DIR / "05_Embedded-Output"  / f"{stem}.json",
        ])

    def _refresh_pdf_list(self):
        """Scan PDFs/ recursively for .pdf files, skipping the Processed/ subfolder."""
        pdfs = []
        if PDFS_DIR.exists():
            for p in sorted(PDFS_DIR.rglob("*.pdf")):
                # Skip anything inside Processed/
                try:
                    p.relative_to(PROCESSED_DIR)
                    continue  # it's inside Processed — skip
                except ValueError:
                    pass
                pdfs.append(p)

        self._pdf_list = pdfs
        lb = self._pdf_listbox
        lb.delete(0, "end")
        for i, p in enumerate(pdfs):
            # Show relative path from PDFs/ so subfolders are visible
            try:
                display = str(p.relative_to(PDFS_DIR))
            except ValueError:
                display = p.name
            lb.insert("end", display)
            if self._is_pdf_fully_done(p):
                lb.itemconfig(i, fg="#4ADE80", selectforeground="#FFFFFF")
            else:
                lb.itemconfig(i, fg=C["sidebar_text"], selectforeground="#FFFFFF")

        count = len(pdfs)
        self._pdf_count_var.set(f"{count} PDF{'s' if count != 1 else ''} found")

        # Auto-select if current pdf_path is in the list
        current = self.cfg.get("pdf_path", tk.StringVar()).get().strip()
        if current:
            for i, p in enumerate(pdfs):
                if str(p) == current:
                    lb.selection_clear(0, "end")
                    lb.selection_set(i)
                    lb.see(i)
                    break

    def _on_pdf_select(self, event=None):
        """Called when a PDF is selected in the listbox — updates cfg['pdf_path']."""
        lb = self._pdf_listbox
        sel = lb.curselection()
        if not sel:
            return
        idx = sel[0]
        if idx < len(self._pdf_list):
            chosen = self._pdf_list[idx]
            self.cfg["pdf_path"].set(str(chosen))
            # Refresh step output detection and IO labels for the new PDF
            self._detect_existing_outputs()
            self._refresh_io_labels()
            self._append_log(f"[PDF] Selected: {chosen.name}\n", "success")

    def _move_pdf_to_processed(self):
        """Move the currently selected PDF into PDFs/Processed/ after successful pipeline."""
        pdf_path_str = self.cfg.get("pdf_path", tk.StringVar()).get().strip()
        if not pdf_path_str:
            return
        src = Path(pdf_path_str)
        if not src.exists():
            return
        # Only move PDFs that live inside PDFs/ (don't move externally-picked files)
        try:
            src.relative_to(PDFS_DIR)
        except ValueError:
            self._append_log(
                f"[PDF] Skipping move — file is outside PDFs/ folder.\n", "warn"
            )
            return

        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        dest = PROCESSED_DIR / src.name
        # Avoid overwriting — append a counter if needed
        if dest.exists():
            stem, suffix = src.stem, src.suffix
            counter = 1
            while dest.exists():
                dest = PROCESSED_DIR / f"{stem}_{counter}{suffix}"
                counter += 1

        src.rename(dest)
        self._append_log(
            f"[PDF] Moved  {src.name}  →  PDFs/Processed/\n", "success"
        )
        # Clear selection and refresh list
        self.cfg["pdf_path"].set("")
        self._refresh_pdf_list()

    def _build_summary_pills(self):
        for w in self._summary_frame.winfo_children():
            w.destroy()
        for step_def in STEP_DEFS:
            sid = step_def["id"]
            state = self.step_states.get(sid, StepState.IDLE)
            bg, fg = BADGE[state]
            row = tk.Frame(self._summary_frame, bg=C["sidebar_bg"])
            row.pack(fill="x", pady=2)
            # Number pill
            tk.Label(row, text=step_def["num"],
                     font=(FONT_FAMILY, 8, "bold"),
                     bg=bg, fg=fg,
                     width=4, anchor="center"
                     ).pack(side="left")
            tk.Label(row, text="  " + step_def["label"],
                     font=(FONT_FAMILY, 9),
                     bg=C["sidebar_bg"], fg=C["sidebar_text"],
                     anchor="w"
                     ).pack(side="left", fill="x", expand=True)
            state_lbl = tk.Label(row, text=state.value,
                                 font=(FONT_FAMILY, 8),
                                 bg=C["sidebar_bg"], fg=fg,
                                 width=8, anchor="e")
            state_lbl.pack(side="right")
            # Store summary label refs for updating
            if "summary_state_lbl" not in self.step_widgets.get(sid, {}):
                if sid not in self.step_widgets:
                    self.step_widgets[sid] = {}
                self.step_widgets[sid]["summary_state_lbl"] = state_lbl
                self.step_widgets[sid]["summary_num_lbl"] = row.winfo_children()[0]

    # ------------------------------------------------------------------ #
    #  STEPS PANE                                                          #
    # ------------------------------------------------------------------ #

    def _build_steps_pane(self, parent):
        # Header
        hdr = tk.Frame(parent, bg=C["root_bg"])
        hdr.pack(fill="x", padx=16, pady=(14, 6))
        tk.Label(hdr, text="Pipeline Steps",
                 font=(FONT_FAMILY, 13, "bold"),
                 bg=C["root_bg"], fg=C["text_primary"]
                 ).pack(side="left")
        tk.Label(hdr,
                 text="Enable / disable each step then click Run or Run All",
                 font=(FONT_FAMILY, 9),
                 bg=C["root_bg"], fg=C["text_muted"]
                 ).pack(side="left", padx=12)

        # Scrollable canvas
        canvas = tk.Canvas(parent, bg=C["root_bg"],
                           highlightthickness=0, borderwidth=0)
        vsb = ttk.Scrollbar(parent, orient="vertical",
                            command=canvas.yview,
                            style="Vertical.TScrollbar")
        canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        inner = tk.Frame(canvas, bg=C["root_bg"])
        win_id = canvas.create_window((0, 0), window=inner, anchor="nw")

        inner.bind("<Configure>",
                   lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>",
                    lambda e: canvas.itemconfig(win_id, width=e.width))
        canvas.bind("<Enter>", lambda e: self._set_active_scroll_canvas(canvas))
        canvas.bind("<Leave>", lambda e: self._set_active_scroll_canvas(None))
        inner.bind("<Enter>", lambda e: self._set_active_scroll_canvas(canvas))

        for step_def in STEP_DEFS:
            self._build_step_card(inner, step_def)

        # Bottom padding
        tk.Frame(inner, bg=C["root_bg"], height=16).pack()

    def _build_step_card(self, parent, step_def):
        sid = step_def["id"]

        # Outer card
        card = tk.Frame(parent, bg=C["surface"],
                        highlightbackground=C["border"],
                        highlightthickness=1)
        card.pack(fill="x", padx=16, pady=5)

        # ── TOP ROW ────────────────────────────────────────────
        top = tk.Frame(card, bg=C["surface"])
        top.pack(fill="x", padx=14, pady=(12, 6))

        # Step number pill
        num_bg = C["pill_bg"] if step_def["default_enabled"] else C["pill_dis_bg"]
        num_lbl = tk.Label(top, text=step_def["num"],
                           font=(FONT_FAMILY, 9, "bold"),
                           bg=num_bg, fg=C["pill_fg"],
                           width=4, anchor="center",
                           padx=2, pady=2)
        num_lbl.pack(side="left", padx=(0, 10))

        # Title + desc (stacked)
        title_block = tk.Frame(top, bg=C["surface"])
        title_block.pack(side="left", fill="x", expand=True)
        tk.Label(title_block,
                 text=step_def["label"],
                 font=(FONT_FAMILY, 11, "bold"),
                 bg=C["surface"], fg=C["text_primary"],
                 anchor="w").pack(anchor="w")
        tk.Label(title_block,
                 text=step_def["desc"],
                 font=(FONT_FAMILY, 9),
                 bg=C["surface"], fg=C["text_muted"],
                 anchor="w").pack(anchor="w")

        # Right controls: badge + run btn
        right_ctrl = tk.Frame(top, bg=C["surface"])
        right_ctrl.pack(side="right", padx=(8, 0))

        badge_lbl = tk.Label(right_ctrl, text="Idle",
                             font=(FONT_FAMILY, 9, "bold"),
                             bg=C["idle_bg"], fg=C["idle_fg"],
                             padx=10, pady=3,
                             relief="flat")
        badge_lbl.pack(side="left", padx=(0, 8))

        run_btn = tk.Button(right_ctrl,
                            text="Run",
                            font=(FONT_FAMILY, 9, "bold"),
                            bg=C["btn_run_bg"], fg=C["btn_run_fg"],
                            activebackground=C["btn_primary_hov"],
                            activeforeground="#fff",
                            relief="flat", cursor="hand2",
                            padx=14, pady=4,
                            command=lambda s=sid: self._run_step(s))
        run_btn.pack(side="left")

        # ── SEPARATOR ──────────────────────────────────────────
        tk.Frame(card, bg=C["border"], height=1).pack(fill="x", padx=14)

        # ── IO INFO ROW ────────────────────────────────────────
        io_frame = tk.Frame(card, bg=C["surface_alt"])
        io_frame.pack(fill="x", padx=0, pady=0)

        # Two sub-rows: input and output
        for row_label, var_key, ok_color in [
            ("IN ",  "input_var",  C["text_muted"]),
            ("OUT",  "output_var", C["done_fg"]),
        ]:
            io_row = tk.Frame(io_frame, bg=C["surface_alt"])
            io_row.pack(fill="x", padx=14, pady=(3, 0))
            tk.Label(io_row, text=row_label,
                     font=(FONT_FAMILY, 8, "bold"),
                     bg=C["surface_alt"], fg=C["text_muted"],
                     width=4, anchor="w"
                     ).pack(side="left")
            var = tk.StringVar(value="—")
            lbl = tk.Label(io_row, textvariable=var,
                           font=("Courier New", 8),
                           bg=C["surface_alt"], fg=C["text_muted"],
                           anchor="w", wraplength=380, justify="left")
            lbl.pack(side="left", fill="x", expand=True)
            # store both var and label so we can update color too
            if sid not in self.step_widgets:
                self.step_widgets[sid] = {}
            self.step_widgets[sid][var_key] = var
            self.step_widgets[sid][var_key.replace("var", "lbl")] = lbl

        # Spacer padding under the IO rows
        tk.Frame(io_frame, bg=C["surface_alt"], height=4).pack()

        # ── BOTTOM ROW: checkbox ───────────────────────────────
        bot = tk.Frame(card, bg=C["surface_alt"])
        bot.pack(fill="x", padx=0, pady=0)
        tk.Frame(card, bg=C["border"], height=1).pack(fill="x", padx=14)

        enabled_var = tk.BooleanVar(value=step_def["default_enabled"])
        self.step_enabled[sid] = enabled_var

        def _on_toggle(sv=enabled_var, nl=num_lbl, sd=step_def):
            nl.config(bg=C["pill_bg"] if sv.get() else C["pill_dis_bg"])

        tk.Checkbutton(bot,
                       text="Enable this step",
                       variable=enabled_var,
                       bg=C["surface_alt"], fg=C["text_secondary"],
                       selectcolor=C["indigo_light"],
                       activebackground=C["surface_alt"],
                       activeforeground=C["text_primary"],
                       font=(FONT_FAMILY, 9),
                       cursor="hand2",
                       relief="flat",
                       command=lambda: _on_toggle()
                       ).pack(side="left", padx=14, pady=6)

        # Keep output_var alias so _set_step_state still works
        output_var = self.step_widgets[sid]["output_var"]

        # ── SETTINGS (collapsible) ─────────────────────────────
        settings_defs = step_def.get("settings", [])
        if settings_defs:
            settings_frame = tk.Frame(card, bg=C["surface"],
                                      highlightbackground=C["border"],
                                      highlightthickness=0)
            toggle_var = tk.BooleanVar(value=False)

            expand_btn = tk.Button(
                card,
                text="▾  Settings",
                font=(FONT_FAMILY, 8),
                bg=C["surface"], fg=C["indigo"],
                activebackground=C["indigo_light"],
                activeforeground=C["indigo"],
                relief="flat", cursor="hand2",
                anchor="w",
            )
            expand_btn.pack(fill="x", padx=14, pady=(4, 0))

            def _toggle(sf=settings_frame, tv=toggle_var, btn=expand_btn):
                tv.set(not tv.get())
                if tv.get():
                    sf.pack(fill="x", padx=14, pady=(4, 10))
                    btn.config(text="▴  Settings")
                else:
                    sf.pack_forget()
                    btn.config(text="▾  Settings")
            expand_btn.config(command=_toggle)

            self._build_step_settings(settings_frame, settings_defs)

        else:
            # Small spacer for steps with no settings
            tk.Frame(card, bg=C["surface"], height=4).pack()

        # ── Store refs ─────────────────────────────────────────
        if sid not in self.step_widgets:
            self.step_widgets[sid] = {}
        self.step_widgets[sid].update({
            "badge_lbl":  badge_lbl,
            "num_lbl":    num_lbl,
            "output_var": output_var,
            "run_btn":    run_btn,
            "enabled_var": enabled_var,
        })
        self.step_states[sid] = StepState.IDLE

    # ------------------------------------------------------------------ #
    #  IO LABEL REFRESH                                                    #
    # ------------------------------------------------------------------ #

    def _resolve_step_io(self, sid: str) -> tuple[str, str]:
        """Return (input_display, output_display) for a step given current config."""
        pdf_path = self.cfg.get("pdf_path", tk.StringVar()).get().strip()
        stem = Path(pdf_path).stem if pdf_path else ""

        def sc(key):
            return self.step_cfg.get(key, tk.StringVar()).get().strip()

        def exists_str(p: Path) -> str:
            return p.name if p.exists() else f"{p.name}  (not yet)"

        if sid == "step1":
            inp = Path(pdf_path).name if pdf_path else "—"
            out_path = BASE_DIR / "01_Datalab-Output" / f"{stem}.md" if stem else None
            out = exists_str(out_path) if out_path else "—"

        elif sid == "step1_1":
            override = sc("step1_1_input_md")
            if override:
                inp = Path(override).name
            elif stem:
                p = BASE_DIR / "01_Datalab-Output" / f"{stem}.md"
                inp = exists_str(p)
            else:
                inp = "—"
            out = "audit report in log"

        elif sid == "step2":
            p = BASE_DIR / "01_Datalab-Output" / f"{stem}.md" if stem else None
            inp = exists_str(p) if p else "—"
            out_path = BASE_DIR / "02_DO-Spaces-Output" / f"{stem}.md" if stem else None
            out = exists_str(out_path) if out_path else "—"

        elif sid == "step3":
            override = sc("step3_input_md")
            if override:
                inp = Path(override).name
            elif stem:
                p = BASE_DIR / "02_DO-Spaces-Output" / f"{stem}.md"
                inp = exists_str(p)
            else:
                inp = "—"
            out_path = BASE_DIR / "03_Structured-Output" / f"{stem}.json" if stem else None
            out = exists_str(out_path) if out_path else "—"

        elif sid == "step4":
            override = sc("step4_input_dir")
            if override:
                inp = Path(override).name
            elif stem:
                p = BASE_DIR / "03_Structured-Output" / f"{stem}.json"
                inp = exists_str(p)
            else:
                inp = "—"
            out_path = BASE_DIR / "04_Classified-Output" / f"{stem}.json" if stem else None
            out = exists_str(out_path) if out_path else "—"

        elif sid == "step4_1":
            override = sc("step4_1_input_dir")
            if override:
                inp = Path(override).name
            elif stem:
                p = BASE_DIR / "04_Classified-Output" / f"{stem}.json"
                inp = exists_str(p)
            else:
                inp = "—"
            out_path = BASE_DIR / "04_1_Enriched-Output" / f"{stem}.json" if stem else None
            out = exists_str(out_path) if out_path else "—"

        elif sid == "step5":
            override = sc("step5_input_dir")
            if override:
                inp = Path(override).name
            elif stem:
                p = BASE_DIR / "04_1_Enriched-Output" / f"{stem}.json"
                inp = exists_str(p)
            else:
                inp = "—"
            out_path = BASE_DIR / "05_Embedded-Output" / f"{stem}.json" if stem else None
            out = exists_str(out_path) if out_path else "—"

        elif sid == "step6":
            override_dir = sc("step6_input_dir")
            override_file = sc("step6_single_file")
            if override_file:
                inp = Path(override_file).name
            elif override_dir:
                inp = Path(override_dir).name
            else:
                emb = BASE_DIR / "05_Embedded-Output"
                count = len([f for f in emb.glob("*.json") if f.name != "index.json"]) if emb.exists() else 0
                inp = f"05_Embedded-Output/  ({count} files)" if emb.exists() else "05_Embedded-Output/  (not yet)"
            out = "Supabase PostgreSQL"

        else:
            inp = out = "—"

        return inp, out

    def _refresh_io_labels(self):
        """Update all step cards' IN/OUT labels based on current config + disk state."""
        for step_def in STEP_DEFS:
            sid = step_def["id"]
            w = self.step_widgets.get(sid, {})
            if "input_var" not in w:
                continue
            inp, out = self._resolve_step_io(sid)
            w["input_var"].set(inp)

            # Output label: green if file exists, muted if not
            state = self.step_states.get(sid, StepState.IDLE)
            if state == StepState.DONE:
                w["output_lbl"].config(fg=C["done_fg"])
                w["output_var"].set(out)
            elif state == StepState.ERROR:
                w["output_lbl"].config(fg=C["error_fg"])
                w["output_var"].set(out)
            else:
                exists = "not yet" not in out and out not in ("—", "audit report in log", "Supabase PostgreSQL")
                w["output_lbl"].config(fg=C["done_fg"] if exists else C["text_muted"])
                w["output_var"].set(out)

    def _build_step_settings(self, frame, settings_defs):
        for key, label, wtype, default in settings_defs:
            self.step_cfg[key] = tk.StringVar(value=default)
            # Refresh IO labels whenever a setting override changes
            self.step_cfg[key].trace_add("write",
                lambda *_, : self.root.after(50, self._refresh_io_labels))
            row = tk.Frame(frame, bg=C["surface"])
            row.pack(fill="x", pady=3)

            tk.Label(row, text=label,
                     font=(FONT_FAMILY, 8),
                     bg=C["surface"], fg=C["text_secondary"],
                     width=40, anchor="w"
                     ).pack(side="left", padx=(0, 8))

            if wtype == "entry":
                self._input_entry(row, self.step_cfg[key], width=16).pack(side="left")

            elif wtype == "spinbox":
                sb = tk.Spinbox(row, from_=0, to=9999, width=7,
                                textvariable=self.step_cfg[key],
                                bg=C["input_bg"], fg=C["input_fg"],
                                relief="flat", font=(FONT_FAMILY, 9),
                                highlightbackground=C["input_brd"],
                                highlightthickness=1,
                                buttonbackground=C["border"],
                                insertbackground=C["input_fg"])
                sb.pack(side="left")

            elif wtype == "combobox":
                vals = {"parse_mode": ["fast", "balanced", "accurate"]}.get(key, [])
                ttk.Combobox(row, textvariable=self.step_cfg[key],
                             values=vals, width=14,
                             state="readonly",
                             style="TCombobox"
                             ).pack(side="left")

            elif wtype in ("browse_file", "browse_dir"):
                cmd = self._browse_file if wtype == "browse_file" else self._browse_dir
                e = self._input_entry(row, self.step_cfg[key], width=24)
                e.pack(side="left", padx=(0, 4))
                tk.Button(row, text="Browse",
                          font=(FONT_FAMILY, 8),
                          bg=C["btn_ghost_bg"], fg=C["btn_ghost_fg"],
                          activebackground=C["indigo_light"],
                          activeforeground=C["indigo"],
                          relief="flat", cursor="hand2", padx=8, pady=3,
                          highlightbackground=C["btn_ghost_brd"],
                          highlightthickness=1,
                          command=lambda v=self.step_cfg[key], c=cmd: c(v)
                          ).pack(side="left")

    # ------------------------------------------------------------------ #
    #  TOOLS PANE                                                          #
    # ------------------------------------------------------------------ #

    def _build_tools_pane(self, parent):
        # Header
        hdr = tk.Frame(parent, bg=C["root_bg"])
        hdr.pack(fill="x", padx=16, pady=(14, 6))
        tk.Label(hdr, text="Tools",
                 font=(FONT_FAMILY, 13, "bold"),
                 bg=C["root_bg"], fg=C["text_primary"]
                 ).pack(side="left")

        # Scrollable canvas so more tools can be added later
        canvas = tk.Canvas(parent, bg=C["root_bg"],
                           highlightthickness=0, borderwidth=0)
        vsb = ttk.Scrollbar(parent, orient="vertical",
                            command=canvas.yview,
                            style="Vertical.TScrollbar")
        canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        inner = tk.Frame(canvas, bg=C["root_bg"])
        win_id = canvas.create_window((0, 0), window=inner, anchor="nw")
        inner.bind("<Configure>",
                   lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>",
                    lambda e: canvas.itemconfig(win_id, width=e.width))

        canvas.bind("<Enter>", lambda e: self._set_active_scroll_canvas(canvas))
        canvas.bind("<Leave>", lambda e: self._set_active_scroll_canvas(None))
        inner.bind("<Enter>", lambda e: self._set_active_scroll_canvas(canvas))

        self._build_question_stats_card(inner)
        self._build_question_diff_card(inner)

    def _build_question_stats_card(self, parent):
        """Tool card: count questions in a 05_Embedded-Output file by subject/chapter."""
        card = tk.Frame(parent, bg=C["surface"],
                        highlightbackground=C["border"], highlightthickness=1)
        card.pack(fill="x", padx=12, pady=(0, 12))

        # ── Card header ────────────────────────────────────────────────
        card_hdr = tk.Frame(card, bg=C["indigo_light"])
        card_hdr.pack(fill="x")
        tk.Label(card_hdr, text="Question Stats",
                 font=(FONT_FAMILY, 10, "bold"),
                 bg=C["indigo_light"], fg=C["indigo"]
                 ).pack(side="left", padx=12, pady=8)

        # ── File picker row ─────────────────────────────────────────────
        pick_frame = tk.Frame(card, bg=C["surface"])
        pick_frame.pack(fill="x", padx=10, pady=(8, 4))
        tk.Label(pick_frame, text="File (05_Embedded-Output/)",
                 font=(FONT_FAMILY, 8), bg=C["surface"], fg=C["text_secondary"]
                 ).pack(anchor="w", pady=(0, 3))

        row = tk.Frame(pick_frame, bg=C["surface"])
        row.pack(fill="x")

        self._stats_file_var = tk.StringVar()
        e = self._input_entry(row, self._stats_file_var, width=22)
        e.pack(side="left", fill="x", expand=True, padx=(0, 4))

        def _browse_stats_file():
            init = str(BASE_DIR / "05_Embedded-Output")
            path = filedialog.askopenfilename(
                initialdir=init,
                title="Select embedded JSON",
                filetypes=[("JSON files", "*.json")],
            )
            if path:
                self._stats_file_var.set(path)

        tk.Button(row, text="Browse",
                  font=(FONT_FAMILY, 8),
                  bg=C["btn_ghost_bg"], fg=C["btn_ghost_fg"],
                  activebackground=C["indigo_light"], activeforeground=C["indigo"],
                  relief="flat", cursor="hand2", padx=8, pady=3,
                  highlightbackground=C["btn_ghost_brd"], highlightthickness=1,
                  command=_browse_stats_file
                  ).pack(side="left")

        # ── Auto-fill from selected PDF ─────────────────────────────────
        def _autofill_from_pdf():
            pdf = self.cfg.get("pdf_path", tk.StringVar()).get().strip()
            if not pdf:
                return
            candidate = BASE_DIR / "05_Embedded-Output" / f"{Path(pdf).stem}.json"
            if candidate.exists():
                self._stats_file_var.set(str(candidate))

        tk.Button(pick_frame, text="Use selected PDF's file",
                  font=(FONT_FAMILY, 8),
                  bg=C["surface"], fg=C["indigo"],
                  activebackground=C["indigo_light"], activeforeground=C["indigo"],
                  relief="flat", cursor="hand2", anchor="w",
                  command=_autofill_from_pdf
                  ).pack(anchor="w", pady=(3, 0))

        # ── Run button ──────────────────────────────────────────────────
        tk.Button(card, text="Count Questions",
                  font=(FONT_FAMILY, 9, "bold"),
                  bg=C["btn_primary_bg"], fg=C["btn_primary_fg"],
                  activebackground=C["btn_primary_hov"],
                  activeforeground=C["btn_primary_fg"],
                  relief="flat", cursor="hand2", pady=6,
                  command=self._run_question_stats
                  ).pack(fill="x", padx=10, pady=(4, 8))

        # ── Results area ────────────────────────────────────────────────
        self._stats_result_frame = tk.Frame(card, bg=C["surface"])
        self._stats_result_frame.pack(fill="x", padx=10, pady=(0, 10))

    def _run_question_stats(self):
        """Parse the selected embedded JSON and display question counts."""
        path_str = self._stats_file_var.get().strip()
        if not path_str:
            self._show_stats_error("No file selected.")
            return
        path = Path(path_str)
        if not path.exists():
            self._show_stats_error(f"File not found:\n{path.name}")
            return

        try:
            questions = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            self._show_stats_error(f"Failed to read file:\n{e}")
            return

        if not isinstance(questions, list):
            self._show_stats_error("File is not a JSON array.")
            return

        # ── Compute counts ──────────────────────────────────────────────
        total = len(questions)
        errored = sum(1 for q in questions if "error" in q)
        embedded = sum(1 for q in questions if "embedding" in q)

        # subject → chapter → count
        by_subject: dict = collections.defaultdict(lambda: collections.defaultdict(int))
        for q in questions:
            if "error" in q:
                continue
            subj    = q.get("subject", "Unknown")
            chapter = q.get("chapter", "Unknown")
            by_subject[subj][chapter] += 1

        self._render_stats(path.name, total, errored, embedded, by_subject)

    def _show_stats_error(self, msg: str):
        for w in self._stats_result_frame.winfo_children():
            w.destroy()
        tk.Label(self._stats_result_frame, text=msg,
                 font=(FONT_FAMILY, 9), bg=C["surface"], fg=C["log_err"],
                 wraplength=260, justify="left"
                 ).pack(anchor="w")

    def _render_stats(self, filename, total, errored, embedded, by_subject):
        """Render the stats table into _stats_result_frame."""
        f = self._stats_result_frame
        for w in f.winfo_children():
            w.destroy()

        # Divider
        tk.Frame(f, bg=C["border"], height=1).pack(fill="x", pady=(0, 8))

        # File name
        tk.Label(f, text=filename,
                 font=(FONT_FAMILY, 8, "italic"),
                 bg=C["surface"], fg=C["text_muted"],
                 wraplength=260, justify="left"
                 ).pack(anchor="w", pady=(0, 6))

        # Summary row
        summary = tk.Frame(f, bg=C["indigo_light"])
        summary.pack(fill="x", pady=(0, 8))
        for label, value, color in [
            ("Total",    total,    C["text_primary"]),
            ("Embedded", embedded, C["log_ok"]),
            ("Errors",   errored,  C["log_err"] if errored else C["text_muted"]),
        ]:
            cell = tk.Frame(summary, bg=C["indigo_light"])
            cell.pack(side="left", expand=True, fill="x", padx=6, pady=6)
            tk.Label(cell, text=str(value),
                     font=(FONT_FAMILY, 14, "bold"),
                     bg=C["indigo_light"], fg=color
                     ).pack()
            tk.Label(cell, text=label,
                     font=(FONT_FAMILY, 7),
                     bg=C["indigo_light"], fg=C["text_secondary"]
                     ).pack()

        # Per-subject breakdown
        for subj in sorted(by_subject):
            chapters = by_subject[subj]
            subj_total = sum(chapters.values())

            # Subject header
            subj_row = tk.Frame(f, bg=C["surface_alt"])
            subj_row.pack(fill="x", pady=(2, 0))
            tk.Label(subj_row, text=subj,
                     font=(FONT_FAMILY, 9, "bold"),
                     bg=C["surface_alt"], fg=C["text_primary"]
                     ).pack(side="left", padx=8, pady=4)
            tk.Label(subj_row, text=str(subj_total),
                     font=(FONT_FAMILY, 9, "bold"),
                     bg=C["surface_alt"], fg=C["indigo"]
                     ).pack(side="right", padx=8)

            # Chapter rows
            for chapter in sorted(chapters):
                ch_row = tk.Frame(f, bg=C["surface"])
                ch_row.pack(fill="x")
                tk.Label(ch_row, text=f"  {chapter}",
                         font=(FONT_FAMILY, 8),
                         bg=C["surface"], fg=C["text_secondary"],
                         anchor="w"
                         ).pack(side="left", padx=8, pady=2, fill="x", expand=True)
                tk.Label(ch_row, text=str(chapters[chapter]),
                         font=(FONT_FAMILY, 8),
                         bg=C["surface"], fg=C["text_primary"]
                         ).pack(side="right", padx=8)

    def _build_question_diff_card(self, parent):
        """Tool card: compare MD questions vs JSON questions, show which JSON questions are unique."""
        import re as _re

        card = tk.Frame(parent, bg=C["surface"],
                        highlightbackground=C["border"], highlightthickness=1)
        card.pack(fill="x", padx=12, pady=(0, 12))

        # ── Card header ────────────────────────────────────────────────
        card_hdr = tk.Frame(card, bg=C["indigo_light"])
        card_hdr.pack(fill="x")
        tk.Label(card_hdr, text="Question Diff  (MD vs JSON)",
                 font=(FONT_FAMILY, 10, "bold"),
                 bg=C["indigo_light"], fg=C["indigo"]
                 ).pack(side="left", padx=12, pady=8)

        # ── MD file picker ──────────────────────────────────────────────
        md_frame = tk.Frame(card, bg=C["surface"])
        md_frame.pack(fill="x", padx=10, pady=(8, 4))
        tk.Label(md_frame, text="MD file (02_DO-Spaces-Output/)",
                 font=(FONT_FAMILY, 8), bg=C["surface"], fg=C["text_secondary"]
                 ).pack(anchor="w", pady=(0, 3))

        md_row = tk.Frame(md_frame, bg=C["surface"])
        md_row.pack(fill="x")

        self._diff_md_var = tk.StringVar()
        e_md = self._input_entry(md_row, self._diff_md_var, width=22)
        e_md.pack(side="left", fill="x", expand=True, padx=(0, 4))

        def _browse_diff_md():
            path = filedialog.askopenfilename(
                initialdir=str(BASE_DIR / "02_DO-Spaces-Output"),
                title="Select MD file",
                filetypes=[("Markdown files", "*.md")],
            )
            if path:
                self._diff_md_var.set(path)

        tk.Button(md_row, text="Browse",
                  font=(FONT_FAMILY, 8),
                  bg=C["btn_ghost_bg"], fg=C["btn_ghost_fg"],
                  activebackground=C["indigo_light"], activeforeground=C["indigo"],
                  relief="flat", cursor="hand2", padx=8, pady=3,
                  highlightbackground=C["btn_ghost_brd"], highlightthickness=1,
                  command=_browse_diff_md
                  ).pack(side="left")

        def _autofill_diff_md():
            pdf = self.cfg.get("pdf_path", tk.StringVar()).get().strip()
            if not pdf:
                return
            candidate = BASE_DIR / "02_DO-Spaces-Output" / f"{Path(pdf).stem}.md"
            if candidate.exists():
                self._diff_md_var.set(str(candidate))

        tk.Button(md_frame, text="Use selected PDF's file",
                  font=(FONT_FAMILY, 8),
                  bg=C["surface"], fg=C["indigo"],
                  activebackground=C["indigo_light"], activeforeground=C["indigo"],
                  relief="flat", cursor="hand2", anchor="w",
                  command=_autofill_diff_md
                  ).pack(anchor="w", pady=(3, 0))

        # ── JSON file picker ────────────────────────────────────────────
        json_frame = tk.Frame(card, bg=C["surface"])
        json_frame.pack(fill="x", padx=10, pady=(4, 4))
        tk.Label(json_frame, text="JSON file (03_Structured-Output/)",
                 font=(FONT_FAMILY, 8), bg=C["surface"], fg=C["text_secondary"]
                 ).pack(anchor="w", pady=(0, 3))

        json_row = tk.Frame(json_frame, bg=C["surface"])
        json_row.pack(fill="x")

        self._diff_json_var = tk.StringVar()
        e_json = self._input_entry(json_row, self._diff_json_var, width=22)
        e_json.pack(side="left", fill="x", expand=True, padx=(0, 4))

        def _browse_diff_json():
            path = filedialog.askopenfilename(
                initialdir=str(BASE_DIR / "03_Structured-Output"),
                title="Select structured JSON",
                filetypes=[("JSON files", "*.json")],
            )
            if path:
                self._diff_json_var.set(path)

        tk.Button(json_row, text="Browse",
                  font=(FONT_FAMILY, 8),
                  bg=C["btn_ghost_bg"], fg=C["btn_ghost_fg"],
                  activebackground=C["indigo_light"], activeforeground=C["indigo"],
                  relief="flat", cursor="hand2", padx=8, pady=3,
                  highlightbackground=C["btn_ghost_brd"], highlightthickness=1,
                  command=_browse_diff_json
                  ).pack(side="left")

        def _autofill_diff_json():
            pdf = self.cfg.get("pdf_path", tk.StringVar()).get().strip()
            if not pdf:
                return
            candidate = BASE_DIR / "03_Structured-Output" / f"{Path(pdf).stem}.json"
            if candidate.exists():
                self._diff_json_var.set(str(candidate))

        tk.Button(json_frame, text="Use selected PDF's file",
                  font=(FONT_FAMILY, 8),
                  bg=C["surface"], fg=C["indigo"],
                  activebackground=C["indigo_light"], activeforeground=C["indigo"],
                  relief="flat", cursor="hand2", anchor="w",
                  command=_autofill_diff_json
                  ).pack(anchor="w", pady=(3, 0))

        # ── Run button ──────────────────────────────────────────────────
        tk.Button(card, text="Find Unique JSON Questions",
                  font=(FONT_FAMILY, 9, "bold"),
                  bg=C["btn_primary_bg"], fg=C["btn_primary_fg"],
                  activebackground=C["btn_primary_hov"],
                  activeforeground=C["btn_primary_fg"],
                  relief="flat", cursor="hand2", pady=6,
                  command=self._run_question_diff
                  ).pack(fill="x", padx=10, pady=(4, 6))

        # ── Status label (full results go to Output Log) ────────────────
        self._diff_status_var = tk.StringVar(value="Results will appear in Output Log →")
        tk.Label(card,
                 textvariable=self._diff_status_var,
                 font=(FONT_FAMILY, 8, "italic"),
                 bg=C["surface"], fg=C["text_muted"],
                 wraplength=280, justify="left",
                 anchor="w",
                 ).pack(fill="x", padx=12, pady=(0, 10))

    def _run_question_diff(self):
        import re as _re

        def _normalize(text: str) -> str:
            text = _re.sub(r"<[^>]+>", "", text)
            text = text.lower()
            text = _re.sub(r"[^\w\s]", " ", text)
            text = _re.sub(r"\s+", " ", text).strip()
            words = text.split()
            return " ".join(words[:8])

        def _err(msg: str):
            self._diff_status_var.set(f"Error: {msg}")
            self._append_log(f"[Question Diff] ERROR: {msg}\n", force_tag="error")

        md_str = self._diff_md_var.get().strip()
        json_str = self._diff_json_var.get().strip()

        if not md_str:
            _err("No MD file selected.")
            return
        if not json_str:
            _err("No JSON file selected.")
            return

        md_path = Path(md_str)
        json_path = Path(json_str)

        if not md_path.exists():
            _err(f"MD file not found: {md_path.name}")
            return
        if not json_path.exists():
            _err(f"JSON file not found: {json_path.name}")
            return

        try:
            md_text = md_path.read_text(encoding="utf-8")
        except Exception as exc:
            _err(f"Failed to read MD: {exc}")
            return

        try:
            questions = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as exc:
            _err(f"Failed to read JSON: {exc}")
            return

        if not isinstance(questions, list):
            _err("JSON file is not an array.")
            return

        md_keys: set[str] = set()
        for line in md_text.splitlines():
            m = _re.match(r"^\d+\.\s+(.+)", line)
            if m:
                md_keys.add(_normalize(m.group(1)))

        unmatched: list[tuple[int, str, str]] = []
        for idx, q in enumerate(questions):
            q_text = q.get("question", "")
            key = _normalize(q_text)
            if key and key not in md_keys:
                unmatched.append((idx + 1, q.get("subject", "?"), q_text))

        self._render_diff_results(md_path.name, json_path.name, len(questions), unmatched)

    def _render_diff_results(self, md_name: str, json_name: str, total: int, unmatched: list):
        import re as _re

        sep = "─" * 60 + "\n"

        self._append_log(
            f"\n=== Question Diff: {json_name} vs {md_name} ===\n",
            force_tag="header",
        )

        if not unmatched:
            self._diff_status_var.set(f"✓ All {total} matched")
            self._append_log(
                f"✓ All {total} JSON questions found in MD — no unique questions.\n",
                force_tag="success",
            )
            return

        self._diff_status_var.set(f"{len(unmatched)} unmatched — see Output Log")
        self._append_log(
            f"{len(unmatched)} unmatched question(s) out of {total}:\n\n",
            force_tag="warn",
        )

        for q_num, subject, q_text in unmatched:
            clean = _re.sub(r"<[^>]+>", "", q_text)
            self._append_log(f"Q{q_num}  [{subject}]\n", force_tag="header")
            self._append_log(f"{clean}\n\n")
            self._append_log(sep)

    # ------------------------------------------------------------------ #
    #  LOG PANE                                                            #
    # ------------------------------------------------------------------ #

    def _build_log_pane(self, parent):
        # Header row
        hdr = tk.Frame(parent, bg=C["root_bg"])
        hdr.pack(fill="x", padx=16, pady=(14, 6))
        tk.Label(hdr, text="Output Log",
                 font=(FONT_FAMILY, 13, "bold"),
                 bg=C["root_bg"], fg=C["text_primary"]
                 ).pack(side="left")

        btn_row = tk.Frame(hdr, bg=C["root_bg"])
        btn_row.pack(side="right")
        for txt, cmd in [("Clear", self._clear_log), ("Copy", self._copy_log)]:
            tk.Button(btn_row, text=txt,
                      font=(FONT_FAMILY, 8),
                      bg=C["btn_ghost_bg"], fg=C["btn_ghost_fg"],
                      activebackground=C["indigo_light"],
                      activeforeground=C["indigo"],
                      relief="flat", cursor="hand2",
                      padx=12, pady=4,
                      highlightbackground=C["btn_ghost_brd"],
                      highlightthickness=1,
                      command=cmd
                      ).pack(side="left", padx=(0, 6))

        # Log widget
        log_frame = tk.Frame(parent, bg=C["surface"],
                             highlightbackground=C["border"],
                             highlightthickness=1)
        log_frame.pack(fill="both", expand=True, padx=16, pady=(0, 16))

        self.log_widget = scrolledtext.ScrolledText(
            log_frame,
            state="disabled",
            bg=C["log_bg"], fg=C["log_fg"],
            font=("Courier New", 10),
            wrap=tk.WORD,
            insertbackground=C["log_fg"],
            selectbackground=C["indigo_light"],
            selectforeground=C["text_primary"],
            relief="flat",
            borderwidth=0,
            padx=12, pady=8,
        )
        self.log_widget.pack(fill="both", expand=True)

        # Color tags
        self.log_widget.tag_config("error",   foreground=C["log_err"])
        self.log_widget.tag_config("success", foreground=C["log_ok"])
        self.log_widget.tag_config("header",  foreground=C["log_hdr"],
                                   font=("Courier New", 10, "bold"))
        self.log_widget.tag_config("warn",    foreground=C["log_warn"])

    # ------------------------------------------------------------------ #
    #  WIDGET HELPERS                                                      #
    # ------------------------------------------------------------------ #

    def _input_entry(self, parent, textvariable, width=20):
        e = tk.Entry(parent, textvariable=textvariable, width=width,
                     bg=C["input_bg"], fg=C["input_fg"],
                     insertbackground=C["input_fg"],
                     relief="flat", font=(FONT_FAMILY, 9),
                     highlightbackground=C["input_brd"],
                     highlightthickness=1)
        e.bind("<FocusIn>",  lambda ev, w=e: w.config(highlightbackground=C["border_focus"]))
        e.bind("<FocusOut>", lambda ev, w=e: w.config(highlightbackground=C["input_brd"]))
        return e

    def _browse_file(self, var: tk.StringVar):
        path = filedialog.askopenfilename(
            initialdir=str(BASE_DIR),
            filetypes=[("All files", "*.*"), ("PDF files", "*.pdf"),
                       ("Markdown files", "*.md"), ("JSON files", "*.json")],
        )
        if path:
            var.set(path)

    def _browse_dir(self, var: tk.StringVar):
        path = filedialog.askdirectory(initialdir=str(BASE_DIR))
        if path:
            var.set(path)

    # ------------------------------------------------------------------ #
    #  CONFIG HELPERS                                                      #
    # ------------------------------------------------------------------ #

    def _load_config_from_env(self):
        env = _parse_env_file()
        defaults_map = {
            "pdf_path":     (str(BASE_DIR / "PDFs" / "JEE_Mains_2026_Jan_28.pdf"), "PDF_PATH"),
            "output_dir":   (str(BASE_DIR / "01_Datalab-Output"), "OUTPUT_DIR"),
            "poll_interval":("3",   "POLL_INTERVAL_SECONDS"),
            "max_polls":    ("600", "MAX_POLLS"),
        }
        for key, (default, env_key) in defaults_map.items():
            if key in self.cfg:
                self.cfg[key].set(env.get(env_key, default))

        step_env_map = {
            "start_page":        ("START_PAGE",       ""),
            "end_page":          ("END_PAGE",          ""),
            "chunk_size":        ("CHUNK_SIZE",        "6"),
            "min_quality_score": ("MIN_QUALITY_SCORE", "4.0"),
            "max_chunk_retries": ("MAX_CHUNK_RETRIES", "2"),
            "parse_mode":        ("PARSE_MODE",        "balanced"),
            "spaces_folder":     ("SPACES_FOLDER",     ""),
        }
        for cfg_key, (env_key, default) in step_env_map.items():
            if cfg_key in self.step_cfg:
                self.step_cfg[cfg_key].set(env.get(env_key, default))

        self._append_log("[Config] Loaded settings from .env\n", "success")

    def _save_config_to_env(self):
        for cfg_key, env_key in [("poll_interval", "POLL_INTERVAL_SECONDS"),
                                   ("max_polls", "MAX_POLLS")]:
            if cfg_key in self.cfg:
                _write_env_key(env_key, self.cfg[cfg_key].get())

        for cfg_key, env_key in [
            ("start_page", "START_PAGE"), ("end_page", "END_PAGE"),
            ("chunk_size", "CHUNK_SIZE"), ("min_quality_score", "MIN_QUALITY_SCORE"),
            ("max_chunk_retries", "MAX_CHUNK_RETRIES"), ("parse_mode", "PARSE_MODE"),
            ("spaces_folder", "SPACES_FOLDER"),
        ]:
            if cfg_key in self.step_cfg:
                _write_env_key(env_key, self.step_cfg[cfg_key].get())

        self._append_log("[Config] Settings saved to .env\n", "success")

    def _read_config(self) -> dict:
        def _s(key, d="", src=None):
            src = src or self.cfg
            v = src[key].get().strip() if key in src else ""
            return v if v else d

        def _i(key, d=None, src=None):
            v = _s(key, "", src or self.step_cfg)
            try: return int(v) if v else d
            except ValueError: return d

        def _f(key, d=None, src=None):
            v = _s(key, "", src or self.step_cfg)
            try: return float(v) if v else d
            except ValueError: return d

        return {
            "pdf_path":           _s("pdf_path", src=self.cfg),
            "output_dir":         _s("output_dir", str(BASE_DIR / "01_Datalab-Output"), self.cfg),
            "poll_interval":      _i("poll_interval", 3, self.cfg),
            "max_polls":          _i("max_polls", 600, self.cfg),
            "debug":              self.debug_var.get(),
            "start_page":         _i("start_page"),
            "end_page":           _i("end_page"),
            "chunk_size":         _i("chunk_size", 6),
            "min_quality_score":  _f("min_quality_score", 4.0),
            "max_chunk_retries":  _i("max_chunk_retries", 2),
            "parse_mode":         _s("parse_mode", "balanced"),
            "spaces_folder":      _s("spaces_folder") or None,
            "step1_1_input_md":   _s("step1_1_input_md") or None,
            "step3_input_md":     _s("step3_input_md") or None,
            "step4_input_dir":    _s("step4_input_dir") or None,
            "step4_1_input_dir":  _s("step4_1_input_dir") or None,
            "step5_input_dir":    _s("step5_input_dir") or None,
            "step6_input_dir":    _s("step6_input_dir") or None,
            "step6_single_file":  _s("step6_single_file") or None,
        }

    # ------------------------------------------------------------------ #
    #  STATE MANAGEMENT                                                    #
    # ------------------------------------------------------------------ #

    def _set_step_state(self, sid: str, state: StepState, output_path=None):
        self.step_states[sid] = state
        w = self.step_widgets.get(sid, {})
        bg, fg = BADGE[state]

        if "badge_lbl" in w:
            w["badge_lbl"].config(text=state.value, bg=bg, fg=fg)

        # Refresh IO labels on any state change (done/error = new files may exist)
        if state in (StepState.DONE, StepState.ERROR, StepState.IDLE):
            self.root.after(10, self._refresh_io_labels)

        if "run_btn" in w:
            w["run_btn"].config(state="disabled" if state == StepState.RUNNING else "normal")

        # Update sidebar summary
        if "summary_state_lbl" in w:
            w["summary_state_lbl"].config(text=state.value, fg=fg)
        if "summary_num_lbl" in w:
            w["summary_num_lbl"].config(bg=bg, fg=fg)

        # Run All button
        any_running = any(s == StepState.RUNNING for s in self.step_states.values())
        self.run_all_btn.config(state="disabled" if any_running else "normal")

        # Status bar
        msgs = {
            StepState.RUNNING: f"Running  ·  {STEP_BY_ID[sid]['label']}…",
            StepState.DONE:    f"Done  ·  {STEP_BY_ID[sid]['label']}",
            StepState.ERROR:   f"Error in {STEP_BY_ID[sid]['label']}  ·  see log",
            StepState.SKIPPED: f"Skipped  ·  {STEP_BY_ID[sid]['label']}",
        }
        if state in msgs:
            self.status_var.set(msgs[state])

    def _detect_existing_outputs(self):
        # Reset all steps to IDLE first so switching PDFs clears stale Done states
        for sid in list(self.step_states.keys()):
            if self.step_states[sid] != StepState.RUNNING:
                self._set_step_state(sid, StepState.IDLE)

        pdf_path = self.cfg.get("pdf_path", tk.StringVar()).get().strip()
        if not pdf_path:
            return
        stem = Path(pdf_path).stem
        checks = {
            "step1":   BASE_DIR / "01_Datalab-Output"  / f"{stem}.md",
            "step2":   BASE_DIR / "02_DO-Spaces-Output" / f"{stem}.md",
            "step3":   BASE_DIR / "03_Structured-Output" / f"{stem}.json",
            "step4":   BASE_DIR / "04_Classified-Output" / f"{stem}.json",
            "step4_1": BASE_DIR / "04_1_Enriched-Output" / f"{stem}.json",
            "step5":   BASE_DIR / "05_Embedded-Output"  / f"{stem}.json",
        }
        for sid, path in checks.items():
            if path.exists():
                self._set_step_state(sid, StepState.DONE, path)

    def _find_output_for_step(self, sid: str, pdf_path: str) -> "Path | None":
        stem = Path(pdf_path).stem
        lookup = {
            "step1":   BASE_DIR / "01_Datalab-Output"  / f"{stem}.md",
            "step2":   BASE_DIR / "02_DO-Spaces-Output" / f"{stem}.md",
            "step3":   BASE_DIR / "03_Structured-Output" / f"{stem}.json",
            "step4":   BASE_DIR / "04_Classified-Output" / f"{stem}.json",
            "step4_1": BASE_DIR / "04_1_Enriched-Output" / f"{stem}.json",
            "step5":   BASE_DIR / "05_Embedded-Output"  / f"{stem}.json",
        }
        p = lookup.get(sid)
        return p if (p and p.exists()) else None

    # ------------------------------------------------------------------ #
    #  LOG                                                                 #
    # ------------------------------------------------------------------ #

    def _poll_log_queue(self):
        try:
            while True:
                text = self.log_queue.get_nowait()
                self._append_log(text)
        except queue.Empty:
            pass
        self.root.after(50, self._poll_log_queue)

    def _append_log(self, text: str, force_tag: str = None):
        self.log_widget.configure(state="normal")
        for line in text.splitlines(keepends=True):
            tag = force_tag
            if tag is None:
                lo = line.lower()
                if any(k in lo for k in ("error", "failed", "traceback", "exception")):
                    tag = "error"
                elif "===" in line and ("step" in lo or "pipeline" in lo):
                    tag = "header"
                elif any(k in lo for k in ("saved to", "complete", "done", "imported", "created")):
                    tag = "success"
                elif any(k in lo for k in ("warning", "warn", "retry", "skipping")):
                    tag = "warn"
            self.log_widget.insert("end", line, tag or "")
        self.log_widget.see("end")
        self.log_widget.configure(state="disabled")

    def _clear_log(self):
        self.log_widget.configure(state="normal")
        self.log_widget.delete("1.0", "end")
        self.log_widget.configure(state="disabled")

    def _copy_log(self):
        self.root.clipboard_clear()
        self.root.clipboard_append(self.log_widget.get("1.0", "end"))
        self._append_log("[Clipboard] Log copied.\n", "success")

    # ------------------------------------------------------------------ #
    #  STEP CALLABLES (unchanged logic)                                    #
    # ------------------------------------------------------------------ #

    def _build_step_callable(self, sid: str, cfg: dict, writer,
                              md_path=None, spaces_md_path=None):
        if sid == "step1":
            extract = importlib.import_module("01_extract_pdf").extract
            output_dir = cfg["output_dir"] or str(BASE_DIR / "01_Datalab-Output")
            async def step1_coro():
                return await extract(
                    pdf_path=cfg["pdf_path"], output_dir=output_dir,
                    start_page=cfg["start_page"], end_page=cfg["end_page"],
                    debug=cfg["debug"], poll_interval=cfg["poll_interval"],
                    max_polls=cfg["max_polls"], chunk_size=cfg["chunk_size"],
                    min_quality_score=cfg["min_quality_score"],
                    max_chunk_retries=cfg["max_chunk_retries"],
                    parse_mode=cfg["parse_mode"],
                )
            return step1_coro, True

        elif sid == "step1_1":
            audit_questions = importlib.import_module("01_1_audit_questions").audit_questions
            resolved_md = (
                cfg.get("step1_1_input_md")
                or md_path
                or self._find_output_for_step("step1", cfg["pdf_path"])
            )
            if not resolved_md:
                stem = Path(cfg["pdf_path"]).stem
                resolved_md = BASE_DIR / "01_Datalab-Output" / f"{stem}.md"
            def step1_1_func():
                audit_questions(resolved_md)
            return step1_1_func, False

        elif sid == "step2":
            upload_and_rewrite = importlib.import_module("02_upload_digitalocean").upload_and_rewrite
            resolved_md = md_path or self._find_output_for_step("step1", cfg["pdf_path"])
            if not resolved_md:
                stem = Path(cfg["pdf_path"]).stem
                resolved_md = BASE_DIR / "01_Datalab-Output" / f"{stem}.md"
            async def step2_coro():
                _, out_md = await upload_and_rewrite(
                    md_path=resolved_md, spaces_folder=cfg["spaces_folder"])
                return out_md
            return step2_coro, True

        elif sid == "step3":
            structure_markdown = importlib.import_module("03_structure_gemini").structure_markdown
            resolved_md = (cfg.get("step3_input_md") or spaces_md_path
                           or self._find_output_for_step("step2", cfg["pdf_path"]))
            if not resolved_md:
                stem = Path(cfg["pdf_path"]).stem
                resolved_md = BASE_DIR / "02_DO-Spaces-Output" / f"{stem}.md"
            async def step3_coro():
                return await structure_markdown(md_path=resolved_md)
            return step3_coro, True

        elif sid == "step4":
            classify_all = importlib.import_module("04_classify_topic_chapter").classify_all
            input_dir = cfg.get("step4_input_dir") or str(BASE_DIR / "03_Structured-Output")
            stem = Path(cfg["pdf_path"]).stem if cfg.get("pdf_path") else ""
            single = str(BASE_DIR / "03_Structured-Output" / f"{stem}.json") if stem and not cfg.get("step4_input_dir") else None
            async def step4_coro():
                return await classify_all(input_dir=input_dir, single_file=single)
            return step4_coro, True

        elif sid == "step4_1":
            enrich_all = importlib.import_module("04_1_enrich_ids").enrich_all
            input_dir = cfg.get("step4_1_input_dir") or str(BASE_DIR / "04_Classified-Output")
            stem = Path(cfg["pdf_path"]).stem if cfg.get("pdf_path") else ""
            single = str(BASE_DIR / "04_Classified-Output" / f"{stem}.json") if stem and not cfg.get("step4_1_input_dir") else None
            def step4_1_func():
                return enrich_all(input_dir=input_dir, single_file=single)
            return step4_1_func, False

        elif sid == "step5":
            embed_all = importlib.import_module("05_embed_questions").embed_all
            input_dir = cfg.get("step5_input_dir") or str(BASE_DIR / "04_1_Enriched-Output")
            stem = Path(cfg["pdf_path"]).stem if cfg.get("pdf_path") else ""
            single = str(BASE_DIR / "04_1_Enriched-Output" / f"{stem}.json") if stem and not cfg.get("step5_input_dir") else None
            def step5_func():
                return embed_all(input_dir=input_dir, single_file=single)
            return step5_func, False

        elif sid == "step6":
            def step6_func():
                mod = importlib.import_module("06_import_to_supabase")
                in_dir_str = cfg.get("step6_input_dir") or str(BASE_DIR / "05_Embedded-Output")
                single = cfg.get("step6_single_file")
                files = [Path(single)] if single else sorted(
                    f for f in Path(in_dir_str).glob("*.json") if f.name != "index.json")
                total_c = total_s = 0
                for f in files:
                    print(f"Importing {f.name}...", flush=True)
                    created, skipped = mod.import_file(f)
                    print(f"  {created} created, {skipped} skipped", flush=True)
                    total_c += created; total_s += skipped
                print(f"\nTotal: {total_c} created, {total_s} skipped", flush=True)
            return step6_func, False

        raise ValueError(f"Unknown step id: {sid}")

    # ------------------------------------------------------------------ #
    #  RUN LOGIC (unchanged)                                               #
    # ------------------------------------------------------------------ #

    def _stop_pipeline(self):
        """Force-terminate the active pipeline thread."""
        t = self._active_thread
        if t is None or not t.is_alive():
            return
        self._stop_event.set()
        tid = t.ident
        if tid is not None:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(tid),
                ctypes.py_object(SystemExit),
            )
        self._append_log("\n[Stop] Termination requested — pipeline will halt.\n", "warn")
        self.stop_btn.config(state="disabled")
        # Mark any running steps as error
        for sid, state in list(self.step_states.items()):
            if state == StepState.RUNNING:
                self._set_step_state(sid, StepState.ERROR)
        self.run_all_btn.config(state="normal")
        for w in self.step_widgets.values():
            if "run_btn" in w:
                w["run_btn"].config(state="normal")
        self.status_var.set("Stopped by user.")

    def _exec_on_thread(self, sid, factory_fn, is_async, on_done):
        self._stop_event.clear()
        writer = QueueWriter(self.log_queue)
        self.root.after(0, lambda: self.stop_btn.config(state="normal"))
        def thread_target():
            result = None; exc = None
            try:
                with contextlib.redirect_stdout(writer), contextlib.redirect_stderr(writer):
                    if is_async:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            result = loop.run_until_complete(factory_fn())
                        finally:
                            loop.close()
                    else:
                        result = factory_fn()
            except (Exception, SystemExit) as e:
                exc = e if not isinstance(e, SystemExit) else None
                if not isinstance(e, SystemExit):
                    self.log_queue.put(traceback.format_exc())
            finally:
                self.root.after(0, lambda: self.stop_btn.config(state="disabled"))
                self._active_thread = None
                self.root.after(0, lambda: on_done(result, exc))
        t = threading.Thread(target=thread_target, daemon=True)
        self._active_thread = t
        t.start()

    def _run_step(self, sid: str):
        cfg = self._read_config()
        self._append_log(f"\n{'='*52}\n  {STEP_BY_ID[sid]['label']}\n{'='*52}\n", "header")
        self._set_step_state(sid, StepState.RUNNING)
        writer_dummy = QueueWriter(self.log_queue)
        factory_fn, is_async = self._build_step_callable(
            sid, cfg, writer_dummy,
            md_path=self._pipeline_md_path,
            spaces_md_path=self._pipeline_spaces_md_path,
        )
        def on_done(result, exc):
            if exc:
                self._set_step_state(sid, StepState.ERROR)
            else:
                op = self._find_output_for_step(sid, cfg["pdf_path"])
                if result and not op:
                    try: op = Path(result)
                    except Exception: pass
                self._set_step_state(sid, StepState.DONE, op)
                if sid == "step1" and result: self._pipeline_md_path = result
                elif sid == "step2" and result: self._pipeline_spaces_md_path = result
                self._refresh_pdf_list()
        self._exec_on_thread(sid, factory_fn, is_async, on_done)

    def _run_all(self):
        cfg = self._read_config()
        enabled = {sid: self.step_enabled[sid].get() for sid in self.step_states}
        self.run_all_btn.config(state="disabled")
        for w in self.step_widgets.values():
            if "run_btn" in w:
                w["run_btn"].config(state="disabled")
        for sid, is_en in enabled.items():
            if not is_en:
                self._set_step_state(sid, StepState.SKIPPED)

        writer = QueueWriter(self.log_queue)
        self._stop_event.clear()

        def pipeline_thread():
            md_path = None; spaces_md_path = None

            def exec_step(sid):
                nonlocal md_path, spaces_md_path
                if not enabled.get(sid):
                    return True
                self.log_queue.put(f"\n{'='*52}\n  {STEP_BY_ID[sid]['label']}\n{'='*52}\n")
                self.root.after(0, lambda s=sid: self._set_step_state(s, StepState.RUNNING))
                result = None; exc = None
                try:
                    factory_fn, is_async = self._build_step_callable(
                        sid, cfg, writer, md_path=md_path, spaces_md_path=spaces_md_path)
                    with contextlib.redirect_stdout(writer), contextlib.redirect_stderr(writer):
                        if is_async:
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                            try: result = loop.run_until_complete(factory_fn())
                            finally: loop.close()
                        else:
                            result = factory_fn()
                except Exception as e:
                    exc = e; writer.write(traceback.format_exc())
                if exc:
                    self.root.after(0, lambda s=sid: self._set_step_state(s, StepState.ERROR))
                    return False
                else:
                    op = self._find_output_for_step(sid, cfg["pdf_path"])
                    if result and not op:
                        try: op = Path(result)
                        except Exception: pass
                    self.root.after(0, lambda s=sid, p=op:
                                    self._set_step_state(s, StepState.DONE, p))
                    if sid == "step1" and result: md_path = result
                    elif sid == "step2" and result: spaces_md_path = result
                    return True

            pipeline_success = True
            for sid in ["step1", "step1_1", "step2", "step3",
                        "step4", "step4_1", "step5", "step6"]:
                if not exec_step(sid):
                    writer.write("\n[Pipeline stopped due to error]\n")
                    pipeline_success = False
                    break
            else:
                writer.write(f"\n{'='*52}\n  Pipeline complete.\n{'='*52}\n")

            self.root.after(0, lambda ok=pipeline_success: self._on_pipeline_complete(ok))

        t = threading.Thread(target=pipeline_thread, daemon=True)
        self._active_thread = t
        self.stop_btn.config(state="normal")
        t.start()

    def _on_pipeline_complete(self, success: bool = True):
        self.run_all_btn.config(state="normal")
        self.stop_btn.config(state="disabled")
        self._active_thread = None
        for w in self.step_widgets.values():
            if "run_btn" in w:
                w["run_btn"].config(state="normal")
        if success:
            self.status_var.set("Pipeline finished — moving PDF to Processed/")
            self._move_pdf_to_processed()
            self.status_var.set("Pipeline finished.")
        else:
            self.status_var.set("Pipeline stopped — error in one or more steps.")


# ===== ENTRY POINT =====
if __name__ == "__main__":
    root = tk.Tk()
    app = PipelineApp(root)
    root.mainloop()
