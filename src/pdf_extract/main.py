"""
PDF to markdown extraction pipeline.

Text-layer PDFs: markitdown + text LLM (pdfminer does the work, LLM for embedded images).
Image-based PDFs: page-by-page vision OCR via vision LLM chain.

Usage:
    python -m src.pdf_extract.main [--source bb|wd|sb] [--limit N] [--dry-run] [--reprocess]
"""

import argparse
import base64
import concurrent.futures
import os
import sys
import threading
import time
import traceback
from pathlib import Path

import fitz  # pymupdf
from dotenv import load_dotenv
from markitdown import MarkItDown
from openai import OpenAI, RateLimitError

load_dotenv()

DATA_DIR = Path(__file__).parent.parent.parent / "data"
SOURCES = ["bb", "wd", "sb"]

OPENGATEWAY_BASE_URL = "https://opengateway.gitlawb.com/v1"
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
CEREBRAS_BASE_URL = "https://api.cerebras.ai/v1"

# Text models — tried in order, first with a valid API key wins.
# Used by markitdown for LLM-enhanced extraction (e.g. embedded image descriptions).
_OR = ("OpenRouter", "OPENROUTER_API_KEY", OPENROUTER_BASE_URL)
_GV = ("Google AI Studio", "GEMINI_API_KEY", GEMINI_BASE_URL)
_CB = ("Cerebras", "CEREBRAS_API_KEY", CEREBRAS_BASE_URL)
TEXT_MODELS = [
    ("OpenGateway", "OPENGATEWAY_API_KEY", OPENGATEWAY_BASE_URL, "minimax/minimax-m3"),
    (*_GV, "gemini-3-flash"),                            # 1M ctx
    (*_GV, "gemini-2.5-flash"),                          # 1M ctx
    (*_CB, "gpt-oss-120b"),                              # 131K ctx
    (*_OR, "openrouter/owl-alpha"),                      # 1.05M ctx
    (*_OR, "nvidia/nemotron-3-ultra-550b-a55b:free"),   # 1M ctx
    (*_OR, "nvidia/nemotron-3-super-120b-a12b:free"),   # 1M ctx
    (*_OR, "nvidia/nemotron-3-nano-30b-a3b:free"),       # 256K ctx
    (*_OR, "openai/gpt-oss-120b:free"),                  # 131K ctx
    (*_OR, "openai/gpt-oss-20b:free"),                   # 131K ctx
    (*_OR, "z-ai/glm-4.5-air:free"),                     # 131K ctx
    (*_OR, "nvidia/nemotron-nano-9b-v2:free"),           # 128K ctx
]

# Vision OCR models — tried in order when a PDF has no text layer.
# Falls back to next model on rate limit or error.
VISION_MODELS = [
    (*_GV, "gemini-3-flash"),                                      # 1M ctx
    (*_GV, "gemini-2.5-flash"),                                    # 1M ctx
    (*_OR, "nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free"), # 300K
    (*_OR, "moonshotai/kimi-k2.6:free"),                         # 262K
    (*_OR, "google/gemma-4-31b-it:free"),                        # 256K
    (*_OR, "google/gemma-4-26b-a4b-it:free"),                    # 256K
    (*_OR, "nvidia/nemotron-nano-12b-v2-vl:free"),               # 128K
]

VISION_PROMPT = (
    "Extract all text from this wedding venue price list page. "
    "Preserve structure, package names, prices, and terms exactly as shown."
)


def find_pdfs(sources: list[str], reprocess: bool) -> list[Path]:
    pdfs = []
    for source in sources:
        price_lists_dir = DATA_DIR / source / "price-lists"
        if not price_lists_dir.exists():
            continue
        for pdf in sorted(price_lists_dir.rglob("*.pdf")):
            if reprocess or not pdf.with_suffix(".md").exists():
                pdfs.append(pdf)
    return pdfs


API_TIMEOUT = 120  # seconds per API call


def resolve_text_config() -> tuple[str, str, str, str]:
    """Return (label, env_key, base_url, model) for the first text model with a valid key."""
    for label, env_key, base_url, model in TEXT_MODELS:
        if os.getenv(env_key):
            return label, env_key, base_url, model
    keys = " or ".join(k for _, k, _, _ in TEXT_MODELS)
    print(f"Error: set {keys}", file=sys.stderr)
    sys.exit(1)


def _make_text_client(label: str, env_key: str, base_url: str, model: str) -> tuple[OpenAI, str, str, str]:
    """Create an OpenAI client for a text model config (thread-safe factory)."""
    return OpenAI(api_key=os.getenv(env_key), base_url=base_url, timeout=API_TIMEOUT), model, f"{label} ({model})", label


def resolve_vision_configs() -> list[tuple[str, str, str, str]]:
    """Return list of (label, env_key, base_url, model) for vision OCR, in fallback order."""
    configs = []
    for label, env_key, base_url, model in VISION_MODELS:
        if os.getenv(env_key):
            configs.append((label, env_key, base_url, model))
    return configs


def _make_vision_clients(
    configs: list[tuple[str, str, str, str]],
) -> list[tuple[OpenAI, str, str]]:
    """Create vision clients from configs (thread-safe factory)."""
    clients = []
    for label, env_key, base_url, model in configs:
        clients.append((
            OpenAI(api_key=os.getenv(env_key), base_url=base_url, timeout=API_TIMEOUT),
            model,
            f"{label} ({model})",
        ))
    return clients


def call_vision_api(client: OpenAI, model: str, label: str, b64: str) -> str:
    """Call vision API with exponential backoff on rate limit (3 attempts)."""
    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
                        {"type": "text", "text": VISION_PROMPT},
                    ],
                }],
            )
            return resp.choices[0].message.content
        except RateLimitError as e:
            if attempt == 2:
                raise
            wait = 30 * (2 ** attempt)  # 30s, then 60s
            print(f"  [rate limit] {label} — waiting {wait}s before retry {attempt + 2}/3: {e}", file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError("unreachable")


def vision_ocr(vision_clients: list[tuple[OpenAI, str, str]], pdf_path: Path) -> str:
    """Render each page as JPEG and OCR via vision models, falling back on error."""
    doc = fitz.open(str(pdf_path))
    page_count = len(doc)
    pages = []

    for i, page in enumerate(doc):
        print(f"  [vision OCR] page {i + 1}/{page_count}")
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
        b64 = base64.b64encode(pix.tobytes("jpeg", jpg_quality=85)).decode()
        print(f"  [vision OCR] page {i + 1} size: {len(b64) // 1024}KB (JPEG)")

        page_text = None
        for client, model, label in vision_clients:
            try:
                print(f"  [vision OCR] trying {label}")
                page_text = call_vision_api(client, model, label, b64)
                break
            except Exception as e:
                print(f"  [vision error] {label}: {type(e).__name__}: {e} — trying next", file=sys.stderr)

        if page_text is None:
            raise RuntimeError(f"all vision models failed on page {i + 1}/{page_count}")
        pages.append(page_text)

    doc.close()
    return "\n\n---\n\n".join(pages)


def _process_one(
    pdf_path: Path,
    text_config: tuple[str, str, str, str],
    vision_configs: list[tuple[str, str, str, str]],
    print_lock: threading.Lock,
    idx: int,
    total: int,
) -> bool:
    """Process a single PDF (called by worker threads). Returns True on success."""
    rel = pdf_path.relative_to(DATA_DIR)

    text_client, text_model, _, _ = _make_text_client(*text_config)
    converter = MarkItDown(llm_client=text_client, llm_model=text_model)
    vision_clients = _make_vision_clients(vision_configs)

    try:
        with print_lock:
            print(f"[{idx}/{total}] Processing: {rel}")

        result = converter.convert(str(pdf_path))
        text = result.text_content.strip()

        if not text:
            if not vision_clients:
                raise RuntimeError("image-based PDF but no vision API key configured")
            with print_lock:
                print(f"  [vision fallback] {rel} — no text layer")
            text = vision_ocr(vision_clients, pdf_path)

        pdf_path.with_suffix(".md").write_text(text, encoding="utf-8")
        with print_lock:
            print(f"  [{idx}/{total}] saved {pdf_path.with_suffix('.md').name}")
        return True

    except Exception as e:
        with print_lock:
            print(f"  [error] {rel}: {type(e).__name__}: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract PDF price lists to markdown")
    parser.add_argument("--source", choices=SOURCES, help="Process only this source")
    parser.add_argument("--limit", type=int, help="Max number of PDFs to process")
    parser.add_argument("--concurrency", "-c", type=int, default=4, help="Number of parallel workers (default: 4)")
    parser.add_argument("--dry-run", action="store_true", help="List PDFs without processing")
    parser.add_argument("--reprocess", action="store_true", help="Re-extract even if .md exists")
    args = parser.parse_args()

    sources = [args.source] if args.source else SOURCES
    pdfs = find_pdfs(sources, args.reprocess)
    if args.limit:
        pdfs = pdfs[: args.limit]

    print(f"Found {len(pdfs)} PDF(s) to process")

    if args.dry_run:
        for pdf in pdfs:
            print(f"  [dry-run] {pdf.relative_to(DATA_DIR)}")
        return

    text_config = resolve_text_config()
    print(f"Text model:   {text_config[0]} ({text_config[3]})")

    vision_configs = resolve_vision_configs()
    if vision_configs:
        for label, _, _, model in vision_configs:
            print(f"Vision model: {label} ({model})")
    else:
        print("Vision OCR:   disabled (no vision API keys)")

    print(f"Concurrency:  {args.concurrency} workers")

    print_lock = threading.Lock()
    total = len(pdfs)
    processed = failed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = {
            executor.submit(_process_one, pdf, text_config, vision_configs, print_lock, i, total): pdf
            for i, pdf in enumerate(pdfs, 1)
        }
        for future in concurrent.futures.as_completed(futures):
            if future.result():
                processed += 1
            else:
                failed += 1

    print(f"\nDone — processed: {processed}, failed: {failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
