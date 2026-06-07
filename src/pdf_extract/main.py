"""
PDF to markdown extraction pipeline.

Text-layer PDFs: markitdown + text LLM (pdfminer does the work, LLM for embedded images).
Image-based PDFs: page-by-page vision OCR via vision LLM chain.

Usage:
    python -m src.pdf_extract.main [--source bb|wd|sb] [--limit N] [--dry-run] [--reprocess]
"""

import argparse
import base64
import os
import sys
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

# Text models — tried in order, first with a valid API key wins.
# Used by markitdown for LLM-enhanced extraction (e.g. embedded image descriptions).
_OR = ("OpenRouter", "OPENROUTER_API_KEY", OPENROUTER_BASE_URL)
TEXT_MODELS = [
    ("OpenGateway", "OPENGATEWAY_API_KEY", OPENGATEWAY_BASE_URL, "minimax/minimax-m3"),
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


def resolve_text_client() -> tuple[OpenAI, str, str]:
    """Return (client, model, label) for the first text model with a valid key."""
    for label, env_key, base_url, model in TEXT_MODELS:
        api_key = os.getenv(env_key)
        if api_key:
            return OpenAI(api_key=api_key, base_url=base_url), model, f"{label} ({model})"
    keys = " or ".join(k for _, k, _, _ in TEXT_MODELS)
    print(f"Error: set {keys}", file=sys.stderr)
    sys.exit(1)


def resolve_vision_clients() -> list[tuple[OpenAI, str, str]]:
    """Return list of (client, model, label) for vision OCR, in fallback order."""
    clients = []
    for label, env_key, base_url, model in VISION_MODELS:
        api_key = os.getenv(env_key)
        if api_key:
            clients.append((OpenAI(api_key=api_key, base_url=base_url), model, f"{label} ({model})"))
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


def extract_pdf(
    converter: MarkItDown,
    vision_clients: list[tuple[OpenAI, str, str]],
    pdf_path: Path,
) -> str:
    result = converter.convert(str(pdf_path))
    text = result.text_content.strip()
    if not text:
        if not vision_clients:
            raise RuntimeError("image-based PDF but no vision API key configured")
        print("  [vision fallback] no text layer detected")
        text = vision_ocr(vision_clients, pdf_path)
    return text


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract PDF price lists to markdown")
    parser.add_argument("--source", choices=SOURCES, help="Process only this source")
    parser.add_argument("--limit", type=int, help="Max number of PDFs to process")
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

    client, model, text_label = resolve_text_client()
    print(f"Text model:   {text_label}")
    converter = MarkItDown(llm_client=client, llm_model=model)

    vision_clients = resolve_vision_clients()
    if vision_clients:
        for _, _, label in vision_clients:
            print(f"Vision model: {label}")
    else:
        print("Vision OCR:   disabled (no OPENROUTER_API_KEY)", file=sys.stderr)

    processed = failed = 0
    for pdf in pdfs:
        rel = pdf.relative_to(DATA_DIR)
        try:
            print(f"Processing: {rel}")
            text = extract_pdf(converter, vision_clients, pdf_path=pdf)
            pdf.with_suffix(".md").write_text(text, encoding="utf-8")
            print(f"  -> saved {pdf.with_suffix('.md').name}")
            processed += 1
        except Exception as e:
            print(f"  [error] {rel}: {type(e).__name__}: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            failed += 1

    print(f"\nDone — processed: {processed}, failed: {failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
