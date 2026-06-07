"""
PDF to markdown extraction using markitdown + LLM (OpenGateway/minimax-m3 primary, OpenRouter fallback).
Image-based PDFs (no text layer) fall back to page-by-page vision OCR via OpenRouter/Gemma-4.

Usage:
    python -m src.pdf_extract.main [--source bb|wd|sb] [--limit N] [--dry-run] [--reprocess]
"""

import argparse
import base64
import os
import sys
import traceback
from pathlib import Path

import fitz  # pymupdf
from dotenv import load_dotenv
from markitdown import MarkItDown
from openai import OpenAI

load_dotenv()

DATA_DIR = Path(__file__).parent.parent.parent / "data"
SOURCES = ["bb", "wd", "sb"]

OPENGATEWAY_BASE_URL = "https://opengateway.gitlawb.com/v1"
OPENGATEWAY_MODEL = "minimax/minimax-m3"

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
OPENROUTER_TEXT_MODEL = "openrouter/owl-alpha"
# Gemma 4 31B: confirmed multimodal vision support, used exclusively for vision OCR fallback
OPENROUTER_VISION_MODEL = "google/gemma-4-31b-it:free"

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
            md_path = pdf.with_suffix(".md")
            if reprocess or not md_path.exists():
                pdfs.append(pdf)
    return pdfs


def resolve_llm() -> tuple[OpenAI, str, str]:
    """Return (client, model, provider_label) based on available API keys."""
    opengateway_key = os.getenv("OPENGATEWAY_API_KEY")
    openrouter_key = os.getenv("OPENROUTER_API_KEY")

    if opengateway_key:
        return (
            OpenAI(api_key=opengateway_key, base_url=OPENGATEWAY_BASE_URL),
            OPENGATEWAY_MODEL,
            f"OpenGateway ({OPENGATEWAY_MODEL})",
        )
    if openrouter_key:
        return (
            OpenAI(api_key=openrouter_key, base_url=OPENROUTER_BASE_URL),
            OPENROUTER_TEXT_MODEL,
            f"OpenRouter ({OPENROUTER_TEXT_MODEL})",
        )

    print("Error: set OPENGATEWAY_API_KEY or OPENROUTER_API_KEY", file=sys.stderr)
    sys.exit(1)


def resolve_vision_client() -> tuple[OpenAI, str] | None:
    """Return (client, model) for vision OCR fallback, or None if key not set.

    Uses OpenRouter/Gemma-4-31B — a confirmed multimodal model.
    Owl Alpha was tried but returned 404 'No endpoints found that support image input'.
    minimax-m3 on OpenGateway also rejects image payloads.
    """
    openrouter_key = os.getenv("OPENROUTER_API_KEY")
    if not openrouter_key:
        return None
    return OpenAI(api_key=openrouter_key, base_url=OPENROUTER_BASE_URL), OPENROUTER_VISION_MODEL


def vision_ocr(client: OpenAI, model: str, pdf_path: Path) -> str:
    """Render each page as PNG and extract text via vision LLM."""
    doc = fitz.open(str(pdf_path))
    page_count = len(doc)
    pages = []
    for i, page in enumerate(doc):
        print(f"  [vision OCR] page {i + 1}/{page_count} via {model}")
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
        b64 = base64.b64encode(pix.tobytes("png")).decode()
        print(f"  [vision OCR] page {i + 1} image size: {len(b64) // 1024}KB (base64)")
        resp = client.chat.completions.create(
            model=model,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}},
                    {"type": "text", "text": VISION_PROMPT},
                ],
            }],
        )
        pages.append(resp.choices[0].message.content)
    doc.close()
    return "\n\n---\n\n".join(pages)


def extract_pdf(
    converter: MarkItDown,
    vision: tuple[OpenAI, str] | None,
    pdf_path: Path,
) -> str:
    result = converter.convert(str(pdf_path))
    text = result.text_content.strip()
    if not text:
        if vision is None:
            raise RuntimeError("image-based PDF but OPENROUTER_API_KEY not set for vision fallback")
        vision_client, vision_model = vision
        print(f"  [vision fallback] no text layer — OCR via {vision_model}")
        text = vision_ocr(vision_client, vision_model, pdf_path)
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

    client, model, provider_label = resolve_llm()
    print(f"Using: {provider_label}")
    converter = MarkItDown(llm_client=client, llm_model=model)
    vision = resolve_vision_client()
    if vision:
        print(f"Vision fallback: OpenRouter ({OPENROUTER_VISION_MODEL})")
    else:
        print("Vision fallback: disabled (no OPENROUTER_API_KEY)", file=sys.stderr)

    processed = failed = 0
    for pdf in pdfs:
        rel = pdf.relative_to(DATA_DIR)
        try:
            print(f"Processing: {rel}")
            text = extract_pdf(converter, vision, pdf_path=pdf)
            md_path = pdf.with_suffix(".md")
            md_path.write_text(text, encoding="utf-8")
            print(f"  -> saved {md_path.name}")
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
