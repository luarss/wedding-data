"""
PDF to markdown extraction using markitdown + LLM (OpenGateway/minimax-m3 primary, OpenRouter fallback).

Usage:
    python -m src.pdf_extract.main [--source bb|wd|sb] [--limit N] [--dry-run] [--reprocess]
"""

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from markitdown import MarkItDown
from openai import OpenAI

load_dotenv()

DATA_DIR = Path(__file__).parent.parent.parent / "data"
SOURCES = ["bb", "wd", "sb"]

OPENGATEWAY_BASE_URL = "https://opengateway.gitlawb.com/v1"
OPENGATEWAY_MODEL = "minimax/minimax-m3"

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
# Owl Alpha: free, 1M context, native PDF + image support, strong agentic/tool use
OPENROUTER_MODEL = "openrouter/owl-alpha"


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


def resolve_converter() -> tuple[MarkItDown, str]:
    """Return a configured MarkItDown instance and a label for logging."""
    opengateway_key = os.getenv("OPENGATEWAY_API_KEY")
    openrouter_key = os.getenv("OPENROUTER_API_KEY")

    if opengateway_key:
        client = OpenAI(api_key=opengateway_key, base_url=OPENGATEWAY_BASE_URL)
        return MarkItDown(llm_client=client, llm_model=OPENGATEWAY_MODEL), f"OpenGateway ({OPENGATEWAY_MODEL})"

    if openrouter_key:
        client = OpenAI(api_key=openrouter_key, base_url=OPENROUTER_BASE_URL)
        return MarkItDown(llm_client=client, llm_model=OPENROUTER_MODEL), f"OpenRouter ({OPENROUTER_MODEL})"

    print("Error: set OPENGATEWAY_API_KEY or OPENROUTER_API_KEY", file=sys.stderr)
    sys.exit(1)


def extract_pdf(converter: MarkItDown, pdf_path: Path) -> str:
    result = converter.convert(str(pdf_path))
    return result.text_content


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

    converter, provider_label = resolve_converter()
    print(f"Using: {provider_label}")

    processed = failed = 0
    for pdf in pdfs:
        rel = pdf.relative_to(DATA_DIR)
        try:
            print(f"Processing: {rel}")
            text = extract_pdf(converter, pdf)
            md_path = pdf.with_suffix(".md")
            md_path.write_text(text, encoding="utf-8")
            print(f"  -> saved {md_path.name}")
            processed += 1
        except Exception as e:
            print(f"  [error] {rel}: {e}", file=sys.stderr)
            failed += 1

    print(f"\nDone — processed: {processed}, failed: {failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
