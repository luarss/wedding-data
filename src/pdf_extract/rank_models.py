"""
Fetch 7-day token usage from OpenRouter rankings API and print
TEXT_MODELS / VISION_MODELS sorted by popularity.

Usage:
    OPENROUTER_API_KEY=ogw_... uv run python -m src.pdf_extract.rank_models
"""

import os
import sys
from datetime import date, timedelta

import httpx
from dotenv import load_dotenv

from src.pdf_extract.main import TEXT_MODELS, VISION_MODELS

load_dotenv()

RANKINGS_URL = "https://openrouter.ai/api/v1/datasets/rankings-daily"


def fetch_usage(api_key: str, days: int = 7) -> dict[str, int]:
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    resp = httpx.get(
        RANKINGS_URL,
        headers={"Authorization": f"Bearer {api_key}"},
        params={"start_date": str(start), "end_date": str(end)},
        timeout=15,
    )
    resp.raise_for_status()
    totals: dict[str, int] = {}
    for row in resp.json()["data"]:
        slug = row["model_permaslug"]
        totals[slug] = totals.get(slug, 0) + row["total_tokens"]
    return totals


def ranked(models: list[tuple], usage: dict[str, int]) -> list[tuple]:
    def key(m: tuple) -> int:
        slug = m[3].removesuffix(":free")
        return -(usage.get(m[3], 0) or usage.get(slug, 0))
    return sorted(models, key=key)


def print_ranked(name: str, models: list[tuple], usage: dict[str, int]) -> None:
    print(f"\n{name}:")
    for _label, _env_key, _base_url, model in ranked(models, usage):
        tokens = usage.get(model, usage.get(model.removesuffix(":free"), 0))
        rank_str = f"{tokens / 1e9:.1f}B tokens" if tokens else "not in top-50"
        print(f"  {model:<55} {rank_str}")


def main() -> None:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        print("Error: set OPENROUTER_API_KEY", file=sys.stderr)
        sys.exit(1)

    print("Fetching 7-day rankings from OpenRouter…")
    usage = fetch_usage(api_key)

    print_ranked("TEXT_MODELS (suggested order)", TEXT_MODELS, usage)
    print_ranked("VISION_MODELS (suggested order)", VISION_MODELS, usage)
    print("\nPaste the suggested order into TEXT_MODELS / VISION_MODELS in main.py.")


if __name__ == "__main__":
    main()
