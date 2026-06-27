"""Enrich Telegram messages with URL content fetched via Jina Reader API.

Idempotent: caches fetched URLs so the same URL is never fetched twice.
Daily limit: caps the number of new Jina requests per calendar day (UTC).
"""

import argparse
import asyncio
import json
import time
from datetime import date
from pathlib import Path

import httpx

from ..shared.logging import get_logger

logger = get_logger()

JINA_BASE = "https://r.jina.ai"
DEFAULT_DAILY_LIMIT = 100
DEFAULT_DELAY = 1.0  # seconds between Jina requests


def load_json(path: Path) -> dict | list:
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_json(path: Path, data: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


async def fetch_url_content(client: httpx.AsyncClient, url: str) -> str:
    jina_url = f"{JINA_BASE}/{url}"
    try:
        response = await client.get(jina_url, timeout=30)
        response.raise_for_status()
        return response.text
    except httpx.HTTPStatusError as e:
        logger.warning(f"HTTP {e.response.status_code} for {url}")
        return ""
    except Exception as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return ""


async def enrich(
    data_dir: Path,
    daily_limit: int,
    delay: float,
    dry_run: bool,
) -> None:
    messages_path = data_dir / "messages.json"
    cache_path = data_dir / "url_cache.json"
    state_path = data_dir / "enrich_state.json"

    if not messages_path.exists():
        logger.error(f"messages.json not found at {messages_path}")
        return

    messages: list[dict] = load_json(messages_path)  # type: ignore[assignment]
    cache: dict[str, str] = load_json(cache_path)  # type: ignore[assignment]
    state: dict = load_json(state_path)  # type: ignore[assignment]

    today = str(date.today())
    requests_today = state.get("date") == today and state.get("count", 0) or 0
    remaining_budget = daily_limit - requests_today

    logger.info(f"Loaded {len(messages)} messages, {len(cache)} cached URLs")
    logger.info(f"Daily budget: {remaining_budget}/{daily_limit} remaining today ({today})")

    def is_fetchable(url: str) -> bool:
        return url.startswith("http://") or url.startswith("https://")

    # Collect all URLs that still need fetching (absolute URLs only)
    urls_needed: set[str] = set()
    for msg in messages:
        for link in msg.get("links") or []:
            if link and is_fetchable(link) and link not in cache:
                urls_needed.add(link)

    logger.info(f"URLs needing fetch: {len(urls_needed)}")

    if dry_run:
        logger.info("[dry-run] Would fetch these URLs:")
        for url in list(urls_needed)[:20]:
            logger.info(f"  {url}")
        if len(urls_needed) > 20:
            logger.info(f"  ... and {len(urls_needed) - 20} more")
        return

    if not urls_needed:
        logger.info("Nothing to fetch — populating url_contents from cache.")
    else:
        to_fetch = list(urls_needed)[:remaining_budget]
        skipped = len(urls_needed) - len(to_fetch)
        if skipped:
            logger.info(f"Daily limit reached after {len(to_fetch)} fetches; {skipped} URLs deferred to tomorrow")

        async with httpx.AsyncClient(
            headers={"Accept": "text/plain", "X-Return-Format": "markdown"},
            follow_redirects=True,
        ) as client:
            for i, url in enumerate(to_fetch, 1):
                logger.info(f"[{i}/{len(to_fetch)}] Fetching: {url}")
                content = await fetch_url_content(client, url)
                cache[url] = content
                requests_today += 1

                if i < len(to_fetch):
                    time.sleep(delay)

        # Persist cache and state after all fetches
        save_json(cache_path, cache)
        save_json(state_path, {"date": today, "count": requests_today})
        logger.info(f"Cache saved ({len(cache)} entries). Requests today: {requests_today}")

    # Populate url_contents on every message from cache (covers newly fetched + already cached)
    updated = 0
    for msg in messages:
        links = msg.get("links") or []
        contents = [cache.get(link, "") if is_fetchable(link) else "" for link in links]
        if contents != msg.get("url_contents"):
            msg["url_contents"] = contents
            updated += 1

    save_json(messages_path, messages)
    logger.info(f"Updated url_contents on {updated} messages. Saved to {messages_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich Telegram messages with Jina Reader URL content")
    parser.add_argument("--daily-limit", type=int, default=DEFAULT_DAILY_LIMIT, help="Max Jina requests per day (across all channels)")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Seconds between requests")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fetched without making requests")
    args = parser.parse_args()

    channels_path = Path(__file__).parent / "channels.json"
    with open(channels_path, encoding="utf-8") as f:
        channels: list[str] = json.load(f)

    for channel in channels:
        asyncio.run(enrich(Path(f"data/tg/{channel}"), args.daily_limit, args.delay, args.dry_run))


if __name__ == "__main__":
    main()
