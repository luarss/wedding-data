import asyncio
import csv
import json
import re
import time

import httpx
from playwright.async_api import async_playwright

from ..shared.config import get_headers
from ..shared.logging import get_logger
from ..shared.save import save_json

logger = get_logger()

BASE_URL = "https://theweddingnotebook.com/api/v1/listings"
PAGE_BASE_URL = "https://theweddingnotebook.com/catalog/venues"
DETAIL_DELAY_BETWEEN = 5  # seconds between processing each venue

# Realistic UA to avoid Vercel Challenge detection
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def scrape_venues(category="venues", state=None, limit=None):
    """
    Scrape venues from TheWeddingNotebook.com, including detail pages.

    Args:
        category: "venues" (only category currently supported)
        state: Filter by state (e.g., "Selangor", "Kuala Lumpur")
        limit: Max number of venues to scrape

    Returns:
        List of venue dictionaries with detail fields merged in
    """
    listings = _fetch_all_listings(category=category, state=state, limit=limit)
    logger.info(f"Fetching details for {len(listings)} venues...")
    detailed = asyncio.run(_fetch_all_details(listings))
    return detailed


def _fetch_all_listings(category, state, limit):
    all_listings = []
    page = 1

    with httpx.Client(headers=get_headers(), timeout=30) as client:
        while True:
            params = {"category": category, "page": page, "limit": 50}
            if state:
                params["state"] = state

            response = client.get(BASE_URL, params=params)
            for attempt in range(6):
                if response.status_code != 429:
                    break
                wait = 2**attempt * 10
                logger.info(f"Rate limited (429) on page {page}, retrying in {wait}s ({attempt + 1}/6)...")
                time.sleep(wait)
                response = client.get(BASE_URL, params=params)
            if response.status_code == 429:
                raise RuntimeError(f"Still rate-limited on page {page} after 6 retries")
            response.raise_for_status()
            data = response.json()

            listings = data["data"]
            pagination = data["pagination"]
            total = pagination["total"]
            total_pages = pagination["totalPages"]

            all_listings.extend(listings)
            logger.info(f"Page {page}/{total_pages}: Got {len(listings)} venues (total: {len(all_listings)}/{total})")

            if limit and len(all_listings) >= limit:
                all_listings = all_listings[:limit]
                break
            if page >= total_pages:
                break

            page += 1

    return all_listings


def _parse_spaces_and_packages(html: str) -> dict:
    """
    Extract spaces and packages from Next.js RSC flight payload embedded in SSR HTML.
    The public API returns venueDetails=null; this data is only available via SSR.
    """
    chunks = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html, re.DOTALL)
    combined = "".join(chunks)
    try:
        decoded = bytes(combined, "utf-8").decode("unicode_escape")
    except Exception:
        decoded = combined

    spaces = []
    for m in re.finditer(
        r'"id":"([0-9a-f-]{36})","venueId":"[0-9a-f-]{36}"'
        r',"name":"([^"]+)","type":"([^"]+)","indoorOutdoor":"([^"]+)"'
        r'[^}]*?"area":(\d+|null)'
        r'[^}]*?"capacitySeatedMin":(\d+|null),"capacitySeatedMax":(\d+|null)',
        decoded,
    ):
        spaces.append(
            {
                "id": m.group(1),
                "name": m.group(2).strip(),
                "type": m.group(3),
                "indoorOutdoor": m.group(4),
                "area": int(m.group(5)) if m.group(5) != "null" else None,
                "capacitySeatedMin": int(m.group(6)) if m.group(6) != "null" else None,
                "capacitySeatedMax": int(m.group(7)) if m.group(7) != "null" else None,
            }
        )

    packages = []
    pkg_match = re.search(r'"packages":\[(\{.*?\}(?:,\{.*?\})*)\]', decoded)
    if pkg_match:
        try:
            packages = json.loads(f"[{pkg_match.group(1)}]")
            # $undefined is a React serialization artifact — replace with None
            for pkg in packages:
                for k, v in pkg.items():
                    if v == "$undefined":
                        pkg[k] = None
        except json.JSONDecodeError:
            pass

    venue_capacity = {}
    cap_match = re.search(r'"venueCapacity":\{"min":(\d+),"max":(\d+)\}', decoded)
    if cap_match:
        venue_capacity = {"min": int(cap_match.group(1)), "max": int(cap_match.group(2))}

    return {"spaces": spaces, "packages": packages, "venueCapacity": venue_capacity}


async def _retry_api_get(client, url, max_retries=5, base_wait=5):
    """GET an API URL with exponential backoff retry on 429 and transient errors."""
    last_error = None
    for attempt in range(max_retries):
        try:
            resp = await client.get(url)
            if resp.status_code == 429:
                wait = 2**attempt * base_wait
                logger.info(f"  Rate limited (429) on API, retrying in {wait}s ({attempt + 1}/{max_retries})...")
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                wait = 2**attempt * base_wait
                logger.info(f"  Rate limited (429) on API, retrying in {wait}s ({attempt + 1}/{max_retries})...")
                await asyncio.sleep(wait)
                continue
            raise
        except (httpx.ConnectError, httpx.ReadError, httpx.TimeoutException) as e:
            last_error = e
            wait = 2**attempt * base_wait
            logger.warning(f"  Network error on API, retrying in {wait}s ({attempt + 1}/{max_retries}): {e}")
            await asyncio.sleep(wait)
            continue
    raise last_error or RuntimeError(f"All {max_retries} retries exhausted for {url}")


async def _fetch_detail(client, page, listing):
    """Fetch detail API data via httpx and catalog page data via Playwright."""
    try:
        api_resp = await _retry_api_get(client, f"{BASE_URL}/{listing['id']}")
        detail = api_resp.json().get("data", {})

        resp = await page.goto(f"{PAGE_BASE_URL}/{listing['slug']}", wait_until="domcontentloaded", timeout=30000)
        if resp and resp.status == 429:
            await asyncio.sleep(15)
            await page.wait_for_load_state("networkidle", timeout=15000)
        page_data = _parse_spaces_and_packages(await page.content())

        return {**listing, **detail, **page_data}
    except Exception as e:
        logger.warning(f"Warning: failed to fetch detail for {listing['name']}: {e}")
        return listing


async def _fetch_all_details(listings):
    results = []
    async with httpx.AsyncClient(headers=get_headers(), timeout=30) as client, \
               async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            context = await browser.new_context(user_agent=BROWSER_UA)
            page = await context.new_page()
            await page.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            for i, listing in enumerate(listings):
                if i > 0:
                    await asyncio.sleep(DETAIL_DELAY_BETWEEN)
                logger.info(f"  [{i + 1}/{len(listings)}] Fetching {listing['name']}...")
                result = await _fetch_detail(client, page, listing)
                results.append(result)
        finally:
            await browser.close()
    return results


def save_venues(venues, filename="data/twn/venues"):
    """
    Save venues to JSON and CSV.

    Args:
        venues: List of venue dicts from scrape_venues()
        filename: Output filename (without extension)
    """
    import os

    os.makedirs(os.path.dirname(filename), exist_ok=True)

    # Save JSON
    save_json(venues, f"{filename}.json")

    # Save CSV — flatten venueCapacity and first package into columns
    if venues:
        base_keys = ["id", "name", "slug", "vendorType", "state", "city", "address", "postCode"]
        extra_keys = ["capacityMin", "capacityMax", "packagePrice", "packageGuestMin", "packageGuestMax"]

        with open(f"{filename}.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(base_keys + extra_keys)
            for v in venues:
                row = [v.get(k, "") for k in base_keys]
                cap = v.get("venueCapacity") or {}
                pkg = (v.get("packages") or [{}])[0]
                row += [
                    cap.get("min", ""),
                    cap.get("max", ""),
                    pkg.get("price", ""),
                    pkg.get("guestMin", ""),
                    pkg.get("guestMax", ""),
                ]
                writer.writerow(row)

    logger.info(f"Saved {len(venues)} venues to {filename}.json and {filename}.csv")


def main():
    """CLI entry point"""
    import sys

    args = sys.argv[1:]
    state = None
    limit = None
    output = "data/twn/venues"

    for i, arg in enumerate(args):
        if arg == "--state" and i + 1 < len(args):
            state = args[i + 1]
        elif arg == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])
        elif arg == "--output" and i + 1 < len(args):
            output = args[i + 1]

    logger.info(f"Scraping venues{f' in {state}' if state else ''}...")
    venues = scrape_venues(state=state, limit=limit)

    save_venues(venues, output)

    logger.info(f"\nScraped {len(venues)} venues")
    if venues:
        states = {}
        for v in venues:
            s = v.get("state", "Unknown")
            states[s] = states.get(s, 0) + 1
        logger.info("\nBy state:")
        for s, count in sorted(states.items(), key=lambda x: -x[1]):
            logger.info(f"  {s}: {count}")


if __name__ == "__main__":
    main()
