import argparse
import asyncio
import re
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from bs4.element import Tag

from ..shared.config import get_headers
from ..shared.logging import get_logger
from ..shared.save import save_json_csv

logger = get_logger()

BASE_URL = "https://www.weddingvenue.sg"
LISTING_PATH = "/wedding-venues-singapore"
LISTING_LINK_RE = re.compile(rf"^{LISTING_PATH}/[a-z0-9-]+/?$")
PAX_RE = re.compile(r"(NA|\d+)\s*-\s*(\d+)\s*pax")
TAG_RE = re.compile(r"<[^>]+>")


def _clean_pax_text(text: str) -> str:
    """Strip the literal '<span>NA</span>' markup that the site renders as text."""
    return TAG_RE.sub("", text)


def _slugify(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")


def parse_listing_page(html: str) -> tuple[list[dict], int, int]:
    """Parse a venue listing page into summary records, plus (current_page, total_pages)."""
    soup = BeautifulSoup(html, "html.parser")
    venues = []

    for link in soup.find_all("a", href=LISTING_LINK_RE):
        h2 = link.find("h2")
        if not h2:
            # Skip non-card links to the same venue (e.g. comparison table "View Details" buttons)
            continue

        href = str(link["href"])
        slug = href.rstrip("/").split("/")[-1]
        name = h2.get_text(strip=True)

        paragraphs = link.find_all("p")
        pax_min = pax_max = None
        address = None
        headline_price = None

        if len(paragraphs) >= 1:
            pax_match = PAX_RE.search(_clean_pax_text(paragraphs[0].get_text()))
            if pax_match:
                pax_min = None if pax_match.group(1) == "NA" else int(pax_match.group(1))
                pax_max = int(pax_match.group(2))
        if len(paragraphs) >= 2:
            address = paragraphs[1].get_text(strip=True)
        if len(paragraphs) >= 3:
            headline_price = paragraphs[2].get_text(strip=True)

        img = link.find("img")
        image_url = str(img.get("src")) if img and img.get("src") else None

        venues.append(
            {
                "slug": slug,
                "name": name,
                "url": urljoin(BASE_URL, href),
                "address": address,
                "pax_min": pax_min,
                "pax_max": pax_max,
                "headline_price": headline_price,
                "image_url": image_url,
            }
        )

    current_page, total_pages = 1, 1
    nav = soup.find("nav", attrs={"aria-label": "Venue listing pagination"})
    if nav:
        page_match = re.search(r"Page\s+(\d+)\s+of\s+(\d+)", nav.get_text(" ", strip=True))
        if page_match:
            current_page, total_pages = int(page_match.group(1)), int(page_match.group(2))

    return venues, current_page, total_pages


def parse_highlights(soup: BeautifulSoup) -> dict:
    """Parse the 'Venue Highlights' card grid into a flat dict."""
    heading = next((h for h in soup.find_all("h2") if h.get_text(strip=True) == "Venue Highlights"), None)
    if not heading:
        return {}

    container = heading.find_next_sibling("div")
    if not container:
        return {}

    highlights = {}
    for card in container.select("div.flex.flex-col.items-center"):
        spans = card.find_all("span")
        if len(spans) < 2:
            continue
        label = spans[0].get_text(strip=True)
        value = spans[1].get_text(strip=True)
        if label:
            highlights[_slugify(label)] = value

    return highlights


def _parse_package(pkg: Tag) -> dict:
    package = {}

    left = pkg.find("div", class_="min-w-0")
    if left:
        day_p = left.find("p", class_="text-sm")
        if day_p:
            package["day"] = str(day_p.contents[0]).strip() if day_p.contents else None
            meal_span = day_p.find("span")
            if meal_span:
                package["meal"] = meal_span.get_text(strip=True).lstrip("·").strip()

        name_p = left.find("p", class_="truncate")
        if name_p:
            package["package_name"] = name_p.get_text(strip=True)

        capacity_div = left.find("div", class_="shrink-0")
        capacity_span = capacity_div.find("span") if capacity_div else None
        if capacity_span:
            package["capacity"] = capacity_span.get_text(strip=True)

    right = pkg.find("div", class_="text-right")
    if right:
        price_p = right.find("p", class_="text-primary")
        if price_p:
            package["price_per_pax"] = price_p.get_text(strip=True)
        total_p = right.find("p", class_="text-gray-500")
        if total_p:
            package["total_price"] = total_p.get_text(strip=True)

    return package


def parse_rooms(soup: BeautifulSoup) -> list[dict]:
    """Parse the 'Room Packages' section into a list of rooms with their visible packages.

    Note: the page truncates each room's package list behind a "View More" button
    that fetches additional packages client-side, so only the initially rendered
    packages are captured here.
    """
    grid = None
    for heading in soup.find_all("h2"):
        if heading.get_text(strip=True) != "Room Packages":
            continue
        sibling = heading.find_next_sibling("div")
        if sibling and "grid" in (sibling.get("class") or []):
            grid = sibling
            break

    if not grid:
        return []

    rooms = []
    for card in grid.find_all("div", recursive=False):
        h3 = card.find("h3")
        room_name = h3.get_text(strip=True) if h3 else None

        tags = [span.get_text(strip=True) for span in card.find_all("span", class_="bg-[#FFF7E5]")]

        packages = [_parse_package(pkg) for pkg in card.find_all("div", class_="border-primary")]

        rooms.append({"room_name": room_name, "tags": tags, "packages": packages})

    return rooms


def parse_venue_detail(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    description = None
    about_heading = next((h for h in soup.find_all("h2") if h.get_text(strip=True) == "About This Venue"), None)
    if about_heading:
        desc_p = about_heading.find_next_sibling("p")
        if desc_p:
            description = desc_p.get_text(strip=True)

    return {
        "description": description,
        "highlights": parse_highlights(soup),
        "rooms": parse_rooms(soup),
    }


async def fetch_all_listings(client: httpx.AsyncClient, limit: int | None = None) -> list[dict]:
    """Fetch and dedupe venue summaries across all listing pages."""
    seen = {}
    page = 1

    while True:
        url = f"{BASE_URL}{LISTING_PATH}"
        params = {"page": page} if page > 1 else None
        logger.info(f"Fetching listing page {page}: {url}")

        response = await client.get(url, params=params)
        response.raise_for_status()

        venues, current_page, total_pages = parse_listing_page(response.text)
        if not venues:
            logger.info(f"No venues found on page {page}, stopping pagination")
            break

        for venue in venues:
            seen[venue["slug"]] = venue

        logger.info(f"Page {current_page}/{total_pages}: {len(venues)} venues (total unique: {len(seen)})")

        if limit and len(seen) >= limit:
            break
        if current_page >= total_pages:
            break

        page += 1

    result = list(seen.values())
    if limit:
        result = result[:limit]
    return result


async def fetch_venue_detail(client: httpx.AsyncClient, slug: str) -> dict:
    url = f"{BASE_URL}{LISTING_PATH}/{slug}"
    try:
        response = await client.get(url)
        response.raise_for_status()
        return parse_venue_detail(response.text)
    except Exception as e:
        logger.error(f"  ⚠️  Error fetching details for venue '{slug}': {e}")
        return {}


async def extract_all_venues_async(limit: int | None = None, concurrency: int = 5) -> list[dict]:
    async with httpx.AsyncClient(headers=get_headers(), timeout=30, follow_redirects=True) as client:
        summaries = await fetch_all_listings(client, limit=limit)
        logger.info(f"\nFetching details for {len(summaries)} venues...")

        semaphore = asyncio.Semaphore(concurrency)

        async def fetch_with_semaphore(venue: dict) -> dict:
            async with semaphore:
                detail = await fetch_venue_detail(client, venue["slug"])
                merged = {**venue}
                merged.update({k: v for k, v in detail.items() if v})
                return merged

        venues = await asyncio.gather(*(fetch_with_semaphore(v) for v in summaries))

    venues.sort(key=lambda v: (v.get("name") or "").lower())
    return venues


def extract_all_venues(limit: int | None = None, concurrency: int = 5) -> list[dict]:
    """Synchronous wrapper for extract_all_venues_async"""
    return asyncio.run(extract_all_venues_async(limit=limit, concurrency=concurrency))


def main():
    parser = argparse.ArgumentParser(description="Extract WeddingVenue.sg wedding venue data")
    parser.add_argument("--output", type=str, default="data/wv", help="Output directory")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of venues (for testing)")
    parser.add_argument("--concurrency", type=int, default=5, help="Concurrent detail page requests")

    args = parser.parse_args()

    logger.info("\n" + "=" * 60)
    logger.info("EXTRACTING WEDDINGVENUE.SG")
    logger.info("=" * 60 + "\n")

    venues = extract_all_venues(limit=args.limit, concurrency=args.concurrency)

    save_json_csv(venues, f"{args.output}/venues")

    if venues:
        logger.info("\n✅ Extraction complete!")
        logger.info(f"\n📊 Total venues: {len(venues)}")

        logger.info("\nSample venues:")
        for idx, venue in enumerate(venues[:5], 1):
            logger.info(f"\n{idx}. {venue.get('name', 'N/A')}")
            logger.info(f"   Address: {venue.get('address', 'N/A')}")
            logger.info(f"   Pax: {venue.get('pax_min', 'NA')}-{venue.get('pax_max', 'N/A')}")
            logger.info(f"   Headline price: {venue.get('headline_price', 'N/A')}")
            logger.info(f"   Rooms: {len(venue.get('rooms', []))}")


if __name__ == "__main__":
    main()
