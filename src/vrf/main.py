import argparse
import time

import httpx

from ..shared.config import get_random_user_agent
from ..shared.logging import get_logger
from ..shared.save import save_json_csv

logger = get_logger()

API_URL = "https://www.venuerific.com/api/webapp/v1/search.json"
IP_URL = "https://api.ipify.org"
REFERER = "https://www.venuerific.com/sg/search?search%5Bevent_types_supported%5D=Wedding&no_longtail=true&tab=wedding"
PAGE_DELAY = 2  # seconds between pages
MAX_RETRIES = 4


def _get_public_ip(client: httpx.Client) -> str:
    resp = client.get(IP_URL, params={"format": "json"}, timeout=10)
    resp.raise_for_status()
    return resp.json()["ip"]


def _fetch_page(client: httpx.Client, page: int, ip: str, ua: str) -> dict:
    params = {
        "country": "sg",
        "tracking[user_agent]": ua,
        "tracking[remote_ip]": ip,
        "tracking[referer]": REFERER,
        "search[event_types_supported]": "Wedding",
        "search[event_types_supported_parent]": "",
        "search[venue_name]": "",
        "search[venue_type]": "",
        "search[max_capacity]": "",
        "search[location]": "",
        "search[country]": "sg",
        "search[deals]": "",
        "search[cuisine_type]": "",
        "search[search_type]": "",
        "search[packages]": "",
        "search[super_venue]": "",
        "search[start_date]": "",
        "search[start_time]": "",
        "search[current_latitude]": "",
        "search[current_longitude]": "",
        "search[radius_km]": "",
        "no_longtail": "true",
        "page": str(page),
    }
    resp = client.get(API_URL, params=params, timeout=30)
    for attempt in range(MAX_RETRIES):
        if resp.status_code != 429:
            break
        wait = 2**attempt * 10
        logger.warning(f"Rate limited on page {page}, retrying in {wait}s...")
        time.sleep(wait)
        resp = client.get(API_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _transform(v: dict) -> dict:
    # place_name is always the venue name; name is the individual room name
    return {
        "name": v.get("place_name") or v.get("place_title") or v.get("name", ""),
        "url": v.get("place_link") or v.get("venue_url", ""),
        "slug": v.get("venue_slug", ""),
        "area": v.get("place_subtitle", ""),
        "address": v.get("place_address") or v.get("address", ""),
        "lat": v.get("place_location_lat"),
        "lng": v.get("place_location_lng"),
        "capacity_standing": v.get("max_no_of_guest_standing"),
        "capacity_seated": v.get("max_no_of_guest_sitting"),
        "rooms_count": v.get("rooms_count"),
        "price": v.get("venue_price") or v.get("place_price", ""),
        "price_unit": v.get("venue_price_unit", ""),
        "rating": v.get("reviews_score"),
        "reviews": v.get("reviews_total"),
        "mrt_station": v.get("nearby_station", ""),
        "event_types": v.get("event_types", []),
        "fast_response": v.get("fast_response", False),
        "response_time": v.get("response_time", ""),
    }


def _dedup_by_venue(rooms: list[dict]) -> list[dict]:
    """Collapse room-level entries to one record per venue slug."""
    seen: dict[str, dict] = {}
    for r in rooms:
        slug = r["slug"]
        if slug not in seen:
            seen[slug] = r
        else:
            # Keep highest capacity across rooms
            existing = seen[slug]
            if (r["capacity_standing"] or 0) > (existing["capacity_standing"] or 0):
                existing["capacity_standing"] = r["capacity_standing"]
            if (r["capacity_seated"] or 0) > (existing["capacity_seated"] or 0):
                existing["capacity_seated"] = r["capacity_seated"]
    return list(seen.values())


def scrape_venues(limit: int | None = None) -> list[dict]:
    ua = get_random_user_agent()
    headers = {
        "User-Agent": ua,
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": REFERER,
    }

    with httpx.Client(headers=headers, timeout=30) as client:
        ip = _get_public_ip(client)
        logger.info(f"Public IP: {ip}")

        data = _fetch_page(client, 1, ip, ua).get("data", {})
        total_pages = data.get("total_pages", 1)
        first_batch = data.get("venues", [])
        logger.info(f"Pages: {total_pages} (~{total_pages * len(first_batch)} venues)")

        all_venues = [_transform(v) for v in first_batch]

        for page in range(2, total_pages + 1):
            if limit and len(all_venues) >= limit:
                break
            time.sleep(PAGE_DELAY)
            logger.info(f"Page {page}/{total_pages} ({len(all_venues)} so far)...")
            batch = _fetch_page(client, page, ip, ua).get("data", {}).get("venues", [])
            if not batch:
                logger.warning(f"Empty page {page}, stopping early")
                break
            all_venues.extend(_transform(v) for v in batch)

    all_venues = _dedup_by_venue(all_venues)
    logger.info(f"Deduplicated to {len(all_venues)} unique venues")
    return all_venues[:limit] if limit else all_venues


def main():
    parser = argparse.ArgumentParser(description="Scrape wedding venues from Venuerific.com (Singapore)")
    parser.add_argument("--limit", type=int, default=None, help="Max venues (for testing)")
    parser.add_argument("--output", type=str, default="data/vrf/venues", help="Output path without extension")
    args = parser.parse_args()

    logger.info("Starting Venuerific scraper...")
    venues = scrape_venues(limit=args.limit)
    logger.info(f"Scraped {len(venues)} venues")

    save_json_csv(venues, args.output)
    logger.info(f"Saved to {args.output}.json / .csv")


if __name__ == "__main__":
    main()
