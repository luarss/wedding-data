import argparse
import time

import httpx

from ..shared.config import get_random_user_agent
from ..shared.logging import get_logger
from ..shared.save import save_json_csv

logger = get_logger()

SEARCH_PAGE = "https://www.tagvenue.com/sg/search/wedding?location_id=12"
API_URL = "https://www.tagvenue.com/ajax/search-list"
PAGE_DELAY = 2  # seconds between pages
MAX_RETRIES = 4
ITEMS_PER_PAGE = 50

# Singapore bounding box (same as Tagvenue's defaults)
SG_BOUNDS = {
    "latitude_from": 1.040925,
    "latitude_to": 1.566375,
    "longitude_from": 103.4251,
    "longitude_to": 104.2747,
}


def _init_session(ua: str) -> httpx.Client:
    """Create a session with cookies from the search page."""
    client = httpx.Client(
        headers={
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        follow_redirects=True,
        timeout=30,
    )
    resp = client.get(SEARCH_PAGE)
    resp.raise_for_status()
    logger.info(f"Session initialized, cookies: {list(client.cookies.keys())}")
    return client


def _fetch_page(client: httpx.Client, page: int, form_timestamp: int) -> dict:
    params = {
        "room_tag": "wedding",
        "form_timestamp": form_timestamp,
        "getAllRoomsPositions": "true",
        "items_per_page": ITEMS_PER_PAGE,
        "iso_country_code": "SG",
        "neighbourhood": "Singapore",
        **SG_BOUNDS,
        "page": page,
        "REAL_SEARCH": "true",
    }
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": SEARCH_PAGE,
    }
    resp = client.get(API_URL, params=params, headers=headers)
    for attempt in range(MAX_RETRIES):
        if resp.status_code != 429:
            break
        wait = 2**attempt * 10
        logger.warning(f"Rate limited on page {page}, retrying in {wait}s...")
        time.sleep(wait)
        resp = client.get(API_URL, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json()


def _transform(room: dict) -> dict:
    """Map API room fields to normalized schema."""
    pos = room.get("position") or {}
    min_pricing = room.get("min_pricing_info") or {}

    # Extract lowest per-person price if available
    per_person = min_pricing.get("lowest_per_person_total") or {}
    per_person_price = per_person.get("price")
    per_person_type = per_person.get("type", "")

    return {
        "venue_name": room.get("venue_name", ""),
        "room_name": room.get("room_name", ""),
        "url": room.get("room_url", ""),
        "venue_url": room.get("venue_url", ""),
        "venue_id": room.get("venue_id"),
        "room_id": room.get("room_id"),
        "area": room.get("geo_project_location", ""),
        "address": room.get("address_street_address", ""),
        "city": room.get("address_city", ""),
        "postcode": room.get("address_postcode", ""),
        "lat": pos.get("latitude"),
        "lng": pos.get("longitude"),
        "capacity_standing": room.get("room_standing_capacity"),
        "capacity_seated": room.get("room_seating_capacity"),
        "min_attendees": room.get("room_min_attendees_per_event"),
        "price": room.get("room_price", ""),
        "price_method": room.get("room_price_method", ""),
        "price_type": room.get("room_price_type", ""),
        "price_per_person": per_person_price,
        "price_per_person_type": per_person_type,
        "rating": room.get("room_rating_decimal") or room.get("room_rating"),
        "reviews": room.get("room_reviews_count"),
        "mrt_station": room.get("metro_station", ""),
        "mrt_distance": room.get("metro_distance_label", ""),
        "venue_type": room.get("venue_type_label", ""),
        "space_type": room.get("space_type_label", ""),
        "venue_part": room.get("venue_part_short_label", ""),
        "is_supervenue": room.get("is_supervenue", False),
        "response_time_min": room.get("response_time"),
        "response_rate": room.get("response_rate"),
        "catering_options": room.get("catering_options_ids", []),
        "room_features": room.get("room_features_ids", []),
        "cuisine": room.get("main_cuisine_label", ""),
        "opening_hours": room.get("opening_hours_specification", []),
    }


def _dedup_by_venue(rooms: list[dict]) -> list[dict]:
    """Collapse room-level entries to one record per venue, keeping richest room."""
    seen: dict[int, dict] = {}
    for r in rooms:
        vid = r["venue_id"]
        if vid not in seen:
            seen[vid] = r
        else:
            # Keep the room with higher seated capacity
            existing = seen[vid]
            if (r["capacity_seated"] or 0) > (existing["capacity_seated"] or 0):
                seen[vid] = r
    return list(seen.values())


def scrape_venues(limit: int | None = None) -> list[dict]:
    ua = get_random_user_agent()
    form_ts = int(time.time())

    client = _init_session(ua)
    try:
        # First page to get total count
        data = _fetch_page(client, 1, form_ts)
        total_hits = data["meta"]["hits_count"]
        first_rooms = data["rooms"]
        total_pages = -(-total_hits // ITEMS_PER_PAGE)  # ceiling division
        logger.info(f"Total rooms: {total_hits}, pages: {total_pages}")

        all_rooms = [_transform(r) for r in first_rooms]

        for page in range(2, total_pages + 1):
            if limit and len(all_rooms) >= limit:
                break
            time.sleep(PAGE_DELAY)
            logger.info(f"Page {page}/{total_pages} ({len(all_rooms)} rooms so far)...")
            batch = _fetch_page(client, page, form_ts).get("rooms", [])
            if not batch:
                logger.warning(f"Empty page {page}, stopping early")
                break
            all_rooms.extend(_transform(r) for r in batch)
    finally:
        client.close()

    venues = _dedup_by_venue(all_rooms)
    logger.info(f"Deduplicated to {len(venues)} unique venues (from {len(all_rooms)} rooms)")
    return venues[:limit] if limit else venues


def main():
    parser = argparse.ArgumentParser(description="Scrape wedding venues from Tagvenue.com (Singapore)")
    parser.add_argument("--limit", type=int, default=None, help="Max venues (for testing)")
    parser.add_argument("--output", type=str, default="data/tv/venues", help="Output path without extension")
    args = parser.parse_args()

    logger.info("Starting Tagvenue scraper...")
    venues = scrape_venues(limit=args.limit)
    logger.info(f"Scraped {len(venues)} venues")

    save_json_csv(venues, args.output)
    logger.info(f"Saved to {args.output}.json / .csv")


if __name__ == "__main__":
    main()
