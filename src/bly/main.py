import json

import httpx

from src.shared import get_logger, save_csv, save_json

logger = get_logger()

BASE_URL = "https://www.bridely.sg"
HEADERS = {
    "accept": "application/json",
    "referer": f"{BASE_URL}/venues",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
}


def _format_price(room: dict) -> str:
    price = room.get("startingPrice")
    if not price:
        return ""
    amount = price.get("amount", 0)
    if price.get("kind") == "per_pax":
        return f"From ${amount}/pax"
    if price.get("kind") == "min_spend":
        return f"From ${amount:,}"
    return f"${amount}"


def _format_capacity(room: dict) -> str:
    min_pax = room.get("minPax")
    max_pax = room.get("maxPax")
    if min_pax and max_pax:
        return f"{min_pax}-{max_pax} pax"
    if min_pax:
        return f"From {min_pax} pax"
    return ""


def _transform(venue: dict) -> dict:
    rooms = [
        {
            "name": r.get("name", ""),
            "capacity": _format_capacity(r),
            "price": _format_price(r),
        }
        for r in venue.get("rooms", [])
    ]
    first_room = rooms[0] if rooms else {}
    tags = [t["label"] for t in venue.get("displayTags", [])]

    return {
        "recordId": venue["recordId"],
        "name": venue["name"],
        "url": f"{BASE_URL}{venue['href']}",
        "location": venue.get("location", ""),
        "overallRating": venue.get("rating", ""),
        "venueRating": venue.get("ratings", {}).get("venue", ""),
        "serviceRating": venue.get("ratings", {}).get("service", ""),
        "foodRating": venue.get("ratings", {}).get("food", ""),
        "reviews": venue.get("reviewCount", ""),
        "tags": ", ".join(tags),
        "price": first_room.get("price", ""),
        "capacity": first_room.get("capacity", ""),
        "rooms": json.dumps(rooms),
    }


def fetch_all_venues() -> list[dict]:
    venues = []
    offset = 0
    sponsor_ids: list[str] = []

    seen_ids: set[str] = set()
    total_count: int | None = None

    with httpx.Client(headers=HEADERS, timeout=30) as client:
        while True:
            params: dict = {"offset": offset}
            if sponsor_ids:
                params["sponsorVendorIds"] = ",".join(sponsor_ids)

            logger.info(f"Fetching offset={offset}...")
            resp = client.get(f"{BASE_URL}/api/venues/directory", params=params)
            resp.raise_for_status()
            data = resp.json()

            if total_count is None:
                total_count = data.get("totalCount", 0)

            results = data.get("results", [])
            new_results = [v for v in results if v["recordId"] not in seen_ids]
            for v in new_results:
                seen_ids.add(v["recordId"])
            venues.extend(new_results)

            logger.info(f"  Got {len(new_results)} new venues (total: {len(venues)} / {total_count})")

            next_offset = data.get("nextOffset")
            if not data.get("hasMore") or next_offset == offset or len(venues) >= (total_count or 0):
                break

            offset = next_offset
            if data.get("sponsorVendorIds"):
                sponsor_ids = data["sponsorVendorIds"]

    return venues


def main():
    logger.info("Fetching all venues from Bridely.sg API...")
    raw = fetch_all_venues()

    logger.info(f"\nTotal venues fetched: {len(raw)}")

    if len(raw) == 0:
        logger.error("No venues found — skipping save to avoid overwriting existing data")
        return

    venues = [_transform(v) for v in raw]

    save_csv(
        venues,
        "data/bly/venues.csv",
        fieldnames=[
            "recordId", "name", "url", "location",
            "overallRating", "venueRating", "serviceRating", "foodRating",
            "reviews", "tags", "price", "capacity", "rooms",
        ],
    )
    save_json(venues, "data/bly/venues.json")

    logger.info("\nFirst 5 venues:")
    for i, venue in enumerate(venues[:5], 1):
        logger.info(f"{i}. {venue['name']} — {venue['price']} — {venue['capacity']} — {venue['location']}")


if __name__ == "__main__":
    main()
