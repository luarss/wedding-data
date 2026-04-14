import asyncio
import csv
import json

import httpx

from ..shared.config import get_headers

BASE_URL = "https://theweddingnotebook.com/api/v1/listings"
DETAIL_CONCURRENCY = 10

# Fields from venueDetails that mirror the old GraphQL venue sub-object
VENUE_DETAIL_KEYS = ["minCapacity", "maxCapacity", "minPrice", "maxPrice", "indoorOutdoor"]


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
    print(f"Fetching details for {len(listings)} venues...")
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
            response.raise_for_status()
            data = response.json()

            listings = data["data"]
            pagination = data["pagination"]
            total = pagination["total"]
            total_pages = pagination["totalPages"]

            all_listings.extend(listings)
            print(f"Page {page}/{total_pages}: Got {len(listings)} venues (total: {len(all_listings)}/{total})")

            if limit and len(all_listings) >= limit:
                all_listings = all_listings[:limit]
                break
            if page >= total_pages:
                break

            page += 1

    return all_listings


async def _fetch_detail(client, semaphore, listing):
    async with semaphore:
        try:
            response = await client.get(f"{BASE_URL}/{listing['id']}")
            response.raise_for_status()
            detail = response.json().get("data", {})
            # Merge detail fields into listing, preserving list fields
            merged = {**listing, **detail}
            return merged
        except Exception as e:
            print(f"Warning: failed to fetch detail for {listing['name']}: {e}")
            return listing


async def _fetch_all_details(listings):
    semaphore = asyncio.Semaphore(DETAIL_CONCURRENCY)
    async with httpx.AsyncClient(headers=get_headers(), timeout=30) as client:
        tasks = [_fetch_detail(client, semaphore, listing) for listing in listings]
        return await asyncio.gather(*tasks)


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
    with open(f"{filename}.json", "w", encoding="utf-8") as f:
        json.dump(venues, f, indent=2, ensure_ascii=False)

    # Save CSV — flatten venueDetails into venue_* columns for schema parity
    if venues:
        base_keys = ["id", "name", "slug", "vendorType", "state", "city", "address", "postCode"]
        venue_keys = VENUE_DETAIL_KEYS

        with open(f"{filename}.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(base_keys + [f"venue_{k}" for k in venue_keys])
            for v in venues:
                row = [v.get(k, "") for k in base_keys]
                venue_detail = v.get("venueDetails") or {}
                row += [venue_detail.get(k, "") for k in venue_keys]
                writer.writerow(row)

    print(f"Saved {len(venues)} venues to {filename}.json and {filename}.csv")


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

    print(f"Scraping venues{f' in {state}' if state else ''}...")
    venues = scrape_venues(state=state, limit=limit)

    save_venues(venues, output)

    print(f"\nScraped {len(venues)} venues")
    if venues:
        states = {}
        for v in venues:
            s = v.get("state", "Unknown")
            states[s] = states.get(s, 0) + 1
        print("\nBy state:")
        for s, count in sorted(states.items(), key=lambda x: -x[1]):
            print(f"  {s}: {count}")


if __name__ == "__main__":
    main()
