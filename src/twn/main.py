import csv
import json

import httpx

from ..shared.config import get_headers

BASE_URL = "https://theweddingnotebook.com/api/v1/listings"


def scrape_venues(category="venues", state=None, limit=None):
    """
    Scrape venues from TheWeddingNotebook.com

    Args:
        category: "venues" (only category currently supported)
        state: Filter by state (e.g., "Selangor", "Kuala Lumpur")
        limit: Max number of venues to scrape

    Returns:
        List of venue dictionaries
    """
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


def save_venues(venues, filename="data/twn/venues"):
    """
    Save venues to JSON and CSV

    Args:
        venues: List of venue dicts from scrape_venues()
        filename: Output filename (without extension)
    """
    import os

    os.makedirs(os.path.dirname(filename), exist_ok=True)

    # Save JSON
    with open(f"{filename}.json", "w", encoding="utf-8") as f:
        json.dump(venues, f, indent=2, ensure_ascii=False)

    # Save CSV
    if venues:
        keys = ["id", "name", "slug", "vendorType", "state", "city", "description", "createdAt"]

        with open(f"{filename}.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(keys)
            for v in venues:
                writer.writerow([v.get(k, "") for k in keys])

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
