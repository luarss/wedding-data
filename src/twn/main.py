import csv
import json

from gql import Client, gql
from gql.transport.httpx import HTTPXTransport

from ..shared.config import get_headers

transport = HTTPXTransport(
    url="https://twnprod.theweddingnotebook.com/graphql",
    headers=get_headers(),
    timeout=30,
)
client = Client(transport=transport, fetch_schema_from_transport=False)

# The query
QUERY = gql("""
query GetListings($record: GetListingsInput) {
  getListings(record: $record) {
    listings {
      _id
      name
      slug
      category
      state
      city
      address
      venue {
        minCapacity
        maxCapacity
        minPrice
        maxPrice
        indoorOutdoor
      }
    }
    totalCount
  }
}
""")


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

    while True:
        # Build request
        variables = {"record": {"category": category, "page": page, "limit": 50}}
        if state:
            variables["record"]["state"] = state

        # Execute query
        result = client.execute(QUERY, variable_values=variables)
        listings = result["getListings"]["listings"]
        total = result["getListings"]["totalCount"]

        all_listings.extend(listings)
        print(f"Page {page}: Got {len(listings)} venues (total: {len(all_listings)}/{total})")

        # Stop conditions
        if limit and len(all_listings) >= limit:
            all_listings = all_listings[:limit]
            break
        if len(listings) < 50:  # Last page
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
        keys = ["_id", "name", "slug", "category", "state", "city", "address"]
        venue_keys = ["minCapacity", "maxCapacity", "minPrice", "maxPrice", "indoorOutdoor"]

        with open(f"{filename}.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            # Header
            writer.writerow(keys + [f"venue_{k}" for k in venue_keys])
            # Rows
            for v in venues:
                row = [v.get(k, "") for k in keys]
                venue = v.get("venue") or {}
                row += [venue.get(k, "") for k in venue_keys]
                writer.writerow(row)

    print(f"✅ Saved {len(venues)} venues to {filename}.json and {filename}.csv")


def main():
    """CLI entry point"""
    import sys

    # Parse args
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

    # Scrape
    print(f"Scraping venues{f' in {state}' if state else ''}...")
    venues = scrape_venues(state=state, limit=limit)

    # Save
    save_venues(venues, output)

    # Summary
    print(f"\nScraped {len(venues)} venues")
    if venues:
        states = {}
        for v in venues:
            s = v.get("state", "Unknown")
            states[s] = states.get(s, 0) + 1
        print("\nBy state:")
        for state, count in sorted(states.items(), key=lambda x: -x[1]):
            print(f"  {state}: {count}")


if __name__ == "__main__":
    main()
