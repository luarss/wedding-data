"""CLI commands for wedding venue data.

This module provides command-line tools for searching and querying
wedding venue data from the database.
"""

import argparse
import json
import sys
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.search import VenueSearch


def cmd_search(args: argparse.Namespace) -> int:
    """Search venues by text query."""
    db_path = Path(args.db_path)

    if not db_path.exists():
        print(f"Error: Database not found at {db_path}", file=sys.stderr)
        print("Run: uv run python -m src.db.ingest --init", file=sys.stderr)
        return 1

    search = VenueSearch(db_path)
    results = search.search(args.query, limit=args.limit)

    if not results:
        print(f"No venues found matching '{args.query}'")
        return 0

    print(f"Found {len(results)} venue(s) matching '{args.query}':\n")

    for r in results:
        print(f"  {r.name}")
        if r.neighborhood:
            print(f"    Location: {r.neighborhood}")
        if r.capacity_max:
            print(f"    Capacity: {r.capacity_max} guests")
        if r.price_range_min:
            print(f"    Price: from ${r.price_range_min:.0f}")
        if r.rating_overall:
            print(f"    Rating: {r.rating_overall:.1f}/5")
        print()

    return 0


def cmd_show(args: argparse.Namespace) -> int:
    """Show details for a specific venue."""
    db_path = Path(args.db_path)

    if not db_path.exists():
        print(f"Error: Database not found at {db_path}", file=sys.stderr)
        return 1

    search = VenueSearch(db_path)
    venue = search.get_venue(args.venue_id)

    if not venue:
        print(f"Venue not found: {args.venue_id}")
        return 1

    print(f"\n{venue['name']}")
    print("=" * len(venue['name']))

    if venue.get('description'):
        print(f"\n{venue['description']}")

    if venue.get('address'):
        print(f"\nAddress: {venue['address']}")

    if venue.get('neighborhood'):
        print(f"Neighborhood: {venue['neighborhood']}")

    if venue.get('phone') or venue.get('email'):
        print("\nContact:")
        if venue.get('phone'):
            print(f"  Phone: {venue['phone']}")
        if venue.get('email'):
            print(f"  Email: {venue['email']}")

    if venue.get('capacity_min') or venue.get('capacity_max'):
        cap_min = venue.get('capacity_min') or '?'
        cap_max = venue.get('capacity_max') or '?'
        print(f"\nCapacity: {cap_min} - {cap_max} guests")

    if venue.get('price_range_min') or venue.get('price_range_max'):
        price_min = f"${venue['price_range_min']:.0f}" if venue.get('price_range_min') else '?'
        price_max = f"${venue['price_range_max']:.0f}" if venue.get('price_range_max') else '?'
        print(f"Price Range: {price_min} - {price_max}")

    if venue.get('rating_overall'):
        print(f"\nRating: {venue['rating_overall']:.1f}/5")
        if venue.get('review_count'):
            print(f"Reviews: {venue['review_count']}")

    if venue.get('rooms'):
        print(f"\nRooms ({len(venue['rooms'])}):")
        for room in venue['rooms'][:3]:
            print(f"  - {room['name']}")
            if room.get('capacity_max'):
                print(f"    Capacity: {room['capacity_max']} guests")

    if venue.get('pricing'):
        print(f"\nPricing ({len(venue['pricing'])} tiers):")
        for price in venue['pricing'][:5]:
            name = price.get('name', 'Standard')
            min_p = price.get('price_min')
            max_p = price.get('price_max')
            if min_p and max_p:
                if min_p == max_p:
                    print(f"  - {name}: ${min_p:.0f}")
                else:
                    print(f"  - {name}: ${min_p:.0f} - ${max_p:.0f}")

    if venue.get('sources'):
        print(f"\nSources: {', '.join(s['source'] for s in venue['sources'])}")

    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    """Show database statistics."""
    db_path = Path(args.db_path)

    if not db_path.exists():
        print(f"Error: Database not found at {db_path}", file=sys.stderr)
        return 1

    search = VenueSearch(db_path)

    print("\nWedding Venue Database Statistics")
    print("=" * 40)

    # Get price ranges
    price_info = search.get_price_ranges()
    print(f"\nPrice ranges:")
    print(f"  Min: ${price_info.get('min', 0):.0f}")
    print(f"  Max: ${price_info.get('max', 0):.0f}")

    # Get capacity ranges
    cap_info = search.get_capacity_ranges()
    print(f"\nCapacity ranges:")
    print(f"  Min: {cap_info.get('min', 0)} guests")
    print(f"  Max: {cap_info.get('max', 0)} guests")

    # Get neighborhoods
    neighborhoods = search.get_neighborhoods()
    print(f"\nTop neighborhoods:")
    for name, count in neighborhoods[:10]:
        print(f"  {name}: {count} venues")

    # Get venue types
    vtypes = search.get_venue_types()
    print(f"\nVenue types:")
    for vtype, count in vtypes[:10]:
        print(f"  {vtype}: {count} venues")

    return 0


def cmd_export(args: argparse.Namespace) -> int:
    """Export venues to JSON or CSV."""
    db_path = Path(args.db_path)
    output_path = Path(args.output)

    if not db_path.exists():
        print(f"Error: Database not found at {db_path}", file=sys.stderr)
        return 1

    search = VenueSearch(db_path)

    # Get all venues using search with empty query
    results = search.search("", limit=10000)

    # Convert to export format
    venues = []
    for r in results:
        venues.append({
            "id": r.venue_id,
            "name": r.name,
            "address": r.address,
            "neighborhood": r.neighborhood,
            "capacity_min": r.capacity_min,
            "capacity_max": r.capacity_max,
            "price_range_min": r.price_range_min,
            "price_range_max": r.price_range_max,
            "rating_overall": r.rating_overall,
        })

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.format == "json":
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(venues, f, indent=2)
        print(f"Exported {len(venues)} venues to {output_path}")

    elif args.format == "csv":
        import csv

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            if venues:
                writer = csv.DictWriter(f, fieldnames=venues[0].keys())
                writer.writeheader()
                writer.writerows(venues)
        print(f"Exported {len(venues)} venues to {output_path}")

    return 0


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        prog="wedscraper",
        description="Wedding venue data CLI",
    )
    parser.add_argument(
        "--db-path",
        default="data/wedding_venues.db",
        help="Path to database file (default: data/wedding_venues.db)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Search command
    search_parser = subparsers.add_parser("search", help="Search venues by text")
    search_parser.add_argument("query", help="Search query")
    search_parser.add_argument("--limit", type=int, default=20, help="Maximum results")
    search_parser.set_defaults(func=cmd_search)

    # Show command
    show_parser = subparsers.add_parser("show", help="Show venue details")
    show_parser.add_argument("venue_id", help="Venue ID")
    show_parser.set_defaults(func=cmd_show)

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show database statistics")
    stats_parser.set_defaults(func=cmd_stats)

    # Export command
    export_parser = subparsers.add_parser("export", help="Export venues")
    export_parser.add_argument("output", help="Output file path")
    export_parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Export format",
    )
    export_parser.set_defaults(func=cmd_export)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
