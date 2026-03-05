"""Transform raw scraped data into unified format.

This script loads venue data from all sources, deduplicates and merges
records, and outputs unified venues to data/unified/venues.json.
"""

import argparse
import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.shared.deduplication import deduplicate_venues, save_unified_venues


def main():
    parser = argparse.ArgumentParser(
        description="Transform and unify wedding venue data from all sources"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Path to data directory (default: data)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/unified/venues.json"),
        help="Output file path (default: data/unified/venues.json)",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Print statistics about the unified data",
    )

    args = parser.parse_args()

    # Run deduplication
    print("=" * 60)
    print("Wedding Venue Data Unification")
    print("=" * 60)

    unified_venues = deduplicate_venues(args.data_dir)

    # Save results
    save_unified_venues(unified_venues, args.output)

    # Print statistics
    if args.stats:
        print("\n" + "=" * 60)
        print("Statistics")
        print("=" * 60)

        print(f"\nTotal unified venues: {len(unified_venues)}")

        # Count by number of sources
        source_counts: dict[int, int] = {}
        for v in unified_venues:
            count = len(v.source_ids)
            source_counts[count] = source_counts.get(count, 0) + 1

        print("\nVenues by number of sources:")
        for count in sorted(source_counts.keys()):
            print(f"  {count} source(s): {source_counts[count]} venues")

        # Count by source
        source_venues: dict[str, int] = {}
        for v in unified_venues:
            for source in v.source_ids:
                source_venues[source] = source_venues.get(source, 0) + 1

        print("\nVenues by source:")
        for source, count in sorted(source_venues.items()):
            print(f"  {source}: {count} venues")

        # Pricing statistics
        venues_with_pricing = [v for v in unified_venues if v.pricing]
        print(f"\nVenues with pricing data: {len(venues_with_pricing)}")

        # Sample venues
        print("\n" + "=" * 60)
        print("Sample Unified Venues")
        print("=" * 60)

        # Show venues with multiple sources first
        multi_source = [v for v in unified_venues if len(v.source_ids) > 1][:5]

        if multi_source:
            print("\nVenues merged from multiple sources:")
            for v in multi_source:
                print(f"\n  {v.name}")
                print(f"    Sources: {', '.join(v.source_ids)}")
                if v.price_range_min and v.price_range_max:
                    print(f"    Price: ${v.price_range_min:,.0f} - ${v.price_range_max:,.0f}")
                if v.sources:
                    for s in v.sources:
                        print(f"    - {s.source}: {s.source_name}")

    print("\n✓ Done!")


if __name__ == "__main__":
    main()
