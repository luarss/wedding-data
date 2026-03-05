"""Full-text search and filtering for wedding venues.

This module provides search functionality using SQLite FTS5 and
additional filters for price range, capacity, location, etc.
"""

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class SearchResult:
    """Result of a venue search."""

    venue_id: str
    name: str
    address: str | None
    neighborhood: str | None
    capacity_min: int | None
    capacity_max: int | None
    price_range_min: float | None
    price_range_max: float | None
    rating_overall: float | None
    match_info: dict[str, Any] | None = None


class VenueSearch:
    """Search interface for wedding venues."""

    def __init__(self, db_path: Path):
        """Initialize search with database path.

        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path

    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection with row factory."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def search(
        self,
        query: str | None = None,
        city: str | None = None,
        neighborhood: str | None = None,
        min_capacity: int | None = None,
        max_capacity: int | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        venue_types: list[str] | None = None,
        cuisines: list[str] | None = None,
        min_rating: float | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[SearchResult]:
        """Search venues with full-text and filters.

        Args:
            query: Full-text search query
            city: Filter by city
            neighborhood: Filter by neighborhood
            min_capacity: Minimum guest capacity
            max_capacity: Maximum guest capacity
            min_price: Minimum price
            max_price: Maximum price
            venue_types: List of venue types to include
            cuisines: List of cuisines to include
            min_rating: Minimum overall rating
            limit: Maximum results to return
            offset: Offset for pagination

        Returns:
            List of matching venues
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Build query
        if query:
            # Use FTS5 for text search
            sql = """
                SELECT v.* FROM venues v
                JOIN venues_fts fts ON v.rowid = fts.rowid
                WHERE venues_fts MATCH ?
            """
            params = [query]
        else:
            sql = "SELECT v.* FROM venues v WHERE 1=1"
            params = []

        # Add filters
        if city:
            sql += " AND v.city = ?"
            params.append(city)

        if neighborhood:
            sql += " AND v.neighborhood LIKE ?"
            params.append(f"%{neighborhood}%")

        if min_capacity:
            sql += " AND (v.capacity_max >= ? OR v.capacity_min >= ?)"
            params.extend([min_capacity, min_capacity])

        if max_capacity:
            sql += " AND (v.capacity_min <= ? OR v.capacity_max <= ?)"
            params.extend([max_capacity, max_capacity])

        if min_price:
            sql += " AND v.price_range_max >= ?"
            params.append(min_price)

        if max_price:
            sql += " AND (v.price_range_min <= ? OR v.price_range_min IS NULL)"
            params.append(max_price)

        if min_rating:
            sql += " AND v.rating_overall >= ?"
            params.append(min_rating)

        # Add ordering and pagination
        if query:
            # Order by FTS rank if using text search
            sql += " ORDER BY rank"
        else:
            sql += " ORDER BY v.name"

        sql += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        cursor.execute(sql, params)
        rows = cursor.fetchall()

        results = []
        for row in rows:
            # Check venue types filter (requires JSON parsing)
            if venue_types and row["venue_types"]:
                venue_type_list = json.loads(row["venue_types"])
                if not any(vt in venue_type_list for vt in venue_types):
                    continue

            # Check cuisines filter (requires JSON parsing)
            if cuisines and row["cuisines"]:
                cuisine_list = json.loads(row["cuisines"])
                if not any(c in cuisine_list for c in cuisines):
                    continue

            results.append(
                SearchResult(
                    venue_id=row["id"],
                    name=row["name"],
                    address=row["address"],
                    neighborhood=row["neighborhood"],
                    capacity_min=row["capacity_min"],
                    capacity_max=row["capacity_max"],
                    price_range_min=row["price_range_min"],
                    price_range_max=row["price_range_max"],
                    rating_overall=row["rating_overall"],
                )
            )

        conn.close()
        return results

    def get_venue(self, venue_id: str) -> dict[str, Any] | None:
        """Get full venue details by ID.

        Args:
            venue_id: Venue ID

        Returns:
            Venue details or None if not found
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Get venue
        cursor.execute("SELECT * FROM venues WHERE id = ?", (venue_id,))
        row = cursor.fetchone()

        if not row:
            conn.close()
            return None

        venue = dict(row)

        # Parse JSON fields
        for field in ["venue_types", "amenities", "cuisines", "features", "images", "source_ids"]:
            if venue.get(field):
                try:
                    venue[field] = json.loads(venue[field])
                except json.JSONDecodeError:
                    pass

        # Get sources
        cursor.execute("SELECT * FROM venue_sources WHERE venue_id = ?", (venue_id,))
        venue["sources"] = [dict(r) for r in cursor.fetchall()]

        # Get rooms
        cursor.execute("SELECT * FROM venue_rooms WHERE venue_id = ?", (venue_id,))
        rooms = []
        for room_row in cursor.fetchall():
            room = dict(room_row)
            for field in ["room_types", "features", "images"]:
                if room.get(field):
                    try:
                        room[field] = json.loads(room[field])
                    except json.JSONDecodeError:
                        pass

            # Get room pricing
            cursor.execute(
                "SELECT * FROM pricing_tiers WHERE room_id = ?",
                (room["id"],)
            )
            room["pricing"] = [dict(r) for r in cursor.fetchall()]

            rooms.append(room)
        venue["rooms"] = rooms

        # Get venue-level pricing
        cursor.execute(
            "SELECT * FROM pricing_tiers WHERE venue_id = ? AND room_id IS NULL",
            (venue_id,)
        )
        venue["pricing"] = [dict(r) for r in cursor.fetchall()]

        # Get PDFs
        cursor.execute("SELECT * FROM pdf_attachments WHERE venue_id = ?", (venue_id,))
        venue["pdfs"] = [dict(r) for r in cursor.fetchall()]

        conn.close()
        return venue

    def get_price_ranges(self) -> dict[str, float]:
        """Get min and max prices across all venues.

        Returns:
            Dict with min and max price values
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                MIN(price_range_min) as min_price,
                MAX(price_range_max) as max_price
            FROM venues
            WHERE price_range_min IS NOT NULL
        """)
        row = cursor.fetchone()
        conn.close()

        return {
            "min": row["min_price"] or 0,
            "max": row["max_price"] or 0,
        }

    def get_capacity_ranges(self) -> dict[str, int]:
        """Get min and max capacity across all venues.

        Returns:
            Dict with min and max capacity values
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                MIN(capacity_min) as min_capacity,
                MAX(capacity_max) as max_capacity
            FROM venues
            WHERE capacity_max IS NOT NULL
        """)
        row = cursor.fetchone()
        conn.close()

        return {
            "min": row["min_capacity"] or 0,
            "max": row["max_capacity"] or 0,
        }

    def get_neighborhoods(self) -> list[str]:
        """Get list of all neighborhoods.

        Returns:
            List of neighborhood names
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT DISTINCT neighborhood
            FROM venues
            WHERE neighborhood IS NOT NULL
            ORDER BY neighborhood
        """)
        rows = cursor.fetchall()
        conn.close()

        return [r["neighborhood"] for r in rows]

    def get_venue_types(self) -> list[str]:
        """Get list of all venue types.

        Returns:
            List of venue type names
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT venue_types FROM venues WHERE venue_types IS NOT NULL")
        rows = cursor.fetchall()

        types: set[str] = set()
        for row in rows:
            try:
                type_list = json.loads(row["venue_types"])
                types.update(type_list)
            except json.JSONDecodeError:
                continue

        conn.close()
        return sorted(types)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Search wedding venues")
    parser.add_argument("--db-path", type=Path, default=Path("data/wedding_venues.db"))
    parser.add_argument("query", nargs="?", help="Search query")
    parser.add_argument("--max-price", type=float, help="Maximum price")
    parser.add_argument("--min-capacity", type=int, help="Minimum capacity")
    parser.add_argument("--neighborhood", help="Neighborhood")
    parser.add_argument("--limit", type=int, default=20, help="Result limit")
    parser.add_argument("--details", action="store_true", help="Show full details")

    args = parser.parse_args()

    search = VenueSearch(args.db_path)

    if args.query or args.max_price or args.min_capacity or args.neighborhood:
        results = search.search(
            query=args.query,
            max_price=args.max_price,
            min_capacity=args.min_capacity,
            neighborhood=args.neighborhood,
            limit=args.limit,
        )

        print(f"Found {len(results)} venues:\n")

        for r in results:
            print(f"{r.name}")
            if r.neighborhood:
                print(f"  Location: {r.neighborhood}")
            if r.capacity_max:
                print(f"  Capacity: {r.capacity_min or '?'} - {r.capacity_max} guests")
            if r.price_range_max:
                print(f"  Price: ${r.price_range_min or '?'} - ${r.price_range_max}")
            if r.rating_overall:
                print(f"  Rating: {r.rating_overall}/5")
            print()

            if args.details:
                venue = search.get_venue(r.venue_id)
                if venue:
                    print(f"  ID: {venue['id']}")
                    print(f"  Address: {venue.get('address', 'N/A')}")
                    if venue.get("pricing"):
                        print(f"  Pricing tiers: {len(venue['pricing'])}")
                    if venue.get("rooms"):
                        print(f"  Rooms: {len(venue['rooms'])}")
                    print()
    else:
        # Show stats
        print("Venue Statistics")
        print("=" * 40)
        price_range = search.get_price_ranges()
        print(f"Price range: ${price_range['min']:,.0f} - ${price_range['max']:,.0f}")

        capacity_range = search.get_capacity_ranges()
        print(f"Capacity range: {capacity_range['min']} - {capacity_range['max']} guests")

        print(f"\nNeighborhoods:")
        for n in search.get_neighborhoods()[:10]:
            print(f"  - {n}")

        print(f"\nVenue Types:")
        for t in search.get_venue_types()[:10]:
            print(f"  - {t}")
