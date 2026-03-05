"""Data ingestion pipeline for unified venues.

This module provides functions to load unified venue data into the SQLite database.
"""

import json
import sqlite3
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


def to_json_string(value: Any) -> str | None:
    """Convert value to JSON string.

    Args:
        value: Value to convert

    Returns:
        JSON string or None if value is empty
    """
    if value is None:
        return None
    if isinstance(value, (list, dict)) and not value:
        return None
    return json.dumps(value)


def decimal_to_float(value: Decimal | None) -> float | None:
    """Convert Decimal to float for SQLite storage.

    Args:
        value: Decimal value

    Returns:
        Float value or None
    """
    if value is None:
        return None
    return float(value)


def ingest_venues(db_path: Path, venues_json_path: Path) -> int:
    """Ingest unified venues into the database.

    Args:
        db_path: Path to SQLite database
        venues_json_path: Path to unified venues JSON file

    Returns:
        Number of venues ingested
    """
    # Load unified venues
    with open(venues_json_path, encoding="utf-8") as f:
        venues = json.load(f)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    count = 0

    for venue in venues:
        venue_id = venue["id"]

        # Insert venue
        cursor.execute(
            """
            INSERT OR REPLACE INTO venues (
                id, name, description, venue_types,
                address, city, state, postal_code, country,
                latitude, longitude, neighborhood,
                phone, email, website,
                capacity_min, capacity_max, tables_min, tables_max, room_count,
                price_range_min, price_range_max, price_unit,
                rating_overall, rating_venue, rating_service, rating_food, rating_value, review_count,
                amenities, cuisines, features, images, source_ids,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                venue_id,
                venue.get("name"),
                venue.get("description"),
                to_json_string(venue.get("venue_types")),
                venue.get("location", {}).get("address") if venue.get("location") else None,
                venue.get("location", {}).get("city") if venue.get("location") else None,
                venue.get("location", {}).get("state") if venue.get("location") else None,
                venue.get("location", {}).get("postal_code") if venue.get("location") else None,
                venue.get("location", {}).get("country", "Singapore") if venue.get("location") else "Singapore",
                venue.get("location", {}).get("latitude") if venue.get("location") else None,
                venue.get("location", {}).get("longitude") if venue.get("location") else None,
                venue.get("location", {}).get("neighborhood") if venue.get("location") else None,
                venue.get("contact", {}).get("phone") if venue.get("contact") else None,
                venue.get("contact", {}).get("email") if venue.get("contact") else None,
                venue.get("contact", {}).get("website") if venue.get("contact") else None,
                venue.get("capacity_min"),
                venue.get("capacity_max"),
                venue.get("tables_min"),
                venue.get("tables_max"),
                venue.get("room_count"),
                decimal_to_float(venue.get("price_range_min")),
                decimal_to_float(venue.get("price_range_max")),
                venue.get("price_unit"),
                venue.get("rating", {}).get("overall") if venue.get("rating") else None,
                venue.get("rating", {}).get("venue") if venue.get("rating") else None,
                venue.get("rating", {}).get("service") if venue.get("rating") else None,
                venue.get("rating", {}).get("food") if venue.get("rating") else None,
                venue.get("rating", {}).get("value") if venue.get("rating") else None,
                venue.get("rating", {}).get("review_count") if venue.get("rating") else None,
                to_json_string(venue.get("amenities")),
                to_json_string(venue.get("cuisines")),
                to_json_string(venue.get("features")),
                to_json_string(venue.get("images")),
                to_json_string(venue.get("source_ids")),
                venue.get("created_at", datetime.utcnow().isoformat()),
                venue.get("updated_at", datetime.utcnow().isoformat()),
            ),
        )

        # Insert sources
        for source in venue.get("sources", []):
            cursor.execute(
                """
                INSERT OR REPLACE INTO venue_sources (
                    venue_id, source, source_id, source_name, url, scraped_at, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    venue_id,
                    source.get("source"),
                    source.get("source_id"),
                    source.get("source_name"),
                    source.get("url"),
                    source.get("scraped_at"),
                    to_json_string(source.get("raw_data")),
                ),
            )

        # Insert rooms
        for room in venue.get("rooms", []):
            cursor.execute(
                """
                INSERT INTO venue_rooms (
                    venue_id, name, description, capacity_min, capacity_max,
                    tables_min, tables_max, room_types, features, images
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    venue_id,
                    room.get("name"),
                    room.get("description"),
                    room.get("capacity_min"),
                    room.get("capacity_max"),
                    room.get("tables_min"),
                    room.get("tables_max"),
                    to_json_string(room.get("room_types")),
                    to_json_string(room.get("features")),
                    to_json_string(room.get("images")),
                ),
            )
            room_id = cursor.lastrowid

            # Insert pricing for room
            for pricing in room.get("pricing", []):
                price = pricing.get("price", {})
                cursor.execute(
                    """
                    INSERT INTO pricing_tiers (
                        venue_id, room_id, name, price_raw, price_min, price_max,
                        price_unit, currency, service_charge, gst_included,
                        days, time_of_day, capacity_min, capacity_max,
                        tables_min, tables_max, menu_type, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        venue_id,
                        room_id,
                        pricing.get("name"),
                        price.get("price_raw"),
                        decimal_to_float(price.get("price_min")),
                        decimal_to_float(price.get("price_max")),
                        price.get("price_unit"),
                        price.get("currency"),
                        price.get("service_charge"),
                        price.get("gst_included"),
                        to_json_string(price.get("days")),
                        price.get("time_of_day"),
                        pricing.get("capacity_min"),
                        pricing.get("capacity_max"),
                        pricing.get("tables_min"),
                        pricing.get("tables_max"),
                        pricing.get("menu_type"),
                        price.get("notes"),
                    ),
                )

        # Insert venue-level pricing
        for pricing in venue.get("pricing", []):
            price = pricing.get("price", {})
            cursor.execute(
                """
                INSERT INTO pricing_tiers (
                    venue_id, name, price_raw, price_min, price_max,
                    price_unit, currency, service_charge, gst_included,
                    days, time_of_day, capacity_min, capacity_max,
                    tables_min, tables_max, menu_type, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    venue_id,
                    pricing.get("name"),
                    price.get("price_raw"),
                    decimal_to_float(price.get("price_min")),
                    decimal_to_float(price.get("price_max")),
                    price.get("price_unit"),
                    price.get("currency"),
                    price.get("service_charge"),
                    price.get("gst_included"),
                    to_json_string(price.get("days")),
                    price.get("time_of_day"),
                    pricing.get("capacity_min"),
                    pricing.get("capacity_max"),
                    pricing.get("tables_min"),
                    pricing.get("tables_max"),
                    pricing.get("menu_type"),
                    price.get("notes"),
                ),
            )

        # Insert PDFs
        for pdf in venue.get("pdfs", []):
            cursor.execute(
                """
                INSERT INTO pdf_attachments (
                    venue_id, filename, url, local_path, title
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    venue_id,
                    pdf.get("filename"),
                    pdf.get("url"),
                    pdf.get("local_path"),
                    pdf.get("title"),
                ),
            )

        count += 1

    conn.commit()
    conn.close()

    return count


def update_fts_index(db_path: Path) -> None:
    """Update the FTS search index with all venues.

    Args:
        db_path: Path to SQLite database
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # For external content FTS, we need to rebuild using the rebuild command
    cursor.execute("INSERT INTO venues_fts(venues_fts) VALUES('rebuild')")

    conn.commit()
    conn.close()

    print("FTS search index updated")


if __name__ == "__main__":
    import argparse

    from src.db.init import init_database

    parser = argparse.ArgumentParser(description="Ingest unified venues into database")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/wedding_venues.db"),
        help="Path to database file",
    )
    parser.add_argument(
        "--venues-json",
        type=Path,
        default=Path("data/unified/venues.json"),
        help="Path to unified venues JSON file",
    )
    parser.add_argument(
        "--init",
        action="store_true",
        help="Initialize database before ingestion",
    )
    args = parser.parse_args()

    if args.init or not args.db_path.exists():
        print("Initializing database...")
        init_database(args.db_path)

    print(f"Ingesting venues from {args.venues_json}...")
    count = ingest_venues(args.db_path, args.venues_json)
    print(f"Ingested {count} venues")

    print("Updating search index...")
    update_fts_index(args.db_path)

    print("Done!")
