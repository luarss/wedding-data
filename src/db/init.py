"""Database initialization and management.

This module provides functions to create and manage the SQLite database
with proper FTS5 search indexes.
"""

import sqlite3
from pathlib import Path


def create_tables(db_path: Path) -> None:
    """Create all database tables.

    Args:
        db_path: Path to SQLite database file
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Enable foreign keys
    cursor.execute("PRAGMA foreign_keys = ON")

    # Venues table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS venues (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            venue_types TEXT,  -- JSON array
            address TEXT,
            city TEXT,
            state TEXT,
            postal_code TEXT,
            country TEXT DEFAULT 'Singapore',
            latitude REAL,
            longitude REAL,
            neighborhood TEXT,
            phone TEXT,
            email TEXT,
            website TEXT,
            capacity_min INTEGER,
            capacity_max INTEGER,
            tables_min INTEGER,
            tables_max INTEGER,
            room_count INTEGER,
            price_range_min REAL,
            price_range_max REAL,
            price_unit TEXT,
            rating_overall REAL,
            rating_venue REAL,
            rating_service REAL,
            rating_food REAL,
            rating_value REAL,
            review_count INTEGER,
            amenities TEXT,  -- JSON array
            cuisines TEXT,  -- JSON array
            features TEXT,  -- JSON array
            images TEXT,  -- JSON array of URLs
            source_ids TEXT,  -- JSON array of source identifiers
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Venue sources table (one row per source per venue)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS venue_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venue_id TEXT NOT NULL,
            source TEXT NOT NULL,
            source_id TEXT,
            source_name TEXT,
            url TEXT,
            scraped_at TIMESTAMP,
            raw_data TEXT,  -- JSON
            FOREIGN KEY (venue_id) REFERENCES venues(id) ON DELETE CASCADE,
            UNIQUE(venue_id, source)
        )
    """)

    # Venue rooms table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS venue_rooms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venue_id TEXT NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            capacity_min INTEGER,
            capacity_max INTEGER,
            tables_min INTEGER,
            tables_max INTEGER,
            room_types TEXT,  -- JSON array
            features TEXT,  -- JSON array
            images TEXT,  -- JSON array
            FOREIGN KEY (venue_id) REFERENCES venues(id) ON DELETE CASCADE
        )
    """)

    # Pricing tiers table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pricing_tiers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venue_id TEXT NOT NULL,
            room_id INTEGER,
            name TEXT,
            price_raw TEXT,
            price_min REAL,
            price_max REAL,
            price_unit TEXT,
            currency TEXT DEFAULT 'SGD',
            service_charge BOOLEAN,
            gst_included BOOLEAN,
            days TEXT,  -- JSON array of day names
            time_of_day TEXT,
            capacity_min INTEGER,
            capacity_max INTEGER,
            tables_min INTEGER,
            tables_max INTEGER,
            menu_type TEXT,
            notes TEXT,
            FOREIGN KEY (venue_id) REFERENCES venues(id) ON DELETE CASCADE,
            FOREIGN KEY (room_id) REFERENCES venue_rooms(id) ON DELETE CASCADE
        )
    """)

    # PDF attachments table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pdf_attachments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venue_id TEXT NOT NULL,
            filename TEXT NOT NULL,
            url TEXT NOT NULL,
            local_path TEXT,
            title TEXT,
            FOREIGN KEY (venue_id) REFERENCES venues(id) ON DELETE CASCADE
        )
    """)

    # Price history table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venue_id TEXT NOT NULL,
            pricing_tier_id INTEGER,
            price_raw TEXT,
            price_min REAL,
            price_max REAL,
            price_unit TEXT,
            currency TEXT,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source TEXT,
            notes TEXT,
            FOREIGN KEY (venue_id) REFERENCES venues(id) ON DELETE CASCADE,
            FOREIGN KEY (pricing_tier_id) REFERENCES pricing_tiers(id) ON DELETE CASCADE
        )
    """)

    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_venues_name ON venues(name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_venues_city ON venues(city)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_venues_neighborhood ON venues(neighborhood)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_venues_price_min ON venues(price_range_min)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_venues_price_max ON venues(price_range_max)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_venues_capacity ON venues(capacity_max)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_venue_sources_venue ON venue_sources(venue_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_venue_sources_source ON venue_sources(source)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pricing_venue ON pricing_tiers(venue_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_history_venue ON price_history(venue_id)")

    conn.commit()
    conn.close()


def create_fts_tables(db_path: Path) -> None:
    """Create Full-Text Search (FTS5) virtual tables.

    Args:
        db_path: Path to SQLite database file
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # FTS5 virtual table for venue search (using external content)
    cursor.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS venues_fts USING fts5(
            name,
            description,
            address,
            neighborhood,
            amenities,
            cuisines,
            features,
            content='venues',
            content_rowid='rowid'
        )
    """)

    # Triggers to keep FTS index in sync
    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS venues_fts_insert AFTER INSERT ON venues BEGIN
            INSERT INTO venues_fts(rowid, name, description, address, neighborhood, amenities, cuisines, features)
            VALUES (new.rowid, new.name, new.description, new.address, new.neighborhood, new.amenities, new.cuisines, new.features);
        END
    """)

    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS venues_fts_update AFTER UPDATE ON venues BEGIN
            UPDATE venues_fts SET
                name = new.name,
                description = new.description,
                address = new.address,
                neighborhood = new.neighborhood,
                amenities = new.amenities,
                cuisines = new.cuisines,
                features = new.features
            WHERE rowid = old.rowid;
        END
    """)

    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS venues_fts_delete AFTER DELETE ON venues BEGIN
            DELETE FROM venues_fts WHERE rowid = old.rowid;
        END
    """)

    conn.commit()
    conn.close()


def init_database(db_path: Path) -> None:
    """Initialize the database with all tables and indexes.

    Args:
        db_path: Path to SQLite database file
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    create_tables(db_path)
    create_fts_tables(db_path)

    print(f"Database initialized at {db_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Initialize wedding venue database")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/wedding_venues.db"),
        help="Path to database file",
    )
    args = parser.parse_args()

    init_database(args.db_path)
