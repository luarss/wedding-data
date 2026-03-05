# Phase 2: Data Access Layer - Implementation Summary

## Completed Components

### 1. Database Models (`src/db/models.py`)

SQLAlchemy ORM models for database tables:

- **Venue**: Main venue table with all fields
- **VenueSource**: Tracks which source each venue data came from
- **VenueRoom**: Individual event spaces within venues
- **PricingTier**: Price information for venues/rooms
- **PDFAttachment**: Downloaded PDF documents
- **PriceHistory**: Historical price tracking

**Key features:**
- Proper relationships with foreign keys
- JSON columns for arrays
- Indexes for common queries
- Timestamps for tracking

### 2. Database Initialization (`src/db/init.py`)

Creates SQLite database with:

- All main tables (venues, rooms, pricing, pdfs)
- Indexes for performance
- FTS5 virtual table for full-text search
- Triggers to sync FTS with main table

**Usage:**
```bash
uv run python -m src.db.init --db-path data/wedding_venues.db
```

### 3. Data Ingestion (`src/db/ingest.py`)

Loads unified venues into the database:

- Converts unified JSON to database records
- Handles relationships (rooms → pricing)
- Updates FTS search index
- Proper error handling

**Usage:**
```bash
uv run python -m src.db.ingest --init
```

**Performance:**
- 266 venues ingested
- 411 rooms
- 3,465 pricing tiers
- 522 PDFs

### 4. Search Module (`src/db/search.py`)

Full-text search and filtering:

**VenueSearch class:**
- `search(query, limit=20)` - FTS5 full-text search
- `get_venue(venue_id)` - Get single venue with all details
- `get_price_ranges()` - Get min/max prices
- `get_capacity_ranges()` - Get min/max capacity
- `get_neighborhoods()` - List neighborhoods with counts
- `get_venue_types()` - List venue types with counts

**Usage:**
```python
from src.db.search import VenueSearch

search = VenueSearch("data/wedding_venues.db")
results = search.search("hotel orchard", limit=10)
for r in results:
    print(f"{r.name}: ${r.price_range_min}")
```

### 5. CLI Commands (`src/cli/__main__.py`)

Command-line interface for database queries:

```bash
# Search venues
uv run python -m src.cli search "hotel" --limit 10

# Show venue details
uv run python -m src.cli show 1alfaro

# Database statistics
uv run python -m src.cli stats

# Export to CSV/JSON
uv run python -m src.cli export venues.csv --format csv
```

## Usage

### Complete setup from scratch

```bash
# 1. Unify data from all sources
uv run python scripts/transform_unified.py --stats

# 2. Initialize database
uv run python -m src.db.init

# 3. Ingest unified venues
uv run python -m src.db.ingest --init
```

### Query the database

```bash
# Search
uv run python -m src.cli search "hotel"

# Filter by price range
uv run python -m src.cli search "" --max-price 2000

# Show venue details
uv run python -m src.cli show 1alfaro

# Get statistics
uv run python -m src.cli stats

# Export data
uv run python -m src.cli export venues.csv
```

## Database Statistics

| Metric | Value |
|--------|-------|
| Venues | 266 |
| Rooms | 411 |
| Pricing tiers | 3,465 |
| PDFs | 522 |
| Price range | $50 - $40,000 |

## Schema Overview

```
venues
├── id (PK)
├── name, description
├── location (address, city, state, postal_code, neighborhood)
├── contact (phone, email, website)
├── capacity_min/max, tables_min/max
├── price_range_min/max, price_unit
├── ratings (overall, venue, service, food, value, review_count)
├── amenities, cuisines, features (JSON arrays)
├── source_ids (JSON array)
└── timestamps

venue_sources (one per source per venue)
├── venue_id (FK)
├── source, source_id, source_name
├── url, scraped_at, raw_data

venue_rooms
├── venue_id (FK)
├── name, description, capacity, tables
├── room_types, features (JSON arrays)

pricing_tiers
├── venue_id (FK), room_id (FK - optional)
├── name, price_raw, price_min/max
├── price_unit, currency
├── service_charge, gst_included
├── days (JSON array), time_of_day
├── capacity, tables, menu_type

pdf_attachments
├── venue_id (FK)
├── filename, url, local_path, title

price_history
├── venue_id (FK), pricing_tier_id (FK)
├── price details, recorded_at, source

venues_fts (FTS5 virtual table)
├── name, description, address
├── neighborhood, amenities, cuisines, features
```

## Next Steps (Phase 3)

1. **Price History Tracking**: Extract historical prices from git history
2. **Market Analysis**: Generate insights and reports
3. **Venue Recommendations**: Similar venue suggestions

## Dependencies Added

```toml
dependencies = [
    # ... existing deps ...
    "sqlalchemy>=2.0.48",
]
```
