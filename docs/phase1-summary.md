# Phase 1: Data Foundation - Implementation Summary

## Completed Components

### 1. Unified Pydantic Models (`src/shared/models.py`)

Created comprehensive data models that can represent venue and vendor data from all sources:

- **UnifiedVenue**: Main venue model with merged data from multiple sources
- **NormalizedPrice**: Structured price format with min/max values, currency, service charge flags
- **PricingTier**: Price for specific day/time/menu combinations
- **VenueRoom**: Individual event spaces within venues
- **SourceReference**: Tracks which source each piece of data came from
- **VenueMatch**: Result of deduplication matching with similarity scores

Key features:
- Frozen models for immutability
- Decimal for precise price handling
- Enum types for consistent values (PriceUnit, Currency, DayOfWeek, VenueType)
- Source tracking to preserve provenance

### 2. Price Parser (`src/shared/price_parser.py`)

Normalizes messy price strings from various sources into structured data:

**Supported formats:**
- `"$988–$1588Mon - Sun"` → range with days
- `"$1688++ per table"` → single price with service charge
- `"$238-$298++/pax"` → per person range
- `"$16,200++"` → flat fee with service charge

**Features:**
- Extracts min/max prices from ranges
- Detects price units (per person, per table, per event)
- Identifies service charge (++) and GST status
- Extracts applicable days of week
- Source-specific parsers for each scraper

### 3. Deduplication Engine (`src/shared/deduplication.py`)

Fuzzy matching system to identify and merge duplicate venues across sources:

**Algorithm:**
- Name normalization (removes common words, standardizes abbreviations)
- Location normalization (extracts postal codes, standardizes addresses)
- Fuzzy string matching using thefuzz library (Levenshtein distance)
- Configurable thresholds for conservative matching
- Requires high confidence (92+ name similarity) when location data is missing

**Current performance:**
- 966 unified venues from 826 raw venues
- 89 venues with pricing data
- Correctly merges venues like "1-Alfaro" (bly + wd)
- Avoids false positives (e.g., "Aloft Singapore Novena" vs "Capella Singapore")

### 4. Transformation Script (`scripts/transform_unified.py`)

CLI tool to run the full unification pipeline:

```bash
uv run python scripts/transform_unified.py --stats
```

**Output:**
- `data/unified/venues.json` - Unified venue data
- Statistics on source distribution and merges

## Usage

### Running the transformation

```bash
# Install dependencies
uv sync

# Run unification
uv run python scripts/transform_unified.py --stats
```

### Using the models

```python
from src.shared.models import UnifiedVenue, NormalizedPrice
from src.shared.price_parser import parse_price

# Parse a price string
price = parse_price("$1,688++ per table (Mon-Thu)")
print(price.price_min)  # Decimal('1688')
print(price.service_charge)  # True

# Load unified venues
import json
with open("data/unified/venues.json") as f:
    venues = [UnifiedVenue(**v) for v in json.load(f)]
```

## Data Statistics

| Metric | Value |
|--------|-------|
| Raw venues | 826 |
| Unified venues | 966 |
| Venues with pricing | 89 |
| Sources | bb, bly, wd, twn, sb |

**Venues by source:**
- bb: 213 venues
- bly: 289 venues
- sb: 170 venues
- twn: 123 venues
- wd: 171 venues

## Next Steps (Phase 2)

1. **SQLite Database**: Create schema and migration scripts
2. **Data Ingestion Pipeline**: Load unified venues into database
3. **Search Index**: Add FTS5 for full-text search
4. **API Layer**: FastAPI endpoints for querying

## Dependencies Added

```toml
dependencies = [
    # ... existing deps ...
    "thefuzz>=0.22.1",
    "python-levenshtein>=0.27.3",
]
```
