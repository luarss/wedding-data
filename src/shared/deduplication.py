"""Venue deduplication using fuzzy string matching.

This module identifies and merges duplicate venues across different sources
using venue name and address similarity.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from thefuzz import fuzz

from .models import (
    PDFAttachment,
    PricingTier,
    Rating,
    SourceReference,
    UnifiedVenue,
    VenueMatch,
    VenueRoom,
)
from .price_parser import parse_bb_price, parse_bly_price, parse_sb_price, parse_wd_price


# Similarity thresholds for matching
NAME_MATCH_THRESHOLD = 80  # Fuzzy name similarity (0-100)
LOCATION_MATCH_THRESHOLD = 70  # Fuzzy location similarity
OVERALL_MATCH_THRESHOLD = 75  # Combined score threshold


def normalize_name(name: str) -> str:
    """Normalize venue name for comparison.

    - Convert to lowercase
    - Remove common suffixes (hotel, restaurant, etc.)
    - Remove common location words (singapore, etc.)
    - Remove punctuation and extra spaces
    - Standardize common abbreviations
    """
    if not name:
        return ""

    normalized = name.lower()

    # Remove common location words that don't help distinguish venues
    location_words = [
        r"\bsingapore\b",
        r"\bkl\b",
        r"\bkuala\s+lumpur\b",
        r"\bmalaysia\b",
    ]
    for pattern in location_words:
        normalized = re.sub(pattern, "", normalized, flags=re.IGNORECASE)

    # Remove common suffixes/prefixes that don't affect identity
    suffixes = [
        r"\s+hotel\s*$",
        r"\s+restaurant\s*$",
        r"\s+resort\s*$",
        r"\s+ballroom\s*$",
        r"\s+at\s+.*$",  # "X at Y" -> "X"
        r"^the\s+",  # Remove leading "the"
    ]

    for pattern in suffixes:
        normalized = re.sub(pattern, "", normalized, flags=re.IGNORECASE)

    # Standardize common words
    replacements = {
        r"&": "and",
        r"@": "at",
        r"\.": "",
        r",": "",
        r"\s+": " ",  # Normalize whitespace
    }

    for pattern, replacement in replacements.items():
        normalized = re.sub(pattern, replacement, normalized)

    return normalized.strip()


def normalize_address(address: str) -> str:
    """Normalize address for comparison.

    - Convert to lowercase
    - Extract postal code if present
    - Remove common words (singapore, road, street, etc.)
    - Standardize abbreviations
    """
    if not address:
        return ""

    normalized = address.lower()

    # Extract postal code (Singapore format: 6 digits)
    postal_match = re.search(r"\b(\d{6})\b", normalized)
    postal_code = postal_match.group(1) if postal_match else None

    # Remove common words
    common_words = [
        r"\bsingapore\b",
        r"\broad\b",
        r"\bst\b",
        r"\bstreet\b",
        r"\bavenue\b",
        r"\bave\b",
        r"\bdrive\b",
        r"\bdr\b",
        r"\blane\b",
        r"\bway\b",
        r"\bplace\b",
        r"\bpl\b",
    ]

    for pattern in common_words:
        normalized = re.sub(pattern, "", normalized)

    # Standardize abbreviations
    replacements = {
        r"\bst\.?\s": "street ",
        r"\bave\.?\s": "avenue ",
        r"\bdr\.?\s": "drive ",
        r"\bpl\.?\s": "place ",
        r"\bjln\.?\s": "jalan ",
        r"\blk\.?\s": "block ",
        r"\b#": "",  # Remove floor/unit indicators
        r"\d{6}": "",  # Remove postal code from text
        r"\s+": " ",
    }

    for pattern, replacement in replacements.items():
        normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)

    # Include postal code at end if found (strong signal)
    result = normalized.strip()
    if postal_code:
        result += f" [{postal_code}]"

    return result


def calculate_name_similarity(name1: str, name2: str) -> float:
    """Calculate similarity between two venue names (0-100 scale).

    Uses a combination of token sort and token set ratios for better
    handling of word order and extra words.
    """
    norm1 = normalize_name(name1)
    norm2 = normalize_name(name2)

    if not norm1 or not norm2:
        return 0.0

    # Exact match after normalization
    if norm1 == norm2:
        return 100.0

    # Use multiple fuzz ratios and take the best
    ratios = [
        fuzz.ratio(norm1, norm2),
        fuzz.partial_ratio(norm1, norm2),
        fuzz.token_sort_ratio(norm1, norm2),
        fuzz.token_set_ratio(norm1, norm2),
    ]

    return max(ratios)


def calculate_location_similarity(loc1: str | None, loc2: str | None) -> float:
    """Calculate similarity between two location strings (0-100 scale)."""
    if not loc1 and not loc2:
        # Both missing - neutral
        return 50.0

    if not loc1 or not loc2:
        # One missing - penalize slightly
        return 30.0

    norm1 = normalize_address(loc1)
    norm2 = normalize_address(loc2)

    if not norm1 or not norm2:
        return 0.0

    # Check for matching postal codes (strong signal)
    postal1 = re.search(r"\[(\d{6})\]", norm1)
    postal2 = re.search(r"\[(\d{6})\]", norm2)

    if postal1 and postal2:
        if postal1.group(1) == postal2.group(1):
            return 100.0  # Same postal code = same venue

    # Use fuzzy matching on address
    ratios = [
        fuzz.ratio(norm1, norm2),
        fuzz.partial_ratio(norm1, norm2),
        fuzz.token_set_ratio(norm1, norm2),
    ]

    return max(ratios)


def calculate_match_score(
    name1: str,
    name2: str,
    location1: str | None = None,
    location2: str | None = None,
) -> tuple[float, float, float]:
    """Calculate match scores for two venues.

    Returns:
        Tuple of (name_similarity, location_similarity, overall_score)
    """
    name_sim = calculate_name_similarity(name1, name2)
    loc_sim = calculate_location_similarity(location1, location2)

    # For a match, we need:
    # 1. High name similarity (>=85)
    # 2. Either high location similarity OR one location missing
    # If both locations exist and are very different (<50), it's not a match

    if loc_sim < 50 and location1 and location2:
        # Very different locations - likely not a match even if names are similar
        overall = name_sim * 0.5
    else:
        overall = name_sim

    return name_sim, loc_sim, overall


def should_merge(
    name1: str,
    name2: str,
    location1: str | None = None,
    location2: str | None = None,
) -> tuple[bool, VenueMatch | None]:
    """Determine if two venues should be merged.

    Returns:
        Tuple of (should_merge, match_details)
    """
    name_sim, loc_sim, overall = calculate_match_score(name1, name2, location1, location2)

    # Determine if match and reason
    is_match = False
    reason = None

    # Check if we have location data for both venues
    has_location_data = bool(location1 and location2)

    if has_location_data:
        # With location data, we can be more confident
        if overall >= OVERALL_MATCH_THRESHOLD:
            is_match = True
            reason = f"High similarity score: {overall:.1f}"
        elif name_sim >= 95 and loc_sim >= 50:
            # Very high name similarity + at least some location similarity
            is_match = True
            reason = f"Very high name similarity: {name_sim:.1f}"
    else:
        # Without location data, require much higher name similarity
        if name_sim >= 98:
            # Near-exact name match required when no location data
            is_match = True
            reason = f"Near-exact name match (no location data): {name_sim:.1f}"

    match = VenueMatch(
        venue_id_1=name1,  # Will be replaced with actual IDs
        venue_id_2=name2,
        name_similarity=name_sim,
        location_similarity=loc_sim,
        overall_score=overall,
        is_match=is_match,
        reason=reason,
    )

    return is_match, match


@dataclass
class RawVenue:
    """Intermediate representation of a venue from raw data."""

    source: str
    source_id: str | None
    name: str
    location: str | None
    raw_data: dict[str, Any]


def load_venues_from_source(data_dir: Path, source: str) -> list[RawVenue]:
    """Load venues from a source's JSON file.

    Args:
        data_dir: Path to data directory
        source: Source identifier (bb, bly, wd, twn, sb)

    Returns:
        List of RawVenue objects
    """
    file_path = data_dir / source / "venues.json"

    if not file_path.exists():
        return []

    with open(file_path, encoding="utf-8") as f:
        data = json.load(f)

    venues: list[RawVenue] = []

    for item in data:
        # Extract name and location based on source format
        name = item.get("name", "")
        location = None
        source_id = None

        if source == "bb":
            location = item.get("location") or item.get("address")
            source_id = item.get("venue_id") or item.get("profile_url")

        elif source == "bly":
            # Bridely doesn't have location in basic venue list
            name = item.get("name", "")
            source_id = item.get("recordId")

        elif source == "wd":
            location = None  # Wedded locations need to be scraped separately
            source_id = item.get("vendor_id") or item.get("slug")

        elif source == "twn":
            location = item.get("address")
            source_id = item.get("_id") or item.get("slug")

        elif source == "sb":
            location = item.get("address")
            source_id = item.get("name")  # SB doesn't have IDs, use name

        venues.append(
            RawVenue(
                source=source,
                source_id=source_id,
                name=name,
                location=location,
                raw_data=item,
            )
        )

    return venues


def find_matches(venues: list[RawVenue]) -> list[tuple[RawVenue, RawVenue, VenueMatch]]:
    """Find all potential matches between venues.

    Args:
        venues: List of all venues from all sources

    Returns:
        List of tuples (venue1, venue2, match_details)
    """
    matches = []
    seen = set()

    for i, v1 in enumerate(venues):
        for v2 in venues[i + 1 :]:
            # Skip if same source (usually not duplicates within source)
            if v1.source == v2.source:
                continue

            # Create unique key for this pair
            pair_key = tuple(sorted([f"{v1.source}:{v1.name}", f"{v2.source}:{v2.name}"]))
            if pair_key in seen:
                continue
            seen.add(pair_key)

            should_merge_result, match = should_merge(
                v1.name,
                v2.name,
                v1.location,
                v2.location,
            )

            if should_merge_result:
                matches.append((v1, v2, match))

    return matches


def generate_venue_slug(name: str) -> str:
    """Generate a URL-friendly slug from venue name.

    Args:
        name: Venue name

    Returns:
        URL slug
    """
    # Convert to lowercase
    slug = name.lower()

    # Replace & with 'and'
    slug = slug.replace("&", "and")

    # Remove non-alphanumeric characters except spaces
    slug = re.sub(r"[^a-z0-9\s]", "", slug)

    # Replace spaces with hyphens
    slug = re.sub(r"\s+", "-", slug.strip())

    # Remove consecutive hyphens
    slug = re.sub(r"-+", "-", slug)

    return slug


def merge_venues(venues: list[RawVenue]) -> UnifiedVenue:
    """Merge multiple RawVenue objects into a single UnifiedVenue.

    Args:
        venues: List of venues to merge (assumed to be duplicates)

    Returns:
        UnifiedVenue with merged data
    """
    if not venues:
        raise ValueError("Cannot merge empty list of venues")

    # Use the most common name or the longest one
    names = [v.name for v in venues]
    name_counts: dict[str, int] = {}
    for n in names:
        norm = normalize_name(n)
        name_counts[norm] = name_counts.get(norm, 0) + 1

    # Prefer the most common name, fallback to longest
    if name_counts:
        canonical_name = max(name_counts.keys(), key=lambda x: (name_counts[x], len(x)))
    else:
        canonical_name = max(names, key=len)

    # Find the original case version
    canonical_name = next(n for n in names if normalize_name(n) == canonical_name)

    venue_id = generate_venue_slug(canonical_name)

    # Build sources list
    sources: list[SourceReference] = []
    for v in venues:
        sources.append(
            SourceReference(
                source=v.source,
                source_id=v.source_id,
                source_name=v.name,
                url=None,  # Will be populated later
                raw_data=v.raw_data,
            )
        )

    # Merge location - prefer the most detailed one
    best_location = None
    for v in venues:
        if v.location and len(v.location) > len(best_location or ""):
            best_location = v.location

    # Parse source-specific data to extract pricing and other details
    pricing: list[PricingTier] = []
    rooms: list[VenueRoom] = []
    pdfs: list[PDFAttachment] = []
    rating = None

    for v in venues:
        data = v.raw_data

        if v.source == "bb":
            # Parse BB pricing
            if lunch := data.get("lunch_price"):
                parsed = parse_bb_price(lunch)
                if parsed.price_min:
                    pricing.append(
                        PricingTier(
                            name="Lunch",
                            price=parsed,
                            time_of_day="lunch",
                        )
                    )
            if dinner := data.get("dinner_price"):
                parsed = parse_bb_price(dinner)
                if parsed.price_min:
                    pricing.append(
                        PricingTier(
                            name="Dinner",
                            price=parsed,
                            time_of_day="dinner",
                        )
                    )
            # Tables
            tables_min = None
            tables_max = None
            if tables := data.get("tables_range"):
                parts = tables.replace(" ", "").split("-")
                if len(parts) == 2:
                    try:
                        tables_min = int(parts[0])
                        tables_max = int(parts[1])
                    except ValueError:
                        pass

            # PDFs
            if price_lists := data.get("price_lists"):
                for url in price_lists:
                    filename = url.split("/")[-1]
                    pdfs.append(
                        PDFAttachment(
                            filename=filename,
                            url=url,
                        )
                    )

            # Rating
            if rating_val := data.get("rating"):
                try:
                    rating = Rating(overall=float(rating_val))
                except ValueError:
                    pass

        elif v.source == "bly":
            # Parse Bridely pricing (per person)
            if price_str := data.get("price"):
                parsed = parse_bly_price(price_str)
                if parsed.price_min:
                    pricing.append(
                        PricingTier(
                            name="Standard",
                            price=parsed,
                        )
                    )

            # Rating
            if venue_rating := data.get("venueRating"):
                try:
                    rating = Rating(
                        overall=float(venue_rating),
                        service=float(data.get("serviceRating", 0)) or None,
                        food=float(data.get("foodRating", 0)) or None,
                        review_count=int(data.get("reviews", 0)) or None,
                    )
                except ValueError:
                    pass

            # Capacity
            capacity = data.get("capacity", "")

        elif v.source == "wd":
            # Parse Wedded rooms and pricing
            for room_data in data.get("rooms", []):
                room = VenueRoom(
                    name=room_data.get("name", "Main"),
                    room_types=room_data.get("types", []),
                )

                for pkg in room_data.get("packages", []):
                    if price_str := pkg.get("price"):
                        parsed = parse_wd_price(price_str)
                        room.pricing.append(
                            PricingTier(
                                name=pkg.get("menu", "Package"),
                                price=parsed,
                                capacity_min=pkg.get("capacity_min"),
                                capacity_max=pkg.get("capacity_max"),
                                day=pkg.get("day"),
                            )
                        )

                rooms.append(room)

            # PDFs
            for pdf_data in data.get("pdfs", []):
                pdfs.append(
                    PDFAttachment(
                        filename=pdf_data.get("filename", ""),
                        url=pdf_data.get("url", ""),
                        local_path=pdf_data.get("local_path"),
                    )
                )

        elif v.source == "twn":
            # TWN has structured price data
            venue_data = data.get("venue", {})
            min_price = venue_data.get("minPrice")
            max_price = venue_data.get("maxPrice")

            if min_price or max_price:
                from .price_parser import parse_twn_price

                parsed = parse_twn_price(min_price, max_price)
                if parsed.price_min:
                    pricing.append(
                        PricingTier(
                            name="Standard",
                            price=parsed,
                        )
                    )

            # Capacity
            capacity_min = venue_data.get("minCapacity")
            capacity_max = venue_data.get("maxCapacity")

        elif v.source == "sb":
            # Parse SB pricing (structured by day)
            for day_pricing in data.get("pricing", []):
                day = day_pricing.get("day", "")

                if lunch := day_pricing.get("lunch_price"):
                    parsed = parse_sb_price(lunch)
                    if parsed.price_min:
                        parsed.days.extend(extract_days_from_string(day))
                        pricing.append(
                            PricingTier(
                                name=f"Lunch ({day})",
                                price=parsed,
                                time_of_day="lunch",
                                tables_min=parse_tables(day_pricing.get("tables")),
                            )
                        )

                if dinner := day_pricing.get("dinner_price"):
                    parsed = parse_sb_price(dinner)
                    if parsed.price_min:
                        parsed.days.extend(extract_days_from_string(day))
                        pricing.append(
                            PricingTier(
                                name=f"Dinner ({day})",
                                price=parsed,
                                time_of_day="dinner",
                                tables_min=parse_tables(day_pricing.get("tables")),
                            )
                        )

            # Contact info
            phone = data.get("phone")
            email = data.get("email")

    # Calculate price range
    price_values = [
        p.price.price_min for p in pricing if p.price.price_min is not None
    ]
    price_range_min = min(price_values) if price_values else None
    price_range_max = max(price_values) if price_values else None

    # Determine most common price unit
    unit_counts: dict[str, int] = {}
    for p in pricing:
        unit_counts[p.price.price_unit] = unit_counts.get(p.price.price_unit, 0) + 1

    if unit_counts:
        price_unit = max(unit_counts.items(), key=lambda x: x[1])[0]
    else:
        price_unit = None

    # Build unified venue
    unified = UnifiedVenue(
        id=venue_id,
        name=canonical_name,
        pricing=pricing,
        rooms=rooms,
        pdfs=pdfs,
        rating=rating,
        sources=sources,
        source_ids=[v.source for v in venues],
        price_range_min=price_range_min,
        price_range_max=price_range_max,
        price_unit=price_unit,
    )

    return unified


def extract_days_from_string(day_str: str) -> list[str]:
    """Extract day names from a string like 'Mon–Thu'."""
    days = []
    day_str_lower = day_str.lower()

    day_mapping = {
        "mon": "monday",
        "tue": "tuesday",
        "wed": "wednesday",
        "thu": "thursday",
        "fri": "friday",
        "sat": "saturday",
        "sun": "sunday",
    }

    for short, full in day_mapping.items():
        if short in day_str_lower:
            days.append(full)

    return days


def parse_tables(tables_str: str | None) -> int | None:
    """Parse table count from string like '10-40'."""
    if not tables_str:
        return None

    # Extract first number
    match = re.search(r"(\d+)", str(tables_str))
    if match:
        return int(match.group(1))

    return None


def deduplicate_venues(data_dir: Path) -> list[UnifiedVenue]:
    """Main entry point for venue deduplication.

    Loads venues from all sources, finds duplicates, and merges them.

    Args:
        data_dir: Path to data directory containing source subdirectories

    Returns:
        List of unified venues with duplicates merged
    """
    # Load all venues
    all_venues: list[RawVenue] = []
    sources = ["bb", "bly", "wd", "twn", "sb"]

    for source in sources:
        venues = load_venues_from_source(data_dir, source)
        all_venues.extend(venues)
        print(f"Loaded {len(venues)} venues from {source}")

    print(f"\nTotal venues: {len(all_venues)}")

    # Find matches
    matches = find_matches(all_venues)
    print(f"Found {len(matches)} potential duplicates")

    # Group venues into clusters
    from collections import defaultdict

    venue_clusters: dict[str, list[RawVenue]] = defaultdict(list)
    venue_to_cluster: dict[str, str] = {}

    for v1, v2, match in matches:
        # Check if either venue is already in a cluster
        cluster_id = venue_to_cluster.get(f"{v1.source}:{v1.name}")

        if cluster_id is None:
            cluster_id = venue_to_cluster.get(f"{v2.source}:{v2.name}")

        if cluster_id is None:
            # Create new cluster
            cluster_id = generate_venue_slug(v1.name)

        # Add both venues to cluster
        cluster = venue_clusters[cluster_id]

        v1_key = f"{v1.source}:{v1.name}"
        v2_key = f"{v2.source}:{v2.name}"

        if v1_key not in [f"{v.source}:{v.name}" for v in cluster]:
            cluster.append(v1)
            venue_to_cluster[v1_key] = cluster_id

        if v2_key not in [f"{v.source}:{v.name}" for v in cluster]:
            cluster.append(v2)
            venue_to_cluster[v2_key] = cluster_id

    # Add unmatched venues as single-venue clusters
    for v in all_venues:
        v_key = f"{v.source}:{v.name}"
        if v_key not in venue_to_cluster:
            cluster_id = generate_venue_slug(v.name)
            venue_clusters[cluster_id].append(v)
            venue_to_cluster[v_key] = cluster_id

    # Merge each cluster
    unified_venues: list[UnifiedVenue] = []

    for cluster_id, venues in venue_clusters.items():
        try:
            unified = merge_venues(venues)
            unified_venues.append(unified)
        except Exception as e:
            print(f"Error merging cluster {cluster_id}: {e}")
            # Create simple unified venue from first venue
            v = venues[0]
            unified = UnifiedVenue(
                id=generate_venue_slug(v.name),
                name=v.name,
                sources=[
                    SourceReference(
                        source=v.source,
                        source_id=v.source_id,
                        source_name=v.name,
                        raw_data=v.raw_data,
                    )
                ],
                source_ids=[v.source],
            )
            unified_venues.append(unified)

    # Sort by name
    unified_venues.sort(key=lambda v: v.name)

    return unified_venues


def save_unified_venues(
    venues: list[UnifiedVenue],
    output_path: Path,
) -> None:
    """Save unified venues to JSON file.

    Args:
        venues: List of unified venues
        output_path: Path to output file
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Convert to dict, handling Decimal and datetime
    data = []
    for v in venues:
        v_dict = v.model_dump(mode="json")
        data.append(v_dict)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\nSaved {len(venues)} unified venues to {output_path}")
