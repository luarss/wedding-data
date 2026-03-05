"""Unified Pydantic models for wedding venue and vendor data.

This module defines consistent data structures that can represent data
from all sources (bb, bly, wd, twn, sb) with source-specific fields preserved.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PriceUnit(str, Enum):
    """Unit of pricing."""

    PER_PERSON = "per_person"
    PER_TABLE = "per_table"
    PER_EVENT = "per_event"
    PER_HOUR = "per_hour"
    UNKNOWN = "unknown"


class Currency(str, Enum):
    """Supported currencies."""

    SGD = "SGD"
    MYR = "MYR"
    USD = "USD"


class DayOfWeek(str, Enum):
    """Days of the week for pricing."""

    MONDAY = "monday"
    TUESDAY = "tuesday"
    WEDNESDAY = "wednesday"
    THURSDAY = "thursday"
    FRIDAY = "friday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"
    WEEKDAY = "weekday"  # Mon-Thu
    WEEKEND = "weekend"  # Fri-Sun
    DAILY = "daily"  # All days


class VenueType(str, Enum):
    """Types of wedding venues."""

    HOTEL = "hotel"
    RESTAURANT = "restaurant"
    BANQUET_HALL = "banquet_hall"
    COUNTRY_CLUB = "country_club"
    GARDEN = "garden"
    BEACH = "beach"
    ROOFTOP = "rooftop"
    HERITAGE = "heritage"
    GLASSHOUSE = "glasshouse"
    OUTDOOR = "outdoor"
    INDOOR = "indoor"
    CHURCH = "church"
    TEMPLE = "temple"
    UNIQUE = "unique"
    UNKNOWN = "unknown"


class VendorCategory(str, Enum):
    """Categories of wedding vendors."""

    VENUE = "venue"
    PHOTOGRAPHER = "photographer"
    VIDEOGRAPHER = "videographer"
    CATERER = "caterer"
    FLORIST = "florist"
    DECORATOR = "decorator"
    EMCEE = "emcee"
    MUSICIAN = "musician"
    DJ = "dj"
    MAKEUP_ARTIST = "makeup_artist"
    WEDDING_PLANNER = "wedding_planner"
    BRIDAL_STUDIO = "bridal_studio"
    GROOMSMEN_ATTIRE = "groomsmen_attire"
    CAKE_DESIGNER = "cake_designer"
    INVITATION_DESIGNER = "invitation_designer"
    FAVORS_GIFTS = "favors_gifts"
    TRANSPORTATION = "transportation"
    ACCOMMODATION = "accommodation"
    UNKNOWN = "unknown"


class NormalizedPrice(BaseModel):
    """Normalized price structure parsed from various source formats.

    Examples of raw price formats that can be normalized:
    - "$988–$1588Mon - Sun" -> min=988, max=1588, days=[mon-sun]
    - "$1688++ per table" -> min=1688, max=1688, service_charge=True
    - "$238-$298++/pax" -> min=238, max=298, unit=per_person
    - "$16,200++" -> min=16200, max=16200, unit=per_event
    """

    model_config = ConfigDict(frozen=True)

    price_raw: str = Field(description="Original price string from source")
    price_min: Decimal | None = Field(None, description="Minimum price value")
    price_max: Decimal | None = Field(None, description="Maximum price value (same as min if single price)")
    price_unit: PriceUnit = Field(default=PriceUnit.UNKNOWN, description="Unit of pricing")
    currency: Currency = Field(default=Currency.SGD, description="Currency code")
    service_charge: bool | None = Field(None, description="Whether ++ (service charge + GST) applies")
    gst_included: bool | None = Field(None, description="Whether GST is included in the price")
    days: list[DayOfWeek] = Field(default_factory=list, description="Applicable days of week")
    time_of_day: str | None = Field(None, description="Lunch, dinner, or specific time")
    notes: str | None = Field(None, description="Additional price notes")

    @field_validator("price_min", "price_max", mode="before")
    @classmethod
    def convert_int_to_decimal(cls, v: int | float | Decimal | None) -> Decimal | None:
        """Convert integer or float values to Decimal."""
        if v is None:
            return None
        if isinstance(v, int):
            return Decimal(v)
        if isinstance(v, float):
            return Decimal(str(v))  # Avoid float precision issues
        return v


class PricingTier(BaseModel):
    """A pricing tier for a specific day/time combination."""

    model_config = ConfigDict(frozen=True)

    name: str | None = Field(None, description="Name of the tier/package")
    price: NormalizedPrice
    capacity_min: int | None = Field(None, description="Minimum capacity for this tier")
    capacity_max: int | None = Field(None, description="Maximum capacity for this tier")
    tables_min: int | None = Field(None, description="Minimum tables (for per-table pricing)")
    tables_max: int | None = Field(None, description="Maximum tables (for per-table pricing)")
    menu_type: str | None = Field(None, description="Menu type (Chinese, Western, Buffet, etc.)")
    inclusions: list[str] = Field(default_factory=list, description="What's included in this price")


class Location(BaseModel):
    """Normalized location information."""

    model_config = ConfigDict(frozen=True)

    address: str | None = Field(None, description="Full street address")
    city: str | None = Field(None, description="City or town")
    state: str | None = Field(None, description="State or region")
    postal_code: str | None = Field(None, description="Postal/ZIP code")
    country: str = Field(default="Singapore", description="Country name")
    latitude: float | None = Field(None, description="GPS latitude")
    longitude: float | None = Field(None, description="GPS longitude")
    neighborhood: str | None = Field(None, description="Neighborhood or district (e.g., Orchard, Sentosa)")


class Rating(BaseModel):
    """Rating information from various sources."""

    model_config = ConfigDict(frozen=True)

    overall: float | None = Field(None, ge=0, le=5, description="Overall rating (0-5)")
    venue: float | None = Field(None, ge=0, le=5, description="Venue rating")
    service: float | None = Field(None, ge=0, le=5, description="Service rating")
    food: float | None = Field(None, ge=0, le=5, description="Food rating")
    value: float | None = Field(None, ge=0, le=5, description="Value for money rating")
    review_count: int | None = Field(None, ge=0, description="Number of reviews")


class ContactInfo(BaseModel):
    """Contact information for a venue or vendor."""

    model_config = ConfigDict(frozen=True)

    phone: str | None = Field(None, description="Primary phone number")
    phone_secondary: str | None = Field(None, description="Secondary phone number")
    email: str | None = Field(None, description="Email address")
    website: str | None = Field(None, description="Website URL")
    contact_form: str | None = Field(None, description="Contact form URL")


class SourceReference(BaseModel):
    """Reference to the original source data."""

    model_config = ConfigDict(frozen=True)

    source: str = Field(description="Source identifier (bb, bly, wd, twn, sb)")
    source_id: str | None = Field(None, description="Original ID from source")
    source_name: str | None = Field(None, description="Name as it appears in source")
    url: str | None = Field(None, description="URL to the source page")
    scraped_at: datetime | None = Field(None, description="When this data was scraped")
    raw_data: dict[str, Any] | None = Field(None, description="Original raw data for debugging")


class UnifiedVenue(BaseModel):
    """Unified venue model combining data from all sources.

    This model represents a deduplicated venue that may have data
    merged from multiple sources (e.g., Andaz Singapore from bb, sb, wd).
    """

    model_config = ConfigDict(frozen=True)

    # Unique identifier (generated from deduplication)
    id: str = Field(description="Unique venue ID (slug format)")
    name: str = Field(description="Canonical venue name")

    # Core venue information
    description: str | None = Field(None, description="Venue description")
    venue_types: list[VenueType] = Field(default_factory=list, description="Types of venue")
    location: Location | None = Field(None, description="Location information")
    contact: ContactInfo | None = Field(None, description="Contact information")

    # Capacity and space
    capacity_min: int | None = Field(None, ge=0, description="Minimum guest capacity")
    capacity_max: int | None = Field(None, ge=0, description="Maximum guest capacity")
    tables_min: int | None = Field(None, ge=0, description="Minimum tables")
    tables_max: int | None = Field(None, ge=0, description="Maximum tables")
    room_count: int | None = Field(None, ge=0, description="Number of event spaces/rooms")
    rooms: list["VenueRoom"] = Field(default_factory=list, description="Individual room details")

    # Pricing
    pricing: list[PricingTier] = Field(default_factory=list, description="All pricing tiers")
    price_range_min: Decimal | None = Field(None, description="Lowest price found")
    price_range_max: Decimal | None = Field(None, description="Highest price found")
    price_unit: PriceUnit | None = Field(None, description="Most common price unit")

    # Ratings and reviews
    rating: Rating | None = Field(None, description="Aggregated ratings")

    # Amenities and features
    amenities: list[str] = Field(default_factory=list, description="List of amenities")
    cuisines: list[str] = Field(default_factory=list, description="Available cuisines")
    features: list[str] = Field(default_factory=list, description="Special features")

    # Media
    images: list[str] = Field(default_factory=list, description="Image URLs")
    pdfs: list["PDFAttachment"] = Field(default_factory=list, description="Attached PDF documents")

    # Source tracking
    sources: list[SourceReference] = Field(default_factory=list, description="Data from each source")
    source_ids: list[str] = Field(default_factory=list, description="List of source identifiers")

    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow, description="When this record was created")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="When this record was last updated")

    def get_best_price(self) -> NormalizedPrice | None:
        """Get the lowest available price from all pricing tiers."""
        if not self.pricing:
            return None
        valid_prices = [p for p in self.pricing if p.price.price_min is not None]
        if not valid_prices:
            return None
        return min(valid_prices, key=lambda p: p.price.price_min or Decimal("Infinity")).price

    def get_primary_source(self) -> SourceReference | None:
        """Get the primary (most recent/best) source reference."""
        if not self.sources:
            return None
        # Prefer sources with most data, then most recent
        return sorted(
            self.sources,
            key=lambda s: (s.scraped_at or datetime.min),
            reverse=True,
        )[0]


class VenueRoom(BaseModel):
    """Individual room or event space within a venue."""

    model_config = ConfigDict(frozen=True)

    name: str = Field(description="Room name")
    description: str | None = Field(None, description="Room description")
    capacity_min: int | None = Field(None, ge=0)
    capacity_max: int | None = Field(None, ge=0)
    tables_min: int | None = Field(None, ge=0)
    tables_max: int | None = Field(None, ge=0)
    room_types: list[str] = Field(default_factory=list, description="Room types (indoor, outdoor, etc.)")
    features: list[str] = Field(default_factory=list, description="Room-specific features")
    pricing: list[PricingTier] = Field(default_factory=list, description="Room-specific pricing")
    images: list[str] = Field(default_factory=list, description="Room images")


class PDFAttachment(BaseModel):
    """Attached PDF document (usually a price list or brochure)."""

    model_config = ConfigDict(frozen=True)

    filename: str = Field(description="PDF filename")
    url: str = Field(description="Original URL")
    local_path: str | None = Field(None, description="Local file path if downloaded")
    title: str | None = Field(None, description="Document title/description")


class UnifiedVendor(BaseModel):
    """Unified vendor model for non-venue vendors (photographers, florists, etc.)."""

    model_config = ConfigDict(frozen=True)

    id: str = Field(description="Unique vendor ID")
    name: str = Field(description="Canonical vendor name")
    category: VendorCategory = Field(description="Primary category")
    categories: list[VendorCategory] = Field(default_factory=list, description="All applicable categories")

    description: str | None = Field(None, description="Vendor description")
    specialties: list[str] = Field(default_factory=list, description="Specialties or services offered")

    location: Location | None = Field(None, description="Location information")
    contact: ContactInfo | None = Field(None, description="Contact information")

    # Pricing
    pricing: list[PricingTier] = Field(default_factory=list, description="Pricing information")
    price_range_min: Decimal | None = Field(None, description="Starting price")
    price_range_max: Decimal | None = Field(None, description="Maximum/common price")

    # Ratings and reviews
    rating: Rating | None = Field(None, description="Aggregated ratings")

    # Portfolio
    images: list[str] = Field(default_factory=list, description="Portfolio images")
    videos: list[str] = Field(default_factory=list, description="Video URLs")

    # Source tracking
    sources: list[SourceReference] = Field(default_factory=list, description="Data from each source")
    source_ids: list[str] = Field(default_factory=list, description="List of source identifiers")

    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class PriceHistoryEntry(BaseModel):
    """Historical price record for tracking price changes over time."""

    model_config = ConfigDict(frozen=True)

    venue_id: str = Field(description="Venue ID")
    price: NormalizedPrice
    recorded_at: datetime = Field(default_factory=datetime.utcnow, description="When this price was recorded")
    source: str = Field(description="Source of this price record")
    notes: str | None = Field(None, description="Additional context")


class VenueMatch(BaseModel):
    """Result of venue deduplication matching."""

    model_config = ConfigDict(frozen=True)

    venue_id_1: str = Field(description="First venue ID")
    venue_id_2: str = Field(description="Second venue ID")
    name_similarity: float = Field(ge=0, le=100, description="Name similarity score (0-100)")
    location_similarity: float = Field(ge=0, le=100, description="Location similarity score (0-100)")
    overall_score: float = Field(ge=0, le=100, description="Overall match score (0-100)")
    is_match: bool = Field(description="Whether these venues should be merged")
    reason: str | None = Field(None, description="Reason for match decision")


# Resolve forward references
UnifiedVenue.model_rebuild()
