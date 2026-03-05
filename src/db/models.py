"""Database models for wedding venue data.

Uses SQLAlchemy 2.0 with type hints and dataclass-style models.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    create_engine,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    mapped_column,
    relationship,
    sessionmaker,
)


class Base(DeclarativeBase, MappedAsDataclass):
    """Base class for all models."""

    pass


class Venue(Base):
    """Wedding venue entity."""

    __tablename__ = "venues"

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False, index=True)
    slug: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Location
    address: Mapped[str | None] = mapped_column(Text, nullable=True)
    city: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
    state: Mapped[str | None] = mapped_column(String(100), nullable=True)
    postal_code: Mapped[str | None] = mapped_column(String(20), nullable=True)
    country: Mapped[str] = mapped_column(String(100), default="Singapore")
    latitude: Mapped[Decimal | None] = mapped_column(Numeric(10, 8), nullable=True)
    longitude: Mapped[Decimal | None] = mapped_column(Numeric(11, 8), nullable=True)
    neighborhood: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)

    # Capacity
    capacity_min: Mapped[int | None] = mapped_column(Integer, nullable=True)
    capacity_max: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tables_min: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tables_max: Mapped[int | None] = mapped_column(Integer, nullable=True)
    room_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Pricing summary
    price_range_min: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    price_range_max: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    price_unit: Mapped[str | None] = mapped_column(String(20), nullable=True)
    currency: Mapped[str] = mapped_column(String(3), default="SGD")

    # Ratings
    rating_overall: Mapped[Decimal | None] = mapped_column(Numeric(2, 1), nullable=True)
    rating_venue: Mapped[Decimal | None] = mapped_column(Numeric(2, 1), nullable=True)
    rating_service: Mapped[Decimal | None] = mapped_column(Numeric(2, 1), nullable=True)
    rating_food: Mapped[Decimal | None] = mapped_column(Numeric(2, 1), nullable=True)
    rating_value: Mapped[Decimal | None] = mapped_column(Numeric(2, 1), nullable=True)
    review_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Contact
    phone: Mapped[str | None] = mapped_column(String(100), nullable=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    website: Mapped[str | None] = mapped_column(String(500), nullable=True)

    # Features
    venue_types: Mapped[list[str]] = mapped_column(JSON, default=list)
    amenities: Mapped[list[str]] = mapped_column(JSON, default=list)
    cuisines: Mapped[list[str]] = mapped_column(JSON, default=list)
    features: Mapped[list[str]] = mapped_column(JSON, default=list)

    # Media
    images: Mapped[list[str]] = mapped_column(JSON, default=list)

    # Metadata
    created_at: Mapped[datetime] = mapped_column(DateTime, default_factory=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default_factory=datetime.utcnow,
        onupdate=datetime.utcnow,
    )
    source_count: Mapped[int] = mapped_column(Integer, default=0)

    # Relationships
    rooms: Mapped[list["VenueRoom"]] = relationship(
        back_populates="venue",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    prices: Mapped[list["Price"]] = relationship(
        back_populates="venue",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    price_history: Mapped[list["PriceHistory"]] = relationship(
        back_populates="venue",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    sources: Mapped[list["VenueSource"]] = relationship(
        back_populates="venue",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    pdfs: Mapped[list["PDFDocument"]] = relationship(
        back_populates="venue",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class VenueRoom(Base):
    """Individual room or event space within a venue."""

    __tablename__ = "venue_rooms"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    venue_id: Mapped[str] = mapped_column(ForeignKey("venues.id"), nullable=False)

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    capacity_min: Mapped[int | None] = mapped_column(Integer, nullable=True)
    capacity_max: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tables_min: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tables_max: Mapped[int | None] = mapped_column(Integer, nullable=True)

    room_types: Mapped[list[str]] = mapped_column(JSON, default=list)
    features: Mapped[list[str]] = mapped_column(JSON, default=list)
    images: Mapped[list[str]] = mapped_column(JSON, default=list)

    # Relationship
    venue: Mapped[Venue] = relationship(back_populates="rooms")


class Price(Base):
    """Pricing tier for a venue."""

    __tablename__ = "prices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    venue_id: Mapped[str] = mapped_column(ForeignKey("venues.id"), nullable=False)

    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Price values
    price_raw: Mapped[str] = mapped_column(String(500), nullable=False)
    price_min: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    price_max: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    price_unit: Mapped[str] = mapped_column(String(20), default="unknown")
    currency: Mapped[str] = mapped_column(String(3), default="SGD")

    # Price modifiers
    service_charge: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    gst_included: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # Applicability
    days: Mapped[list[str]] = mapped_column(JSON, default=list)
    time_of_day: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # Capacity for this price tier
    capacity_min: Mapped[int | None] = mapped_column(Integer, nullable=True)
    capacity_max: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tables_min: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tables_max: Mapped[int | None] = mapped_column(Integer, nullable=True)

    menu_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    inclusions: Mapped[list[str]] = mapped_column(JSON, default=list)

    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Source tracking
    source: Mapped[str] = mapped_column(String(10), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default_factory=datetime.utcnow)

    # Relationship
    venue: Mapped[Venue] = relationship(back_populates="prices")


class PriceHistory(Base):
    """Historical price record for tracking changes over time."""

    __tablename__ = "price_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    venue_id: Mapped[str] = mapped_column(ForeignKey("venues.id"), nullable=False)

    price_raw: Mapped[str] = mapped_column(String(500), nullable=False)
    price_min: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    price_max: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    price_unit: Mapped[str] = mapped_column(String(20), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), default="SGD")

    recorded_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(10), nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Relationship
    venue: Mapped[Venue] = relationship(back_populates="price_history")


class VenueSource(Base):
    """Source reference for venue data provenance."""

    __tablename__ = "venue_sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    venue_id: Mapped[str] = mapped_column(ForeignKey("venues.id"), nullable=False)

    source: Mapped[str] = mapped_column(String(10), nullable=False)
    source_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_name: Mapped[str | None] = mapped_column(String(500), nullable=True)
    url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    scraped_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Raw data for debugging
    raw_data: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    # Relationship
    venue: Mapped[Venue] = relationship(back_populates="sources")


class PDFDocument(Base):
    """PDF attachment (price list or brochure)."""

    __tablename__ = "pdf_documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    venue_id: Mapped[str] = mapped_column(ForeignKey("venues.id"), nullable=False)

    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    url: Mapped[str] = mapped_column(String(500), nullable=False)
    local_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Relationship
    venue: Mapped[Venue] = relationship(back_populates="pdfs")


# FTS5 virtual table for full-text search
FTS5_SCHEMA = """
CREATE VIRTUAL TABLE IF NOT EXISTS venue_search USING fts5(
    venue_id UNINDEXED,
    name,
    description,
    address,
    neighborhood,
    amenities,
    cuisines,
    features,
    content='',
    content_rowid='rowid'
);

-- Trigger to keep FTS index in sync
CREATE TRIGGER IF NOT EXISTS venue_search_insert AFTER INSERT ON venues BEGIN
    INSERT INTO venue_search(rowid, venue_id, name, description, address, neighborhood, amenities, cuisines, features)
    VALUES (new.rowid, new.id, new.name, new.description, new.address, new.neighborhood, new.amenities, new.cuisines, new.features);
END;

CREATE TRIGGER IF NOT EXISTS venue_search_delete AFTER DELETE ON venues BEGIN
    INSERT INTO venue_search(venue_search, rowid, venue_id, name, description, address, neighborhood, amenities, cuisines, features)
    VALUES ('delete', old.rowid, old.id, old.name, old.description, old.address, old.neighborhood, old.amenities, old.cuisines, old.features);
END;

CREATE TRIGGER IF NOT EXISTS venue_search_update AFTER UPDATE ON venues BEGIN
    INSERT INTO venue_search(venue_search, rowid, venue_id, name, description, address, neighborhood, amenities, cuisines, features)
    VALUES ('delete', old.rowid, old.id, old.name, old.description, old.address, old.neighborhood, old.amenities, old.cuisines, old.features);
    INSERT INTO venue_search(rowid, venue_id, name, description, address, neighborhood, amenities, cuisines, features)
    VALUES (new.rowid, new.id, new.name, new.description, new.address, new.neighborhood, new.amenities, new.cuisines, new.features);
END;
"""


def create_database(db_path: str = "data/wedding.db") -> tuple[Any, Any]:
    """Create database engine and session factory.

    Args:
        db_path: Path to SQLite database file

    Returns:
        Tuple of (engine, sessionmaker)
    """
    engine = create_engine(f"sqlite:///{db_path}", echo=False)

    # Create tables
    Base.metadata.create_all(engine)

    # Create FTS5 virtual table and triggers
    with engine.connect() as conn:
        conn.execute(FTS5_SCHEMA)
        conn.commit()

    Session = sessionmaker(bind=engine)
    return engine, Session


def get_db_session(db_path: str = "data/wedding.db"):
    """Get a database session.

    Args:
        db_path: Path to SQLite database file

    Returns:
        SQLAlchemy session
    """
    engine = create_engine(f"sqlite:///{db_path}")
    Session = sessionmaker(bind=engine)
    return Session()
