"""Price parsing and normalization for wedding venue data.

This module handles the messy price formats from various sources and normalizes
them into structured data with min/max values, currency, service charges, etc.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Pattern

from .models import Currency, DayOfWeek, NormalizedPrice, PriceUnit


# Price patterns for different formats
PRICE_PATTERNS = {
    # Range: $988–$1588 or $988-$1588 or $988 to $1588
    "range": re.compile(
        r"\$?\s*(?P<min>[\d,]+(?:\.\d{2})?)\s*[-–—to]+\s*\$?\s*(?P<max>[\d,]+(?:\.\d{2})?)",
        re.IGNORECASE,
    ),
    # Single price: $1688 or $1,688 or 1688
    "single": re.compile(
        r"\$?\s*(?P<price>[\d,]+(?:\.\d{2})?)",
        re.IGNORECASE,
    ),
    # Per person: /pax, per pax, per person, pp
    "per_person": re.compile(
        r"(?:\/|\s+)(?:pax|person|pp)(?:\b|$)",
        re.IGNORECASE,
    ),
    # Per table: /table, per table
    "per_table": re.compile(
        r"(?:\/|\s+)(?:table|tbl)(?:s?\b|$)",
        re.IGNORECASE,
    ),
    # Service charge indicator: ++
    "service_charge": re.compile(r"\+\+|plus\s*plus", re.IGNORECASE),
    # GST indicator
    "gst": re.compile(
        r"(?:\+\s*GST|plus\s*GST|incl\.?\s*GST|GST\s*(?:incl|included))",
        re.IGNORECASE,
    ),
    # Day patterns
    "mon_thu": re.compile(r"mon\s*[-–]\s*thu(?:rs?)?", re.IGNORECASE),
    "fri_sun": re.compile(r"fri\s*[-–]\s*sun(?:day)?", re.IGNORECASE),
    "weekday": re.compile(r"weekday|mon\s*[-–]\s*fri", re.IGNORECASE),
    "weekend": re.compile(r"weekend|sat\s*[-–]\s*sun", re.IGNORECASE),
    "daily": re.compile(r"daily|mon\s*[-–]\s*sun|every\s*day", re.IGNORECASE),
}

# Day name patterns for individual days
DAY_PATTERNS: dict[DayOfWeek, Pattern] = {
    DayOfWeek.MONDAY: re.compile(r"\bmon(?:day)?\b", re.IGNORECASE),
    DayOfWeek.TUESDAY: re.compile(r"\btue(?:sday)?\b", re.IGNORECASE),
    DayOfWeek.WEDNESDAY: re.compile(r"\bwed(?:nesday)?\b", re.IGNORECASE),
    DayOfWeek.THURSDAY: re.compile(r"\bthu(?:rsday)?\b", re.IGNORECASE),
    DayOfWeek.FRIDAY: re.compile(r"\bfri(?:day)?\b", re.IGNORECASE),
    DayOfWeek.SATURDAY: re.compile(r"\bsat(?:urday)?\b", re.IGNORECASE),
    DayOfWeek.SUNDAY: re.compile(r"\bsun(?:day)?\b", re.IGNORECASE),
}

# Time of day patterns
TIME_PATTERNS = {
    "lunch": re.compile(r"lunch|luncheon|noon", re.IGNORECASE),
    "dinner": re.compile(r"dinner|evening|night", re.IGNORECASE),
    "full_day": re.compile(r"full\s*day|whole\s*day|24\s*hour", re.IGNORECASE),
}


def parse_price_value(price_str: str) -> Decimal | None:
    """Parse a price string into a Decimal.

    Args:
        price_str: Price string like "$1,688.50" or "1688"

    Returns:
        Decimal value or None if parsing fails
    """
    if not price_str:
        return None

    # Remove currency symbols, spaces, and commas
    cleaned = price_str.replace("$", "").replace(",", "").replace(" ", "").strip()

    # Handle empty or non-numeric strings
    if not cleaned or not any(c.isdigit() for c in cleaned):
        return None

    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def extract_price_range(price_str: str) -> tuple[Decimal | None, Decimal | None]:
    """Extract min and max prices from a price string.

    Args:
        price_str: Price string like "$988–$1588" or "$1688"

    Returns:
        Tuple of (min_price, max_price). For single prices, both values are the same.
    """
    if not price_str:
        return None, None

    # Try range pattern first
    if match := PRICE_PATTERNS["range"].search(price_str):
        min_val = parse_price_value(match.group("min"))
        max_val = parse_price_value(match.group("max"))
        return min_val, max_val

    # Try single price
    if match := PRICE_PATTERNS["single"].search(price_str):
        price = parse_price_value(match.group("price"))
        return price, price

    return None, None


def detect_price_unit(price_str: str) -> PriceUnit:
    """Detect the price unit from the string.

    Args:
        price_str: Price string to analyze

    Returns:
        Detected PriceUnit
    """
    if not price_str:
        return PriceUnit.UNKNOWN

    if PRICE_PATTERNS["per_person"].search(price_str):
        return PriceUnit.PER_PERSON

    if PRICE_PATTERNS["per_table"].search(price_str):
        return PriceUnit.PER_TABLE

    # Check for event/flat fee indicators
    event_indicators = [
        r"flat\s*fee",
        r"starting\s*from",
        r"from\s+\$",
        r"(?:package|event)\s*(?:price|fee)",
    ]
    for pattern in event_indicators:
        if re.search(pattern, price_str, re.IGNORECASE):
            return PriceUnit.PER_EVENT

    return PriceUnit.UNKNOWN


def detect_service_charge(price_str: str) -> bool | None:
    """Detect if service charge (++ or ++++) applies.

    Args:
        price_str: Price string to analyze

    Returns:
        True if ++ detected, False if explicitly stated as included,
        None if unclear
    """
    if not price_str:
        return None

    upper = price_str.upper()

    # Check for ++
    if "++" in upper or "PLUS PLUS" in upper:
        return True

    # Check for ++++ (service charge + GST)
    if "++++" in upper:
        return True

    # Check if explicitly stated as nett or all-inclusive
    nett_indicators = [r"nett\b", r"all.inclusive", r"inclusive\s*of", r"incl\s*\+\+"]
    for pattern in nett_indicators:
        if re.search(pattern, price_str, re.IGNORECASE):
            return False

    return None


def detect_gst(price_str: str) -> bool | None:
    """Detect GST inclusion status.

    Args:
        price_str: Price string to analyze

    Returns:
        True if GST is explicitly included, False if likely excluded,
        None if unclear
    """
    if not price_str:
        return None

    # Check for GST included
    gst_included = re.compile(r"(?:incl\.?\s*GST|GST\s*(?:incl|included)|incl\.?\s*tax)", re.IGNORECASE)
    if gst_included.search(price_str):
        return True

    # Check for GST excluded (+GST or ++ which includes GST)
    if "+GST" in price_str.upper() or "PLUS GST" in price_str.upper():
        return False

    # ++ usually means service charge + GST
    if "++" in price_str:
        return False

    return None


def extract_days(price_str: str) -> list[DayOfWeek]:
    """Extract applicable days of week from price string.

    Args:
        price_str: Price string containing day information

    Returns:
        List of DayOfWeek values
    """
    if not price_str:
        return []

    days: set[DayOfWeek] = set()

    # Check for daily pattern first
    if PRICE_PATTERNS["daily"].search(price_str):
        return list(DayOfWeek)[:7]  # All 7 days

    # Check for weekday pattern (Mon-Fri)
    if PRICE_PATTERNS["weekday"].search(price_str) or PRICE_PATTERNS["mon_thu"].search(price_str):
        days.update([DayOfWeek.MONDAY, DayOfWeek.TUESDAY, DayOfWeek.WEDNESDAY, DayOfWeek.THURSDAY])
        # Check if Friday is included
        if not re.search(r"mon\s*[-–]\s*thu", price_str, re.IGNORECASE):
            days.add(DayOfWeek.FRIDAY)

    # Check for weekend pattern (Fri-Sun or Sat-Sun)
    if PRICE_PATTERNS["weekend"].search(price_str):
        if "fri" in price_str.lower():
            days.add(DayOfWeek.FRIDAY)
        days.update([DayOfWeek.SATURDAY, DayOfWeek.SUNDAY])

    # Check for individual days
    for day, pattern in DAY_PATTERNS.items():
        if pattern.search(price_str):
            days.add(day)

    return sorted(days, key=lambda d: list(DayOfWeek).index(d))


def extract_time_of_day(price_str: str) -> str | None:
    """Extract time of day (lunch/dinner) from price string.

    Args:
        price_str: Price string to analyze

    Returns:
        Time of day description or None
    """
    if not price_str:
        return None

    for time_name, pattern in TIME_PATTERNS.items():
        if pattern.search(price_str):
            return time_name

    return None


def parse_price(price_str: str, currency: Currency = Currency.SGD) -> NormalizedPrice:
    """Parse a price string into a normalized format.

    This is the main entry point for price parsing. It handles various formats:
    - "$988–$1588Mon - Sun" -> range with days
    - "$1688++ per table" -> single price with service charge
    - "$238-$298++/pax" -> per person range
    - "$16,200++" -> flat fee

    Args:
        price_str: Raw price string from source
        currency: Currency code (default: SGD)

    Returns:
        NormalizedPrice with all extracted information

    Examples:
        >>> parse_price("$988–$1588Mon - Sun")
        NormalizedPrice(price_raw='$988–$1588Mon - Sun', price_min=Decimal('988'), ...)

        >>> parse_price("$1688++ per table")
        NormalizedPrice(price_raw='$1688++ per table', price_min=Decimal('1688'), ...)
    """
    if not price_str or not isinstance(price_str, str):
        return NormalizedPrice(
            price_raw=str(price_str) if price_str else "",
            price_min=None,
            price_max=None,
            price_unit=PriceUnit.UNKNOWN,
            currency=currency,
        )

    # Clean the string
    cleaned = price_str.strip()

    # Extract price range
    price_min, price_max = extract_price_range(cleaned)

    # Detect unit
    unit = detect_price_unit(cleaned)

    # Detect service charge
    service_charge = detect_service_charge(cleaned)

    # Detect GST
    gst_included = detect_gst(cleaned)

    # Extract days
    days = extract_days(cleaned)

    # Extract time of day
    time_of_day = extract_time_of_day(cleaned)

    # Build notes from what we couldn't parse
    notes = None
    if "++" in cleaned and service_charge is None:
        notes = "May include service charge and GST"

    return NormalizedPrice(
        price_raw=price_str,
        price_min=price_min,
        price_max=price_max,
        price_unit=unit,
        currency=currency,
        service_charge=service_charge,
        gst_included=gst_included,
        days=days,
        time_of_day=time_of_day,
        notes=notes,
    )


def parse_bb_price(price_str: str) -> NormalizedPrice:
    """Parse BlissfulBrides price format.

    BB prices often look like:
    - "$988–$1588Mon - Sun"
    - "$$1888 ++Mon - Fri"
    - "$1484–$1484Mon - Sun"
    """
    if not price_str:
        return NormalizedPrice(price_raw="", currency=Currency.SGD)

    # Handle the double dollar sign issue
    cleaned = price_str.replace("$$", "$")

    return parse_price(cleaned, Currency.SGD)


def parse_bly_price(price_str: str) -> NormalizedPrice:
    """Parse Bridely price format.

    Bridely prices look like:
    - "$238-$298++/pax"
    - "$120++/pax"
    """
    return parse_price(price_str, Currency.SGD)


def parse_wd_price(price_str: str) -> NormalizedPrice:
    """Parse Wedded.sg price format.

    Wedded prices look like:
    - "$16,200++"
    - "$12,960++"
    """
    return parse_price(price_str, Currency.SGD)


def parse_twn_price(min_price: str | int | None, max_price: str | int | None, currency: Currency = Currency.MYR) -> NormalizedPrice:
    """Parse The Wedding Notebook price format.

    TWN provides min_price and max_price as separate fields in MYR.

    Args:
        min_price: Minimum price value
        max_price: Maximum price value
        currency: Currency (default: MYR)

    Returns:
        NormalizedPrice
    """
    min_val = parse_price_value(str(min_price)) if min_price is not None else None
    max_val = parse_price_value(str(max_price)) if max_price is not None else None

    # Build raw string for reference
    raw_parts = []
    if min_price is not None:
        raw_parts.append(f"{min_price}")
    if max_price is not None:
        raw_parts.append(f"{max_price}")
    raw = " - ".join(raw_parts) if len(raw_parts) > 1 else raw_parts[0] if raw_parts else ""

    return NormalizedPrice(
        price_raw=raw,
        price_min=min_val,
        price_max=max_val,
        price_unit=PriceUnit.PER_EVENT,  # Usually per event for venues
        currency=currency,
    )


def parse_sb_price(price_str: str) -> NormalizedPrice:
    """Parse SingaporeBrides price format.

    SB prices look similar to BB:
    - "$1688++ (Mon-Thu)"
    - "$2088++ (Fri-Sun)"
    """
    return parse_price(price_str, Currency.SGD)


def format_price_display(price: NormalizedPrice) -> str:
    """Format a normalized price for display.

    Args:
        price: NormalizedPrice to format

    Returns:
        Human-readable price string
    """
    if price.price_min is None and price.price_max is None:
        return "Price on request"

    parts = []

    # Add currency symbol
    if price.currency == Currency.SGD:
        parts.append("$")
    elif price.currency == Currency.MYR:
        parts.append("RM ")
    else:
        parts.append(f"{price.currency} ")

    # Add price range
    if price.price_min == price.price_max or price.price_max is None:
        if price.price_min is not None:
            parts.append(f"{price.price_min:,.0f}")
    else:
        if price.price_min is not None and price.price_max is not None:
            parts.append(f"{price.price_min:,.0f} - {price.price_max:,.0f}")
        elif price.price_max is not None:
            parts.append(f"up to {price.price_max:,.0f}")
        elif price.price_min is not None:
            parts.append(f"from {price.price_min:,.0f}")

    # Add unit
    if price.price_unit == PriceUnit.PER_PERSON:
        parts.append(" / pax")
    elif price.price_unit == PriceUnit.PER_TABLE:
        parts.append(" / table")

    # Add service charge indicator
    if price.service_charge:
        parts.append("++")

    result = "".join(parts)

    # Add days if present
    if price.days and len(price.days) < 7:
        day_names = [d.value[:3].title() for d in price.days]
        result += f" ({', '.join(day_names)})"

    return result
