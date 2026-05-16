#!/usr/bin/env python3
"""
venue_filter.py — Filter Singapore wedding venues across BB, BLY, WD, SB data sources.

Commands:
  search   Find venues matching criteria (budget, guests, day, style)
  compare  Look up specific venues by name, returning pricing + local PDF paths
  budget   Analyze how many venues fit a given budget and guest count
"""

from __future__ import annotations

import argparse
import json
import math
import re
import difflib
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[4]


# ─── Price Utilities ──────────────────────────────────────────────────────────

def parse_price_range(price_str: str) -> tuple[int, int] | None:
    """Extract (min, max) dollar amounts from any price string format.

    Handles all formats seen across data sources:
      "$988–$1588Mon - Sun"            → (988, 1588)
      "$2144–$2144Mon - Thu$2264–$2264Fri - Sun" → (2144, 2264)
      "$68++/pax"                      → (68, 68)
      "$148-$168++/pax"                → (148, 168)
      "$16,200++"                      → (16200, 16200)
      "$988–$1588"                     → (988, 1588)
    """
    if not price_str:
        return None
    amounts = [int(x.replace(",", "")) for x in re.findall(r"\d[\d,]*", price_str)]
    # Filter noise: price amounts in SG wedding context are $100–$100,000
    amounts = [a for a in amounts if 100 <= a <= 100_000]
    return (min(amounts), max(amounts)) if amounts else None


def is_per_pax(price_str: str) -> bool:
    return bool(price_str and "/pax" in price_str.lower())


def effective_total_range(
    price_range: tuple[int, int], per_pax: bool, is_lump_sum: bool, guests: int
) -> tuple[int, int]:
    """Convert a price range to an estimated total cost for given guest count."""
    if is_lump_sum:
        return price_range  # WD packages are already total cost
    if per_pax:
        return (price_range[0] * guests, price_range[1] * guests)
    tables = math.ceil(guests / 10)
    return (price_range[0] * tables, price_range[1] * tables)


# ─── Capacity Utilities ───────────────────────────────────────────────────────

def parse_capacity_pax(venue: dict, source: str) -> tuple[int, int] | None:
    """Return (min_pax, max_pax) for a venue. BB/SB use table counts × 10."""
    if source == "bb":
        tmin, tmax = venue.get("tables_min"), venue.get("tables_max")
        if tmin and tmax:
            try:
                return (int(tmin) * 10, int(tmax) * 10)
            except ValueError:
                pass
    elif source == "bly":
        cap = venue.get("capacity", "")
        nums = [int(x.replace(",", "")) for x in re.findall(r"\d[\d,]*", cap)]
        if len(nums) >= 2:
            return (nums[0], nums[1])
        if len(nums) == 1:
            return (nums[0], nums[0])
    elif source == "wd":
        mins_, maxs_ = [], []
        for room in venue.get("rooms", []):
            for pkg in room.get("packages", []):
                if pkg.get("capacity_min"):
                    mins_.append(pkg["capacity_min"])
                if pkg.get("capacity_max"):
                    maxs_.append(pkg["capacity_max"])
        if mins_ and maxs_:
            return (min(mins_), max(maxs_))
    elif source == "sb":
        tables_str = (
            venue.get("mon_thu_tables")
            or venue.get("saturday_tables")
            or venue.get("friday_tables")
            or ""
        )
        nums = [int(x) for x in re.findall(r"\d+", tables_str)]
        if len(nums) >= 2:
            return (nums[0] * 10, nums[1] * 10)
    return None


# ─── Day-Specific Price Extraction ────────────────────────────────────────────

_SB_DAY_FIELDS = {
    "weekday": ("mon_thu_lunch", "mon_thu_dinner"),
    "friday":  ("friday_lunch",  "friday_dinner"),
    "saturday":("saturday_lunch","saturday_dinner"),
    "sunday":  ("sunday_lunch",  "sunday_dinner"),
}

_WD_DAY_KEYWORDS = {
    "weekday": ["monday", "tuesday", "wednesday", "thursday", "mon", "tue", "wed", "thu", "weekday"],
    "friday":  ["friday", "fri"],
    "saturday":["saturday", "sat"],
    "sunday":  ["sunday", "sun"],
}


def get_sb_price(venue: dict, day: str, meal: str) -> tuple[int, int] | None:
    lunch_key, dinner_key = _SB_DAY_FIELDS.get(day, ("mon_thu_lunch", "mon_thu_dinner"))
    key = lunch_key if meal == "lunch" else dinner_key
    price_str = venue.get(key) or ""
    return parse_price_range(price_str)


def get_wd_packages_for_day(venue: dict, day: str) -> list[dict]:
    """Return packages across all rooms that match the requested day."""
    keywords = _WD_DAY_KEYWORDS.get(day, [])
    result = []
    for room in venue.get("rooms", []):
        for pkg in room.get("packages", []):
            pkg_day = pkg.get("day", "").lower()
            if not day or any(kw in pkg_day for kw in keywords):
                result.append({**pkg, "_room_types": room.get("types", [])})
    return result


def get_wd_price_and_cap(
    venue: dict, day: str
) -> tuple[tuple[int, int] | None, tuple[int, int] | None]:
    pkgs = get_wd_packages_for_day(venue, day)
    if not pkgs:
        return None, None
    prices, caps_min, caps_max = [], [], []
    for pkg in pkgs:
        pr = parse_price_range(pkg.get("price", ""))
        if pr:
            prices.extend(pr)
        if pkg.get("capacity_min"):
            caps_min.append(pkg["capacity_min"])
        if pkg.get("capacity_max"):
            caps_max.append(pkg["capacity_max"])
    price_range = (min(prices), max(prices)) if prices else None
    cap_range   = (min(caps_min), max(caps_max)) if caps_min else None
    return price_range, cap_range


# ─── Style Tags ───────────────────────────────────────────────────────────────

_STYLE_MAP = {
    "indoor":     ["Indoor", "Ballroom", "Hotel Ballroom"],
    "outdoor":    ["Outdoor", "Garden & Greenery", "Waterfront"],
    "rooftop":    ["Rooftop"],
    "garden":     ["Garden & Greenery"],
    "glasshouse": ["Glasshouse"],
    "waterfront": ["Waterfront"],
    "restaurant": ["Restaurant"],
}


def venue_matches_style(venue: dict, source: str, style: str) -> bool:
    if source != "wd":
        return True  # only WD has style tags; don't exclude other sources
    tags = _STYLE_MAP.get(style.lower(), [style])
    all_types = [t for room in venue.get("rooms", []) for t in room.get("types", [])]
    return any(tag in all_types for tag in tags)


def get_wd_style_tags(venue: dict) -> list[str]:
    tags: set[str] = set()
    for room in venue.get("rooms", []):
        tags.update(room.get("types", []))
    return sorted(tags)


# ─── Local PDF Path Resolution ────────────────────────────────────────────────

def get_local_pdfs(venue: dict, source: str) -> list[str]:
    """Return local PDF paths for a venue record (relative to project root)."""
    if source == "wd":
        return [p["local_path"] for p in venue.get("pdfs", []) if p.get("local_path")]
    if source == "sb":
        slug = venue.get("slug", "")
        if slug:
            sb_dir = PROJECT_ROOT / "data" / "sb" / "price-lists" / slug
            if sb_dir.exists():
                return [str(f.relative_to(PROJECT_ROOT)) for f in sb_dir.glob("*.pdf")]
    if source == "bb":
        name_slug = re.sub(r"[^\w\s-]", "", venue.get("name", "")).strip().lower()
        name_slug = re.sub(r"\s+", "-", name_slug)
        bb_dir = PROJECT_ROOT / "data" / "bb" / "price-lists" / name_slug
        if bb_dir.exists():
            return [str(f.relative_to(PROJECT_ROOT)) for f in bb_dir.glob("*.pdf")]
    return []


# ─── Venue Normalization ──────────────────────────────────────────────────────

def normalize_bb(venue: dict, day: str, meal: str, guests: int | None) -> dict | None:
    name = venue.get("name", "")
    if not name or venue.get("source") != "banquet-price-list":
        return None
    meal_key = "lunch_price" if meal == "lunch" else "dinner_price"
    price_str = venue.get(meal_key, "")
    price_range = parse_price_range(price_str)
    if not price_range:
        return None
    cap = parse_capacity_pax(venue, "bb")
    total = effective_total_range(price_range, False, False, guests) if guests else None
    return {
        "name": name,
        "sources": ["bb"],
        "address": venue.get("location", ""),
        "capacity_pax": {"min": cap[0], "max": cap[1]} if cap else None,
        "price_per_table": list(price_range),
        "price_per_pax": None,
        "is_per_pax": False,
        "is_lump_sum": False,
        "effective_total": {"min": total[0], "max": total[1]} if total else None,
        "style_tags": [],
        "ratings": {},
        "contact": {"url": venue.get("profile_url", "")},
        "pdf_paths": get_local_pdfs(venue, "bb"),
    }


def normalize_bly(venue: dict, day: str, meal: str, guests: int | None) -> dict | None:
    name = venue.get("name", "")
    if not name:
        return None
    price_str = venue.get("price", "")
    price_range = parse_price_range(price_str)
    per_pax = is_per_pax(price_str)
    cap = parse_capacity_pax(venue, "bly")
    total = effective_total_range(price_range, per_pax, False, guests) if (price_range and guests) else None
    ratings = {}
    for key, field in [("venue", "venueRating"), ("food", "foodRating"), ("service", "serviceRating")]:
        if venue.get(field):
            try:
                ratings[key] = float(venue[field])
            except ValueError:
                pass
    return {
        "name": name,
        "sources": ["bly"],
        "address": "",
        "capacity_pax": {"min": cap[0], "max": cap[1]} if cap else None,
        "price_per_table": None if per_pax else (list(price_range) if price_range else None),
        "price_per_pax": list(price_range) if per_pax and price_range else None,
        "is_per_pax": per_pax,
        "is_lump_sum": False,
        "effective_total": {"min": total[0], "max": total[1]} if total else None,
        "style_tags": [],
        "ratings": ratings,
        "contact": {"url": venue.get("url", ""), "reviews": venue.get("reviews", "")},
        "pdf_paths": [],
    }


def normalize_wd(venue: dict, day: str, meal: str, guests: int | None) -> dict | None:
    name = venue.get("name", "")
    if not name:
        return None
    price_range, cap_range = get_wd_price_and_cap(venue, day)
    if not price_range:
        return None

    # Detect per-pax vs lump-sum by checking any matching package's price string
    pkgs = get_wd_packages_for_day(venue, day)
    per_pax = any(is_per_pax(p.get("price", "")) for p in pkgs)
    is_lump = not per_pax

    total = None
    if guests and per_pax:
        t = effective_total_range(price_range, True, False, guests)
        total = {"min": t[0], "max": t[1]}
    elif is_lump:
        total = {"min": price_range[0], "max": price_range[1]}

    style_tags = get_wd_style_tags(venue)
    return {
        "name": name,
        "sources": ["wd"],
        "address": "",
        "capacity_pax": {"min": cap_range[0], "max": cap_range[1]} if cap_range else None,
        "price_per_table": None,
        "price_per_pax": list(price_range) if per_pax else None,
        "is_per_pax": per_pax,
        "is_lump_sum": is_lump,
        "price_total_package": list(price_range) if is_lump else None,
        "effective_total": total,
        "style_tags": style_tags,
        "ratings": {},
        "contact": {"url": venue.get("url", "")},
        "pdf_paths": get_local_pdfs(venue, "wd"),
    }


def normalize_sb(venue: dict, day: str, meal: str, guests: int | None) -> dict | None:
    name = venue.get("name", "")
    if not name:
        return None
    price_range = get_sb_price(venue, day, meal)
    if not price_range:
        return None
    cap = parse_capacity_pax(venue, "sb")
    total = effective_total_range(price_range, False, False, guests) if guests else None
    return {
        "name": name,
        "sources": ["sb"],
        "address": venue.get("address", ""),
        "capacity_pax": {"min": cap[0], "max": cap[1]} if cap else None,
        "price_per_table": list(price_range),
        "price_per_pax": None,
        "is_per_pax": False,
        "is_lump_sum": False,
        "effective_total": {"min": total[0], "max": total[1]} if total else None,
        "style_tags": [],
        "ratings": {},
        "contact": {
            "phone": venue.get("phone", ""),
            "email": venue.get("email", ""),
            "url": venue.get("url", ""),
            "facebook": venue.get("facebook", ""),
            "instagram": venue.get("instagram", ""),
        },
        "pdf_paths": get_local_pdfs(venue, "sb"),
    }


# ─── Deduplication / Merge ────────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    return re.sub(r"\W+", " ", name).strip().lower()


def merge_venues(records: list[dict]) -> list[dict]:
    """Merge records with the same normalized name across sources."""
    by_name: dict[str, dict] = {}
    for rec in records:
        key = normalize_name(rec["name"])
        if key not in by_name:
            by_name[key] = dict(rec)
            by_name[key]["pdf_paths"] = list(rec.get("pdf_paths", []))
        else:
            existing = by_name[key]
            existing["sources"] = sorted(set(existing["sources"] + rec["sources"]))
            # Prefer non-empty values for scalar fields
            for field in ("address", "style_tags", "capacity_pax", "effective_total"):
                if not existing.get(field) and rec.get(field):
                    existing[field] = rec[field]
            # Merge dicts
            existing["ratings"].update(rec.get("ratings") or {})
            for k, v in (rec.get("contact") or {}).items():
                if v and not existing["contact"].get(k):
                    existing["contact"][k] = v
            # Merge price fields (prefer per-table from SB/BB, per-pax from BLY)
            for field in ("price_per_table", "price_per_pax", "price_total_package"):
                if not existing.get(field) and rec.get(field):
                    existing[field] = rec[field]
            # Merge PDFs (deduplicate)
            existing_pdfs = set(existing["pdf_paths"])
            for p in rec.get("pdf_paths", []):
                if p not in existing_pdfs:
                    existing["pdf_paths"].append(p)
                    existing_pdfs.add(p)
    return list(by_name.values())


# ─── Filtering ────────────────────────────────────────────────────────────────

def fits_budget(
    venue: dict,
    budget_total: int | None,
    budget_per_table: int | None,
    budget_per_pax: int | None,
) -> bool:
    if not (budget_total or budget_per_table or budget_per_pax):
        return True
    if budget_per_table and venue.get("price_per_table"):
        return venue["price_per_table"][0] <= budget_per_table
    if budget_per_pax and venue.get("price_per_pax"):
        return venue["price_per_pax"][0] <= budget_per_pax
    if budget_total and venue.get("effective_total"):
        return venue["effective_total"]["min"] <= budget_total
    return True  # no pricing data — include


def fits_guests(venue: dict, guests: int | None) -> bool:
    if not guests:
        return True
    cap = venue.get("capacity_pax")
    if not cap:
        return True  # unknown capacity — include
    return cap["max"] >= guests


# ─── Ranking ──────────────────────────────────────────────────────────────────

def rank_score(venue: dict, guests: int | None) -> float:
    score = 0.0
    ratings = venue.get("ratings", {})
    if ratings:
        score += sum(ratings.values()) / len(ratings)  # 0–5
    if venue.get("address"):
        score += 0.5
    contact = venue.get("contact", {})
    if contact.get("phone") or contact.get("email"):
        score += 0.5
    if venue.get("style_tags"):
        score += 0.3
    if len(venue.get("sources", [])) > 1:
        score += 0.5  # cross-source verification bonus
    if venue.get("pdf_paths"):
        score += 0.2
    cap = venue.get("capacity_pax")
    if guests and cap:
        if cap["min"] <= guests <= cap["max"]:
            score += 2.0
        elif guests < cap["min"]:
            score -= 1.0
    return score


# ─── Data Loading ─────────────────────────────────────────────────────────────

def load_json(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path) as f:
        return json.load(f)


def load_all_venues() -> dict[str, list[dict]]:
    return {
        "bb":  load_json(PROJECT_ROOT / "data/bb/venues.json"),
        "bly": load_json(PROJECT_ROOT / "data/bly/venues.json"),
        "wd":  load_json(PROJECT_ROOT / "data/wd/venues.json"),
        "sb":  load_json(PROJECT_ROOT / "data/sb/venues.json"),
    }


# ─── Commands ─────────────────────────────────────────────────────────────────

def cmd_search(args):
    data = load_all_venues()
    sources = [s.strip() for s in args.sources.split(",")] if args.sources else ["bb", "bly", "wd", "sb"]
    day    = (args.day  or "saturday").lower()
    meal   = (args.meal or "dinner").lower()
    guests = args.guests
    style  = (args.style or "").lower()

    all_venues: list[dict] = []

    if "bb" in sources:
        for v in data["bb"]:
            norm = normalize_bb(v, day, meal, guests)
            if norm:
                all_venues.append(norm)

    if "bly" in sources:
        for v in data["bly"]:
            norm = normalize_bly(v, day, meal, guests)
            if norm:
                all_venues.append(norm)

    if "wd" in sources:
        for v in data["wd"]:
            if style and not venue_matches_style(v, "wd", style):
                continue
            norm = normalize_wd(v, day, meal, guests)
            if norm:
                all_venues.append(norm)

    if "sb" in sources:
        for v in data["sb"]:
            norm = normalize_sb(v, day, meal, guests)
            if norm:
                all_venues.append(norm)

    all_venues = merge_venues(all_venues)
    all_venues = [v for v in all_venues if fits_guests(v, guests)]
    all_venues = [
        v for v in all_venues
        if fits_budget(v, args.budget_total, args.budget_per_table, args.budget_per_pax)
    ]
    all_venues.sort(key=lambda v: rank_score(v, guests), reverse=True)

    top = all_venues[: args.top]
    print(json.dumps(top, indent=2))


def cmd_compare(args):
    data = load_all_venues()
    names = [n.strip() for n in args.names.split(",")]

    results = []
    for target in names:
        target_norm = normalize_name(target)
        candidates: list[tuple[float, str, dict]] = []

        for source, venues in data.items():
            for v in venues:
                vname = v.get("name", "")
                ratio = difflib.SequenceMatcher(None, target_norm, normalize_name(vname)).ratio()
                if ratio >= 0.65:
                    candidates.append((ratio, source, v))

        if not candidates:
            results.append({"query": target, "found": False})
            continue

        candidates.sort(key=lambda x: x[0], reverse=True)

        # Only include records that are close matches (≥0.75) to avoid mixing in unrelated venues
        matched_records: list[tuple[str, dict]] = [
            (src, v) for ratio, src, v in candidates if ratio >= 0.75
        ]
        # Always include the best match even if it falls below 0.75
        if not matched_records and candidates:
            best_ratio, best_src, best_v = candidates[0]
            matched_records = [(best_src, best_v)]

        # Build normalized records for each matched raw record
        normalized: list[dict] = []
        all_pdf_paths: list[str] = []
        for src, v in matched_records:
            # Collect PDFs from raw record before normalizing
            pdfs = get_local_pdfs(v, src)
            all_pdf_paths.extend(p for p in pdfs if p not in all_pdf_paths)

            # Normalize with Saturday dinner as default (full price reference)
            if src == "bb":
                n = normalize_bb(v, "saturday", "dinner", None)
            elif src == "bly":
                n = normalize_bly(v, "saturday", "dinner", None)
            elif src == "wd":
                n = normalize_wd(v, "saturday", "dinner", None)
            elif src == "sb":
                n = normalize_sb(v, "saturday", "dinner", None)
            else:
                continue
            if n:
                normalized.append(n)

        if not normalized:
            results.append({"query": target, "found": False})
            continue

        merged = merge_venues(normalized)
        record = merged[0]
        record["query"] = target
        record["found"] = True
        # Ensure all PDF paths are collected (merge may drop some)
        record["pdf_paths"] = all_pdf_paths

        # Add full day-of-week pricing from SB if available
        sb_match = next(
            (v for ratio, src, v in candidates if src == "sb" and ratio >= 0.65), None
        )
        if sb_match:
            record["pricing_by_day"] = []
            for day_key, (lunch_field, dinner_field) in _SB_DAY_FIELDS.items():
                lp = parse_price_range(sb_match.get(lunch_field, ""))
                dp = parse_price_range(sb_match.get(dinner_field, ""))
                tables_str = sb_match.get(
                    f"{lunch_field.replace('_lunch','')}_tables", ""
                )
                if lp or dp:
                    record["pricing_by_day"].append({
                        "day": day_key,
                        "lunch": list(lp) if lp else None,
                        "dinner": list(dp) if dp else None,
                        "tables_raw": tables_str,
                    })

        results.append(record)

    print(json.dumps(results, indent=2))


def cmd_budget(args):
    data = load_all_venues()
    guests       = args.guests
    budget_total = args.budget_total
    meal         = (args.meal or "both").lower()

    tables              = math.ceil(guests / 10)
    budget_per_table    = budget_total / tables
    stretch_per_table   = budget_per_table * 1.2

    result = {
        "inputs": {
            "guests": guests,
            "budget_total": budget_total,
            "tables": tables,
            "effective_budget_per_table": round(budget_per_table),
        },
        "tier": _budget_tier(budget_per_table),
        "scenarios": {},
    }

    meals = ["lunch", "dinner"] if meal == "both" else [meal]

    for day in ["weekday", "friday", "saturday", "sunday"]:
        for m in meals:
            fitting, stretch = [], []

            # Use SB (most complete day-specific) + BB as supplementary
            per_table_venues: list[dict] = []
            for v in data["sb"]:
                norm = normalize_sb(v, day, m, guests)
                if norm:
                    per_table_venues.append(norm)
            for v in data["bb"]:
                norm = normalize_bb(v, day, m, guests)
                if norm:
                    per_table_venues.append(norm)

            per_table_venues = merge_venues(per_table_venues)

            for v in per_table_venues:
                pt = v.get("price_per_table")
                if not pt:
                    continue
                entry = {
                    "name": v["name"],
                    "price_per_table": pt,
                    "total_min": pt[0] * tables,
                    "total_max": pt[1] * tables,
                    "address": v.get("address", ""),
                }
                if pt[0] <= budget_per_table:
                    fitting.append(entry)
                elif pt[0] <= stretch_per_table:
                    stretch.append(entry)

            # Sort: fitting → highest price first (best you can get); stretch → cheapest first
            fitting.sort(key=lambda x: x["price_per_table"][0], reverse=True)
            stretch.sort(key=lambda x: x["price_per_table"][0])

            result["scenarios"][f"{day}_{m}"] = {
                "fitting_count": len(fitting),
                "stretch_count": len(stretch),
                "fitting":  fitting[:10],
                "stretch":  stretch[:5],
            }

    print(json.dumps(result, indent=2))


def _budget_tier(budget_per_table: float) -> str:
    if budget_per_table < 1200:
        return "Budget — Chinese restaurants, community clubs, smaller hotel function rooms"
    if budget_per_table < 1800:
        return "Mid-range — 3–4 star hotels, boutique venues, garden venues"
    if budget_per_table < 2500:
        return "Premium — 5-star hotels, landmark venues"
    return "Luxury — Iconic properties (Capella, Shangri-La, Fullerton, Mandarin Oriental)"


# ─── CLI Entry Point ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Filter Singapore wedding venues")
    sub = parser.add_subparsers(dest="command")

    s = sub.add_parser("search", help="Find venues matching criteria")
    s.add_argument("--guests",           type=int,   help="Number of guests (pax)")
    s.add_argument("--budget-total",     type=int,   dest="budget_total",     help="Max total F&B budget")
    s.add_argument("--budget-per-table", type=int,   dest="budget_per_table", help="Max price per table")
    s.add_argument("--budget-per-pax",   type=int,   dest="budget_per_pax",   help="Max price per pax")
    s.add_argument("--day",              default="saturday", choices=["weekday","friday","saturday","sunday"])
    s.add_argument("--meal",             default="dinner",   choices=["lunch","dinner"])
    s.add_argument("--style",            default="",   help="indoor|outdoor|rooftop|garden|glasshouse|waterfront|restaurant")
    s.add_argument("--sources",          default="",   help="Comma-separated: bb,bly,wd,sb (default: all)")
    s.add_argument("--top",              type=int,   default=10)

    c = sub.add_parser("compare", help="Compare specific venues by name")
    c.add_argument("--names", required=True, help='Comma-separated venue names (fuzzy matched)')

    b = sub.add_parser("budget", help="Analyze venues fitting a budget")
    b.add_argument("--guests",       type=int, required=True)
    b.add_argument("--budget-total", type=int, required=True, dest="budget_total")
    b.add_argument("--meal",         default="both", choices=["lunch","dinner","both"])

    args = parser.parse_args()

    if args.command == "search":
        cmd_search(args)
    elif args.command == "compare":
        cmd_compare(args)
    elif args.command == "budget":
        cmd_budget(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
