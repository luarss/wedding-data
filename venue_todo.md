# Venue Data Source Expansion — TODO

Tracks potential new data sources, scouting status, and what each would add.

## Current Sources

| ID | Site | Records | Key Data |
|----|------|---------|----------|
| `bb` | BlissfulBrides.sg | ~215 | Banquet price/table, tables min/max |
| `bly` | Bridely.sg | ~247 | Per-pax price, 3-axis ratings, tags |
| `wd` | Wedded.sg | ~142 | Room types (Rooftop/Garden/Glasshouse), PDFs |
| `sb` | SingaporeBrides.com | ~128 | Full day-of-week pricing, phone/email |
| `vrf` | Venuerific.com | ~50 | Geocoords, standing/seated cap, MRT station |
| `tv` | Tagvenue.com | ~304 | Geo coords, MRT + distance, venue/space type, per-person pricing |
| `twn` | TheWeddingNotebook.com | — | Malaysia venues only |

---

## Candidates — Scouted

### Hitcheed (hitcheed.com)
- **Status:** ❌ Blocked — entire domain redirects to wedding expo RSVP until after July 2026
- **Revisit:** August 2026
- **What it adds:** Singapore wedding vendor directory; may have unique boutique venues and vendor-linked pricing not on other platforms

### We Are Spaces (wearespaces.com)
- **Status:** ⚠️ Investigated — not suitable as-is
- **Why skipped:** 863 total spaces but `event_type=Wedding` returns 0 results; organises by space type not event type; no wedding pricing
- **Potential salvage:** Query by `venue_type` (Rooftop / Garden / Gallery / Ballroom) and post-filter for wedding-suitable spaces; would surface art galleries, shophouses, lofts not on bridal platforms
- **API:** `GET /api/v1/spaces/search-listing?venue_types=<type>&page=<n>&limit=9` — clean REST, no auth

### Tagvenue (tagvenue.com/sg)
- **Status:** ✅ Done — `src/tv/` (304 venues extracted)
- **Data:** Geo coords, MRT + distance, venue/space type, per-person pricing, standing/seated capacity, reviews, cuisine, opening hours
- **Approach:** Session-based AJAX API (`/ajax/search-list`) — init session for cookies, then paginate JSON. No Playwright needed.
- **Overlap with VRF:** Partial — Tagvenue has more hotel/banquet venues, room-level granularity, and per-person pricing; VRF has unique boutique venues

### Giggster (giggster.com/find/singapore--sg)
- **Status:** ⚠️ Skip for now
- **Why:** 615 locations but pricing is **per-hour venue hire only** ($65–200 SGD/hr) — not per-pax wedding packages. Skews toward photography studios, coworking, creative spaces. No wedding-specific data (no banquet, no F&B).
- **Overlap:** Low overlap with hotel-focused platforms, but also low relevance for wedding banquet planning
- **Extraction approach:** Client-side rendered, no visible API/JSON. Would need Playwright.
- **Verdict:** Not useful for wedding data — it's a venue hire platform, not a wedding platform

### NParks Venue Booking (nparks.gov.sg/services/book-event-venue)
- **Priority:** Low
- **Why:** Government-official list of park/nature venues (Gardens by the Bay, Fort Canning, etc.); authoritative and unlikely to appear on commercial platforms
- **What to check:** Whether listings are machine-readable or behind a booking form; pricing structure (likely per-hour hire, no F&B)

### WeddingWire Singapore
- **Priority:** Low
- **Why:** May have user reviews and ratings not available elsewhere
- **What to check:** Whether SG coverage is substantial (primarily US-focused); overlap with BLY ratings

### Empathy Weddings / The Aurora Wedding (editorial blogs)
- **Priority:** Skip
- **Why:** Editorial content only — no structured data, prices are illustrative not contractual

---

## Data Gaps Across All Current Sources

Even with all 6 sources combined, these fields are missing or incomplete:

| Gap | Notes |
|-----|-------|
| Package inclusions | What's actually included in F&B (alcohol, décor, AV, etc.) |
| Availability / auspicious dates | No source tracks booking calendars |
| Halal certification | Only partially tagged in BLY |
| Outdoor contingency / rain plan | Not captured anywhere |
| Minimum spend vs per-pax distinction | Inconsistent across sources |
| Coordinator contact (name, not just email) | Only SB has phone/email |
