---
name: compare-wedding-venues
description: Compare 2 to 5 specific Singapore wedding venues side by side, including pricing, capacity, ratings, day-of-week pricing breakdown, and wedding package perks extracted from downloaded price list PDFs. Use this skill when someone wants to compare specific venues, asks "which is better between X and Y", "can you compare these venues for me", shows a shortlist, mentions they are deciding between specific places, pastes a list of venue names, or says "help me decide between these options". Also trigger when someone has narrowed down to 2–5 venues and wants a detailed breakdown.
version: 1.0.0
---

# Compare Wedding Venues

You produce a rich side-by-side comparison of 2–5 specific Singapore wedding venues, including live pricing data and perks extracted from downloaded wedding package PDFs.

## Step 1: Identify the Venues

Extract 2–5 venue names from the user's message.

- If a name is ambiguous (e.g. "Fullerton" → Fullerton Hotel or Fullerton Bay?), ask before running.
- If the user lists more than 5, ask them to narrow to their top 5.
- Accept partial names — the script fuzzy-matches across all sources.

Optionally ask for guest count — it's needed for total cost estimates:
> "How many guests? I'll calculate total cost estimates for each venue."

## Step 2: Run the Compare Script

```bash
uv run python .claude/skills/find-wedding-venue/scripts/venue_filter.py compare \
  --names "Venue One,Venue Two,Venue Three"
```

The script returns one merged JSON record per venue, combining data from all 4 sources (BB, BLY, WD, SB). Each record includes:
- `price_per_table`, `price_per_pax`, `price_total_package` (whichever applies)
- `pricing_by_day` — weekday/Friday/Saturday/Sunday breakdown from SB
- `capacity_pax` — min/max pax
- `style_tags` — venue types (Indoor/Outdoor/Rooftop/Garden/Glasshouse)
- `ratings` — venue, food, service from Bridely
- `contact` — phone, email, website, social links
- `pdf_paths` — local paths to downloaded price list PDFs
- `sources` — which data sources matched (BB/BLY/WD/SB)

If a venue is not found (`"found": false`), say so clearly and offer to search by partial name.

## Step 3: Read PDFs for Perks

For each venue, read up to **3 local PDFs** from `pdf_paths` to extract wedding package perks and inclusions.

```
Read: <pdf_path>   (e.g. "data/sb/price-lists/capella-singapore/Wedding_Package_2026.pdf")
```

Focus on extracting:
- Complimentary items (honeymoon suite, bridal room, cake, champagne, floral)
- Package inclusions (free flow beverages, cocktail hour, table décor)
- Minimum table/pax requirements
- Promotion / early-bird perks
- Guest room block rates
- Any unique differentiators (signature dishes, outdoor option, solemnisation packages)

If PDFs are large (>10 pages), read pages 1–5 first. Skip PDFs with filenames that look like admin/legal documents.

If no local PDFs exist for a venue, note it and suggest the user request price lists directly.

## Step 4: Produce the Comparison

### Section 1: Pricing Overview

For each venue, show pricing clearly. Note that "++" = add ~21% (10% service + 9% GST).

**If the user gave a guest count**, include estimated total cost for each venue.

| | Venue A | Venue B | Venue C |
|-|---------|---------|---------|
| **Weekday dinner** | $X–$Y/table | ... | ... |
| **Saturday dinner** | $X–$Y/table | ... | ... |
| **Price type** | Per-table | Per-pax | Lump-sum |
| **Est. total (N guests)** | ~$XX,XXX | ... | ... |

**Day-of-week pricing table** (from SB data when available):

| Day | Lunch | Dinner |
|-----|-------|--------|
| Mon–Thu | $X–$Y | $X–$Y |
| Friday | ... | ... |
| Saturday | ... | ... |
| Sunday | ... | ... |

### Section 2: Capacity & Style

| | Venue A | Venue B | Venue C |
|-|---------|---------|---------|
| **Capacity** | min–max pax | ... | ... |
| **Venue types** | Indoor, Ballroom | Garden, Outdoor | ... |

### Section 3: Ratings

| | Venue A | Venue B | Venue C |
|-|---------|---------|---------|
| **Venue rating** | 4.8 ★ | ... | ... |
| **Food rating** | 4.6 ★ | ... | ... |
| **Service rating** | 4.7 ★ | ... | ... |
| **Reviews** | 45 | ... | ... |

### Section 4: Perks & Inclusions

Summarize each venue's key perks from the PDFs you read. Format as a short bulleted list per venue:

**Venue A — Key perks:**
- Complimentary 1-night honeymoon suite
- Free-flow soft drinks and oolong tea throughout dinner
- Signature dessert buffet for up to 20 pax
- Early-bird discount: 10% off packages booked 12 months ahead

**Venue B — Key perks:**
- ...

If no PDF was available, note: "Price list PDF not available locally — contact venue for full inclusions."

### Section 5: Contact & Booking

| | Venue A | Venue B | Venue C |
|-|---------|---------|---------|
| **Phone** | ... | ... | ... |
| **Email** | ... | ... | ... |
| **Website** | [link] | [link] | [link] |
| **Instagram** | ... | ... | ... |

### Section 6: Recommendation

End with a concise "which one and why" based on the user's stated priorities:

- **Best value**: [Venue] — lowest per-table price that still fits guest count
- **Best for style**: [Venue] — has outdoor/rooftop/garden that others lack
- **Best rated**: [Venue] — highest food + service scores
- **Best perks**: [Venue] — most generous inclusions from price list

Ask: "Is any factor more important to you? I can weight this differently."

## Notes on Data Coverage

- Not all venues appear in all sources — note which sources contributed.
- WD is the only source with style/type tags.
- SB has the most complete day-of-week pricing breakdown.
- BLY is the only source with three-dimensional ratings (venue, food, service).
- PDFs are available for SB and WD venues primarily; BB and BLY rarely have local PDFs.
