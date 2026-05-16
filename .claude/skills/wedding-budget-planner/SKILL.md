---
name: wedding-budget-planner
description: Analyze Singapore wedding venue costs by budget and guest count. Use this skill when someone asks what venues they can afford, what fits within a budget, how much a wedding costs in Singapore, wants to understand weekday vs weekend price differences, asks "what can I get for $X", "is $50,000 enough for 200 guests", "how much does a hotel ballroom wedding cost", "what's the difference between Saturday and weekday pricing", or describes a budget and asks what it buys them. Trigger on any cost analysis, budget breakdown, affordability question, or "what can I afford" question related to Singapore weddings.
version: 1.0.0
---

# Wedding Budget Planner

You help couples understand what their Singapore wedding budget can realistically achieve — with honest weekday vs weekend breakdowns, tier analysis, and concrete venue lists for each scenario.

## Step 1: Gather Inputs

Collect these before running analysis. Ask conversationally, not as a checklist:

- **Total F&B budget** — food and beverage budget specifically (not total wedding spend)
- **Guest count** — number of pax (essential for per-table conversion)
- **Meal type** — lunch, dinner, or either?
- **Day flexibility** — are they open to weekdays? (weekday = 20–35% cheaper)

**If they give a total wedding budget**, not just F&B, help them estimate:
> "Food & beverage is usually 60–70% of total wedding spend in Singapore. For a $100,000 total budget, that's roughly $60,000–$70,000 for F&B. Want me to work with $65,000?"

**If no guest count is given**, ask — it's required to convert per-table prices to totals.

## Step 2: Run Budget Analysis

```bash
uv run python .claude/skills/find-wedding-venue/scripts/venue_filter.py budget \
  --guests <N> \
  --budget-total <B> \
  --meal <lunch|dinner|both>
```

The script returns:
- `inputs.tables` — number of tables (guests ÷ 10, rounded up)
- `inputs.effective_budget_per_table` — total budget ÷ tables
- `tier` — which venue tier the budget unlocks
- `scenarios` — for each day+meal combo:
  - `fitting` — venues where min price/table ≤ budget/table (up to 10)
  - `stretch` — venues within 20% over budget (up to 5)
  - `fitting_count` / `stretch_count` — how many exist total

## Step 3: Present the Budget Picture

### Part A: The Math

Show the per-table conversion clearly:

```
Your budget: $50,000 F&B, 200 guests (20 tables)
Per-table budget: $50,000 ÷ 20 = $2,500/table
```

Remind them about "++": prices before 10% service charge + 9% GST (~21% total).
So a venue listed at $2,500/table costs approximately $3,025/table all-in.

### Part B: Venue Tier

Map the per-table budget to a tier:
- **< $1,200/table** — Chinese restaurants, community clubs, smaller hotel function rooms
- **$1,200–$1,800/table** — 3–4 star hotels, boutique venues, garden and outdoor venues
- **$1,800–$2,500/table** — 5-star hotels, landmark venues
- **> $2,500/table** — Iconic properties (Capella, Shangri-La, Fullerton, Mandarin Oriental)

### Part C: Scenario Comparison

Show how many venues fit for each day/meal combo that's relevant. Lead with the scenarios the user cares about.

**Example layout:**

| Scenario | Venues fitting | Stretch options |
|----------|---------------|-----------------|
| Weekday dinner | X | Y |
| Saturday dinner | X | Y |
| Sunday dinner | X | Y |

For the 2–3 most relevant scenarios, list the top fitting venues:

**Saturday dinner — X venues fit $2,500/table:**
| Venue | Price/table | Total (20 tables) |
|-------|------------|-------------------|
| [Name] | $1,988 | $39,760 |
| ... | ... | ... |

**Stretch options (up to $3,000/table on Saturday):**
| Venue | Price/table | Over budget by |
|-------|------------|----------------|
| [Name] | $2,688 | +$3,760 |

### Part D: Ways to Stretch the Budget

Always include practical tips:
1. **Choose a weekday (Mon–Thu)** — 20–35% cheaper, same venue, same food
2. **Choose lunch** — typically 10–20% cheaper than dinner
3. **Reduce guest count** — fewer tables lowers both minimum spend and total cost
4. **Ask about off-peak promotions** — many venues offer early-bird or promo packages not listed publicly
5. **Consider non-hotel venues** — garden and restaurant venues often have lower minimums and inclusive décor

## Step 4: Offer Next Steps

Always end with:
- "Want me to search for specific venues that fit your budget? (Use `/find-wedding-venue`)"
- "Want to see how the numbers change with a different guest count or day?"
- "Want to compare specific venues from this list?"

## Singapore Wedding Context (use when relevant)

- Typical Singapore Chinese banquet: ~10 pax per table; ~20–40 tables for 200–400 guests
- Hotel minimums: usually 15–25 tables
- Chinese restaurant minimums: 10–20 tables
- Saturday dinner = most expensive; Monday lunch = cheapest (often 35% less)
- Average mid-range wedding: $1,500–$2,000/table; premium: $2,500–$3,500/table
- "++" means +10% service charge +9% GST = ~21.9% on top of listed price
