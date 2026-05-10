# PLAN.md

## Repo Improvement Areas

Assessment of the wedding-data scraping project. The codebase generally works (daily CI scrapes succeed) but has accumulated technical debt from scraper-by-scraper development without shared infrastructure.

---

### 1. Zero Tests (Highest Risk)

No test files exist anywhere. The project scrapes live websites whose HTML/CSS/API structure can change at any time. A single CSS class rename silently breaks a scraper.

Specific testable units:
- `bb/main.py` `merge_venue_data()` — complex dict-merging with deduplication
- `wd/main.py` `parse_venue_slug()` — URL parsing
- Retry/backoff math in `twn/main.py` `_retry_api_get()`
- Data transformation (JSON → CSV flattening) in every scraper

**Recommendation:** Add `pytest` to dev deps, test pure-logic functions first. Snapshot tests against saved HTML fixtures for regression catching.

---

### 2. CI No Error Isolation (High Risk)

`.github/workflows/daily-scrape.yml` — All 7 scraper steps run sequentially with no `continue-on-error`. A single transient 429 blocks all subsequent scrapers and the commit step, losing all data for that day.

Also missing:
- No failure notifications (Slack/email/Discord)
- No job-level `timeout-minutes` — a hung Playwright browser could burn 6 hours
- No step-level timeouts
- No artifact uploads on failure (screenshots, HAR logs)

**Recommendation:** Add `continue-on-error: true` per step, add notification, set timeouts.

---

### 3. Massive Code Duplication (High Maintenance Cost)

6+ patterns duplicated across modules:

| Pattern | Duplicated In | Lines |
|---|---|---|
| `get_browser_page()` context manager | `bly/main.py`, `wd/main.py`, `sb/main.py` | ~12 × 3 |
| PDF download with skip-if-exists | `bb/main.py`, `wd/main.py`, `sb/main.py` | ~20 × 3 |
| JSON save (`json.dump` with indent) | All 5 modules | 6 variants |
| CSV flatten + `csv.DictWriter` | `wd/main.py`, `wd/photographers.py`, `bly/main.py`, `bly/vendors.py`, `sb/main.py`, `bb/main.py` | ~50-100 × 4 |
| Sitemap XML fetch | `wd/main.py`, `wd/photographers.py` | ~8 × 2 |
| Venue slug parsing | `wd/main.py`, `wd/photographers.py` | ~3 × 2 |

`src/shared/` currently contains only a user-agent rotator.

**Recommendation:** Extract shared utilities starting with browser context manager and JSON/CSV save helpers — most duplicated, lowest risk.

---

### 4. No Shared Data Schema (Limits Usefulness)

Every scraper produces incompatible "venue" JSON schemas. Only common field: `name`. Cross-source aggregation is impossible programmatically.

**Recommendation:** Define a shared `Venue` Pydantic model (pydantic is already a dep but unused). Map scraper fields into it with adapters. Start with ~6 common fields: `name`, `url`, `address`, `capacity`, `price_range`, `source`.

---

### 5. Error Handling Gaps

| Scraper | Network retry? | Rate-limit handling? |
|---|---|---|
| bb | None | None |
| twn | Best (exponential backoff) | 429 with backoff |
| bly (venues) | None | None |
| bly (vendors) | None | None |
| wd | Page nav only (2 retries) | Semaphore only |
| sb | Full re-scrape (3 retries) | 0.5s sleep |

Specific bugs:
- `bb/main.py:205` — Sync `httpx.get()` inside async function, blocks event loop
- `twn/main.py:52-59` — Retry loop falls through to `raise_for_status()` on still-429 response
- `bly/vendors.py` imports `from dotenv import load_dotenv` but `python-dotenv` not in `pyproject.toml`

---

### 6. Logging — All `print()`, No `logging`

Zero use of Python's `logging` module. All output is bare `print()` with inconsistent prefixes (`"⚠️"`, `"ERROR:"`, `"Warning:"`, `"!"`). No verbosity control, no structured output for CI.

**Recommendation:** Replace `print()` with `logging` at appropriate levels. Add `--verbose` flag.

---

### 7. CLI Inconsistency

Three approaches: `argparse` (bb, sb, bly/vendors), manual `sys.argv` (wd, twn), none (bly/main). Option naming inconsistent: `--limit` vs positional int vs `--limit-per-category`. Empty `__pycache__` in `src/cli/` suggests a unified CLI was started but never written.

---

### 8. Dead / Missing Dependencies

- **Dead**: `pydantic`, `pydantic-settings` — declared but imported nowhere
- **Dead**: `gql` — mentioned in CLAUDE.md but not used (TWN uses httpx, not GraphQL)
- **Missing**: `python-dotenv` — imported in `bly/vendors.py` but not declared
- **Missing**: `pytest` — needed for testing

---

### 9. Stale Artifacts

- `src/shared/__pycache__/` has `.pyc` files (`models.pyc`, `deduplication.pyc`, `price_parser.pyc`) with no `.py` source
- `src/cli/__pycache__/` and `src/db/__pycache__/` — empty dirs, no source
- `.playwright-mcp/` — 12 YAML artifacts at project root, not gitignored

---

### 10. Lower-Priority Items

- No delta/incremental scraping — every run is full. No `ETag`/`If-Modified-Since`
- No browser stealth — only TWN hides `navigator.webdriver`
- Output files lack `last_scraped_at` or `source_url` metadata
- Makefile only has `make sb`; no `make all` or targets for other scrapers
- README missing SingaporeBrides scraper docs
- Type annotations sparse and inconsistent across modules
