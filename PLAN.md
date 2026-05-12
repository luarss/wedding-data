# PLAN.md

## Remaining Improvements

Lower-priority items not yet addressed:

---

### Delta / Incremental Scraping

Every run is a full re-scrape. No `ETag`/`If-Modified-Since` support. Consider adding conditional requests or tracking last-modified timestamps to avoid re-downloading unchanged data.

---

### Browser Stealth

Only TWN hides `navigator.webdriver`. Other Playwright-based scrapers (bly, wd, sb) lack anti-detection. Consider adding stealth measures for scrapers hitting sites that may block automation.

---

### Output Metadata

Output files lack `last_scraped_at` or `source_url` metadata. Adding these would improve data provenance and debugging.

---

### Makefile

Only has `make sb`. Missing `make all` and targets for other scrapers. Also missing a dedicated `make sync` target (currently in CLAUDE.md but no Makefile entry).

---

### README

Missing SingaporeBrides (sb) scraper documentation.

---

### Type Annotations

Sparse and inconsistent across modules. Some functions have full type hints; others have none.
