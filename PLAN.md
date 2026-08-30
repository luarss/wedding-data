# PLAN.md

## Remaining Improvements

Lower-priority items not yet addressed:

---

### Delta / Incremental Extraction

Every run is a full re-extraction. No `ETag`/`If-Modified-Since` support. Consider adding conditional requests or tracking last-modified timestamps to avoid re-downloading unchanged data.

---

### Browser Stealth

Only TWN hides `navigator.webdriver`. Other Playwright-based extractors (bly, wd, sb) lack anti-detection. Consider adding stealth measures for extractors hitting sites that may block automation.

---

### Output Metadata

Output files lack `last_extracted_at` or `source_url` metadata. Adding these would improve data provenance and debugging.

---

### Makefile

Only has `make sb`. Missing `make all` and targets for other extractors. Also missing a dedicated `make sync` target (currently in CLAUDE.md but no Makefile entry).

---

### README

Missing SingaporeBrides (sb) extractor documentation.

---

### Type Annotations

Sparse and inconsistent across modules. Some functions have full type hints; others have none.
