# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python web extraction project that collects wedding vendor and venue data from Singapore and Malaysia wedding websites. The project is organized as a monorepo of independent extractors, each targeting a specific website.

## Architecture

### Extractor Modules (`src/`)

Each subdirectory in `src/` is an independent extractor with its own entry point:

| Module | Source | Description | Entry Point |
|--------|--------|-------------|-------------|
| `bb/` | BlissfulBrides.sg | Singapore wedding venues (banquet prices + booking) | `python -m src.bb.main` |
| `twn/` | TheWeddingNotebook.com | Malaysia wedding venues (GraphQL API) | `python -m src.twn.main` |
| `bly/` | Bridely.sg | Singapore venues & vendors (API + Playwright) | `python -m src.bly.main` / `python -m src.bly.vendors` |
| `wd/` | Wedded.sg | Singapore venues & photographers (Playwright) | `python -m src.wd.main` / `python -m src.wd.photographers` |
| `sb/` | SingaporeBrides.com | Singapore wedding banquet prices | `python -m src.sb.main` |
| `tv/` | Tagvenue.com | Singapore wedding venues (JSON API) | `python -m src.tv.main` |
| `wv/` | WeddingVenue.sg | Singapore wedding venues (rooms + package pricing) | `python -m src.wv.main` |
| `shared/` | - | Common utilities (HTTP headers, config) | - |

### Data Flow Pattern

1. **Fetch**: HTTP requests (httpx) or browser automation (Playwright)
2. **Extract**: Parse HTML (BeautifulSoup) or execute JS in browser (Playwright page.evaluate)
3. **Transform**: Clean and structure data into dictionaries
4. **Save**: Output to `data/<module>/` as both JSON (full data) and CSV (flattened)

### Extraction Techniques Used

- **HTTP + HTML parsing**: `bb`, `twn`, `wv`, `sb` - Direct HTTP requests with BeautifulSoup
- **GraphQL API**: `twn` - Uses `gql` library with HTTPX transport
- **Session-based AJAX API**: `tv` - Session cookies from search page, then JSON API (`/ajax/search-list`)
- **Browser automation**: `bly`, `wd` - Playwright for JavaScript-rendered content
- **External JS extractors**: `wd` loads JavaScript files (`extractor.js`, `photographers_extractor.js`) for page evaluation

## Common Commands

### Setup

```bash
# Install dependencies
make sync           # uv sync --all-extras

# Install Playwright browsers (required for wd, bly, sb)
uv run playwright install chromium
```

### Running Extractors

```bash
# BlissfulBrides (Singapore venues)
uv run python -m src.bb.main
uv run python -m src.bb.main --source banquet      # Banquet prices only
uv run python -m src.bb.main --source booking      # Booking page only

# The Wedding Notebook (Malaysia venues)
uv run python -m src.twn.main
uv run python -m src.twn.main --state "Selangor"  # Filter by state
uv run python -m src.twn.main --limit 100

# Bridely (Singapore)
uv run python -m src.bly.main                      # Venues
uv run python -m src.bly.vendors                   # All vendor categories

# Wedded.sg (Singapore)
uv run python -m src.wd.main                       # Venues
uv run python -m src.wd.photographers              # Photographers
uv run python -m src.wd.main 5                     # Limit to 5 for testing

# SingaporeBrides
uv run python -m src.sb.main
uv run python -m src.sb.main --no-details          # Skip detail pages
uv run python -m src.sb.main --no-pdfs             # Skip downloading PDFs

# Tagvenue
uv run python -m src.tv.main
uv run python -m src.tv.main --limit 10            # Limit for testing

# WeddingVenue.sg
uv run python -m src.wv.main
uv run python -m src.wv.main --limit 10            # Limit for testing
uv run python -m src.wv.main --concurrency 10      # Concurrent detail page requests
```

### Code Quality

```bash
make format         # ruff format + ruff check --fix
make check          # ruff check
```

## Project Configuration

- **Package manager**: `uv` (modern Python package manager)
- **Python version**: 3.13+ (see `.python-version`)
- **Linting/Formatting**: Ruff (configured in `pyproject.toml`)
  - Line length: 120
  - Target: Python 3.13
- **Dependencies**: httpx, playwright, beautifulsoup4, lxml, pandas, gql, pydantic

## Environment Variables

Required for Bridely extractor (`bly`). See `.env.example`:

- `BRIDELY_BASE_URL` - API base URL
- `BRIDELY_APP_ID` - App identifier
- `BRIDELY_*_ENDPOINT` - Various vendor category endpoints

## Data Output Structure

```
data/
├── bb/              # BlissfulBrides
│   ├── venues.json
│   ├── venues.csv
│   └── price-lists/ # Downloaded PDFs
├── bly/             # Bridely
│   ├── venues.json
│   ├── venues.csv
│   └── vendors.json
├── twn/             # The Wedding Notebook
│   ├── venues.json
│   └── venues.csv
├── wd/              # Wedded.sg
│   ├── venues.json
│   ├── venues.csv
│   ├── photographers.json
│   └── price-lists/ # Downloaded PDFs
├── sb/              # SingaporeBrides
│   ├── venues.json
│   ├── venues.csv
│   └── price-lists/ # Downloaded PDFs
├── tv/              # Tagvenue
│   ├── venues.json
│   └── venues.csv
└── wv/              # WeddingVenue.sg
    ├── venues.json
    └── venues.csv
```

## CI/CD

GitHub Actions workflow (`.github/workflows/daily-extraction.yml`):
- Runs weekly, Sundays at 19:23 UTC
- Executes all extractors sequentially
- Commits data changes automatically
- Requires repository secrets for Bridely API endpoints

## Key Implementation Patterns

### Browser Page Context Manager

All Playwright-based extractors use this pattern:

```python
@asynccontextmanager
async def get_browser_page(headless=True):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        try:
            page = await browser.new_page()
            yield page
        finally:
            await browser.close()
```

### Concurrent Extraction with Semaphore

Used in `wd/` for rate-limited parallel processing:

```python
semaphore = asyncio.Semaphore(concurrent_limit)

async def extract_with_semaphore(browser, url: str, index: int):
    async with semaphore:
        page = await browser.new_page()
        # ... extraction logic
```

### Shared Configuration

HTTP headers with rotating user agents in `src/shared/config.py`.

## Testing Extractors

All extractors support limiting results for faster testing:
- `wd`: Pass integer as CLI argument (e.g., `python -m src.wd.main 5`)
- `twn`: Use `--limit N`
- `bly/vendors`: Use `--limit-per-category N`
