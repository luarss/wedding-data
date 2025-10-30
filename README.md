# Wedding Data Scrapers

Python web scrapers that collect and archive wedding vendor and venue data from various Singapore and Malaysia wedding websites.

## Disclaimer

This project is for **educational purposes only** and **not for profit**. The data collected is intended for learning web scraping techniques, data analysis, and software development practices. This project respects the terms of service of all data sources and is not intended for commercial use or redistribution.

## Data Sources

- **BlissfulBrides.sg** - Singapore wedding venues and marketplace packages
- **TheWeddingNotebook.com** - Malaysia wedding venues
- **Bridely.sg** - Singapore wedding venues and vendors

## Setup

1. Install dependencies using `uv`:
```bash
uv venv
uv sync
```

## Usage

### BlissfulBrides (Singapore)

Scrape wedding venue data from multiple sources:

```bash
# Scrape from all sources (default - recommended)
uv run python -m src.bb.main

# Scrape only from banquet price list
uv run python -m src.bb.main --source banquet

# Scrape only from wedding venues booking
uv run python -m src.bb.main --source booking

# Custom output directory
uv run python -m src.bb.main --output data/custom
```


### The Wedding Notebook (Malaysia)

Scrape wedding venues using GraphQL API:

```bash
# Scrape all venues
uv run python src/twn/main.py

# Filter by state
uv run python src/twn/main.py --state "Selangor"

# Limit number of results
uv run python src/twn/main.py --limit 100

# Custom output path
uv run python src/twn/main.py --output data/twn/my-venues
```

Data is saved to `data/twn/venues.json` and `data/twn/venues.csv`.

### Bridely (Singapore)

Scrape wedding venues and vendors using the Bridely API:

```bash
# Scrape venues
uv run python -m src.bly.main

# Scrape vendors from all categories
uv run python -m src.bly.vendors

# Options for venues
uv run python -m src.bly.main --limit 50 --output data/bly/venues

# Options for vendors
uv run python -m src.bly.vendors --limit-per-category 20 --output data/bly/vendors
```

## Project Structure

```
.
├── src/
│   ├── bb/          # BlissfulBrides.sg scraper
│   ├── bly/         # Bridely.sg scraper
│   ├── twn/         # TheWeddingNotebook.com scraper
│   └── shared/      # Shared utilities and config
├── data/            # Scraped data output
│   ├── bb/
│   │   ├── venues.json
│   │   ├── venues.csv
│   │   └── price-lists/
│   ├── bly/
│   │   ├── venues.json
│   │   ├── venues.csv
│   │   ├── vendors.json
│   │   └── vendors.csv
│   └── twn/
│       ├── venues.json
│       └── venues.csv
└── logs/            # Application logs
```

## License

See [LICENSE](LICENSE) file for details.
