# Wedding Data Scrapers

Python web scrapers that collect and archive wedding vendor and venue data from various Singapore and Malaysia wedding websites.

## Disclaimer

This project is for **educational purposes only** and **not for profit**. The data collected is intended for learning web scraping techniques, data analysis, and software development practices. This project respects the terms of service of all data sources and is not intended for commercial use or redistribution.

## Data Sources

- **BlissfulBrides.sg** - Singapore wedding venues and marketplace packages
- **TheWeddingNotebook.com** - Malaysia wedding venues
- **Bridely.sg** - Singapore wedding vendors (currently unavailable)

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

## Features

- **Multi-source scraping**: Collect data from multiple wedding websites
- **PDF downloads**: Automatically download venue price lists and brochures
- **Structured data**: Export to both JSON and CSV formats
- **Respectful scraping**: Configurable delays between requests
- **Error handling**: Robust error handling and logging

## Data Output

Each scraper generates:
- **JSON files**: Complete structured data with all fields
- **CSV files**: Tabular format for easy analysis
- **PDF files**: Price lists and brochures (BlissfulBrides only)

## Project Structure

```
.
├── src/
│   ├── bb/          # BlissfulBrides.sg scraper
│   ├── twn/         # TheWeddingNotebook.com scraper
└── data/            # Scraped data output
    ├── bb/
    │   ├── venues.json
    │   ├── venues.csv
    │   └── price-lists/        # PDF price lists
    └── twn/
        ├── venues.json
        └── venues.csv
```

## License

See [LICENSE](LICENSE) file for details.
