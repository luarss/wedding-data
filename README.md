# Wedding Data Extractors

Python web extractors that collect and archive wedding vendor and venue data from various Singapore and Malaysia wedding websites.

## Disclaimer

This project is for **educational purposes only** and **not for profit**. The data collected is intended for learning web extraction techniques, data analysis, and software development practices. This project respects the terms of service of all data sources and is not intended for commercial use or redistribution.

## Data Sources

- **BlissfulBrides.sg** - Singapore wedding venues and marketplace packages
- **TheWeddingNotebook.com** - Malaysia wedding venues
- **Bridely.sg** - Singapore wedding venues and vendors
- **Wedded.sg** - Singapore wedding venues
- **SingaporeBrides.com** - Singapore wedding banquet prices
- **Venuerific.com** - Singapore wedding venues (geocoords, capacity, MRT)

## Setup

1. Install dependencies using `uv`:
```bash
uv venv
uv sync
```

## Usage

### BlissfulBrides (Singapore)

Extract wedding venue data from multiple sources:

```bash
# Extract from all sources (default - recommended)
uv run python -m src.bb.main

# Extract only from banquet price list
uv run python -m src.bb.main --source banquet

# Extract only from wedding venues booking
uv run python -m src.bb.main --source booking

# Custom output directory
uv run python -m src.bb.main --output data/custom
```


### The Wedding Notebook (Malaysia)

Extract wedding venues using GraphQL API:

```bash
# Extract all venues
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

Extract wedding venues and vendors using the Bridely API:

```bash
# Extract venues
uv run python -m src.bly.main

# Extract vendors from all categories
uv run python -m src.bly.vendors

# Options for venues
uv run python -m src.bly.main --limit 50 --output data/bly/venues

# Options for vendors
uv run python -m src.bly.vendors --limit-per-category 20 --output data/bly/vendors
```

### Wedded.sg (Singapore)

Extract wedding venues/vendors using Playwright:

```bash
# Extract all venues
uv run python -m src.wd.main

# Extract all photographers
uv run python -m src.wd.photographers
```

## License

See [LICENSE](LICENSE) file for details.
