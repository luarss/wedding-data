---
name: web-scout
description: Autonomously analyze and discover scraping strategies for websites. Use this skill when the user wants to scrape data from a website, extract information from web pages, figure out how to crawl a site, or needs help understanding a website's structure for data extraction. This skill should be triggered for requests like "scrape data from example.com", "help me extract listings from a website", "how do I get data from this site", or any web scraping exploration tasks.
version: 1.0.0
---

# Web Scout - Autonomous Web Scraping Discovery

This skill autonomously explores websites to discover optimal scraping strategies. It analyzes site structure, identifies data patterns, detects anti-scraping measures, and generates working extraction code.

## Overview

Web Scout uses Playwright to navigate websites, analyze their structure, and determine the best approach for data extraction. It handles dynamic content, pagination, and various site architectures.

## When to Use This Skill

- The user wants to scrape data from a specific website
- The user needs to extract structured data from web pages
- The user wants to understand how to navigate a site's structure programmatically
- The user needs help with pagination, infinite scroll, or dynamic content
- The user wants to generate scraping code for a website

## Workflow

### Phase 1: Discovery

1. **Navigate to the target URL** using Playwright
2. **Take a snapshot** to understand the page structure
3. **Identify the data type** the user wants to extract
4. **Analyze the DOM** to find repeating patterns (lists, tables, cards)

### Phase 2: Exploration

1. **Scroll through the page** to detect lazy-loaded content
2. **Test pagination** mechanisms (numbered pages, "Load More", infinite scroll)
3. **Check for anti-scraping measures** (CAPTCHA, rate limiting, bot detection)
4. **Analyze API calls** in the Network tab for data endpoints
5. **Look for JSON data** in script tags or window variables

### Phase 3: Strategy Selection

Based on findings, determine the best approach:

| Approach | When to Use |
|----------|-------------|
| **Direct DOM scraping** | Static content, clear selectors |
| **API endpoint extraction** | XHR/fetch calls with clean JSON |
| **JavaScript execution** | Data in `window.__INITIAL_STATE__` or similar |
| **Headless browser automation** | Heavy JavaScript, complex interactions |

### Phase 4: Code Generation

Generate Python code using Playwright or requests/BeautifulSoup depending on the strategy.

## Key Discovery Techniques

### Finding List Items

Look for:
- Container elements with repeating children (lists, grids)
- Common class patterns (`item`, `card`, `listing`, `row`)
- Semantic HTML (`<article>`, `<li>` with similar structure)
- Data attributes (`data-id`, `data-sku`, etc.)

### Handling Pagination

Check for:
- Numbered page links (`?page=2`, `/page/2/`)
- "Next" buttons with href or JavaScript handlers
- Infinite scroll (scroll event listeners)
- "Load More" buttons

### Detecting Dynamic Content

- Monitor Network tab for XHR/Fetch requests
- Check if content loads after initial page load
- Look for skeleton loaders or loading states

### Finding Hidden Data Sources

- Search page source for JSON blobs in `<script>` tags
- Look for `window.__` prefixed variables
- Check for GraphQL endpoints or REST APIs

## Anti-Scraping Detection

Watch for:
- CAPTCHA challenges (reCAPTCHA, hCaptcha)
- Rate limiting (429 errors, delays)
- Bot detection pages
- Required headers/cookies
- Fingerprinting scripts

If detected, note the specific measures and suggest workarounds.

## Code Templates

### Template 1: Static Page Scraping (BeautifulSoup)

```python
import requests
from bs4 import BeautifulSoup

def scrape_static(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    items = []
    for element in soup.select('CSS_SELECTOR_HERE'):
        item = {
            'field': element.select_one('SELECTOR').get_text(strip=True),
        }
        items.append(item)

    return items
```

### Template 2: Dynamic Content (Playwright)

```python
from playwright.sync_api import sync_playwright

def scrape_dynamic(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)

        # Wait for content to load
        page.wait_for_selector('CSS_SELECTOR_HERE')

        items = page.eval_on_selector_all('ITEM_SELECTOR', '''
            elements => elements.map(el => ({
                field: el.querySelector('SELECTOR')?.textContent?.trim(),
            }))
        ''')

        browser.close()
        return items
```

### Template 3: API Endpoint Extraction

```python
import requests

def scrape_api(endpoint):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
    }

    all_items = []
    page = 1

    while True:
        response = requests.get(f'{endpoint}?page={page}', headers=headers)
        data = response.json()

        items = data.get('results', data.get('items', []))
        if not items:
            break

        all_items.extend(items)
        page += 1

    return all_items
```

## Output Format

After exploration, provide:

1. **Site Analysis Summary**
   - Site type (static, SPA, e-commerce, etc.)
   - Technology stack (React, Vue, jQuery, etc.)
   - Anti-scraping measures detected

2. **Recommended Strategy**
   - Best approach with justification
   - Alternative approaches considered

3. **Working Code**
   - Complete, runnable Python script
   - Comments explaining key selectors
   - Error handling for edge cases

4. **Selector Reference**
   - Key CSS selectors discovered
   - XPath alternatives if needed

5. **Limitations & Considerations**
   - Rate limiting concerns
   - Data volume estimates
   - Legal/ethical considerations

## Example Session

**User**: "Help me scrape wedding vendor listings from singaporebrides.com"

**Approach**:
1. Navigate to the vendor directory page
2. Analyze the listing structure (cards, grid layout)
3. Identify selectors for: name, category, location, rating, image
4. Check pagination mechanism
5. Generate extraction code with proper waiting
6. Test on 2-3 pages to verify

## Best Practices

- Always respect robots.txt
- Add delays between requests (be polite)
- Handle errors gracefully (missing fields, changed structure)
- Use session persistence for cookies if needed
- Start with small test runs before full extraction
- Document discovered selectors for maintenance