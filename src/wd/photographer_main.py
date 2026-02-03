import asyncio
import csv
import json
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx
from playwright.async_api import async_playwright


def parse_photographer_slug(url: str) -> str:
    path = urlparse(url).path
    return path.strip("/").split("/")[-1]


async def fetch_photographer_sitemap() -> list[str]:
    sitemap_url = "https://wedded.sg/sitemap.xml"
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(sitemap_url)
        root = ET.fromstring(response.content)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        locs = root.findall(".//sm:loc", ns)
        return [loc.text for loc in locs if "/photographers/" in loc.text and loc.text.count("/") == 4]


def load_photographer_scraper_script() -> str:
    script_path = Path(__file__).parent / "photographer_scraper.js"
    with open(script_path, encoding="utf-8") as f:
        content = f.read()
    return f"(() => {{ {content} return scrapePhotographerPage(); }})()"


async def scrape_photographer_page(page, url: str, max_retries: int = 2) -> dict:
    slug = parse_photographer_slug(url)

    for attempt in range(max_retries + 1):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_selector("h1", timeout=15000)
            await asyncio.sleep(1)
            break
        except Exception:
            if attempt == max_retries:
                raise
            await asyncio.sleep(2**attempt)

    scraper_script = load_photographer_scraper_script()
    photographer_data = await page.evaluate(scraper_script)

    vendor_id = photographer_data.get("vendorId") or None
    contact = photographer_data.get("contact", {})
    social = photographer_data.get("social", {})
    packages = photographer_data.get("packages", [])
    portfolio = photographer_data.get("portfolio", {})
    reviews = photographer_data.get("reviews", {})

    return {
        "name": photographer_data.get("name", ""),
        "slug": slug,
        "url": url,
        "vendor_id": vendor_id,
        "business_type": photographer_data.get("businessType", "photographer"),
        "contact": {
            "email": contact.get("email"),
            "whatsapp_number": contact.get("whatsapp_number"),
        },
        "social": {
            "website": social.get("website"),
            "instagram": social.get("instagram"),
            "facebook": social.get("facebook"),
        },
        "description": photographer_data.get("description", ""),
        "packages": packages,
        "portfolio": {
            "photobook_count": portfolio.get("photobook_count", 0),
            "sample_images": portfolio.get("sample_images", []),
        },
        "reviews": {
            "average_rating": reviews.get("average_rating"),
            "review_count": reviews.get("review_count", 0),
        },
        "scraped_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }


async def scrape_all_photographers(urls: list[str], concurrent_limit: int = 5) -> list[dict]:
    semaphore = asyncio.Semaphore(concurrent_limit)

    async def scrape_with_semaphore(browser, url: str, index: int) -> dict:
        async with semaphore:
            print(f"[{index}/{len(urls)}] Scraping: {url}")
            try:
                page = await browser.new_page()
                try:
                    result = await scrape_photographer_page(page, url)
                    package_count = len(result.get("packages", []))
                    photobook_count = result.get("portfolio", {}).get("photobook_count", 0)
                    print(f"  -> {result['name']}: {package_count} packages, {photobook_count} photobooks")
                    await asyncio.sleep(1)
                    return result
                finally:
                    await page.close()
            except Exception as e:
                print(f"  -> ERROR: {type(e).__name__}: {str(e)[:100]}")
                return None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            tasks = [scrape_with_semaphore(browser, url, i + 1) for i, url in enumerate(urls)]
            results = await asyncio.gather(*tasks)
        finally:
            await browser.close()

    return [r for r in results if r is not None]


def save_to_json(photographers: list[dict], filename: str):
    Path(filename).parent.mkdir(parents=True, exist_ok=True)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(photographers, f, indent=2, ensure_ascii=False)

    print(f"Saved to {filename}")


def save_to_csv(photographers: list[dict], filename: str):
    if not photographers:
        print("No photographers to save")
        return

    Path(filename).parent.mkdir(parents=True, exist_ok=True)

    flattened = []
    for photographer in photographers:
        packages = photographer.get("packages", [])
        contact = photographer.get("contact", {})
        social = photographer.get("social", {})
        portfolio = photographer.get("portfolio", {})
        reviews = photographer.get("reviews", {})

        if packages:
            for package in packages:
                flattened.append(
                    {
                        "name": photographer.get("name", ""),
                        "slug": photographer.get("slug", ""),
                        "url": photographer.get("url", ""),
                        "vendor_id": photographer.get("vendor_id", ""),
                        "business_type": photographer.get("business_type", ""),
                        "email": contact.get("email", ""),
                        "whatsapp_number": contact.get("whatsapp_number", ""),
                        "website": social.get("website", ""),
                        "instagram": social.get("instagram", ""),
                        "facebook": social.get("facebook", ""),
                        "description": photographer.get("description", ""),
                        "package_title": package.get("title", ""),
                        "package_duration": package.get("duration", ""),
                        "package_price": package.get("price", ""),
                        "package_details": package.get("details", "").replace("\n", " | "),
                        "photobook_count": portfolio.get("photobook_count", 0),
                        "average_rating": reviews.get("average_rating", ""),
                        "review_count": reviews.get("review_count", 0),
                        "scraped_at": photographer.get("scraped_at", ""),
                    }
                )
        else:
            flattened.append(
                {
                    "name": photographer.get("name", ""),
                    "slug": photographer.get("slug", ""),
                    "url": photographer.get("url", ""),
                    "vendor_id": photographer.get("vendor_id", ""),
                    "business_type": photographer.get("business_type", ""),
                    "email": contact.get("email", ""),
                    "whatsapp_number": contact.get("whatsapp_number", ""),
                    "website": social.get("website", ""),
                    "instagram": social.get("instagram", ""),
                    "facebook": social.get("facebook", ""),
                    "description": photographer.get("description", ""),
                    "package_title": "",
                    "package_duration": "",
                    "package_price": "",
                    "package_details": "",
                    "photobook_count": portfolio.get("photobook_count", 0),
                    "average_rating": reviews.get("average_rating", ""),
                    "review_count": reviews.get("review_count", 0),
                    "scraped_at": photographer.get("scraped_at", ""),
                }
            )

    fieldnames = [
        "name",
        "slug",
        "url",
        "vendor_id",
        "business_type",
        "email",
        "whatsapp_number",
        "website",
        "instagram",
        "facebook",
        "description",
        "package_title",
        "package_duration",
        "package_price",
        "package_details",
        "photobook_count",
        "average_rating",
        "review_count",
        "scraped_at",
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(flattened)

    print(f"Saved to {filename}")


async def main(limit: int = 0):
    print("Fetching photographer URLs from sitemap...")
    urls = await fetch_photographer_sitemap()
    if limit > 0:
        urls = urls[:limit]
        print(f"Limited to {limit} photographers for testing")
    print(f"Found {len(urls)} photographer URLs to scrape")

    print("Scraping photographers from wedded.sg using Playwright...")
    photographers = await scrape_all_photographers(urls)

    print(f"\nTotal photographers scraped: {len(photographers)}")

    total_packages = sum(len(p.get("packages", [])) for p in photographers)
    total_photobooks = sum(p.get("portfolio", {}).get("photobook_count", 0) for p in photographers)
    photographers_with_packages = sum(1 for p in photographers if p.get("packages"))
    print(f"Total packages: {total_packages} ({photographers_with_packages} photographers have packages)")
    print(f"Total photobooks: {total_photobooks}")

    json_file = "data/wd/photographers.json"
    save_to_json(photographers, json_file)

    csv_file = "data/wd/photographers.csv"
    save_to_csv(photographers, csv_file)

    print("\nFirst 3 photographers:")
    for i, photographer in enumerate(photographers[:3], 1):
        package_count = len(photographer.get("packages", []))
        photobook_count = photographer.get("portfolio", {}).get("photobook_count", 0)
        print(f"{i}. {photographer['name']}: {package_count} packages, {photobook_count} photobooks")


if __name__ == "__main__":
    import sys

    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    asyncio.run(main(limit))
