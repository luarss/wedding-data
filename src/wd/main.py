import asyncio
import csv
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlparse

import httpx
from playwright.async_api import async_playwright

from src.shared import get_logger, save_json

logger = get_logger()


def parse_venue_slug(url: str) -> str:
    path = urlparse(url).path
    return path.strip("/").split("/")[-1]


async def fetch_sitemap() -> list[str]:
    sitemap_url = "https://wedded.sg/sitemap.xml"
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(sitemap_url)
        root = ET.fromstring(response.content)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        locs = root.findall(".//sm:loc", ns)
        return [loc.text for loc in locs if "/venues/" in loc.text and loc.text.count("/") == 4]


async def download_pdf(url: str, venue_slug: str, filename: str) -> str | None:
    pdf_dir = Path("data/wd/price-lists") / venue_slug
    pdf_dir.mkdir(parents=True, exist_ok=True)

    filepath = pdf_dir / filename

    if filepath.exists():
        return str(filepath)

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()

            filepath.write_bytes(response.content)
            return str(filepath)
    except Exception as e:
        logger.error(f"    ! Failed to download PDF: {filename} - {e}")
        return None


def load_scraper_script() -> str:
    script_path = Path(__file__).parent / "scraper.js"
    with open(script_path, encoding="utf-8") as f:
        content = f.read()
    return f"(() => {{ {content} return scrapeVenuePage(); }})()"


async def scrape_venue_page(page, url: str, max_retries: int = 2) -> dict:
    slug = parse_venue_slug(url)

    for attempt in range(max_retries + 1):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_selector("#packages", timeout=15000)
            await asyncio.sleep(1)
            break
        except Exception:
            if attempt == max_retries:
                raise
            await asyncio.sleep(2**attempt)

    scraper_script = load_scraper_script()
    venue_data = await page.evaluate(scraper_script)

    vendor_id = venue_data.get("vendorId") or None
    pdf_links = venue_data.get("pdfLinks", [])

    pdfs = []
    if vendor_id and pdf_links:
        for pdf in pdf_links:
            filename = pdf.get("filename", "").replace("/", "_")
            if not filename.endswith(".pdf"):
                filename = filename + ".pdf"

            local_path = await download_pdf(pdf.get("url", ""), slug, filename)

            pdfs.append({"filename": pdf.get("filename", ""), "url": pdf.get("url", ""), "local_path": local_path})

    return {
        "name": venue_data.get("name", ""),
        "slug": slug,
        "url": url,
        "vendor_id": vendor_id,
        "rooms": venue_data.get("rooms", []),
        "pdfs": pdfs,
    }


async def scrape_all_venues(urls: list[str], concurrent_limit: int = 5) -> list[dict]:
    semaphore = asyncio.Semaphore(concurrent_limit)

    async def scrape_with_semaphore(browser, url: str, index: int) -> dict:
        async with semaphore:
            logger.info(f"[{index}/{len(urls)}] Scraping: {url}")
            try:
                page = await browser.new_page()
                try:
                    result = await scrape_venue_page(page, url)
                    pdf_count = len(result.get("pdfs", []))
                    pdf_info = f", {pdf_count} PDFs" if pdf_count > 0 else ""
                    logger.info(f"  -> {result['name']}: {len(result['rooms'])} rooms{pdf_info}")
                    await asyncio.sleep(1)
                    return result
                finally:
                    await page.close()
            except Exception as e:
                logger.error(f"  -> ERROR: {type(e).__name__}: {str(e)[:100]}")
                return None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            tasks = [scrape_with_semaphore(browser, url, i + 1) for i, url in enumerate(urls)]
            results = await asyncio.gather(*tasks)
        finally:
            await browser.close()

    return [r for r in results if r is not None]



def save_to_csv(venues: list[dict], filename: str):
    if not venues:
        logger.info("No venues to save")
        return

    Path(filename).parent.mkdir(parents=True, exist_ok=True)

    flattened = []
    for venue in venues:
        pdfs = venue.get("pdfs", [])
        pdf_count = len(pdfs)
        pdf_filenames = " | ".join([p.get("filename", "") for p in pdfs])

        for room in venue.get("rooms", []):
            for package in room.get("packages", []):
                flattened.append(
                    {
                        "name": venue.get("name", ""),
                        "slug": venue.get("slug", ""),
                        "url": venue.get("url", ""),
                        "vendor_id": venue.get("vendor_id", ""),
                        "pdf_count": pdf_count,
                        "pdf_filenames": pdf_filenames,
                        "room_name": room.get("name", ""),
                        "room_types": ", ".join(room.get("types", [])),
                        "room_id": room.get("room_id", ""),
                        "day": package.get("day", ""),
                        "menu": package.get("menu", ""),
                        "capacity_min": package.get("capacity_min", ""),
                        "capacity_max": package.get("capacity_max", ""),
                        "price": package.get("price", ""),
                    }
                )
            if not room.get("packages"):
                flattened.append(
                    {
                        "name": venue.get("name", ""),
                        "slug": venue.get("slug", ""),
                        "url": venue.get("url", ""),
                        "vendor_id": venue.get("vendor_id", ""),
                        "pdf_count": pdf_count,
                        "pdf_filenames": pdf_filenames,
                        "room_name": room.get("name", ""),
                        "room_types": ", ".join(room.get("types", [])),
                        "room_id": room.get("room_id", ""),
                        "day": "",
                        "menu": "",
                        "capacity_min": "",
                        "capacity_max": "",
                        "price": "",
                    }
                )
        if not venue.get("rooms"):
            flattened.append(
                {
                    "name": venue.get("name", ""),
                    "slug": venue.get("slug", ""),
                    "url": venue.get("url", ""),
                    "vendor_id": venue.get("vendor_id", ""),
                    "pdf_count": pdf_count,
                    "pdf_filenames": pdf_filenames,
                    "room_name": "",
                    "room_types": "",
                    "room_id": "",
                    "day": "",
                    "menu": "",
                    "capacity_min": "",
                    "capacity_max": "",
                    "price": "",
                }
            )

    fieldnames = [
        "name",
        "slug",
        "url",
        "vendor_id",
        "pdf_count",
        "pdf_filenames",
        "room_name",
        "room_types",
        "room_id",
        "day",
        "menu",
        "capacity_min",
        "capacity_max",
        "price",
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(flattened)

    logger.info(f"Saved to {filename}")


async def main(limit: int = 0):
    logger.info("Fetching venue URLs from sitemap...")
    urls = await fetch_sitemap()
    if limit > 0:
        urls = urls[:limit]
        logger.info(f"Limited to {limit} venues for testing")
    logger.info(f"Found {len(urls)} venue URLs to scrape")

    logger.info("Scraping venues from wedded.sg using Playwright...")
    venues = await scrape_all_venues(urls)

    logger.info(f"\nTotal venues scraped: {len(venues)}")

    total_pdfs = sum(len(v.get("pdfs", [])) for v in venues)
    venues_with_pdfs = sum(1 for v in venues if v.get("pdfs"))
    logger.info(f"Total PDFs downloaded: {total_pdfs} ({venues_with_pdfs} venues have PDFs)")

    json_file = "data/wd/venues.json"
    save_json(venues, json_file)

    csv_file = "data/wd/venues.csv"
    save_to_csv(venues, csv_file)

    logger.info("\nFirst 3 venues:")
    for i, venue in enumerate(venues[:3], 1):
        pdf_count = len(venue.get("pdfs", []))
        pdf_info = f", {pdf_count} PDFs" if pdf_count > 0 else ""
        logger.info(f"{i}. {venue['name']}{pdf_info}")
        for room in venue.get("rooms", []):
            logger.info(f"   - {room['name']}: {len(room['packages'])} packages")


if __name__ == "__main__":
    import sys

    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    asyncio.run(main(limit))
