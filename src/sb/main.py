"""SingaporeBrides.com Wedding Banquet Price List Scraper

Scrapes wedding venue pricing data from singaporebrides.com/wedding-banquet-price-list/
"""

import argparse
import asyncio
import csv
import json
import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from playwright.async_api import async_playwright

BASE_URL = "https://singaporebrides.com"
PRICE_LIST_URL = f"{BASE_URL}/wedding-banquet-price-list/"


@asynccontextmanager
async def get_browser_page(headless=True):
    async with async_playwright() as p:
        browser = None
        try:
            browser = await p.chromium.launch(headless=headless)
            page = await browser.new_page()
            yield page
        finally:
            if browser is not None:
                await browser.close()


async def extract_venues_from_page(page) -> list[dict[str, Any]]:
    """Extract all venue data from the price list page using JavaScript"""
    print("Extracting venue data from page...")

    venues = await page.evaluate(r"""() => {
        const venues = [];

        // Find all list items that contain venue information
        const listItems = document.querySelectorAll('ul li');

        listItems.forEach(li => {
            // Check if this li contains a venue card (h3 heading + table)
            const h3 = li.querySelector('h3');
            const table = li.querySelector('table');

            if (!h3 || !table) return;

            const venueData = {};

            // Extract venue name from h3
            venueData.name = h3.textContent.trim();

            // Check if there's a link to venue detail page
            const venueLink = h3.closest('a') || li.querySelector('a[href^="/d/"]');
            if (venueLink && venueLink.href) {
                venueData.url = venueLink.href;
                const slugMatch = venueLink.href.match(/\/d\/([^/]+)/);
                if (slugMatch) {
                    venueData.slug = slugMatch[1];
                }
            }

            // Extract contact info from definition list
            const dl = li.querySelector('dl');
            if (dl) {
                const dts = dl.querySelectorAll('dt');
                const dds = dl.querySelectorAll('dd');

                dts.forEach((dt, idx) => {
                    const key = dt.textContent.trim().toLowerCase();
                    const dd = dds[idx];
                    if (!dd) return;

                    if (key === 'phone') {
                        venueData.phone = dd.textContent.trim();
                    } else if (key === 'email') {
                        const emailLink = dd.querySelector('a');
                        venueData.email = emailLink ? emailLink.textContent.trim() : dd.textContent.trim();
                    } else if (key === 'address') {
                        venueData.address = dd.textContent.trim();
                    }
                });
            }

            // Extract pricing table data
            // Table structure: | Day | Lunch | Dinner | Tables | Day |
            const rows = table.querySelectorAll('tr');
            const pricing = [];

            rows.forEach(row => {
                const cells = row.querySelectorAll('td');
                if (cells.length >= 4) {
                    // Based on the actual HTML, columns are:
                    // cells[0] = Day (Mon–Thu, Friday, etc.)
                    // cells[1] = Lunch price
                    // cells[2] = Dinner price
                    // cells[3] = Tables range
                    // cells[4] = Day (repeated, may not exist in all rows)
                    const day = cells[0] ? cells[0].textContent.trim() : '';
                    const lunchPrice = cells[1] ? cells[1].textContent.trim() : '';
                    const dinnerPrice = cells[2] ? cells[2].textContent.trim() : '';
                    const tables = cells[3] ? cells[3].textContent.trim() : '';

                    pricing.push({
                        day: day,
                        lunch_price: lunchPrice,
                        dinner_price: dinnerPrice,
                        tables: tables
                    });
                }
            });

            if (pricing.length > 0) {
                venueData.pricing = pricing;

                // Also extract flat fields for CSV compatibility
                pricing.forEach(p => {
                    const dayLower = p.day.toLowerCase();
                    if (dayLower.includes('mon') || dayLower.includes('thu')) {
                        venueData.mon_thu_lunch = p.lunch_price;
                        venueData.mon_thu_dinner = p.dinner_price;
                        venueData.mon_thu_tables = p.tables;
                    } else if (dayLower.includes('fri')) {
                        venueData.friday_lunch = p.lunch_price;
                        venueData.friday_dinner = p.dinner_price;
                        venueData.friday_tables = p.tables;
                    } else if (dayLower.includes('sat')) {
                        venueData.saturday_lunch = p.lunch_price;
                        venueData.saturday_dinner = p.dinner_price;
                        venueData.saturday_tables = p.tables;
                    } else if (dayLower.includes('sun')) {
                        venueData.sunday_lunch = p.lunch_price;
                        venueData.sunday_dinner = p.dinner_price;
                        venueData.sunday_tables = p.tables;
                    }
                });
            }

            if (venueData.name) {
                venues.push(venueData);
            }
        });

        return venues;
    }""")

    print(f"Extracted {len(venues)} venues")
    return venues


async def scrape_price_list_async(headless: bool = True) -> list[dict[str, Any]]:
    """Scrape the wedding banquet price list page"""
    print(f"Navigating to {PRICE_LIST_URL}...")

    async with get_browser_page(headless=headless) as page:
        await page.goto(PRICE_LIST_URL, wait_until="load", timeout=60000)
        await page.wait_for_timeout(3000)  # Wait for dynamic content

        venues = await extract_venues_from_page(page)

        return venues


async def download_pdf(client: httpx.AsyncClient, url: str, save_path: Path) -> bool:
    """Download a PDF file to the specified path"""
    try:
        if save_path.exists():
            print(f"  Skipping {save_path.name} (already exists)")
            return True

        response = await client.get(url, timeout=30, follow_redirects=True)
        response.raise_for_status()

        save_path.parent.mkdir(parents=True, exist_ok=True)

        with open(save_path, "wb") as f:
            f.write(response.content)

        print(f"  Downloaded {save_path.name}")
        return True
    except Exception as e:
        print(f"  Error downloading PDF {url}: {e}")
        return False


async def scrape_venue_details(client: httpx.AsyncClient, slug: str) -> dict[str, Any]:
    """Scrape individual venue page for additional details"""
    url = f"{BASE_URL}/d/{slug}/"
    try:
        response = await client.get(url, timeout=30, follow_redirects=True)
        response.raise_for_status()

        details: dict[str, Any] = {"url": url, "slug": slug}
        html = response.text

        # Extract about section
        about_match = re.search(
            r'<h2[^>]*>About</h2>(.*?)(?:<h2|<div[^>]*class="[^"]*section)', html, re.DOTALL | re.IGNORECASE
        )
        if about_match:
            about_text = re.sub(r"<[^>]+>", " ", about_match.group(1))
            about_text = re.sub(r"\s+", " ", about_text).strip()
            details["about"] = about_text[:1000] if len(about_text) > 1000 else about_text

        # Extract PDF links from Wedding Packages section
        pdf_pattern = r'href="(/d/system/documents/contents/[^"]+\.pdf[^"]*)"'
        pdf_matches = re.findall(pdf_pattern, html)
        if pdf_matches:
            details["pdf_urls"] = [f"{BASE_URL}{url}" for url in pdf_matches]

        # Extract contact info
        contact_match = re.search(
            r'<h2[^>]*>Contact</h2>(.*?)(?:<h2|<div[^>]*class="[^"]*section|<footer)', html, re.DOTALL | re.IGNORECASE
        )
        if contact_match:
            contact_html = contact_match.group(1)

            # Phone
            phone_match = re.search(r"T:\s*([+\d\s]+)", contact_html)
            if phone_match:
                details["phone"] = phone_match.group(1).strip()

            # Email
            email_match = re.search(r'E:\s*<a[^>]*href="mailto:([^"?]+)', contact_html)
            if email_match:
                details["email"] = email_match.group(1).strip()

            # Website
            website_match = re.search(r'W:\s*<a[^>]*href="([^"]+)"', contact_html)
            if website_match and "mailto:" not in website_match.group(1):
                details["website"] = website_match.group(1).strip()

            # Address
            addr_match = re.search(r"A:\s*([^<]+)", contact_html)
            if addr_match:
                details["address"] = addr_match.group(1).strip()

        # Extract social links
        facebook_match = re.search(r'href="(https://www\.facebook\.com/[^"]+)"', html)
        if facebook_match:
            details["facebook"] = facebook_match.group(1)

        instagram_match = re.search(r'href="(https://www\.instagram\.com/[^"]+)"', html)
        if instagram_match:
            details["instagram"] = instagram_match.group(1)

        # Extract video links
        video_pattern = r'href="(https://(?:youtu\.be|www\.youtube\.com|streamable\.com)[^"]*)"'
        video_matches = re.findall(video_pattern, html)
        if video_matches:
            details["videos"] = video_matches

        return details

    except Exception as e:
        print(f"  Error fetching details for {slug}: {e}")
        return {}


async def enrich_venues_with_details(venues: list[dict[str, Any]], download_pdfs: bool = True) -> list[dict[str, Any]]:
    """Enrich venue data with details from individual venue pages"""
    print("\nEnriching venues with detailed information...")

    async with httpx.AsyncClient(timeout=60) as client:
        for idx, venue in enumerate(venues, 1):
            slug = venue.get("slug")
            if not slug:
                # Try to extract slug from URL
                url = venue.get("url", "")
                if url:
                    slug_match = re.search(r"/d/([^/]+)/?", url)
                    if slug_match:
                        slug = slug_match.group(1)
                        venue["slug"] = slug

            if not slug:
                # Generate slug from name
                name = venue.get("name", "unknown")
                slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
                venue["slug"] = slug

            print(f"  [{idx}/{len(venues)}] Fetching details for {venue.get('name', 'N/A')}...")

            details = await scrape_venue_details(client, slug)

            # Merge details into venue
            for key, value in details.items():
                if key not in venue or not venue.get(key):
                    venue[key] = value

            # Download PDFs if requested
            if download_pdfs and venue.get("pdf_urls"):
                pdf_dir = Path(f"data/sb/price-lists/{slug}")
                downloaded_pdfs = []

                for pdf_url in venue["pdf_urls"]:
                    filename = pdf_url.split("/")[-1].split("?")[0]
                    save_path = pdf_dir / filename

                    success = await download_pdf(client, pdf_url, save_path)
                    if success:
                        downloaded_pdfs.append(pdf_url)

                venue["downloaded_pdfs"] = downloaded_pdfs

            # Rate limiting
            await asyncio.sleep(0.5)

    return venues


def save_to_files(data: list[dict[str, Any]], output_path: str):
    """Save data to JSON and CSV files"""
    if not data:
        print("No data to save")
        return

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    json_file = output_file.with_suffix(".json")
    csv_file = output_file.with_suffix(".csv")

    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    if data:
        all_keys = set()
        for item in data:
            all_keys.update(item.keys())

        # Exclude nested pricing from CSV
        csv_keys = sorted([k for k in all_keys if k != "pricing"])

        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=csv_keys, extrasaction="ignore")
            writer.writeheader()
            for item in data:
                row = item.copy()
                for key, value in row.items():
                    if isinstance(value, list):
                        row[key] = json.dumps(value)
                writer.writerow(row)

    print(f"Saved {len(data)} items to:")
    print(f"   - {json_file}")
    print(f"   - {csv_file}")


def scrape_price_list(headless: bool = True) -> list[dict[str, Any]]:
    """Synchronous wrapper for async scraper"""
    return asyncio.run(scrape_price_list_async(headless=headless))


async def scrape_all_async(
    headless: bool = True, fetch_details: bool = True, download_pdfs: bool = True
) -> list[dict[str, Any]]:
    """Main async function to scrape all data"""
    print("=" * 60)
    print("SCRAPING SINGAPOREBRIDES.COM WEDDING BANQUET PRICE LIST")
    print("=" * 60 + "\n")

    venues = await scrape_price_list_async(headless=headless)

    if fetch_details and venues:
        venues = await enrich_venues_with_details(venues, download_pdfs=download_pdfs)

    return venues


def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(description="Scrape SingaporeBrides.com wedding venue data")

    parser.add_argument("--output", type=str, default="data/sb/venues", help="Output file path (without extension)")
    parser.add_argument("--no-headless", action="store_true", help="Show browser window")
    parser.add_argument("--no-details", action="store_true", help="Skip fetching individual venue details")
    parser.add_argument("--no-pdfs", action="store_true", help="Skip downloading PDFs")

    args = parser.parse_args()

    venues = asyncio.run(
        scrape_all_async(
            headless=not args.no_headless, fetch_details=not args.no_details, download_pdfs=not args.no_pdfs
        )
    )

    save_to_files(venues, args.output)

    if venues:
        print("\n" + "=" * 60)
        print("SCRAPING COMPLETE")
        print("=" * 60)
        print(f"\nTotal venues: {len(venues)}")

        # Count stats
        with_details = sum(1 for v in venues if v.get("about"))
        with_pdfs = sum(1 for v in venues if v.get("pdf_urls"))
        downloaded = sum(1 for v in venues if v.get("downloaded_pdfs"))

        print(f"Venues with details: {with_details}")
        print(f"Venues with PDF links: {with_pdfs}")
        print(f"Venues with downloaded PDFs: {downloaded}")

        print("\nSample venues:")
        for idx, venue in enumerate(venues[:5], 1):
            print(f"\n{idx}. {venue.get('name', 'N/A')}")
            if venue.get("phone"):
                print(f"   Phone: {venue['phone']}")
            if venue.get("address"):
                print(f"   Address: {venue['address'][:60]}...")
            if venue.get("mon_thu_dinner"):
                print(f"   Mon-Thu Dinner: {venue['mon_thu_dinner']}")
            if venue.get("saturday_dinner"):
                print(f"   Saturday Dinner: {venue['saturday_dinner']}")


if __name__ == "__main__":
    main()
