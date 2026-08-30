"""SingaporeBrides.com Wedding Banquet Price List Extractor

Extracts wedding venue pricing data from singaporebrides.com/wedding-banquet-price-list/
"""

import argparse
import asyncio
import re
from pathlib import Path
from typing import Any

import httpx
from bs4 import BeautifulSoup

from src.shared import get_logger, save_json_csv
from src.shared.config import get_headers

logger = get_logger()
BASE_URL = "https://singaporebrides.com"
PRICE_LIST_URL = f"{BASE_URL}/wedding-banquet-price-list/"


def decode_cf_email(cfemail: str) -> str:
    """Decode a Cloudflare-obfuscated email (data-cfemail attribute)"""
    key = int(cfemail[:2], 16)
    return "".join(chr(int(cfemail[i : i + 2], 16) ^ key) for i in range(2, len(cfemail), 2))


def extract_venues_from_html(html: str) -> list[dict[str, Any]]:
    """Extract all venue data from the price list page HTML"""
    soup = BeautifulSoup(html, "html.parser")
    venues = []

    for li in soup.select("ul.pricelist li"):
        h3 = li.find("h3")
        table = li.find("table")
        if not h3 or not table:
            continue

        venue_data: dict[str, Any] = {"name": h3.get_text(strip=True)}

        # The detail page URL is stored in div.venue's data-link attribute
        venue_div = h3.find_parent("div", class_="venue")
        if venue_div and venue_div.get("data-link"):
            link = str(venue_div["data-link"])
            venue_data["url"] = link
            slug_match = re.search(r"/d/([^/]+)", link)
            if slug_match:
                venue_data["slug"] = slug_match.group(1)

        # Extract contact info from definition list
        dl = li.find("dl")
        if dl:
            dts = list(dl.find_all("dt"))
            dds = list(dl.find_all("dd"))
            for dt, dd in zip(dts, dds, strict=False):
                key = dt.get_text(strip=True).lower()
                if key == "phone":
                    venue_data["phone"] = dd.get_text(strip=True)
                elif key == "email":
                    cf_span = dd.find("span", class_="__cf_email__")
                    if cf_span and cf_span.get("data-cfemail"):
                        venue_data["email"] = decode_cf_email(str(cf_span["data-cfemail"]))
                    else:
                        a = dd.find("a")
                        venue_data["email"] = a.get_text(strip=True) if a else dd.get_text(strip=True)
                elif key == "address":
                    venue_data["address"] = dd.get_text(separator="\n", strip=True)

        # Extract pricing table data
        # Table structure: | Day | Lunch | Dinner | Tables | Day |
        pricing = []
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 4:
                pricing.append(
                    {
                        "day": cells[0].get_text(strip=True),
                        "lunch_price": cells[1].get_text(strip=True),
                        "dinner_price": cells[2].get_text(strip=True),
                        "tables": cells[3].get_text(strip=True),
                    }
                )

        if pricing:
            venue_data["pricing"] = pricing

            # Also extract flat fields for CSV compatibility
            for p in pricing:
                day_lower = p["day"].lower()
                if "mon" in day_lower or "thu" in day_lower:
                    prefix = "mon_thu"
                elif "fri" in day_lower:
                    prefix = "friday"
                elif "sat" in day_lower:
                    prefix = "saturday"
                elif "sun" in day_lower:
                    prefix = "sunday"
                else:
                    continue
                venue_data[f"{prefix}_lunch"] = p["lunch_price"]
                venue_data[f"{prefix}_dinner"] = p["dinner_price"]
                venue_data[f"{prefix}_tables"] = p["tables"]

        if venue_data.get("name"):
            venues.append(venue_data)

    return venues


async def extract_price_list_async() -> list[dict[str, Any]]:
    """Extract the wedding banquet price list page"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                response = await client.get(PRICE_LIST_URL, headers=get_headers())
                response.raise_for_status()

            venues = extract_venues_from_html(response.text)
            if venues:
                return venues
            logger.warning("Attempt %d: no venues found, retrying...", attempt + 1)
        except Exception as e:
            logger.warning("Attempt %d failed: %s", attempt + 1, e)
        if attempt < max_retries - 1:
            await asyncio.sleep(2**attempt)
    return []


async def download_pdf(client: httpx.AsyncClient, url: str, save_path: Path) -> bool:
    """Download a PDF file to the specified path"""
    try:
        if save_path.exists():
            return True

        response = await client.get(url, timeout=30, follow_redirects=True)
        response.raise_for_status()

        save_path.parent.mkdir(parents=True, exist_ok=True)

        with open(save_path, "wb") as f:
            f.write(response.content)

        return True
    except Exception as e:
        logger.error("Failed to download %s: %s", save_path.name, e)
        return False


async def extract_venue_details(client: httpx.AsyncClient, slug: str) -> dict[str, Any]:
    """Extract individual venue page for additional details"""
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
        logger.error(f"    ! Error fetching details for {slug}: {e}")
        return {}


async def enrich_venues_with_details(venues: list[dict[str, Any]], download_pdfs: bool = True) -> list[dict[str, Any]]:
    """Enrich venue data with details from individual venue pages"""
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
                logger.debug(f"  Skipping {venue.get('name', 'N/A')}: no detail URL found")
                continue

            logger.debug(f"[{idx}/{len(venues)}] Fetching details for {venue.get('name', 'N/A')}")

            details = await extract_venue_details(client, slug)

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


def extract_price_list() -> list[dict[str, Any]]:
    """Synchronous wrapper for async extractor"""
    return asyncio.run(extract_price_list_async())


async def extract_all_async(fetch_details: bool = True, download_pdfs: bool = True) -> list[dict[str, Any]]:
    """Main async function to extract all data"""
    logger.info("Extracting SingaporeBrides.com wedding venues...")

    venues = await extract_price_list_async()
    logger.info(f"Found {len(venues)} venues")

    if fetch_details and venues:
        venues = await enrich_venues_with_details(venues, download_pdfs=download_pdfs)

    return venues


def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(description="Extract SingaporeBrides.com wedding venue data")

    parser.add_argument("--output", type=str, default="data/sb/venues", help="Output file path (without extension)")
    parser.add_argument("--no-details", action="store_true", help="Skip fetching individual venue details")
    parser.add_argument("--no-pdfs", action="store_true", help="Skip downloading PDFs")

    args = parser.parse_args()

    venues = asyncio.run(extract_all_async(fetch_details=not args.no_details, download_pdfs=not args.no_pdfs))

    save_json_csv(venues, args.output)

    if venues:
        with_details = sum(1 for v in venues if v.get("about"))
        with_pdfs = sum(1 for v in venues if v.get("pdf_urls"))
        logger.info(f"\nTotal: {len(venues)} venues ({with_details} with details, {with_pdfs} with PDFs)")


if __name__ == "__main__":
    main()
