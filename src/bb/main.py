import argparse
import csv
import json
import re
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

from ..shared.config import get_headers

BASE_URL = "https://www.blissfulbrides.sg"


async def download_pdf(client: httpx.AsyncClient, url: str, save_path: Path) -> bool:
    """Download a PDF file to the specified path"""
    try:
        if save_path.exists():
            print(f"  ⏭️  Skipping {save_path.name} (already exists)")
            return True

        response = await client.get(url, timeout=30, follow_redirects=True)
        response.raise_for_status()

        save_path.parent.mkdir(parents=True, exist_ok=True)

        with open(save_path, "wb") as f:
            f.write(response.content)

        return True
    except Exception as e:
        print(f"  ⚠️  Error downloading PDF {url}: {e}")
        return False


async def download_pdfs_for_vendor(client: httpx.AsyncClient, vendor_data: dict) -> dict:
    """Download PDFs for a single vendor"""
    if "price_lists" not in vendor_data or not vendor_data["price_lists"]:
        return vendor_data

    profile_url = vendor_data.get("profile_url", "")
    if not profile_url:
        return vendor_data

    url_parts = profile_url.rstrip("/").split("/")
    venue_slug = url_parts[-1] if url_parts else "unknown"
    venue_slug = venue_slug.lower()
    venue_slug = re.sub(r"-+", "-", venue_slug)
    venue_slug = venue_slug.strip("-")

    pdf_dir = Path(f"data/bb/price-lists/{venue_slug}")
    downloaded_pdfs = []

    for pdf_url in vendor_data["price_lists"]:
        filename = pdf_url.split("/")[-1]
        save_path = pdf_dir / filename

        if save_path.exists():
            print(f"  ⏭️  Skipping {filename} (already exists)")
            downloaded_pdfs.append(pdf_url)
            continue

        print(f"  📄 Downloading {filename}...")
        success = await download_pdf(client, pdf_url, save_path)
        if success:
            downloaded_pdfs.append(pdf_url)

    vendor_data["price_lists"] = downloaded_pdfs
    return vendor_data


async def scrape_venue_booking_details(client: httpx.AsyncClient, venue_id: str) -> dict:
    """Scrape venue booking detail page for additional information"""
    url = f"{BASE_URL}/wedding-venues-booking-details/{venue_id}"
    try:
        response = await client.get(url, timeout=30, follow_redirects=True)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        details = {}

        capacity_seated = soup.find("p", string=lambda x: x and "Seated" in str(x))
        if capacity_seated:
            seated_text = capacity_seated.get_text(strip=True)
            seated_match = re.search(r"Seated:\s*(\d+)[\s-]*(\d*)", seated_text)
            if seated_match:
                details["capacity_seated_min"] = seated_match.group(1)
                if seated_match.group(2):
                    details["capacity_seated_max"] = seated_match.group(2)

        capacity_standing = soup.find("p", string=lambda x: x and "Standing" in str(x))
        if capacity_standing:
            standing_text = capacity_standing.get_text(strip=True)
            standing_match = re.search(r"Standing:\s*(\d+)", standing_text)
            if standing_match:
                details["capacity_standing"] = standing_match.group(1)

        location_elem = soup.find("p", class_="location")
        if not location_elem:
            location_elem = soup.find(string=lambda x: x and "Singapore" in str(x) and re.search(r"\d{6}", str(x)))
        if location_elem:
            details["location"] = (
                location_elem.get_text(strip=True) if hasattr(location_elem, "get_text") else str(location_elem).strip()
            )

        pdf_links = soup.find_all("a", href=lambda x: x and x.endswith(".pdf"))
        if pdf_links:
            details["price_lists"] = []
            for link in pdf_links:
                href = link.get("href")
                if href:
                    pdf_url = f"{BASE_URL}{href}" if href.startswith("/") else href
                    details["price_lists"].append(pdf_url)

        return details

    except Exception as e:
        print(f"  ⚠️  Error fetching details for venue {venue_id}: {e}")
        return {}


async def scrape_wedding_venues_booking_async() -> list[dict]:
    """Scrape wedding venues booking page with pagination"""
    venues = []
    page = 1

    try:
        async with httpx.AsyncClient(headers=get_headers(), timeout=60) as client:
            while True:
                url = f"{BASE_URL}/wedding-venues-booking?page={page}"
                print(f"Fetching page {page}: {url}...")

                response = await client.get(url, timeout=30, follow_redirects=True)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")

                h2_tags = soup.find_all("h2")
                venue_links = []
                for h2 in h2_tags:
                    link = h2.find("a", href=lambda x: x and "/wedding-venues-booking-details/" in x)
                    if link:
                        venue_links.append(link)

                if not venue_links:
                    print(f"No venues found on page {page}, stopping pagination")
                    break

                seen_ids = set()
                for link in venue_links:
                    href = link.get("href")
                    venue_name = link.get_text(strip=True)

                    if href and "/wedding-venues-booking-details/" in href:
                        venue_id = href.split("/")[-1]

                        if venue_id in seen_ids:
                            continue
                        seen_ids.add(venue_id)

                        venue_data = {"venue_id": venue_id, "source": "wedding-venues-booking", "name": venue_name}

                        venue_card = link.find_parent("div", class_="_Featured")
                        if not venue_card:
                            venue_card = link.find_parent("div")

                        if venue_card:
                            vendor_link = venue_card.find("a", href=lambda x: x and "/detail/" in x)
                            if vendor_link:
                                vendor_name = vendor_link.get_text(strip=True)
                                vendor_href = vendor_link.get("href")
                                if vendor_name:
                                    venue_data["vendor_name"] = vendor_name
                                if vendor_href:
                                    venue_data["profile_url"] = (
                                        f"{BASE_URL}{vendor_href}" if vendor_href.startswith("/") else vendor_href
                                    )

                        if venue_data.get("name") and venue_data["name"] not in [v.get("name") for v in venues]:
                            print(f"  Found: {venue_data.get('name', 'N/A')}")
                            details = await scrape_venue_booking_details(client, venue_id)
                            venue_data.update(details)
                            venues.append(venue_data)

                page += 1

                if page > 20:
                    print("Reached page limit (20), stopping")
                    break

        print(f"\nFound {len(venues)} venues from wedding venues booking")
        return venues

    except Exception as e:
        print(f"Error scraping wedding venues booking: {e}")
        return venues


async def scrape_banquet_prices_async() -> list[dict]:
    """Scrape banquet price list table and download PDFs"""
    url = f"{BASE_URL}/wedding-banquet-price-list"

    try:
        print(f"Fetching {url}...")
        response = httpx.get(url, headers=get_headers(), timeout=30, follow_redirects=True)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        vendors = []

        table = soup.find("table", class_="table")
        if not table:
            table = soup.find("table")

        if not table:
            print("⚠️  No table found on banquet price list page")
            return []

        rows = table.find_all("tr")

        for row in rows:
            tds = row.find_all("td")

            if len(tds) >= 4:
                vendor_data = {"source": "banquet-price-list"}

                vendor_info_td = tds[0]
                strong_tag = vendor_info_td.find("strong")
                if strong_tag:
                    vendor_data["name"] = strong_tag.get_text(strip=True)
                else:
                    p_tag = vendor_info_td.find("p", style=lambda x: x and "font-size: 18px" in x)
                    if p_tag:
                        vendor_data["name"] = p_tag.get_text(strip=True)

                profile_link = vendor_info_td.find("a", href=lambda x: x and "/detail/" in x)
                if profile_link and profile_link.get("href"):
                    vendor_data["profile_url"] = (
                        f"{BASE_URL}{profile_link['href']}"
                        if profile_link["href"].startswith("/")
                        else profile_link["href"]
                    )

                rating_input = vendor_info_td.find("input", {"id": "merchant_score"})
                if rating_input and rating_input.get("value"):
                    vendor_data["rating"] = rating_input["value"]

                lunch_td = tds[1] if len(tds) > 1 else None
                if lunch_td:
                    lunch_text = lunch_td.get_text(strip=True)
                    vendor_data["lunch_price"] = lunch_text

                dinner_td = tds[2] if len(tds) > 2 else None
                if dinner_td:
                    dinner_text = dinner_td.get_text(strip=True)
                    vendor_data["dinner_price"] = dinner_text

                tables_td = tds[3] if len(tds) > 3 else None
                if tables_td:
                    tables_text = tables_td.get_text(strip=True)
                    tables_match = re.search(r"(\d+)\s*-\s*(\d+)", tables_text)
                    if tables_match:
                        vendor_data["tables_min"] = tables_match.group(1)
                        vendor_data["tables_max"] = tables_match.group(2)
                    vendor_data["tables_range"] = tables_text

                pricelist_td = tds[4] if len(tds) > 4 else None
                if pricelist_td:
                    pdf_links = pricelist_td.find_all("a", href=True)
                    price_list_urls = []
                    for link in pdf_links:
                        href = link.get("href")
                        if href and isinstance(href, str) and href.strip() and href.strip() != "#":
                            href = href.strip()
                            pdf_url = f"{BASE_URL}{href}" if href.startswith("/") else href
                            price_list_urls.append(pdf_url)
                    if price_list_urls:
                        vendor_data["price_lists"] = price_list_urls

                if vendor_data.get("name"):
                    vendors.append(vendor_data)

        print(f"Found {len(vendors)} vendors from banquet price list")
        return vendors

    except Exception as e:
        print(f"Error scraping banquet prices: {e}")
        return []


def merge_venue_data(banquet_data: list[dict], booking_data: list[dict]) -> list[dict]:
    """Merge data from both sources, preferring more complete records"""
    merged = {}

    for venue in banquet_data:
        name = venue.get("name")
        profile_url = venue.get("profile_url")

        if name:
            key = name.lower().strip()
            merged[key] = venue.copy()
            if profile_url:
                merged[key]["_profile_url"] = profile_url

    for venue in booking_data:
        name = venue.get("name")
        profile_url = venue.get("profile_url")

        if name:
            key = name.lower().strip()

            if key in merged:
                existing = merged[key]
                for field, value in venue.items():
                    if value and (field not in existing or not existing[field]):
                        existing[field] = value

                if existing.get("source") == "banquet-price-list":
                    existing["source"] = "both"
            else:
                merged[key] = venue.copy()
                if profile_url:
                    merged[key]["_profile_url"] = profile_url

    for venue in merged.values():
        if "_profile_url" in venue:
            del venue["_profile_url"]

    result = list(merged.values())
    result.sort(key=lambda x: x.get("name", "").lower())

    return result


async def scrape_all_venues_async() -> list[dict]:
    """Scrape from both sources and merge the data"""

    print("\n" + "=" * 60)
    print("SCRAPING WEDDING VENUES FROM MULTIPLE SOURCES")
    print("=" * 60 + "\n")

    print("📋 Phase 1: Scraping banquet price list...")
    print("-" * 60)
    banquet_venues = await scrape_banquet_prices_async()

    print("\n📋 Phase 2: Scraping wedding venues booking...")
    print("-" * 60)
    booking_venues = await scrape_wedding_venues_booking_async()

    print("\n🔄 Phase 3: Merging data from both sources...")
    print("-" * 60)
    merged_venues = merge_venue_data(banquet_venues, booking_venues)

    print("\n📊 Summary:")
    print(f"  - Banquet price list: {len(banquet_venues)} venues")
    print(f"  - Wedding venues booking: {len(booking_venues)} venues")
    print(f"  - Total unique venues: {len(merged_venues)}")

    both_sources = [v for v in merged_venues if v.get("source") == "both"]
    print(f"  - Found in both sources: {len(both_sources)}")

    print("\n📥 Phase 4: Downloading PDFs...")
    print("-" * 60)
    venues_with_pdfs = [v for v in merged_venues if v.get("price_lists")]
    if venues_with_pdfs:
        async with httpx.AsyncClient(headers=get_headers(), timeout=60) as client:
            for idx, venue in enumerate(venues_with_pdfs, 1):
                print(f"\n[{idx}/{len(venues_with_pdfs)}] {venue.get('name', 'N/A')}")
                await download_pdfs_for_vendor(client, venue)

    return merged_venues


def scrape_banquet_prices() -> list[dict]:
    """Synchronous wrapper for scrape_banquet_prices_async"""
    import asyncio

    return asyncio.run(scrape_banquet_prices_async())


def scrape_all_venues() -> list[dict]:
    """Synchronous wrapper for scrape_all_venues_async"""
    import asyncio

    return asyncio.run(scrape_all_venues_async())


def save_to_files(data: list[dict], output_path: str):
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
        keys = sorted(all_keys)

        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            for item in data:
                row = item.copy()
                for key, value in row.items():
                    if isinstance(value, list):
                        row[key] = json.dumps(value)
                writer.writerow(row)

    print(f"✅ Saved {len(data)} items to:")
    print(f"   - {json_file}")
    print(f"   - {csv_file}")


def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(description="Scrape BlissfulBrides.sg wedding venue data")

    parser.add_argument("--output", type=str, default="data/bb", help="Output directory")
    parser.add_argument(
        "--source",
        type=str,
        choices=["all", "banquet", "booking"],
        default="all",
        help="Data source to scrape (default: all)",
    )

    args = parser.parse_args()

    if args.source == "banquet":
        print("\n" + "=" * 60)
        print("SCRAPING BANQUET PRICES ONLY")
        print("=" * 60 + "\n")
        vendors = scrape_banquet_prices()
    elif args.source == "booking":
        print("\n" + "=" * 60)
        print("SCRAPING WEDDING VENUES BOOKING ONLY")
        print("=" * 60 + "\n")
        import asyncio

        vendors = asyncio.run(scrape_wedding_venues_booking_async())
    else:
        vendors = scrape_all_venues()

    save_to_files(vendors, f"{args.output}/venues")

    if vendors:
        print("\n✅ Scraping complete!")
        print(f"\n📊 Total venues: {len(vendors)}")

        sources_count = {}
        for v in vendors:
            source = v.get("source", "unknown")
            sources_count[source] = sources_count.get(source, 0) + 1

        if sources_count:
            print("\nVenues by source:")
            for source, count in sorted(sources_count.items()):
                print(f"  - {source}: {count}")

        print("\nSample venues:")
        for idx, vendor in enumerate(vendors[:5], 1):
            print(f"\n{idx}. {vendor.get('name', 'N/A')}")
            if "source" in vendor:
                print(f"   Source: {vendor['source']}")
            if "vendor_name" in vendor:
                print(f"   Vendor: {vendor['vendor_name']}")
            if "rating" in vendor:
                print(f"   Rating: {vendor['rating']}/5")
            if "lunch_price" in vendor:
                print(f"   Lunch: {vendor['lunch_price']}")
            if "dinner_price" in vendor:
                print(f"   Dinner: {vendor['dinner_price']}")
            if "tables_range" in vendor:
                print(f"   Tables: {vendor['tables_range']}")
            if "capacity_seated_min" in vendor:
                seated = f"{vendor['capacity_seated_min']}"
                if "capacity_seated_max" in vendor:
                    seated += f"-{vendor['capacity_seated_max']}"
                print(f"   Capacity (seated): {seated}")
            if "location" in vendor:
                print(f"   Location: {vendor['location']}")


if __name__ == "__main__":
    main()
