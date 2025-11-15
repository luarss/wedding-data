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
                vendor_data = {}

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

        print(f"Found {len(vendors)} vendors")

        vendors_with_pdfs = [v for v in vendors if v.get("price_lists")]
        if vendors_with_pdfs:
            print(f"\n📥 Downloading PDFs for {len(vendors_with_pdfs)} vendors...")

            async with httpx.AsyncClient(headers=get_headers(), timeout=60) as client:
                for idx, vendor in enumerate(vendors_with_pdfs, 1):
                    print(f"\n[{idx}/{len(vendors_with_pdfs)}] {vendor['name']}")
                    await download_pdfs_for_vendor(client, vendor)

        return vendors

    except Exception as e:
        print(f"Error scraping banquet prices: {e}")
        return []


def scrape_banquet_prices() -> list[dict]:
    """Synchronous wrapper for scrape_banquet_prices_async"""
    import asyncio

    return asyncio.run(scrape_banquet_prices_async())


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
    parser = argparse.ArgumentParser(description="Scrape BlissfulBrides.sg banquet pricing data")

    parser.add_argument("--output", type=str, default="data/bb", help="Output directory")

    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("SCRAPING BANQUET PRICES")
    print("=" * 60 + "\n")

    vendors = scrape_banquet_prices()
    save_to_files(vendors, f"{args.output}/venues")

    if vendors:
        print(f"\n📊 Total vendors: {len(vendors)}")
        print("\nSample vendors:")
        for idx, vendor in enumerate(vendors[:5], 1):
            print(f"\n{idx}. {vendor.get('name', 'N/A')}")
            if "rating" in vendor:
                print(f"   Rating: {vendor['rating']}/5")
            if "lunch_price" in vendor:
                print(f"   Lunch: {vendor['lunch_price']}")
            if "dinner_price" in vendor:
                print(f"   Dinner: {vendor['dinner_price']}")
            if "tables_range" in vendor:
                print(f"   Tables: {vendor['tables_range']}")


if __name__ == "__main__":
    main()
