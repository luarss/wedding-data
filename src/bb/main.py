import argparse
import asyncio
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


def get_urls_from_sitemap(url_pattern: str) -> list[str]:
    """Fetch URLs matching pattern from sitemap.xml"""
    sitemap_url = f"{BASE_URL}/sitemap.xml"

    try:
        response = httpx.get(sitemap_url, headers=get_headers(), timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "xml")
        locs = soup.find_all("loc")

        urls = [loc.text for loc in locs if url_pattern in loc.text]
        unique_urls = list(dict.fromkeys(urls))

        print(f"Found {len(unique_urls)} URLs matching '{url_pattern}'")
        return unique_urls

    except Exception as e:
        print(f"Error fetching sitemap: {e}")
        return []


async def scrape_venue_detail(client: httpx.AsyncClient, url: str) -> dict | None:
    """Scrape a single venue detail page"""
    try:
        response = await client.get(url, timeout=30, follow_redirects=True)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        venue_id = url.split("/detail/")[1].split("/")[0] if "/detail/" in url else None
        slug = url.split("/detail/")[1].split("/")[1] if "/detail/" in url else None

        data = {
            "id": venue_id,
            "slug": slug,
            "url": url,
        }

        title = soup.find("h1") or soup.find("title")
        if title:
            data["name"] = title.get_text(strip=True).replace(" - Blissful Brides Singapore", "")

        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            content = meta_desc.get("content", "")
            data["description"] = str(content) if content else ""

        og_image = soup.find("meta", attrs={"property": "og:image"})
        if og_image:
            content = og_image.get("content", "")
            data["image_url"] = str(content) if content else ""

        breadcrumb = soup.find("ol", class_="breadcrumb")
        if breadcrumb:
            links = breadcrumb.find_all("a")
            if len(links) >= 2:
                data["category"] = links[1].get_text(strip=True)

        for link in soup.find_all("a", href=True):
            href = str(link.get("href", ""))
            if "tel:" in href:
                data["phone"] = href.replace("tel:", "").strip()
            elif "mailto:" in href:
                data["email"] = href.replace("mailto:", "").strip()

        public_banquet_url = f"{BASE_URL}/public/banquet/{slug}/wedding-banquet-price-list/"

        try:
            listing_response = await client.get(public_banquet_url, timeout=30, follow_redirects=True)
            listing_response.raise_for_status()

            listing_soup = BeautifulSoup(listing_response.text, "html.parser")
            pdf_links = listing_soup.find_all("a", href=lambda x: x and x.endswith(".pdf"))

            if pdf_links:
                pdf_list = []
                pdf_dir = Path(f"data/bb/price-lists/{venue_id}-{slug}")

                for link in pdf_links:
                    pdf_filename = link.get("href")
                    if not pdf_filename or pdf_filename == "#":
                        continue

                    pdf_url = f"{public_banquet_url}{pdf_filename}"
                    pdf_save_path = pdf_dir / pdf_filename

                    print(f"  📄 Downloading {pdf_filename} to {venue_id}-{slug}/...")
                    success = await download_pdf(client, pdf_url, pdf_save_path)

                    if success:
                        pdf_list.append(pdf_url)

                data["price_list_pdfs"] = pdf_list
                data["has_price_list"] = len(pdf_list) > 0
                data["price_list_count"] = len(pdf_list)
            else:
                data["has_price_list"] = False
                data["price_list_count"] = 0
        except Exception:
            data["has_price_list"] = False
            data["price_list_count"] = 0

        for elem in soup.find_all(string=re.compile(r"Address", re.IGNORECASE)):
            parent = elem.find_parent() if hasattr(elem, "find_parent") else None
            if parent and parent.name in ["dt", "label", "th", "strong", "b"]:
                next_sibling = parent.find_next_sibling()
                if next_sibling and next_sibling.name in ["dd", "td", "div", "p"]:
                    addr_text = next_sibling.get_text(strip=True)
                    if addr_text and len(addr_text) < 500 and "singapore" in addr_text.lower():
                        data["address"] = addr_text
                        break

        website_elem = soup.find("a", string=re.compile(r"^Website$", re.IGNORECASE))
        if website_elem:
            website_url = website_elem.get("href", "")
            if isinstance(website_url, str) and website_url.startswith("http"):
                data["website"] = website_url

        for elem in soup.find_all(string=re.compile(r"capacity|pax|guests|seating", re.IGNORECASE)):
            text = elem.strip() if isinstance(elem, str) else elem.get_text(strip=True)
            if text and len(text) < 200:
                numbers = re.findall(r"\d+", text)
                if numbers:
                    if "capacity" not in data:
                        data["capacity"] = text
                    break

        return data

    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None


def scrape_marketplace_package(url: str) -> dict | None:
    """Scrape a single marketplace package page"""
    try:
        response = httpx.get(url, headers=get_headers(), timeout=30, follow_redirects=True)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        package_id = url.split("/wedding-market-place/")[1].split("/")[0]
        slug = url.split("/wedding-market-place/")[1].split("/")[1]

        data = {
            "id": package_id,
            "slug": slug,
            "url": url,
        }

        title = soup.find("h1")
        if title:
            data["title"] = title.get_text(strip=True)

        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            content = meta_desc.get("content", "")
            data["description"] = str(content) if content else ""

        return data

    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None


def scrape_banquet_prices() -> list[dict]:
    """Scrape banquet price list table from the main price list page"""
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
        return vendors

    except Exception as e:
        print(f"Error scraping banquet prices: {e}")
        return []


async def scrape_items_parallel(
    url_pattern: str, scraper_func, limit: int | None, workers: int, output_path: str | None = None
) -> list[dict]:
    """Generic parallel scraping function"""
    urls = get_urls_from_sitemap(url_pattern)

    if limit:
        urls = urls[:limit]

    print(f"🚀 Scraping {len(urls)} items with {workers} concurrent workers")

    items = []

    async with httpx.AsyncClient(headers=get_headers(), timeout=30) as client:
        semaphore = asyncio.Semaphore(workers)

        async def scrape_with_semaphore(url: str, index: int):
            async with semaphore:
                print(f"[{index}/{len(urls)}] Scraping {url}")
                return await scraper_func(client, url)

        tasks = [scrape_with_semaphore(url, i + 1) for i, url in enumerate(urls)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                print(f"⚠️  Error: {result}")
            elif result:
                items.append(result)

    print(f"Successfully scraped {len(items)}/{len(urls)} items")
    return items


def scrape_items(
    url_pattern: str, scraper_func, limit: int | None, delay: float, output_path: str | None = None
) -> list[dict]:
    """Generic scraping function with caching (synchronous wrapper for backward compatibility)"""
    workers = max(1, int(1.0 / delay)) if delay > 0 else 10
    return asyncio.run(scrape_items_parallel(url_pattern, scraper_func, limit, workers, output_path))


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
    parser = argparse.ArgumentParser(description="Scrape BlissfulBrides.sg wedding data")

    parser.add_argument(
        "type", choices=["venues", "marketplace", "banquet-prices", "all"], help="Type of data to scrape"
    )
    parser.add_argument("--limit", type=int, help="Max items to scrape")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests (seconds)")
    parser.add_argument("--output", type=str, default="data/bb", help="Output directory")

    args = parser.parse_args()

    if args.type in ["venues", "all"]:
        print("\n" + "=" * 60)
        print("SCRAPING VENUES")
        print("=" * 60 + "\n")

        output_path = f"{args.output}/venues"
        venues = scrape_items("/detail/", scrape_venue_detail, args.limit, args.delay, output_path)
        save_to_files(venues, output_path)

        if venues:
            print(f"\n📊 Total venues: {len(venues)}")
            categories = {}
            for v in venues:
                cat = v.get("category", "Unknown")
                categories[cat] = categories.get(cat, 0) + 1

            print("\nBy category:")
            for cat, count in sorted(categories.items(), key=lambda x: -x[1])[:10]:
                print(f"  {cat}: {count}")

    if args.type in ["marketplace", "all"]:
        print("\n" + "=" * 60)
        print("SCRAPING MARKETPLACE")
        print("=" * 60 + "\n")

        packages = scrape_items("/wedding-market-place/", scrape_marketplace_package, args.limit, args.delay)
        save_to_files(packages, f"{args.output}/marketplace")

        if packages:
            print(f"\n📊 Total packages: {len(packages)}")

    if args.type in ["banquet-prices", "all"]:
        print("\n" + "=" * 60)
        print("SCRAPING BANQUET PRICES")
        print("=" * 60 + "\n")

        vendors = scrape_banquet_prices()
        save_to_files(vendors, f"{args.output}/banquet_prices")

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
