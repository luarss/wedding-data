import json
import csv
import time
import argparse
import re
from pathlib import Path
import httpx
from bs4 import BeautifulSoup
from typing import Optional

BASE_URL = "https://www.blissfulbrides.sg"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def download_pdf(url: str, save_path: Path) -> bool:
    """Download a PDF file to the specified path"""
    try:
        response = httpx.get(url, headers=HEADERS, timeout=30, follow_redirects=True)
        response.raise_for_status()

        save_path.parent.mkdir(parents=True, exist_ok=True)

        with open(save_path, 'wb') as f:
            f.write(response.content)

        return True
    except Exception as e:
        print(f"  ⚠️  Error downloading PDF {url}: {e}")
        return False


def scrape_banquet_price_list() -> dict:
    """
    Scrape the wedding banquet price list page for ratings and pricing metadata
    Returns a dictionary mapping venue_id to pricing data
    """
    try:
        url = f"{BASE_URL}/wedding-banquet-price-list"
        response = httpx.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        pricing_data = {}

        rows = soup.find_all('tr')

        for row in rows:
            venue_link = row.find('a', href=re.compile(r'/detail/\d+/'))
            if not venue_link:
                continue

            venue_url = venue_link.get('href', '')
            venue_id = venue_url.split('/detail/')[1].split('/')[0] if '/detail/' in venue_url else None

            if not venue_id:
                continue

            rating_input = row.find('input', {'id': 'merchant_score'})
            rating = float(rating_input.get('value', '0')) if rating_input else 0.0

            tds = row.find_all('td')

            lunch_from = None
            lunch_days = None
            dinner_from = None
            dinner_days = None
            tables_range = None
            tables_days = None

            if len(tds) >= 4:
                lunch_text = tds[1].get_text(strip=True) if len(tds) > 1 else ''
                dinner_text = tds[2].get_text(strip=True) if len(tds) > 2 else ''
                tables_text = tds[3].get_text(strip=True) if len(tds) > 3 else ''

                lunch_match = re.match(r'(\$\d+[,\d]*\+*)\s*(.*)', lunch_text)
                if lunch_match:
                    lunch_from = lunch_match.group(1)
                    lunch_days = lunch_match.group(2).strip()

                dinner_match = re.match(r'(\$\d+[,\d]*\+*)\s*(.*)', dinner_text)
                if dinner_match:
                    dinner_from = dinner_match.group(1)
                    dinner_days = dinner_match.group(2).strip()

                tables_match = re.match(r'(\d+\s*-\s*\d+)\s*(.*)', tables_text)
                if tables_match:
                    tables_range = tables_match.group(1)
                    tables_days = tables_match.group(2).strip()

            pricing_data[venue_id] = {
                'rating': rating,
                'lunch_from': lunch_from,
                'lunch_days': lunch_days,
                'dinner_from': dinner_from,
                'dinner_days': dinner_days,
                'tables_range': tables_range,
                'tables_days': tables_days,
            }

        print(f"✅ Scraped pricing data for {len(pricing_data)} venues from price list")
        return pricing_data

    except Exception as e:
        print(f"⚠️  Error scraping banquet price list: {e}")
        return {}


def get_urls_from_sitemap(url_pattern: str) -> list[str]:
    """Fetch URLs matching pattern from sitemap.xml"""
    sitemap_url = f"{BASE_URL}/sitemap.xml"

    try:
        response = httpx.get(sitemap_url, headers=HEADERS, timeout=30)
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


def scrape_venue_detail(url: str, pricing_data: dict = None) -> Optional[dict]:
    """Scrape a single venue detail page"""
    try:
        response = httpx.get(url, headers=HEADERS, timeout=30, follow_redirects=True)
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
            data["name"] = title.get_text(strip=True).replace(" – Blissful Brides Singapore", "")

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

        pdf_pattern = re.compile(r'https://www\.blissfulbrides\.sg/banquet/[^"\']+\.pdf', re.IGNORECASE)
        pdf_urls = pdf_pattern.findall(response.text)

        if pdf_urls:
            pdf_list = []
            pdf_dir = Path(f"data/bb/price-lists/{venue_id}-{slug}")

            for pdf_url in set(pdf_urls):
                pdf_filename = pdf_url.split('/')[-1]
                pdf_save_path = pdf_dir / pdf_filename

                print(f"  📄 Downloading {pdf_filename}...")
                success = download_pdf(pdf_url, pdf_save_path)

                if success:
                    pdf_list.append(pdf_url)

            data["price_list_pdfs"] = pdf_list
            data["has_price_list"] = len(pdf_list) > 0
            data["price_list_count"] = len(pdf_list)
        else:
            data["has_price_list"] = False
            data["price_list_count"] = 0

        for elem in soup.find_all(string=re.compile(r'Address', re.IGNORECASE)):
            parent = elem.find_parent() if hasattr(elem, 'find_parent') else None
            if parent and parent.name in ['dt', 'label', 'th', 'strong', 'b']:
                next_sibling = parent.find_next_sibling()
                if next_sibling and next_sibling.name in ['dd', 'td', 'div', 'p']:
                    addr_text = next_sibling.get_text(strip=True)
                    if addr_text and len(addr_text) < 500 and 'singapore' in addr_text.lower():
                        data["address"] = addr_text
                        break

        website_elem = soup.find('a', string=re.compile(r'^Website$', re.IGNORECASE))
        if website_elem:
            website_url = website_elem.get('href', '')
            if isinstance(website_url, str) and website_url.startswith('http'):
                data["website"] = website_url

        for elem in soup.find_all(string=re.compile(r'capacity|pax|guests|seating', re.IGNORECASE)):
            text = elem.strip() if isinstance(elem, str) else elem.get_text(strip=True)
            if text and len(text) < 200:
                numbers = re.findall(r'\d+', text)
                if numbers:
                    if 'capacity' not in data:
                        data["capacity"] = text
                    break

        if pricing_data and venue_id in pricing_data:
            data["rating"] = pricing_data[venue_id].get("rating", 0.0)
            data["pricing"] = {
                "lunch_from": pricing_data[venue_id].get("lunch_from"),
                "lunch_days": pricing_data[venue_id].get("lunch_days"),
                "dinner_from": pricing_data[venue_id].get("dinner_from"),
                "dinner_days": pricing_data[venue_id].get("dinner_days"),
                "tables_range": pricing_data[venue_id].get("tables_range"),
                "tables_days": pricing_data[venue_id].get("tables_days"),
            }

        return data

    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None


def scrape_marketplace_package(url: str) -> Optional[dict]:
    """Scrape a single marketplace package page"""
    try:
        response = httpx.get(url, headers=HEADERS, timeout=30, follow_redirects=True)
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


def load_existing_data(output_path: str) -> dict:
    """Load existing scraped data to avoid re-scraping"""
    json_file = Path(output_path).with_suffix(".json")

    if json_file.exists():
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                existing = {item['id']: item for item in data if 'id' in item}
                print(f"📂 Loaded {len(existing)} existing venues from cache")
                return existing
        except Exception as e:
            print(f"⚠️  Error loading existing data: {e}")

    return {}


def scrape_items(url_pattern: str, scraper_func, limit: Optional[int], delay: float, pricing_data: dict = None, output_path: str = None) -> list[dict]:
    """Generic scraping function with caching"""
    urls = get_urls_from_sitemap(url_pattern)

    existing_data = {}
    if output_path:
        existing_data = load_existing_data(output_path)

    if limit:
        urls = urls[:limit]

    items = []
    skipped = 0
    total = len(urls)

    for i, url in enumerate(urls, 1):
        venue_id = url.split('/detail/')[1].split('/')[0] if '/detail/' in url else None

        if venue_id and venue_id in existing_data:
            print(f"[{i}/{total}] ⏭️  Skipping {url} (already scraped)")
            items.append(existing_data[venue_id])
            skipped += 1
            continue

        print(f"[{i}/{total}] Scraping {url}")

        if pricing_data is not None:
            item = scraper_func(url, pricing_data)
        else:
            item = scraper_func(url)

        if item:
            items.append(item)

        if i < total and delay > 0:
            time.sleep(delay)

    print(f"Successfully scraped {len(items) - skipped}/{total} items ({skipped} from cache)")
    return items


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
        keys = list(data[0].keys())
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)

    print(f"✅ Saved {len(data)} items to:")
    print(f"   - {json_file}")
    print(f"   - {csv_file}")


def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(description="Scrape BlissfulBrides.sg wedding data")

    parser.add_argument(
        "type",
        choices=["venues", "marketplace", "both"],
        help="Type of data to scrape"
    )
    parser.add_argument("--limit", type=int, help="Max items to scrape")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests (seconds)")
    parser.add_argument("--output", type=str, default="data/bb", help="Output directory")
    parser.add_argument("--force", action="store_true", help="Force re-scraping even if data exists")

    args = parser.parse_args()

    if args.type in ["venues", "both"]:
        print("\n" + "="*60)
        print("SCRAPING VENUES")
        print("="*60 + "\n")

        print("Fetching pricing data from banquet price list...")
        pricing_data = scrape_banquet_price_list()
        print()

        output_path = f"{args.output}/venues"

        if args.force:
            print("🔄 Force mode: ignoring existing data\n")
            venues = scrape_items("/detail/", scrape_venue_detail, args.limit, args.delay, pricing_data, None)
        else:
            venues = scrape_items("/detail/", scrape_venue_detail, args.limit, args.delay, pricing_data, output_path)

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

    if args.type in ["marketplace", "both"]:
        print("\n" + "="*60)
        print("SCRAPING MARKETPLACE")
        print("="*60 + "\n")

        packages = scrape_items("/wedding-market-place/", scrape_marketplace_package, args.limit, args.delay)
        save_to_files(packages, f"{args.output}/marketplace")

        if packages:
            print(f"\n📊 Total packages: {len(packages)}")


if __name__ == "__main__":
    main()
