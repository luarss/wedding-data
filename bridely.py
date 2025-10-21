"""Bridely.sg venue scraper - focuses on venue listings and brochures"""

import json
import time
from pathlib import Path

import httpx
import pandas as pd
from bs4 import BeautifulSoup


class BridelyScraper:
    """Scraper for bridely.sg wedding vendor data"""

    def __init__(self, output_dir: str = "data"):
        self.base_url = "https://www.bridely.sg"
        self.sitemap_url = f"{self.base_url}/sitemap.xml"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        from src.shared.config import get_headers
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
            headers=get_headers()
        )

    def fetch_sitemap(self) -> list[str]:
        """Fetch and parse sitemap to get all URLs"""
        print("Fetching sitemap...")
        response = self.client.get(self.sitemap_url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml-xml")
        urls = [loc.text for loc in soup.find_all("loc")]

        print(f"Found {len(urls)} URLs in sitemap")
        return urls

    def categorize_urls(self, urls: list[str]) -> dict[str, list[str]]:
        """Categorize URLs by type"""
        categories = {"vendors": [], "venues": [], "venue_brochures": [], "articles": [], "other": []}

        for url in urls:
            path = url.replace(self.base_url + "/", "")
            if path.startswith("vendor/"):
                categories["vendors"].append(url)
            elif path.startswith("venue/") and "/r/" in path:
                categories["venues"].append(url)
            elif path.startswith("venue-brochures/"):
                categories["venue_brochures"].append(url)
            elif path.startswith("articles/"):
                categories["articles"].append(url)
            else:
                categories["other"].append(url)

        for cat, cat_urls in categories.items():
            print(f"{cat}: {len(cat_urls)} URLs")

        return categories

    def scrape_vendor(self, url: str) -> dict | None:
        """Scrape a single vendor page"""
        try:
            response = self.client.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            data = {
                "url": url,
                "name": None,
                "category": None,
                "description": None,
                "phone": None,
                "email": None,
                "website": None,
                "address": None,
                "rating": None,
                "review_count": None,
                "price_range": None,
                "services": [],
            }

            # Extract vendor name (adjust selectors based on actual HTML structure)
            title = soup.find("h1")
            if title:
                data["name"] = title.get_text(strip=True)

            # Extract meta description
            meta_desc = soup.find("meta", {"name": "description"})
            if meta_desc:
                data["description"] = meta_desc.get("content", "").strip()

            # Look for contact information
            # Phone
            phone_link = soup.find("a", href=lambda x: x and "tel:" in x)
            if phone_link:
                data["phone"] = phone_link.get("href", "").replace("tel:", "")

            # Email
            email_link = soup.find("a", href=lambda x: x and "mailto:" in x)
            if email_link:
                data["email"] = email_link.get("href", "").replace("mailto:", "")

            # Website
            website_link = soup.find("a", href=lambda x: x and x.startswith("http") and "bridely.sg" not in x)
            if website_link:
                data["website"] = website_link.get("href", "")

            return data

        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return None

    def scrape_venue(self, url: str) -> dict | None:
        """Scrape a single venue page"""
        try:
            response = self.client.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            data = {
                "url": url,
                "venue_id": url.split("/r/")[-1] if "/r/" in url else None,
                "name": None,
                "type": "venue",
                "description": None,
                "capacity": None,
                "phone": None,
                "email": None,
                "website": None,
                "address": None,
                "rating": None,
                "review_count": None,
                "price_range": None,
                "instagram": None,
                "facebook": None,
                "amenities": [],
                "venue_type": None,
                "indoor_outdoor": None,
                "package_info": None,
            }

            # Extract venue name
            title = soup.find("h1")
            if title:
                data["name"] = title.get_text(strip=True)

            # Extract meta description
            meta_desc = soup.find("meta", {"name": "description"})
            if meta_desc:
                data["description"] = meta_desc.get("content", "").strip()

            phone_link = soup.find("a", href=lambda x: x and "tel:" in x)
            if phone_link:
                data["phone"] = phone_link.get("href", "").replace("tel:", "")

            email_link = soup.find("a", href=lambda x: x and "mailto:" in x)
            if email_link:
                data["email"] = email_link.get("href", "").replace("mailto:", "")

            website_link = soup.find("a", href=lambda x: x and x.startswith("http") and "bridely.sg" not in x)
            if website_link:
                data["website"] = website_link.get("href", "")

            instagram_link = soup.find("a", href=lambda x: x and "instagram.com" in x)
            if instagram_link:
                data["instagram"] = instagram_link.get("href", "")

            facebook_link = soup.find("a", href=lambda x: x and "facebook.com" in x)
            if facebook_link:
                data["facebook"] = facebook_link.get("href", "")

            return data

        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return None

    def scrape_all_vendors(self, urls: list[str], limit: int | None = None):
        """Scrape all vendor pages"""
        vendors = []
        total = len(urls)
        if limit:
            urls = urls[:limit]
            total = limit

        print(f"\nScraping {total} vendor pages...")

        for i, url in enumerate(urls, 1):
            if i % 10 == 0:
                print(f"Progress: {i}/{total}")

            vendor_data = self.scrape_vendor(url)
            if vendor_data:
                vendors.append(vendor_data)

            # Be respectful with rate limiting
            time.sleep(0.5)

        return vendors

    def scrape_all_venues(self, urls: list[str], limit: int | None = None):
        """Scrape all venue pages"""
        venues = []
        total = len(urls)
        if limit:
            urls = urls[:limit]
            total = limit

        print(f"\nScraping {total} venue pages...")

        for i, url in enumerate(urls, 1):
            if i % 10 == 0:
                print(f"Progress: {i}/{total}")

            venue_data = self.scrape_venue(url)
            if venue_data:
                venues.append(venue_data)

            # Be respectful with rate limiting
            time.sleep(0.5)

        return venues

    def save_data(self, data: list[dict], filename: str):
        """Save scraped data to JSON and CSV"""
        if not data:
            print(f"No data to save for {filename}")
            return

        # Save as JSON
        json_path = self.output_dir / f"{filename}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Saved JSON: {json_path}")

        # Save as CSV
        df = pd.DataFrame(data)
        csv_path = self.output_dir / f"{filename}.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8")
        print(f"Saved CSV: {csv_path}")

    def scrape_venue_brochure(self, url: str) -> dict | None:
        """Scrape a venue brochure page"""
        try:
            response = self.client.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            data = {
                "url": url,
                "brochure_id": url.split("/r/")[-1] if "/r/" in url else None,
                "venue_name": None,
                "type": "brochure",
                "download_link": None,
                "description": None,
            }

            title = soup.find("h1")
            if title:
                data["venue_name"] = title.get_text(strip=True)

            meta_desc = soup.find("meta", {"name": "description"})
            if meta_desc:
                data["description"] = meta_desc.get("content", "").strip()

            pdf_link = soup.find("a", href=lambda x: x and x.endswith(".pdf"))
            if pdf_link:
                data["download_link"] = pdf_link.get("href", "")

            return data

        except Exception as e:
            print(f"Error scraping brochure {url}: {e}")
            return None

    def scrape_all_brochures(self, urls: list[str], limit: int | None = None):
        """Scrape all venue brochure pages"""
        brochures = []
        total = len(urls)
        if limit:
            urls = urls[:limit]
            total = limit

        print(f"\nScraping {total} venue brochure pages...")

        for i, url in enumerate(urls, 1):
            if i % 10 == 0:
                print(f"Progress: {i}/{total}")

            brochure_data = self.scrape_venue_brochure(url)
            if brochure_data:
                brochures.append(brochure_data)

            time.sleep(0.5)

        return brochures

    def run(self, test_mode: bool = False, venues_only: bool = True):
        """Run the scraping process - now focused on venues only"""
        print("Starting Bridely.sg VENUE scraper...")

        urls = self.fetch_sitemap()
        categories = self.categorize_urls(urls)

        limit = 5 if test_mode else None

        print(f"\nFound {len(categories['venues'])} venue URLs")
        print(f"Found {len(categories['venue_brochures'])} venue brochure URLs")

        venues = self.scrape_all_venues(categories["venues"], limit=limit)
        self.save_data(venues, "bridely_venues")

        brochures = self.scrape_all_brochures(categories["venue_brochures"], limit=limit)
        self.save_data(brochures, "bridely_venue_brochures")

        print("\n✅ Venue scraping complete!")
        print(f"Total venues scraped: {len(venues)}")
        print(f"Total brochures scraped: {len(brochures)}")

        self.client.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Scrape wedding venue data from bridely.sg")
    parser.add_argument("--test", action="store_true", help="Run in test mode (only scrape 5 pages)")
    parser.add_argument("--output", default="data/bridely", help="Output directory for scraped data")

    args = parser.parse_args()

    scraper = BridelyScraper(output_dir=args.output)
    scraper.run(test_mode=args.test)


if __name__ == "__main__":
    main()
