"""Bridely.sg venue scraper using Playwright for JavaScript-rendered content"""

import json
import time
from pathlib import Path

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import pandas as pd
import httpx


class BridelyPlaywrightScraper:
    """Scraper for Bridely.sg venue data using Playwright"""

    def __init__(self, output_dir: str = "data/bridely"):
        self.base_url = "https://www.bridely.sg"
        self.sitemap_url = f"{self.base_url}/sitemap.xml"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def fetch_sitemap(self) -> list[str]:
        """Fetch and parse sitemap to get all URLs"""
        print("Fetching sitemap...")
        with httpx.Client(timeout=30.0) as client:
            response = client.get(self.sitemap_url)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "lxml-xml")
            urls = [loc.text for loc in soup.find_all("loc")]

            print(f"Found {len(urls)} URLs in sitemap")
            return urls

    def categorize_urls(self, urls: list[str]) -> dict[str, list[str]]:
        """Categorize URLs by type - focus on venues only"""
        categories = {"venues": [], "venue_brochures": []}

        for url in urls:
            path = url.replace(self.base_url + "/", "")
            if path.startswith("venue/") and "/r/" in path:
                categories["venues"].append(url)
            elif path.startswith("venue-brochures/"):
                categories["venue_brochures"].append(url)

        for cat, cat_urls in categories.items():
            print(f"{cat}: {len(cat_urls)} URLs")

        return categories

    def scrape_venue(self, page, url: str) -> dict | None:
        """Scrape a single venue page using Playwright"""
        try:
            print(f"Scraping: {url}")
            page.goto(url, wait_until="networkidle", timeout=30000)

            time.sleep(2)

            html = page.content()
            soup = BeautifulSoup(html, "html.parser")

            venue_id = url.split("/r/")[-1] if "/r/" in url else None

            data = {
                "url": url,
                "venue_id": venue_id,
                "name": None,
                "type": "venue",
                "description": None,
                "address": None,
                "phone": None,
                "email": None,
                "website": None,
                "capacity": None,
                "pricing_lunch": None,
                "pricing_dinner": None,
                "instagram": None,
                "facebook": None,
                "rating": None,
                "review_count": None,
                "promo_available": False,
                "venue_category": None,
                "cuisine_type": None,
            }

            h3_heading = soup.find("h3")
            if h3_heading:
                data["name"] = h3_heading.get_text(strip=True)

            paragraphs = soup.find_all("p")
            for i, p in enumerate(paragraphs):
                text = p.get_text(strip=True)

                if "Singapore" in text and len(text) < 200 and not data["address"]:
                    data["address"] = text

            capacity_divs = soup.find_all("div")
            for div in capacity_divs:
                text = div.get_text(strip=True)
                if text.startswith("Capacity"):
                    capacity_text = text.replace("Capacity", "").strip()
                    data["capacity"] = capacity_text
                    break

            phone_link = soup.find("a", href=lambda x: x and "tel:" in x)
            if phone_link:
                data["phone"] = phone_link.get("href", "").replace("tel:", "")

            email_link = soup.find("a", href=lambda x: x and "mailto:" in x)
            if email_link:
                email_full = email_link.get("href", "").replace("mailto:", "")
                data["email"] = email_full.split("?")[0]

            list_items = soup.find_all("li")
            for li in list_items:
                text = li.get_text(strip=True)
                if "Lunch" in text and "$" in text:
                    data["pricing_lunch"] = text.replace("Lunch:", "").strip()
                elif "Dinner" in text and "$" in text:
                    data["pricing_dinner"] = text.replace("Dinner:", "").strip()

            all_links = soup.find_all("a", href=True)
            for link in all_links:
                href = link.get("href", "")
                if "instagram.com" in href and "bridely.sg" not in href:
                    data["instagram"] = href
                    break

            for link in all_links:
                href = link.get("href", "")
                if "facebook.com" in href and "bridely.sg" not in href:
                    data["facebook"] = href
                    break

            promo_divs = soup.find_all("div", string=lambda x: x and "PROMO" in x)
            if promo_divs:
                data["promo_available"] = True

            category_divs = soup.find_all("div")
            for div in category_divs:
                text = div.get_text(strip=True)
                if text in ["Hotel", "Restaurant", "Bar", "Club", "Outdoor"]:
                    data["venue_category"] = text
                elif text in ["Chinese", "Western", "Japanese", "Italian", "Fusion"]:
                    data["cuisine_type"] = text

            return data

        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return None

    def scrape_venue_brochure(self, page, url: str) -> dict | None:
        """Scrape a venue brochure page"""
        try:
            print(f"Scraping brochure: {url}")
            page.goto(url, wait_until="networkidle", timeout=30000)

            time.sleep(1)

            html = page.content()
            soup = BeautifulSoup(html, "html.parser")

            brochure_id = url.split("/r/")[-1] if "/r/" in url else None

            data = {
                "url": url,
                "brochure_id": brochure_id,
                "venue_name": None,
                "type": "brochure",
                "download_link": None,
                "description": None,
            }

            h3_heading = soup.find("h3")
            if h3_heading:
                data["venue_name"] = h3_heading.get_text(strip=True)

            meta_desc = soup.find("meta", {"name": "description"})
            if meta_desc:
                data["description"] = meta_desc.get("content", "").strip()

            pdf_link = soup.find("a", href=lambda x: x and ".pdf" in x)
            if pdf_link:
                data["download_link"] = pdf_link.get("href", "")

            return data

        except Exception as e:
            print(f"Error scraping brochure {url}: {e}")
            return None

    def scrape_all_venues(self, urls: list[str], limit: int | None = None, headless: bool = True):
        """Scrape all venue pages using Playwright"""
        venues = []
        total = len(urls)
        if limit:
            urls = urls[:limit]
            total = limit

        print(f"\nScraping {total} venue pages with Playwright...")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            page = browser.new_page()

            for i, url in enumerate(urls, 1):
                if i % 10 == 0:
                    print(f"Progress: {i}/{total}")

                venue_data = self.scrape_venue(page, url)
                if venue_data:
                    venues.append(venue_data)

                time.sleep(1)

            browser.close()

        return venues

    def scrape_all_brochures(self, urls: list[str], limit: int | None = None, headless: bool = True):
        """Scrape all venue brochure pages using Playwright"""
        brochures = []
        total = len(urls)
        if limit:
            urls = urls[:limit]
            total = limit

        print(f"\nScraping {total} venue brochure pages with Playwright...")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            page = browser.new_page()

            for i, url in enumerate(urls, 1):
                if i % 10 == 0:
                    print(f"Progress: {i}/{total}")

                brochure_data = self.scrape_venue_brochure(page, url)
                if brochure_data:
                    brochures.append(brochure_data)

                time.sleep(1)

            browser.close()

        return brochures

    def save_data(self, data: list[dict], filename: str):
        """Save scraped data to JSON and CSV"""
        if not data:
            print(f"No data to save for {filename}")
            return

        json_path = self.output_dir / f"{filename}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Saved JSON: {json_path}")

        df = pd.DataFrame(data)
        csv_path = self.output_dir / f"{filename}.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8")
        print(f"Saved CSV: {csv_path}")

    def run(self, test_mode: bool = False, headless: bool = True):
        """Run the scraping process - focused on venues only"""
        print("Starting Bridely.sg VENUE scraper (Playwright)...")

        urls = self.fetch_sitemap()
        categories = self.categorize_urls(urls)

        limit = 5 if test_mode else None

        print(f"\nFound {len(categories['venues'])} venue URLs")
        print(f"Found {len(categories['venue_brochures'])} venue brochure URLs")

        venues = self.scrape_all_venues(categories["venues"], limit=limit, headless=headless)
        self.save_data(venues, "bridely_venues_playwright")

        brochures = self.scrape_all_brochures(
            categories["venue_brochures"], limit=limit, headless=headless
        )
        self.save_data(brochures, "bridely_venue_brochures_playwright")

        print("\n✅ Venue scraping complete!")
        print(f"Total venues scraped: {len(venues)}")
        print(f"Total brochures scraped: {len(brochures)}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Scrape wedding venue data from bridely.sg using Playwright")
    parser.add_argument("--test", action="store_true", help="Run in test mode (only scrape 5 pages)")
    parser.add_argument("--output", default="data/bridely", help="Output directory for scraped data")
    parser.add_argument("--visible", action="store_true", help="Run browser in visible mode (not headless)")

    args = parser.parse_args()

    scraper = BridelyPlaywrightScraper(output_dir=args.output)
    scraper.run(test_mode=args.test, headless=not args.visible)


if __name__ == "__main__":
    main()
