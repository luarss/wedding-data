"""Bridely.sg venue scraper using Playwright with parallel processing"""

import asyncio
import json
from pathlib import Path

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright


class BridelyPlaywrightParallelScraper:
    """Parallel scraper for Bridely.sg venue data using Playwright"""

    def __init__(self, output_dir: str = "data/bridely", concurrency: int = 10):
        self.base_url = "https://www.bridely.sg"
        self.sitemap_url = f"{self.base_url}/sitemap.xml"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.concurrency = concurrency

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

    async def scrape_venue(self, page, url: str) -> dict | None:
        """Scrape a single venue page using Playwright"""
        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(1)

            html = await page.content()
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
            for p in paragraphs:
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

            all_text = soup.get_text()
            if "Lunch" in all_text:
                for div in capacity_divs:
                    text = div.get_text(strip=True)
                    if text.startswith("Lunch"):
                        data["pricing_lunch"] = text.replace("Lunch", "").strip()
                    if text.startswith("Dinner"):
                        data["pricing_dinner"] = text.replace("Dinner", "").strip()

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

            if "promo" in all_text.lower() or "promotion" in all_text.lower():
                data["promo_available"] = True

            for div in capacity_divs:
                text = div.get_text(strip=True)
                if "Restaurant" in text or "Hotel" in text or "Ballroom" in text:
                    data["venue_category"] = text
                    break

            for div in capacity_divs:
                text = div.get_text(strip=True)
                if any(cuisine in text for cuisine in ["Western", "Chinese", "Asian", "Japanese", "Italian"]):
                    data["cuisine_type"] = text
                    break

            return data

        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return None

    async def scrape_venue_brochure(self, page, url: str) -> dict | None:
        """Scrape a venue brochure page"""
        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(0.5)

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            data = {
                "url": url,
                "brochure_id": url.split("/r/")[-1] if "/r/" in url else None,
                "venue_name": None,
                "type": "brochure",
                "download_link": None,
                "description": None,
            }

            title = soup.find("h3")
            if title:
                data["venue_name"] = title.get_text(strip=True)

            pdf_link = soup.find("a", href=lambda x: x and ".pdf" in str(x).lower())
            if pdf_link:
                data["download_link"] = pdf_link.get("href", "")

            paragraphs = soup.find_all("p")
            if paragraphs:
                data["description"] = " ".join([p.get_text(strip=True) for p in paragraphs[:2]])

            return data

        except Exception as e:
            print(f"Error scraping brochure {url}: {e}")
            return None

    async def scrape_worker(self, browser, urls: list[str], scrape_func, results: list, progress: dict):
        """Worker function to scrape URLs"""
        page = await browser.new_page()
        try:
            for url in urls:
                data = await scrape_func(page, url)
                if data:
                    results.append(data)
                progress["completed"] += 1
                if progress["completed"] % 10 == 0:
                    print(f"Progress: {progress['completed']}/{progress['total']}")
        finally:
            await page.close()

    async def scrape_all_parallel(self, urls: list[str], scrape_func, limit: int | None = None, headless: bool = True):
        """Scrape all URLs in parallel using multiple browser contexts"""
        if limit:
            urls = urls[:limit]

        total = len(urls)
        print(f"\nScraping {total} pages in parallel (concurrency: {self.concurrency})...")

        results = []
        progress = {"completed": 0, "total": total}

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)

            chunk_size = (total + self.concurrency - 1) // self.concurrency
            chunks = [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]

            tasks = [
                self.scrape_worker(browser, chunk, scrape_func, results, progress)
                for chunk in chunks
            ]

            await asyncio.gather(*tasks)
            await browser.close()

        return results

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

    async def run_async(self, test_mode: bool = False, headless: bool = True):
        """Run the scraping process asynchronously"""
        print("Starting Bridely.sg VENUE scraper (Playwright Parallel)...")

        urls = self.fetch_sitemap()
        categories = self.categorize_urls(urls)

        limit = 5 if test_mode else None

        print(f"\nFound {len(categories['venues'])} venue URLs")
        print(f"Found {len(categories['venue_brochures'])} venue brochure URLs")

        venues = await self.scrape_all_parallel(
            categories["venues"],
            self.scrape_venue,
            limit=limit,
            headless=headless
        )
        self.save_data(venues, "bridely_venues_playwright")

        brochures = await self.scrape_all_parallel(
            categories["venue_brochures"],
            self.scrape_venue_brochure,
            limit=limit,
            headless=headless
        )
        self.save_data(brochures, "bridely_venue_brochures_playwright")

        print("\n✅ Venue scraping complete!")
        print(f"Total venues scraped: {len(venues)}")
        print(f"Total brochures scraped: {len(brochures)}")

    def run(self, test_mode: bool = False, headless: bool = True):
        """Run the scraping process"""
        asyncio.run(self.run_async(test_mode, headless))


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Scrape wedding venue data from bridely.sg using parallel Playwright")
    parser.add_argument("--test", action="store_true", help="Run in test mode (only scrape 5 pages)")
    parser.add_argument("--output", default="data/bridely", help="Output directory for scraped data")
    parser.add_argument("--visible", action="store_true", help="Run browser in visible mode (not headless)")
    parser.add_argument("--concurrency", type=int, default=10, help="Number of concurrent browser contexts (default: 10)")

    args = parser.parse_args()

    scraper = BridelyPlaywrightParallelScraper(output_dir=args.output, concurrency=args.concurrency)
    scraper.run(test_mode=args.test, headless=not args.visible)


if __name__ == "__main__":
    main()
