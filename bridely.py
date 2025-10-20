"""TODO: bridely.sg database down"""

import httpx
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import json
import time
from typing import Optional


class BridelyScraper:
    """Scraper for bridely.sg wedding vendor data"""

    def __init__(self, output_dir: str = "data"):
        self.base_url = "https://www.bridely.sg"
        self.sitemap_url = f"{self.base_url}/sitemap.xml"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.client = httpx.Client(timeout=30.0, follow_redirects=True)

    def fetch_sitemap(self) -> list[str]:
        """Fetch and parse sitemap to get all URLs"""
        print("Fetching sitemap...")
        response = self.client.get(self.sitemap_url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'lxml-xml')
        urls = [loc.text for loc in soup.find_all('loc')]

        print(f"Found {len(urls)} URLs in sitemap")
        return urls

    def categorize_urls(self, urls: list[str]) -> dict[str, list[str]]:
        """Categorize URLs by type"""
        categories = {
            'vendors': [],
            'venues': [],
            'venue_brochures': [],
            'articles': [],
            'other': []
        }

        for url in urls:
            path = url.replace(self.base_url + '/', '')
            if path.startswith('vendor/'):
                categories['vendors'].append(url)
            elif path.startswith('venue/') and '/r/' in path:
                categories['venues'].append(url)
            elif path.startswith('venue-brochures/'):
                categories['venue_brochures'].append(url)
            elif path.startswith('articles/'):
                categories['articles'].append(url)
            else:
                categories['other'].append(url)

        for cat, cat_urls in categories.items():
            print(f"{cat}: {len(cat_urls)} URLs")

        return categories

    def scrape_vendor(self, url: str) -> Optional[dict]:
        """Scrape a single vendor page"""
        try:
            response = self.client.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            data = {
                'url': url,
                'name': None,
                'category': None,
                'description': None,
                'phone': None,
                'email': None,
                'website': None,
                'address': None,
                'rating': None,
                'review_count': None,
                'price_range': None,
                'services': [],
            }

            # Extract vendor name (adjust selectors based on actual HTML structure)
            title = soup.find('h1')
            if title:
                data['name'] = title.get_text(strip=True)

            # Extract meta description
            meta_desc = soup.find('meta', {'name': 'description'})
            if meta_desc:
                data['description'] = meta_desc.get('content', '').strip()

            # Look for contact information
            # Phone
            phone_link = soup.find('a', href=lambda x: x and 'tel:' in x)
            if phone_link:
                data['phone'] = phone_link.get('href', '').replace('tel:', '')

            # Email
            email_link = soup.find('a', href=lambda x: x and 'mailto:' in x)
            if email_link:
                data['email'] = email_link.get('href', '').replace('mailto:', '')

            # Website
            website_link = soup.find('a', href=lambda x: x and x.startswith('http') and 'bridely.sg' not in x)
            if website_link:
                data['website'] = website_link.get('href', '')

            return data

        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return None

    def scrape_venue(self, url: str) -> Optional[dict]:
        """Scrape a single venue page"""
        try:
            response = self.client.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            data = {
                'url': url,
                'name': None,
                'type': 'venue',
                'description': None,
                'capacity': None,
                'phone': None,
                'email': None,
                'website': None,
                'address': None,
                'rating': None,
                'review_count': None,
                'price_range': None,
            }

            # Extract venue name
            title = soup.find('h1')
            if title:
                data['name'] = title.get_text(strip=True)

            # Extract meta description
            meta_desc = soup.find('meta', {'name': 'description'})
            if meta_desc:
                data['description'] = meta_desc.get('content', '').strip()

            # Contact info (similar to vendor)
            phone_link = soup.find('a', href=lambda x: x and 'tel:' in x)
            if phone_link:
                data['phone'] = phone_link.get('href', '').replace('tel:', '')

            email_link = soup.find('a', href=lambda x: x and 'mailto:' in x)
            if email_link:
                data['email'] = email_link.get('href', '').replace('mailto:', '')

            return data

        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return None

    def scrape_all_vendors(self, urls: list[str], limit: Optional[int] = None):
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

    def scrape_all_venues(self, urls: list[str], limit: Optional[int] = None):
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
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Saved JSON: {json_path}")

        # Save as CSV
        df = pd.DataFrame(data)
        csv_path = self.output_dir / f"{filename}.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"Saved CSV: {csv_path}")

    def run(self, test_mode: bool = False):
        """Run the full scraping process"""
        print("Starting Bridely.sg scraper...")

        # Fetch sitemap
        urls = self.fetch_sitemap()

        # Categorize URLs
        categories = self.categorize_urls(urls)

        # In test mode, only scrape a few pages
        limit = 5 if test_mode else None

        # Scrape vendors
        vendors = self.scrape_all_vendors(categories['vendors'], limit=limit)
        self.save_data(vendors, 'bridely_vendors')

        # Scrape venues
        venues = self.scrape_all_venues(categories['venues'], limit=limit)
        self.save_data(venues, 'bridely_venues')

        print("\nScraping complete!")
        print(f"Total vendors scraped: {len(vendors)}")
        print(f"Total venues scraped: {len(venues)}")

        self.client.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Scrape wedding vendor data from bridely.sg')
    parser.add_argument('--test', action='store_true', help='Run in test mode (only scrape 5 pages)')
    parser.add_argument('--output', default='data', help='Output directory for scraped data')

    args = parser.parse_args()

    scraper = BridelyScraper(output_dir=args.output)
    scraper.run(test_mode=args.test)


if __name__ == "__main__":
    main()
