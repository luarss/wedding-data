"""
Bridely.sg API scraper - clean structured data extraction
Much faster and cleaner than Playwright scraping
"""

import json
import os
from pathlib import Path

import httpx
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


class BridelyAPIScraper:
    """API scraper for Bridely.sg venue data"""

    def __init__(self, output_dir: str = "data/bridely"):
        self.base_url = os.getenv("BRIDELY_BASE_URL")
        self.app_id = os.getenv("BRIDELY_APP_ID")
        self.datasource_id = os.getenv("BRIDELY_VENUES_DATASOURCE_ID")
        self.venues_endpoint_id = os.getenv("BRIDELY_VENUES_ENDPOINT_ID")
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.client = httpx.Client(timeout=30.0)

    def fetch_all_venues(self, max_records: int | None = None):
        """Fetch all venues using pagination"""
        print("Fetching venues from Bridely.sg API...")

        url = f"{self.base_url}/{self.app_id}/{self.datasource_id}/{self.venues_endpoint_id}/data"

        all_venues = []
        seen_ids = set()
        offset = 0
        limit = 100

        while True:
            print(f"Fetching batch (offset={offset}, limit={limit})...")

            payload = {"limit": limit, "offset": offset}
            response = self.client.post(url, json=payload)
            response.raise_for_status()

            data = response.json()
            records = data.get("records", [])

            if not records:
                break

            new_records = []
            for record in records:
                venue_id = record["id"]
                if venue_id not in seen_ids:
                    seen_ids.add(venue_id)
                    new_records.append(record)

            all_venues.extend(new_records)
            print(f"  Retrieved {len(records)} records, {len(new_records)} unique (total unique: {len(all_venues)})")

            if max_records and len(all_venues) >= max_records:
                all_venues = all_venues[:max_records]
                print(f"\n✅ Reached max_records limit: {max_records}")
                break

            if len(new_records) == 0:
                print("\n⚠️  No new unique records in this batch - stopping pagination")
                break

            if len(records) < limit:
                break

            offset += limit

        print(f"\n✅ Total unique venues fetched: {len(all_venues)}")
        return all_venues

    def parse_contact_links(self, contact_links_md: str):
        """Parse markdown contact links to extract phone and email"""
        phone = None
        email = None

        if not contact_links_md:
            return phone, email

        if "tel:" in contact_links_md:
            phone = contact_links_md.split("tel:")[1].split(")")[0]
            phone = phone.replace("+", "").strip()

        if "mailto:" in contact_links_md:
            email_part = contact_links_md.split("mailto:")[1].split("?")[0]
            email = email_part.strip()

        return phone, email

    def transform_venue_data(self, raw_venues: list):
        """Transform raw API data to clean venue records"""
        venues = []

        for record in raw_venues:
            venue_id = record["id"]
            fields = record["fields"]

            phone, email = self.parse_contact_links(fields.get("Contact Links", ""))

            gallery_urls = []
            if fields.get("Gallery"):
                gallery_urls = [img["url"] for img in fields["Gallery"]]

            tags = fields.get("Tags", [])
            if isinstance(tags, list):
                tags = ", ".join(tags)

            venue = {
                "venue_id": venue_id,
                "name": fields.get("name"),
                "address": fields.get("address"),
                "phone": phone,
                "email": email,
                "hero_embed": fields.get("Hero Embed"),
                "video": fields.get("Video"),
                "tags": tags,
                "gallery_count": len(gallery_urls),
                "gallery_urls": json.dumps(gallery_urls) if gallery_urls else None,
                "seo_title": fields.get("SEO:Title"),
                "seo_description": fields.get("SEO:Description"),
                "seo_slug": fields.get("SEO:Slug"),
                "social_title": fields.get("Social:Title"),
                "social_description": fields.get("Social:Description"),
                "related_venues_count": len(fields.get("Related Venues", [])),
                "created_time": record.get("createdTime"),
            }

            venues.append(venue)

        return venues

    def save_data(self, venues: list, filename: str = "bridely_venues_api"):
        """Save venue data to JSON and CSV"""
        if not venues:
            print("No data to save")
            return

        json_path = self.output_dir / f"{filename}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(venues, f, indent=2, ensure_ascii=False)
        print(f"✅ Saved JSON: {json_path} ({len(venues)} venues)")

        df = pd.DataFrame(venues)
        csv_path = self.output_dir / f"{filename}.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8")
        print(f"✅ Saved CSV: {csv_path}")

        print("\n📊 Data Quality:")
        names_pct = df["name"].notna().sum() / len(df) * 100
        print(f"  - Venues with names: {df['name'].notna().sum()} ({names_pct:.1f}%)")
        addresses_pct = df["address"].notna().sum() / len(df) * 100
        print(f"  - Venues with addresses: {df['address'].notna().sum()} ({addresses_pct:.1f}%)")
        phone_pct = df["phone"].notna().sum() / len(df) * 100
        print(f"  - Venues with phone: {df['phone'].notna().sum()} ({phone_pct:.1f}%)")
        email_pct = df["email"].notna().sum() / len(df) * 100
        print(f"  - Venues with email: {df['email'].notna().sum()} ({email_pct:.1f}%)")
        gallery_pct = (df["gallery_count"] > 0).sum() / len(df) * 100
        print(f"  - Venues with gallery: {(df['gallery_count'] > 0).sum()} ({gallery_pct:.1f}%)")
        tags_pct = df["tags"].notna().sum() / len(df) * 100
        print(f"  - Venues with tags: {df['tags'].notna().sum()} ({tags_pct:.1f}%)")

    def run(self, max_records: int | None = None):
        """Run the API scraper"""
        print("=" * 80)
        print("BRIDELY.SG API SCRAPER")
        print("=" * 80)
        print()

        raw_venues = self.fetch_all_venues(max_records=max_records)
        print("\nTransforming data...")
        venues = self.transform_venue_data(raw_venues)

        print("\nSaving data...")
        self.save_data(venues)

        print("\n" + "=" * 80)
        print("✅ SCRAPING COMPLETE")
        print("=" * 80)

        self.client.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Scrape wedding venue data from Bridely.sg API")
    parser.add_argument("--output", default="data/bridely", help="Output directory for scraped data")
    parser.add_argument("--limit", type=int, help="Maximum number of records to fetch")

    args = parser.parse_args()

    scraper = BridelyAPIScraper(output_dir=args.output)
    scraper.run(max_records=args.limit)


if __name__ == "__main__":
    main()
