"""
Bridely.sg Complete Vendor API scraper
Scrapes ALL wedding vendors from all category endpoints
"""

import json
import os
from pathlib import Path

import httpx
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


class BridelyAllVendorsAPIScraper:
    """API scraper for all Bridely.sg vendor categories"""

    def __init__(self, output_dir: str = "data/bridely"):
        self.base_url = os.getenv("BRIDELY_BASE_URL")
        self.app_id = os.getenv("BRIDELY_APP_ID")

        self.vendor_endpoints = {
            "All Vendors": os.getenv("BRIDELY_ALL_VENDORS_ENDPOINT"),
            "Makeup Artists": os.getenv("BRIDELY_MAKEUP_ARTISTS_ENDPOINT"),
            "Videographers": os.getenv("BRIDELY_VIDEOGRAPHERS_ENDPOINT"),
            "Emcees": os.getenv("BRIDELY_EMCEES_ENDPOINT"),
            "Wedding Planners": os.getenv("BRIDELY_WEDDING_PLANNERS_ENDPOINT"),
            "Justices of Peace": os.getenv("BRIDELY_JUSTICES_ENDPOINT"),
            "Photobooths": os.getenv("BRIDELY_PHOTOBOOTHS_ENDPOINT"),
            "Live Music": os.getenv("BRIDELY_LIVE_MUSIC_ENDPOINT"),
            "Wedding Cars": os.getenv("BRIDELY_WEDDING_CARS_ENDPOINT"),
            "Wedding Favours": os.getenv("BRIDELY_WEDDING_FAVOURS_ENDPOINT"),
            "Engagement Rings": os.getenv("BRIDELY_ENGAGEMENT_RINGS_ENDPOINT"),
        }

        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.client = httpx.Client(timeout=30.0)

    def fetch_vendors_from_endpoint(self, category: str, endpoint_path: str, max_records: int | None = None):
        """Fetch all vendors from a specific endpoint with deduplication"""
        print(f"\n{'=' * 80}")
        print(f"Fetching: {category}")
        print(f"{'=' * 80}")

        url = f"{self.base_url}/{self.app_id}/{endpoint_path}/data"

        all_vendors = []
        seen_ids = set()
        offset = 0
        limit = 100

        while True:
            print(f"  Batch (offset={offset}, limit={limit})...", end=" ")

            payload = {"limit": limit, "offset": offset}
            response = self.client.post(url, json=payload)
            response.raise_for_status()

            data = response.json()
            records = data.get("records", [])

            if not records:
                print("No more records")
                break

            new_records = []
            for record in records:
                vendor_id = record["id"]
                if vendor_id not in seen_ids:
                    seen_ids.add(vendor_id)
                    record["_source_category"] = category
                    new_records.append(record)

            all_vendors.extend(new_records)
            print(f"{len(records)} fetched, {len(new_records)} unique (total: {len(all_vendors)})")

            if max_records and len(all_vendors) >= max_records:
                all_vendors = all_vendors[:max_records]
                print(f"✅ Reached max limit: {max_records}")
                break

            if len(new_records) == 0:
                print("⚠️  No new unique records - stopping")
                break

            if len(records) < limit:
                break

            offset += limit

        print(f"✅ {category}: {len(all_vendors)} unique vendors")
        return all_vendors

    def fetch_all_vendors(self, max_records_per_category: int | None = None):
        """Fetch vendors from all category endpoints"""
        print("=" * 80)
        print("BRIDELY.SG COMPLETE VENDOR SCRAPER")
        print("=" * 80)
        print(f"\nFetching from {len(self.vendor_endpoints)} vendor categories...")

        all_vendors = []
        global_seen_ids = set()

        category_stats = {}

        for category, endpoint_path in self.vendor_endpoints.items():
            category_vendors = self.fetch_vendors_from_endpoint(
                category, endpoint_path, max_records=max_records_per_category
            )

            unique_in_category = 0
            for vendor in category_vendors:
                vendor_id = vendor["id"]
                if vendor_id not in global_seen_ids:
                    global_seen_ids.add(vendor_id)
                    all_vendors.append(vendor)
                    unique_in_category += 1

            category_stats[category] = {"fetched": len(category_vendors), "unique_globally": unique_in_category}

        print(f"\n{'=' * 80}")
        print("SUMMARY")
        print(f"{'=' * 80}")
        for category, stats in category_stats.items():
            print(f"  {category}: {stats['fetched']} fetched, {stats['unique_globally']} globally unique")

        print(f"\n✅ Total unique vendors across all categories: {len(all_vendors)}")
        return all_vendors

    def transform_vendor_data(self, raw_vendors: list):
        """Transform raw API data to clean vendor records"""
        vendors = []

        for record in raw_vendors:
            vendor_id = record["id"]
            fields = record["fields"]
            source_category = record.get("_source_category", "Unknown")

            categories = fields.get("Category", [])
            if isinstance(categories, list):
                categories = ", ".join(categories)

            gallery_urls = []
            if fields.get("Thumbnails"):
                gallery_urls = [img["url"] for img in fields["Thumbnails"]]

            vendor = {
                "vendor_id": vendor_id,
                "name": fields.get("Name"),
                "source_category": source_category,
                "category": categories,
                "email": fields.get("Email"),
                "phone": fields.get("Phone"),
                "website": fields.get("Website"),
                "ranking_score": fields.get("Ranking Score"),
                "gallery_count": len(gallery_urls),
                "gallery_urls": json.dumps(gallery_urls) if gallery_urls else None,
                "seo_description": fields.get("SEO:Description"),
                "social_description": fields.get("Social:Description"),
                "promo_available": fields.get("Promo Available"),
                "trusted_partner": fields.get("Trusted Partner"),
                "pricing_from": fields.get("Pricing From"),
                "created_time": record.get("createdTime"),
            }

            vendors.append(vendor)

        return vendors

    def save_data(self, vendors: list, filename: str = "bridely_all_vendors_api"):
        """Save vendor data to JSON and CSV"""
        if not vendors:
            print("No data to save")
            return

        json_path = self.output_dir / f"{filename}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(vendors, f, indent=2, ensure_ascii=False)
        print(f"\n✅ Saved JSON: {json_path} ({len(vendors)} vendors)")

        df = pd.DataFrame(vendors)
        csv_path = self.output_dir / f"{filename}.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8")
        print(f"✅ Saved CSV: {csv_path}")

        print("\n📊 Data Quality:")
        names_pct = df["name"].notna().sum() / len(df) * 100
        print(f"  - Vendors with names: {df['name'].notna().sum()} ({names_pct:.1f}%)")
        categories_pct = df["category"].notna().sum() / len(df) * 100
        print(f"  - Vendors with categories: {df['category'].notna().sum()} ({categories_pct:.1f}%)")
        email_pct = df["email"].notna().sum() / len(df) * 100
        print(f"  - Vendors with email: {df['email'].notna().sum()} ({email_pct:.1f}%)")
        phone_pct = df["phone"].notna().sum() / len(df) * 100
        print(f"  - Vendors with phone: {df['phone'].notna().sum()} ({phone_pct:.1f}%)")
        website_pct = df["website"].notna().sum() / len(df) * 100
        print(f"  - Vendors with website: {df['website'].notna().sum()} ({website_pct:.1f}%)")
        gallery_pct = (df["gallery_count"] > 0).sum() / len(df) * 100
        print(f"  - Vendors with gallery: {(df['gallery_count'] > 0).sum()} ({gallery_pct:.1f}%)")

        if df["category"].notna().sum() > 0:
            print("\n📋 Top 15 Vendor Categories:")
            category_counts = df["category"].value_counts().head(15)
            for category, count in category_counts.items():
                print(f"  - {category}: {count}")

    def run(self, max_records_per_category: int | None = None):
        """Run the complete vendor scraper"""
        raw_vendors = self.fetch_all_vendors(max_records_per_category=max_records_per_category)

        print("\nTransforming data...")
        vendors = self.transform_vendor_data(raw_vendors)

        print("\nSaving data...")
        self.save_data(vendors)

        print("\n" + "=" * 80)
        print("✅ SCRAPING COMPLETE")
        print("=" * 80)

        self.client.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Scrape ALL wedding vendor data from Bridely.sg API")
    parser.add_argument("--output", default="data/bridely", help="Output directory for scraped data")
    parser.add_argument("--limit-per-category", type=int, help="Maximum number of records to fetch per category")

    args = parser.parse_args()

    scraper = BridelyAllVendorsAPIScraper(output_dir=args.output)
    scraper.run(max_records_per_category=args.limit_per_category)


if __name__ == "__main__":
    main()
