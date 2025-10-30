import argparse
import json
import os
from pathlib import Path

import httpx
import pandas as pd
from dotenv import load_dotenv

from ..shared.config import get_headers

load_dotenv()

BASE_URL = os.getenv("BRIDELY_BASE_URL")
APP_ID = os.getenv("BRIDELY_APP_ID")

VENDOR_ENDPOINTS = {
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


def fetch_vendors_from_endpoint(
    client: httpx.Client, category: str, endpoint_path: str, max_records: int | None = None
):
    print(f"\n{'=' * 80}")
    print(f"Fetching: {category}")
    print(f"{'=' * 80}")

    url = f"{BASE_URL}/{APP_ID}/{endpoint_path}/data"

    all_vendors = []
    seen_ids = set()
    offset = 0
    limit = 100

    while True:
        print(f"  Batch (offset={offset}, limit={limit})...", end=" ")

        payload = {"limit": limit, "offset": offset}
        response = client.post(url, json=payload)
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


def fetch_all_vendors(max_records_per_category: int | None = None):
    print("=" * 80)
    print("BRIDELY.SG COMPLETE VENDOR SCRAPER")
    print("=" * 80)
    print(f"\nFetching from {len(VENDOR_ENDPOINTS)} vendor categories...")

    all_vendors = []
    global_seen_ids = set()
    category_stats = {}

    client = httpx.Client(headers=get_headers(), timeout=30.0)

    try:
        for category, endpoint_path in VENDOR_ENDPOINTS.items():
            category_vendors = fetch_vendors_from_endpoint(
                client, category, endpoint_path, max_records=max_records_per_category
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

    finally:
        client.close()


def transform_vendors(raw_vendors: list):
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


def save_vendors(vendors: list, filename: str = "data/bly/vendors"):
    if not vendors:
        print("No data to save")
        return

    output_path = Path(filename)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    json_path = output_path.with_suffix(".json")
    csv_path = output_path.with_suffix(".csv")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(vendors, f, indent=2, ensure_ascii=False)

    df = pd.DataFrame(vendors)
    df.to_csv(csv_path, index=False, encoding="utf-8")

    print(f"\n✅ Saved {len(vendors)} vendors to:")
    print(f"   - {json_path}")
    print(f"   - {csv_path}")

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


def main():
    parser = argparse.ArgumentParser(description="Scrape Bridely.sg wedding vendors")
    parser.add_argument("--limit-per-category", type=int, help="Max records to fetch per category")
    parser.add_argument("--output", type=str, default="data/bly/vendors", help="Output file path")

    args = parser.parse_args()

    raw_vendors = fetch_all_vendors(max_records_per_category=args.limit_per_category)

    print("\nTransforming data...")
    vendors = transform_vendors(raw_vendors)

    print("\nSaving data...")
    save_vendors(vendors, args.output)

    print("\n" + "=" * 80)
    print("✅ SCRAPING COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
