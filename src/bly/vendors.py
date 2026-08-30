import argparse
import os
from pathlib import Path

import httpx
import pandas as pd
from dotenv import load_dotenv

from ..shared.config import get_headers
from ..shared.logging import get_logger
from ..shared.save import save_json

logger = get_logger()

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
    logger.info(f"\n{'=' * 80}")
    logger.info(f"Fetching: {category}")
    logger.info(f"{'=' * 80}")

    url = f"{BASE_URL}/{APP_ID}/{endpoint_path}/data"

    all_vendors = []
    seen_ids = set()
    offset = 0
    limit = 100

    while True:
        logger.debug("  Batch (offset=%d, limit=%d)...", offset, limit)

        payload = {"limit": limit, "offset": offset}
        response = client.post(url, json=payload)
        response.raise_for_status()

        data = response.json()
        records = data.get("records", [])

        if not records:
            logger.info("No more records")
            break

        new_records = []
        for record in records:
            vendor_id = record["id"]
            if vendor_id not in seen_ids:
                seen_ids.add(vendor_id)
                record["_source_category"] = category
                new_records.append(record)

        all_vendors.extend(new_records)
        logger.debug(f"{len(records)} fetched, {len(new_records)} unique (total: {len(all_vendors)})")

        if max_records and len(all_vendors) >= max_records:
            all_vendors = all_vendors[:max_records]
            logger.info(f"✅ Reached max limit: {max_records}")
            break

        if len(new_records) == 0:
            logger.warning("⚠️  No new unique records - stopping")
            break

        if len(records) < limit:
            break

        offset += limit

    logger.info(f"✅ {category}: {len(all_vendors)} unique vendors")
    return all_vendors


def fetch_all_vendors(max_records_per_category: int | None = None):
    logger.info("=" * 80)
    logger.info("BRIDELY.SG COMPLETE VENDOR EXTRACTOR")
    logger.info("=" * 80)
    logger.info(f"\nFetching from {len(VENDOR_ENDPOINTS)} vendor categories...")

    all_vendors = []
    global_seen_ids = set()
    category_stats = {}

    client = httpx.Client(headers=get_headers(), timeout=30.0)

    try:
        for category, endpoint_path in VENDOR_ENDPOINTS.items():
            if not endpoint_path:
                logger.warning(f"Skipping {category}: endpoint not configured")
                continue
            try:
                category_vendors = fetch_vendors_from_endpoint(
                    client, category, endpoint_path, max_records=max_records_per_category
                )
            except Exception as e:
                logger.error(f"Failed to fetch {category}: {e}")
                category_stats[category] = {"fetched": 0, "unique_globally": 0}
                continue

            unique_in_category = 0
            for vendor in category_vendors:
                vendor_id = vendor["id"]
                if vendor_id not in global_seen_ids:
                    global_seen_ids.add(vendor_id)
                    all_vendors.append(vendor)
                    unique_in_category += 1

            category_stats[category] = {"fetched": len(category_vendors), "unique_globally": unique_in_category}

        logger.info(f"\n{'=' * 80}")
        logger.info("SUMMARY")
        logger.info(f"{'=' * 80}")
        for category, stats in category_stats.items():
            logger.info(f"  {category}: {stats['fetched']} fetched, {stats['unique_globally']} globally unique")

        logger.info(f"\n✅ Total unique vendors across all categories: {len(all_vendors)}")
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

        vendor = {
            "vendor_id": vendor_id,
            "name": fields.get("Name"),
            "source_category": source_category,
            "category": categories,
            "email": fields.get("Email"),
            "phone": fields.get("Phone"),
            "website": fields.get("Website"),
            "ranking_score": fields.get("Ranking Score"),
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
        logger.info("No data to save")
        return

    output_path = Path(filename)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    json_path = output_path.with_suffix(".json")
    csv_path = output_path.with_suffix(".csv")

    save_json(vendors, json_path)

    df = pd.DataFrame(vendors)
    df.to_csv(csv_path, index=False, encoding="utf-8")

    logger.info(f"\n✅ Saved {len(vendors)} vendors to:")
    logger.info(f"   - {json_path}")
    logger.info(f"   - {csv_path}")

    logger.info("\n📊 Data Quality:")
    names_pct = df["name"].notna().sum() / len(df) * 100
    logger.info(f"  - Vendors with names: {df['name'].notna().sum()} ({names_pct:.1f}%)")
    categories_pct = df["category"].notna().sum() / len(df) * 100
    logger.info(f"  - Vendors with categories: {df['category'].notna().sum()} ({categories_pct:.1f}%)")
    email_pct = df["email"].notna().sum() / len(df) * 100
    logger.info(f"  - Vendors with email: {df['email'].notna().sum()} ({email_pct:.1f}%)")
    phone_pct = df["phone"].notna().sum() / len(df) * 100
    logger.info(f"  - Vendors with phone: {df['phone'].notna().sum()} ({phone_pct:.1f}%)")
    website_pct = df["website"].notna().sum() / len(df) * 100
    logger.info(f"  - Vendors with website: {df['website'].notna().sum()} ({website_pct:.1f}%)")

    if df["category"].notna().sum() > 0:
        logger.info("\n📋 Top 15 Vendor Categories:")
        category_counts = df["category"].value_counts().head(15)
        for category, count in category_counts.items():
            logger.info(f"  - {category}: {count}")


def main():
    parser = argparse.ArgumentParser(description="Extract Bridely.sg wedding vendors")
    parser.add_argument("--limit-per-category", type=int, help="Max records to fetch per category")
    parser.add_argument("--output", type=str, default="data/bly/vendors", help="Output file path")

    args = parser.parse_args()

    raw_vendors = fetch_all_vendors(max_records_per_category=args.limit_per_category)

    logger.info("\nTransforming data...")
    vendors = transform_vendors(raw_vendors)

    logger.info("\nSaving data...")
    save_vendors(vendors, args.output)

    logger.info("\n" + "=" * 80)
    logger.info("✅ EXTRACTION COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
