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
DATASOURCE_ID = os.getenv("BRIDELY_VENUES_DATASOURCE_ID")
VENUES_ENDPOINT_ID = os.getenv("BRIDELY_VENUES_ENDPOINT_ID")


def fetch_venues(max_records: int | None = None):
    url = f"{BASE_URL}/{APP_ID}/{DATASOURCE_ID}/{VENUES_ENDPOINT_ID}/data"

    all_venues = []
    seen_ids = set()
    offset = 0
    limit = 100

    client = httpx.Client(headers=get_headers(), timeout=30.0)

    try:
        while True:
            print(f"Fetching batch (offset={offset}, limit={limit})...")

            payload = {"limit": limit, "offset": offset}
            response = client.post(url, json=payload)
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
            print(f"  Retrieved {len(records)} records, {len(new_records)} unique (total: {len(all_venues)})")

            if max_records and len(all_venues) >= max_records:
                all_venues = all_venues[:max_records]
                print(f"\n✅ Reached max_records limit: {max_records}")
                break

            if len(new_records) == 0:
                print("\n⚠️  No new unique records - stopping")
                break

            if len(records) < limit:
                break

            offset += limit

        print(f"\n✅ Total unique venues: {len(all_venues)}")
        return all_venues

    finally:
        client.close()


def parse_contact_links(contact_links_md: str):
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


def transform_venues(raw_venues: list):
    venues = []

    for record in raw_venues:
        venue_id = record["id"]
        fields = record["fields"]

        phone, email = parse_contact_links(fields.get("Contact Links", ""))

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


def save_venues(venues: list, filename: str = "data/bly/venues"):
    if not venues:
        print("No data to save")
        return

    output_path = Path(filename)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    json_path = output_path.with_suffix(".json")
    csv_path = output_path.with_suffix(".csv")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(venues, f, indent=2, ensure_ascii=False)

    df = pd.DataFrame(venues)
    df.to_csv(csv_path, index=False, encoding="utf-8")

    print(f"✅ Saved {len(venues)} venues to:")
    print(f"   - {json_path}")
    print(f"   - {csv_path}")

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


def main():
    parser = argparse.ArgumentParser(description="Scrape Bridely.sg wedding venues")
    parser.add_argument("--limit", type=int, help="Max number of records to fetch")
    parser.add_argument("--output", type=str, default="data/bly/venues", help="Output file path")

    args = parser.parse_args()

    print("=" * 80)
    print("BRIDELY.SG VENUES SCRAPER")
    print("=" * 80)
    print()

    raw_venues = fetch_venues(max_records=args.limit)
    print("\nTransforming data...")
    venues = transform_venues(raw_venues)

    print("\nSaving data...")
    save_venues(venues, args.output)

    print("\n" + "=" * 80)
    print("✅ SCRAPING COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
