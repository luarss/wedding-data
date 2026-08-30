import argparse
import asyncio
import json
import time
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

from ..shared.config import get_headers
from ..shared.logging import get_logger
from ..shared.save import save_json_csv

logger = get_logger()

BASE_URL = "https://t.me/s"


def parse_messages(soup: BeautifulSoup, channel: str) -> list[dict]:
    messages = []

    for wrap in soup.select(".tgme_widget_message_wrap"):
        msg_div = wrap.select_one(".tgme_widget_message")
        if not msg_div:
            continue

        data_post = msg_div.get("data-post", "")
        if not data_post:
            continue

        msg_id_str = data_post.split("/")[-1]
        try:
            msg_id = int(msg_id_str)
        except ValueError:
            continue

        date_tag = msg_div.select_one(".tgme_widget_message_date time")
        datetime_str = date_tag.get("datetime", "") if date_tag else ""

        text_div = msg_div.select_one(".tgme_widget_message_text")
        text = text_div.get_text(separator="\n", strip=True) if text_div else ""

        links = []
        if text_div:
            for a in text_div.select("a[href]"):
                href = a.get("href", "")
                if href and not href.startswith("https://t.me"):
                    links.append(href)

        # Link preview
        preview_url = ""
        preview_div = msg_div.select_one(".tgme_widget_message_link_preview")
        if preview_div:
            preview_a = preview_div.select_one("a[href]")
            if preview_a:
                preview_url = preview_a.get("href", "")

        images = []
        for img in msg_div.select(".tgme_widget_message_photo_image"):
            src = img.get("src", "")
            if src:
                images.append(src)
        # background-image style on photo wrappers
        for el in msg_div.select("i.tgme_widget_message_photo_image"):
            style = el.get("style", "")
            if "background-image" in style:
                start = style.find("url('") + 5
                end = style.find("')", start)
                if start > 4 and end > start:
                    images.append(style[start:end])

        messages.append(
            {
                "channel": channel,
                "message_id": msg_id,
                "url": f"https://t.me/{channel}/{msg_id}",
                "datetime": datetime_str,
                "text": text,
                "links": links,
                "preview_url": preview_url,
                "images": images,
            }
        )

    return messages


async def extract_channel(
    channel: str,
    limit: int | None = None,
    delay: float = 1.0,
    since_id: int = 0,
) -> list[dict]:
    all_messages: list[dict] = []
    before: int | None = None

    async with httpx.AsyncClient(headers=get_headers(), timeout=30, follow_redirects=True) as client:
        while True:
            url = f"{BASE_URL}/{channel}"
            if before is not None:
                url += f"?before={before}"

            logger.info(f"Fetching {url}...")
            response = await client.get(url)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "lxml")
            page_messages = parse_messages(soup, channel)

            if not page_messages:
                logger.info("No messages found on page, stopping.")
                break

            # In incremental mode, drop already-seen messages and stop paginating
            if since_id > 0:
                new_msgs = [m for m in page_messages if m["message_id"] > since_id]
                all_messages.extend(new_msgs)
                logger.info(f"  Got {len(new_msgs)} new messages (total: {len(all_messages)})")
                if len(new_msgs) < len(page_messages):
                    logger.info(f"  Reached already-extracted messages (since_id={since_id}), stopping.")
                    break
            else:
                all_messages.extend(page_messages)
                logger.info(f"  Got {len(page_messages)} messages (total: {len(all_messages)})")

            if limit and len(all_messages) >= limit:
                all_messages = all_messages[:limit]
                break

            min_id = min(m["message_id"] for m in page_messages)
            if before is not None and min_id >= before:
                break
            before = min_id

            time.sleep(delay)

    all_messages.sort(key=lambda m: m["message_id"], reverse=True)
    return all_messages


def run_channel(channel: str, limit: int | None, delay: float, incremental: bool) -> None:
    output = f"data/tg/{channel}/messages"

    logger.info("=" * 60)
    logger.info(f"EXTRACTING TELEGRAM CHANNEL: @{channel}")
    logger.info("=" * 60)

    existing: list[dict] = []
    since_id = 0

    if incremental:
        output_path = Path(output).with_suffix(".json")
        if output_path.exists():
            with open(output_path, encoding="utf-8") as f:
                existing = json.load(f)
            if existing:
                since_id = max(m["message_id"] for m in existing)
                logger.info(f"Incremental mode: {len(existing)} existing messages, fetching since id={since_id}")

    new_messages = asyncio.run(extract_channel(channel, limit=limit, delay=delay, since_id=since_id))

    if incremental and existing:
        existing_by_id = {m["message_id"]: m for m in existing}
        for m in new_messages:
            existing_by_id[m["message_id"]] = m
        messages = sorted(existing_by_id.values(), key=lambda m: m["message_id"], reverse=True)
        logger.info(f"Merged: {len(new_messages)} new + {len(existing)} existing = {len(messages)} total")
    else:
        messages = new_messages

    if messages:
        save_json_csv(messages, output)
        logger.info(f"\n✅ Saved {len(messages)} messages to {output}.json / .csv")
    else:
        logger.warning("No messages extracted.")


def main():
    parser = argparse.ArgumentParser(description="Extract public Telegram channels listed in channels.json")
    parser.add_argument("--limit", type=int, default=None, help="Max number of messages to fetch per channel")
    parser.add_argument("--delay", type=float, default=1.0, help="Seconds between page requests (default: 1.0)")
    parser.add_argument("--incremental", action="store_true", help="Only fetch messages newer than existing data and merge")
    args = parser.parse_args()

    channels_path = Path(__file__).parent / "channels.json"
    with open(channels_path, encoding="utf-8") as f:
        channels: list[str] = json.load(f)

    for channel in channels:
        run_channel(channel, limit=args.limit, delay=args.delay, incremental=args.incremental)


if __name__ == "__main__":
    main()
