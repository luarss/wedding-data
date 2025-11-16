import asyncio
import csv
import json
from contextlib import asynccontextmanager
from pathlib import Path

from playwright.async_api import async_playwright


@asynccontextmanager
async def get_browser_page(headless=True):
    async with async_playwright() as p:
        browser = None
        try:
            browser = await p.chromium.launch(headless=headless)
            page = await browser.new_page()
            yield page
        finally:
            if browser is not None:
                await browser.close()


async def click_see_more_until_loaded(page):
    print("Clicking 'See more' until all venues are loaded...")
    click_count = 0
    while True:
        see_more_button = page.locator('button:has-text("See more")')

        if await see_more_button.count() == 0:
            print("No 'See more' button found - all venues loaded")
            break

        try:
            await see_more_button.scroll_into_view_if_needed()
            await see_more_button.click(timeout=5000)
            click_count += 1
            print(f"  Clicked 'See more' {click_count} times")
            await asyncio.sleep(1)
        except Exception as e:
            print(f"Could not click 'See more' button: {e}")
            break


async def extract_venues_from_dom(page):
    print("\nDebugging DOM structure...")
    debug_info = await page.evaluate(r"""() => {
        const venueCards = document.querySelectorAll('.vertical-list-item');
        const firstCard = venueCards[0];

        return {
            totalCards: venueCards.length,
            firstCardHTML: firstCard ? firstCard.outerHTML.substring(0, 500) : 'No cards found',
            firstCardLinks: firstCard ? firstCard.querySelectorAll('a').length : 0,
            hasRecordId: firstCard ? !!firstCard.querySelector('a[href*="recordId"]') : false
        };
    }""")

    print(f"Debug info: {debug_info}")

    print("\nExtracting venue data from DOM...")
    venues = await page.evaluate(r"""() => {
        const wrappers = document.querySelectorAll('.list-item-wrapper.vertical');
        const venues = [];

        for (let i = 0; i < wrappers.length; i++) {
            const wrapper = wrappers[i];

            const linkElement = wrapper.querySelector('.list-action-wrapper a[href*="recordId"]');
            if (!linkElement) continue;

            const url = linkElement.href;
            const recordIdMatch = url.match(/recordId=([^&]+)/);
            const recordId = recordIdMatch ? recordIdMatch[1] : '';

            const card = wrapper.querySelector('.vertical-list-item');
            if (!card) continue;

            const allText = card.innerText || card.textContent || '';
            const lines = allText.split('\n').map(l => l.trim()).filter(l => l);

            const priceLine = lines.find(l => l.includes('++') || l.includes('/pax') || l.includes('$'));
            const price = priceLine || '';

            const capacityLine = lines.find(l => l.includes('Capacity:'));
            const capacity = capacityLine ? capacityLine.replace('Capacity:', '').trim() : '';

            const nameElement = card.querySelector('h3');
            const name = nameElement ? nameElement.textContent.trim() : '';

            const venueLabel = lines.find(l => l.startsWith('Venue:'));
            const venueRating = venueLabel ? venueLabel.replace('Venue:', '').trim() : '';

            const serviceLabel = lines.find(l => l.startsWith('Service:'));
            const serviceRating = serviceLabel ? serviceLabel.replace('Service:', '').trim() : '';

            const foodLabel = lines.find(l => l.startsWith('Food:'));
            const foodRating = foodLabel ? foodLabel.replace('Food:', '').trim() : '';

            const reviewMatch = allText.match(/(\d+)\s*reviews?/i);
            const reviews = reviewMatch ? reviewMatch[1] : '';

            venues.push({
                recordId,
                name,
                url,
                price,
                capacity,
                venueRating,
                serviceRating,
                foodRating,
                reviews
            });
        }

        return venues;
    }""")

    print(f"Extracted {len(venues)} venues")
    return venues


async def fetch_all_venues():
    async with get_browser_page() as page:
        print("Navigating to venues page...")
        await page.goto("https://www.bridely.sg/venues", wait_until="networkidle")

        await click_see_more_until_loaded(page)
        venues = await extract_venues_from_dom(page)

        return venues


def save_to_csv(venues, filename):
    if not venues:
        print("No venues to save")
        return

    Path(filename).parent.mkdir(parents=True, exist_ok=True)

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "recordId",
                "name",
                "url",
                "price",
                "capacity",
                "venueRating",
                "serviceRating",
                "foodRating",
                "reviews",
            ],
        )
        writer.writeheader()
        writer.writerows(venues)

    print(f"Saved to {filename}")


def save_to_json(venues, filename):
    Path(filename).parent.mkdir(parents=True, exist_ok=True)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(venues, f, indent=2, ensure_ascii=False)

    print(f"Saved to {filename}")


async def main():
    print("Fetching all venues from Bridely.sg using Playwright...")
    venues = await fetch_all_venues()

    print(f"\nTotal venues fetched: {len(venues)}")

    csv_file = "data/bly/venues.csv"
    save_to_csv(venues, csv_file)

    json_file = "data/bly/venues.json"
    save_to_json(venues, json_file)

    print("\nFirst 5 venues:")
    for i, venue in enumerate(venues[:5], 1):
        print(f"{i}. {venue['name']} - {venue['price']} - {venue['capacity']}")


if __name__ == "__main__":
    asyncio.run(main())
