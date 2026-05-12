import asyncio

from src.shared import get_logger, get_browser_page, save_csv, save_json

logger = get_logger()


async def click_see_more_until_loaded(page):
    logger.info("Clicking 'See more' until all venues are loaded...")
    click_count = 0
    while True:
        see_more_button = page.locator('button:has-text("See more")')

        if await see_more_button.count() == 0:
            logger.info("No 'See more' button found - all venues loaded")
            break

        try:
            await see_more_button.scroll_into_view_if_needed()
            await see_more_button.click(timeout=5000)
            click_count += 1
            logger.info(f"  Clicked 'See more' {click_count} times")
            await asyncio.sleep(1)
        except Exception as e:
            logger.info(f"Could not click 'See more' button: {e}")
            break


async def extract_venues_from_dom(page):
    logger.info("\nDebugging DOM structure...")
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

    logger.info(f"Debug info: {debug_info}")

    logger.info("\nExtracting venue data from DOM...")
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

    logger.info(f"Extracted {len(venues)} venues")
    return venues


async def fetch_all_venues():
    async with get_browser_page() as page:
        logger.info("Navigating to venues page...")
        await page.goto("https://www.bridely.sg/venues", wait_until="networkidle")

        await click_see_more_until_loaded(page)
        venues = await extract_venues_from_dom(page)

        return venues


async def main():
    logger.info("Fetching all venues from Bridely.sg using Playwright...")
    venues = await fetch_all_venues()

    logger.info(f"\nTotal venues fetched: {len(venues)}")

    save_csv(
        venues,
        "data/bly/venues.csv",
        fieldnames=["recordId", "name", "url", "price", "capacity", "venueRating", "serviceRating", "foodRating", "reviews"],
    )
    save_json(venues, "data/bly/venues.json")

    logger.info("\nFirst 5 venues:")
    for i, venue in enumerate(venues[:5], 1):
        logger.info(f"{i}. {venue['name']} - {venue['price']} - {venue['capacity']}")


if __name__ == "__main__":
    asyncio.run(main())
