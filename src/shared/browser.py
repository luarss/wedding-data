from contextlib import asynccontextmanager

from playwright.async_api import async_playwright


@asynccontextmanager
async def get_browser_page(headless=True):
    async with async_playwright() as p:
        browser = None
        try:
            browser = await p.chromium.launch(headless=headless, args=["--no-sandbox", "--disable-setuid-sandbox"])
            page = await browser.new_page(viewport={"width": 1280, "height": 800})
            yield page
        finally:
            if browser is not None:
                await browser.close()
