import re
from pathlib import Path

import httpx


async def download_pdf(client: httpx.AsyncClient, url: str, save_path: Path) -> bool:
    """Download a PDF file, skipping if already exists."""
    try:
        if save_path.exists():
            return True

        response = await client.get(url, timeout=30, follow_redirects=True)
        response.raise_for_status()

        save_path.parent.mkdir(parents=True, exist_ok=True)

        with open(save_path, "wb") as f:
            f.write(response.content)

        return True
    except Exception:
        return False


def slug_from_url(url: str) -> str:
    """Extract a safe directory slug from a URL's last path segment."""
    parts = url.rstrip("/").split("/")
    slug = parts[-1] if parts else "unknown"
    slug = slug.lower()
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-")
