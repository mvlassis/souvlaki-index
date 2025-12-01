import argparse
import asyncio
import logging
from pathlib import Path
from typing import Optional

import nodriver
from urllib.parse import urljoin
from bs4 import BeautifulSoup

import db_utils

LOG_FORMAT = "%(levelname)s:%(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.e-food.gr"
url = "https://www.e-food.gr/delivery/athina?categories=souvlakia"

DB_PATH = Path("db/data.db")


async def main(link_limit: Optional[int] = None):
    conn = db_utils.connect(DB_PATH)
    db_utils.init_db(conn)
    run_id = db_utils.start_run(conn)

    browser = await nodriver.start(headless=True)
    page = await browser.get(url)

    await asyncio.sleep(10)

    content = await page.get_content()
    soup = BeautifulSoup(content, "html.parser")

    # Collect all links that match "/delivery/"
    delivery_links = []
    for link in soup.find_all("a", href=True):
        href = link.get("href")

        if isinstance(href, str) and href.startswith("/delivery/"):
            full_url = urljoin(BASE_URL, href)
            if full_url not in delivery_links:
                delivery_links.append(full_url)

    logger.info(f"Found {len(delivery_links)} delivery links.")
    logger.info("-" * 80)

    delivery_links_to_scrape = delivery_links[:link_limit] if link_limit is not None else delivery_links
    if link_limit:
        logger.info(
            f"Because you have specified link_limit = {link_limit}, we will only scrape the first {link_limit} links found."
        )
    for i, link_url in enumerate(delivery_links_to_scrape, start=1):
        logger.info(f"Visiting ({i}/{len(delivery_links_to_scrape)}): {link_url}")
        html_bytes = await load_and_return_html(browser, link_url, delay=10)

        if html_bytes:
            try:
                db_utils.insert_page_from_html(
                    conn=conn,
                    run_id=run_id,
                    source_url=link_url,
                    html_bytes=html_bytes,
                    http_status=200,
                )
            except Exception as e:
                logger.error(f"DB insert failed for {link_url}: {e}")

        # Optional: small pause between requests
        await asyncio.sleep(2)

    db_utils.finish_run(conn, run_id, status="ok")
    conn.commit()
    conn.close()
    logger.info("✅ Scrape complete.")


async def load_and_return_html(browser, url, delay: int = 8):
    """
    Loads a page, waits for content, retrieves HTML, and returns it as bytes.
    """
    try:
        page = await browser.get(url)
        await asyncio.sleep(delay)  # wait for dynamic content

        html_str = await page.get_content()

        html_bytes = html_str.encode("utf-8")

        logger.info(f"✅ Saved URL: {url}")
        return html_bytes

    except Exception as e:
        logger.error(f"Failed to load {url}: {e}")
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--link-limit",
        type=int,
        default=None,
        help="Max number of links to scrape.)",
    )
    args = parser.parse_args()
    nodriver.loop().run_until_complete(main(link_limit=args.link_limit))
