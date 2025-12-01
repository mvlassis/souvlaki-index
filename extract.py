import argparse
import logging
import os
import re
from bs4 import BeautifulSoup
from bs4.element import NavigableString
from pathlib import Path

import db_utils

DEFAULT_DB_PATH = Path("db/data.db")

LOG_FORMAT = "%(levelname)s:%(name)s: %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def extract_price_from_html(html_str: str, item_name: str):
    """Extract the price of an item from an HTML file based on its name."""
    soup = BeautifulSoup(html_str, "html.parser")
    section = soup.find(id="tylihta")
    if not section:
        return None
    name_pattern = re.compile(rf"{re.escape(item_name).replace(r'\ ', r'\s*')}", re.IGNORECASE)
    name_node = section.find(string=name_pattern)
    price_pattern = re.compile(
        r"""
        (?:
            €\s*([0-9]+(?:[.,][0-9]{1,2})?)   # format: € 3,50
          |
            ([0-9]+(?:[.,][0-9]{1,2})?)\s*€   # format: 3,50 €
        )
        """,
        re.VERBOSE,
    )
    if not name_node:
        return None
    for node in name_node.next_elements:
        if isinstance(node, NavigableString):
            m = price_pattern.search(str(node))
            if m:
                raw = m.group(1) or m.group(2)
                # normalize decimal separator
                value = float(raw.replace(",", "."))
                return value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--item",
        type=str,
        default="Γύρος χοιρινός",
        help="Name of the menu item to search (default: Γύρος χοιρινός)",
    )
    parser.add_argument("--db", type=str, default=os.environ.get("SQLITE_PATH", DEFAULT_DB_PATH))
    parser.add_argument(
        "--schema-path",
        type=str,
        default="db/schema.sql",
        help="Path to the schema file for initializing the database. (default: db/schema.sql)",
    )
    args = parser.parse_args()

    item_name = args.item
    conn = db_utils.connect(args.db)
    db_utils.init_db(conn, args.schema_path)
    logger.info("🔍 Extracting prices only for **unprocessed pages**")

    unprocessed_pages = conn.execute(
        """
        SELECT p.id, p.source_url, p.content_blob
        FROM pages p
        LEFT JOIN prices r
            ON r.page_id = p.id
        LEFT JOIN menu_items m
            ON m.id = r.menu_item_id
            AND m.item_name = :item_name
        WHERE m.id IS NULL
        ORDER BY p.id;
        """,
        {"item_name": item_name},
    ).fetchall()


    logger.info(f"Found {len(unprocessed_pages)} unprocessed pages.")

    for page_id, source_url, content_blob in unprocessed_pages:
        html_str = content_blob.decode("utf-8")
        logger.info(f"🔎 Processing page_id={page_id} url={source_url}")

        price = extract_price_from_html(html_str, item_name)

        if price is None:
            logger.debug(f"No price found for '{item_name}' in {source_url}")
            continue

        # Vendor: derive from URL (temporary heuristic)
        vendor = source_url.rsplit("/", 1)[-1] or "unknown"
        vendor = vendor.replace("-", " ").strip()

        menu_item_id = db_utils.ensure_menu_item(conn, vendor, item_name)

        db_utils.insert_price(conn, menu_item_id, page_id, price)

        logger.info(f"✅ Inserted price {price} for item '{item_name}' at vendor '{vendor}'")

    conn.commit()
    conn.close()

    logger.info("✅ Extraction complete (incremental).")


if __name__ == "__main__":
    main()
