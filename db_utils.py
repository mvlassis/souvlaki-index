import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

DEFAULT_SCHEMA_PATH = Path("db/schema.sql")


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(tzinfo=timezone.utc).isoformat()


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(conn: sqlite3.Connection, schema_path: Path = DEFAULT_SCHEMA_PATH):
    with open(schema_path, "r", encoding="utf-8") as f:
        conn.executescript(f.read())


def start_run(conn) -> int:
    now = iso_utc(datetime.now(timezone.utc))
    cur = conn.execute(
        "INSERT INTO scrape_runs (started_at, status) VALUES(?, ?)",
        (now, "ok"),
    )
    return cur.lastrowid


def finish_run(conn, run_id: int, status: str = "ok"):
    now = iso_utc(datetime.now(timezone.utc))
    conn.execute(
        "UPDATE scrape_runs SET finished_at=?, status=? WHERE id=?",
        (now, status, run_id),
    )


def ensure_menu_item(conn, vendor: str, item_name: str) -> int:
    row = conn.execute(
        "SELECT id FROM menu_items WHERE vendor=? AND item_name=?",
        (vendor, item_name),
    ).fetchone()
    if row:
        return row[0]
    cur = conn.execute(
        "INSERT INTO menu_items (vendor, item_name) VALUES (?, ?)",
        (vendor, item_name),
    )
    return cur.lastrowid


def as_file_url(path: Path) -> str:
    return path.resolve().as_uri()


def compute_file_hash(data: bytes, algorithm="sha256") -> str:
    """Return hash of the file content."""
    hash_func = hashlib.new(algorithm)
    hash_func.update(data)

    return hash_func.hexdigest()


def insert_page_from_html(
    conn,
    run_id: int,
    source_url: str,
    html_bytes: bytes,
    http_status: int,
    fetched_at: Optional[str] = None,
) -> int:
    if fetched_at is None:
        fetched_at = iso_utc(datetime.now(timezone.utc))

    file_hash = compute_file_hash(html_bytes)
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO pages
          (run_id, source_url, fetched_at, content_blob, http_status, content_sha256)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (run_id, source_url, fetched_at, html_bytes, http_status, file_hash),
    )
    if cur.lastrowid:
        return cur.lastrowid

    # Return the ID if the page already exists
    row = conn.execute(
        "SELECT id FROM pages WHERE source_url=? AND fetched_at=?",
        (source_url, fetched_at),
    ).fetchone()
    return row[0]


def fetch_page_html(conn: sqlite3.Connection, page_id: int) -> bytes:
    """Return the raw HTML blob for a given page_id."""
    row = conn.execute(
        "SELECT content_blob FROM pages WHERE id=?",
        (page_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"page_id {page_id} not found")

    return row[0]


def insert_price(
    conn, menu_item_id: int, page_id: int, price_eur: float, currency="EUR"
):
    cents = int(round(price_eur * 100))
    now = iso_utc(datetime.now(timezone.utc))
    conn.execute(
        """
        INSERT INTO prices (menu_item_id, page_id, price_cents, currency, observed_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (menu_item_id, page_id, cents, currency, now),
    )


