import sqlite3
from typing import List

def get_average_latest_prices(conn: sqlite3.Connection):
    sql = """
    WITH latest_prices AS (
        SELECT
            m.item_name,
            p.price_cents,
            p.currency,
            p.observed_at
        FROM menu_items m
        JOIN prices p ON p.menu_item_id = m.id
        WHERE p.observed_at = (
                SELECT MAX(observed_at)
                FROM prices p2
                WHERE p2.menu_item_id = m.id
        )
    )
    SELECT
        item_name,
        AVG(price_cents) AS avg_price_cents,
        currency,
        MAX(observed_at) AS latest_observed_at
    FROM latest_prices
    GROUP BY item_name, currency
    ORDER BY item_name;
    """
    rows = conn.execute(sql).fetchall()
    return [dict(r) for r in rows]


def get_price_history_for_item(conn: sqlite3.Connection, item_name: str):
    sql = """
    SELECT
        DATE(observed_at) AS day,
        AVG(price_cents) AS avg_price_cents,
        currency
    FROM prices p
    JOIN menu_items m ON m.id = p.menu_item_id
    WHERE m.item_name = ?
    GROUP BY day, currency
    ORDER BY day ASC;
    """
    rows = conn.execute(sql, (item_name,)).fetchall()
    return [dict(r) for r in rows]


def list_item_names(conn: sqlite3.Connection) -> List[str]:
    """
    Utility: return a sorted list of all unique item_name values.
    Useful for navigation and building URLs.
    """
    sql = """
    SELECT DISTINCT item_name
    FROM menu_items
    ORDER BY item_name;
    """

    rows = conn.execute(sql).fetchall()
    return [r["item_name"] for r in rows]


def get_item_summary(conn, item_name: str):
    sql = """
    WITH latest_prices AS (
        SELECT
            m.vendor,
            m.item_name,
            p.price_cents,
            p.currency,
            p.observed_at
        FROM menu_items m
        JOIN prices p ON p.menu_item_id = m.id
        WHERE m.item_name = ?
        AND p.observed_at = (
            SELECT MAX(p2.observed_at)
            FROM prices p2
            WHERE p2.menu_item_id = m.id
        )
    )
    SELECT 
        COUNT(*) AS count_prices,
        AVG(price_cents) AS avg_price_cents
    FROM latest_prices;
    """
    row = conn.execute(sql, (item_name,)).fetchone()
    return {
        "count_prices": row["count_prices"],
        "avg_price_cents": row["avg_price_cents"]
    }

