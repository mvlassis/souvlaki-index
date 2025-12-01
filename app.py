from flask import Flask, render_template, abort
from pathlib import Path
import json
import logging
import sqlite3

from db_utils import connect
import queries

DB_PATH = Path("db/data.db")

LOG_FORMAT = "%(levelname)s:%(name)s: %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def format_price_eur(amount: float) -> str:
    """
    Format price as European style: '3,50 €'
    amount is in euros (float).
    """
    return f"{amount:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + " €"


app = Flask(__name__)
app.jinja_env.filters["eur"] = format_price_eur


def get_conn() -> sqlite3.Connection:
    """Create a new SQLite connection per request."""
    conn = connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/")
def index():
    conn = get_conn()
    items = queries.get_average_latest_prices(conn)
    item_slugs = queries.list_item_slugs(conn)
    conn.close()

    return render_template("index.html", items=items, item_slugs=item_slugs)


@app.route("/item/<item_slug>")
def item_page(item_slug):
    conn = get_conn()
    item = queries.get_item_by_slug(conn, item_slug)
    if item is None:
        conn.close()
        abort(404)

    item_name = item["item_name"]
        
    history = queries.get_price_history_for_item(conn, item_name)
    summary = queries.get_item_summary(conn, item_name)
    history_json = json.dumps(history)
    item_slugs = queries.list_item_slugs(conn)
    conn.close()

    return render_template(
        "item.html",
        item_name=item_name,
        number_prices=summary["count_prices"],
        average_price=summary["avg_price_cents"] / 100,
        history_json=history_json,
        item_slugs=item_slugs,
    )


@app.route("/about")
def about_page():
    return render_template("about.html")


if __name__ == "__main__":
    app.run(debug=True)
