PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS scrape_runs (
  id INTEGER PRIMARY KEY,
  started_at TEXT NOT NULL,   -- ISO-8601 UTC
  finished_at TEXT,
  status TEXT NOT NULL CHECK(status IN ('ok','fail'))
);

CREATE TABLE IF NOT EXISTS pages (
  id INTEGER PRIMARY KEY,
  run_id INTEGER NOT NULL REFERENCES scrape_runs(id),
  source_url TEXT NOT NULL,     -- e.g., file://delivery_xxx.html
  fetched_at TEXT NOT NULL,     -- ISO-8601 UTC (use file mtime)
  content_blob BLOB NOT NULL,
  http_status INTEGER,
  content_sha256 TEXT NOT NULL,
  UNIQUE(run_id, source_url)
);

CREATE TABLE IF NOT EXISTS menu_items (
  id INTEGER PRIMARY KEY,
  vendor TEXT NOT NULL,
  item_name TEXT NOT NULL,
  item_slug TEXT NOT NULL,
  UNIQUE (vendor, item_name)
);

CREATE TABLE IF NOT EXISTS prices (
  id INTEGER PRIMARY KEY,
  menu_item_id INTEGER NOT NULL REFERENCES menu_items(id),
  page_id INTEGER REFERENCES pages(id),
  price_cents INTEGER NOT NULL,
  currency TEXT NOT NULL DEFAULT 'EUR',
  observed_at TEXT NOT NULL     -- ISO-8601 UTC
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_prices_item_time ON prices(menu_item_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_menu_items_vendor_item ON menu_items(vendor, item_name);
