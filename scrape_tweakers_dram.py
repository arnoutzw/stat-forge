#!/usr/bin/env python3
"""
Tweakers.net DRAM Price Scraper for StatForge
==============================================
Scrapes internal memory prices from Tweakers Pricewatch and fetches
per-product price history via their AJAX API. Outputs CSV compatible
with StatForge's DRAM dashboard.

Data source: https://tweakers.net/geheugen-intern/vergelijken/
Output:      dram_prices.csv  (date,type,pricePerGB,kitPrice,capacity)

Usage:
    python3 scrape_tweakers_dram.py [--output dram_prices.csv] [--max-pages 3]

Rate limiting: 1.5s between page requests, 1.0s between API calls.
Total runtime: ~30-60 seconds.
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# ─── Config ──────────────────────────────────────────────────────────────────

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

BASE_URL = "https://tweakers.net"
CATEGORY_SLUG = "geheugen-intern"
# Browse URL — using popularity sort ensures DDR4/DDR5 appear first
BROWSE_URL = f"{BASE_URL}/{CATEGORY_SLUG}/vergelijken/"
# DDR type spec filter (specId=137): 2909=DDR4, 8879=DDR5
DDR_FILTERS = {
    "ddr4": "h137=1&f137=2909",
    "ddr5": "h137=1&f137=8879",
}
PRICE_HISTORY_URL = f"{BASE_URL}/ajax/price_chart/{{product_id}}/nl/"

PAGE_DELAY = 1.5       # seconds between category page requests
API_DELAY = 1.0        # seconds between price history API calls
REQUEST_TIMEOUT = 15    # seconds
MAX_RETRIES = 3

# Benchmark capacities we want to track (total kit GB)
BENCHMARK_CONFIGS = [
    {"ddr": "ddr4", "capacity": 16, "label": "DDR4 16GB"},
    {"ddr": "ddr4", "capacity": 32, "label": "DDR4 32GB"},
    {"ddr": "ddr5", "capacity": 32, "label": "DDR5 32GB"},
    {"ddr": "ddr5", "capacity": 64, "label": "DDR5 64GB"},
]

# Known-good product IDs with long price history (fallback if browse can't find them)
PREFERRED_PRODUCTS = {
    # Corsair Vengeance RGB DDR5-6000 32GB (2x16GB) — 1168+ data points from 2023-01
    ("ddr5", 32): 1893878,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tweakers-scraper")


# ─── Session Initialization ──────────────────────────────────────────────────

def init_session():
    """Create a requests session with browser-like headers and consent cookies."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
    })

    # Hit the homepage to obtain session/consent cookies
    try:
        log.info("Initializing session (fetching cookies)...")
        resp = session.get(BASE_URL, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        log.info(f"Session initialized — got {len(session.cookies)} cookies")
    except requests.RequestException as e:
        log.warning(f"Could not init session cookies: {e}")

    return session


# ─── Product Discovery ────────────────────────────────────────────────────────

def browse_products(session, max_pages=3):
    """
    Browse the geheugen-intern category to discover DRAM products.
    Does separate filtered fetches for DDR4 and DDR5 to ensure we find both.
    Returns list of dicts: {product_id, name, spec, price, url}
    """
    products = []

    for ddr_type, filter_params in DDR_FILTERS.items():
        log.info(f"Browsing {ddr_type.upper()} products...")
        for page in range(1, max_pages + 1):
            url = f"{BROWSE_URL}?page={page}&orderField=popularity&{filter_params}"
            log.info(f"  Page {page}/{max_pages}: {url}")

            for attempt in range(MAX_RETRIES):
                try:
                    resp = session.get(url, timeout=REQUEST_TIMEOUT)
                    if resp.status_code == 429:
                        wait = 5 * (2 ** attempt)
                        log.warning(f"Rate limited (429), waiting {wait}s...")
                        time.sleep(wait)
                        continue
                    resp.raise_for_status()
                    break
                except requests.RequestException as e:
                    if attempt < MAX_RETRIES - 1:
                        log.warning(f"Retry {attempt + 1}: {e}")
                        time.sleep(3)
                    else:
                        log.error(f"Failed to fetch page {page}: {e}")
                        break

            soup = BeautifulSoup(resp.text, "html.parser")
            page_products = _parse_product_listing(soup)

            if not page_products:
                log.info(f"  No products found on page {page}, stopping")
                break

            products.extend(page_products)
            log.info(f"  Found {len(page_products)} products (total: {len(products)})")

            if page < max_pages:
                time.sleep(PAGE_DELAY)

        time.sleep(PAGE_DELAY)

    return products


def _parse_product_listing(soup):
    """
    Parse product listing HTML from Tweakers vergelijken page.

    Tweakers structure (verified 2026-03):
      <ul class="item-listing">
        <li>
          <div class="item-image">...</div>
          <div class="item-body">
            <a href="https://tweakers.net/pricewatch/{id}/{slug}.html">Product Name</a>
            <span class="spec-line">32GB DDR5 @ 6000MT/s, kit van 2</span>
          </div>
          <div class="item-price new-item-price">
            <p class="product-price price">vanaf€ 89,95bij 12 winkels</p>
          </div>
        </li>
      </ul>
    """
    products = []
    seen_ids = set()

    item_list = soup.select_one("ul.item-listing")
    if not item_list:
        log.warning("Could not find <ul class='item-listing'> — page structure changed?")
        return products

    items = item_list.find_all("li", recursive=False)
    for li in items:
        # Extract product ID + name from the main link
        link = li.select_one("a[href*='/pricewatch/']")
        if not link:
            continue

        href = link.get("href", "")
        id_match = re.search(r"/pricewatch/(\d+)/", href)
        if not id_match:
            continue

        product_id = int(id_match.group(1))
        if product_id in seen_ids:
            continue
        seen_ids.add(product_id)

        # Product name is the text of the second <a> (first is image wrapper)
        name_links = li.select("div.item-body a[href*='/pricewatch/']")
        name = ""
        for nl in name_links:
            text = nl.get_text(strip=True)
            if len(text) > len(name):
                name = text

        if not name or len(name) < 3:
            name = link.get_text(strip=True)

        # Extract spec line (e.g. "32GB DDR5 @ 6000MT/s, kit van 2")
        spec = ""
        spec_el = li.select_one(".spec-line, .specline, p.ellipsis.specline")
        if spec_el:
            spec = spec_el.get_text(strip=True)

        # Extract price from .product-price
        price = _extract_price(li)

        products.append({
            "product_id": product_id,
            "name": name,
            "spec": spec,
            "price": price,
            "url": href,
        })

    return products


def _extract_price(element):
    """Extract EUR price from a Tweakers product listing <li>."""
    # Look for .product-price element
    price_el = element.select_one("p.product-price, .item-price")
    if price_el:
        text = price_el.get_text(strip=True)
    else:
        text = element.get_text(strip=True)

    # Tweakers uses: "vanaf€ 89,95bij 12 winkels" or "€ 3,57"
    # Also: "€ 0,681" (sub-euro) or "399,-"
    patterns = [
        r"€\s*(\d{1,5}),(\d{1,3})",   # € 89,95  or  € 0,681
        r"€\s*(\d{1,5}),-",            # € 399,-
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            groups = match.groups()
            if len(groups) == 2:
                cents = groups[1]
                if len(cents) == 3:
                    # Sub-euro precision: "0,681" -> 0.681
                    return float(f"{groups[0]}.{cents}")
                return float(f"{groups[0]}.{cents}")
            elif len(groups) == 1:
                return float(groups[0])

    return None


# ─── Product Classification ──────────────────────────────────────────────────

def classify_product(name, spec=""):
    """
    Extract DDR type, capacity, and speed from a product name + spec line.

    Product name examples:
        "Corsair Vengeance DDR5-5600 32GB (2x16GB)"
        "Kingston FURY Beast DDR4-3200 16GB (2x8GB)"
    Tweakers spec line examples:
        "32GB DDR5 @ 6000MT/s, kit van 2"
        "16GB DDR4 @ 3200MT/s, kit van 2"
    """
    combined = f"{name} {spec}"
    result = {"ddr": None, "capacity": None, "speed": None, "kit": None}

    # DDR type
    ddr_match = re.search(r"DDR(\d)", combined, re.IGNORECASE)
    if ddr_match:
        result["ddr"] = f"ddr{ddr_match.group(1)}"

    # Speed from spec line: "@ 6000MT/s" or from name: "DDR5-5600"
    speed_match = re.search(r"@\s*(\d{3,5})\s*MT/s", combined, re.IGNORECASE)
    if not speed_match:
        speed_match = re.search(r"DDR\d[- ](\d{3,5})", combined, re.IGNORECASE)
    if speed_match:
        result["speed"] = int(speed_match.group(1))

    # Capacity from spec line: "32GB DDR5" (capacity before DDR type)
    cap_match = re.search(r"(\d{1,4})\s*GB\s+DDR", combined, re.IGNORECASE)
    if not cap_match:
        # Fallback: standalone GB in name (before parenthetical kit config)
        cap_match = re.search(r"(?<!\dx)(\d{1,4})\s*GB", combined, re.IGNORECASE)
    if cap_match:
        result["capacity"] = int(cap_match.group(1))

    # Kit count from spec: "kit van 2" or from name: "(2x16GB)"
    kit_match = re.search(r"kit\s+van\s+(\d)", combined, re.IGNORECASE)
    if kit_match:
        result["kit"] = int(kit_match.group(1))
    else:
        kit_match = re.search(r"\((\d)x(\d+)\s*GB\)", combined, re.IGNORECASE)
        if kit_match:
            result["kit"] = int(kit_match.group(1))
            kit_total = int(kit_match.group(1)) * int(kit_match.group(2))
            if result["capacity"] is None:
                result["capacity"] = kit_total

    return result


# ─── Benchmark Selection ──────────────────────────────────────────────────────

def select_benchmarks(products):
    """
    For each benchmark config (DDR4 16GB, DDR4 32GB, DDR5 32GB, DDR5 64GB),
    find the best product: prefer known-good IDs with long history, else cheapest.
    """
    selected = []

    for config in BENCHMARK_CONFIGS:
        key = (config["ddr"], config["capacity"])

        # Check if we have a preferred product ID for this config
        preferred_id = PREFERRED_PRODUCTS.get(key)

        candidates = []
        for p in products:
            info = classify_product(p["name"], p.get("spec", ""))
            if info["ddr"] != config["ddr"]:
                continue
            if info["capacity"] != config["capacity"]:
                continue
            candidates.append({**p, "info": info})

        if not candidates and not preferred_id:
            log.warning(f"No candidates for {config['label']}")
            continue

        # Prefer the known-good product if found in candidates
        best = None
        if preferred_id:
            for c in candidates:
                if c["product_id"] == preferred_id:
                    best = c
                    break
            if not best:
                # Use preferred ID directly even if not in browse results
                log.info(f"  Using preferred product ID {preferred_id} for {config['label']}")
                best = {
                    "product_id": preferred_id,
                    "name": f"Preferred benchmark ({config['label']})",
                    "price": None,
                }

        if not best:
            # Sort by price (cheapest first)
            priced = [c for c in candidates if c["price"] is not None]
            unpriced = [c for c in candidates if c["price"] is None]
            if priced:
                best = sorted(priced, key=lambda c: c["price"])[0]
            elif unpriced:
                best = unpriced[0]
            else:
                continue

        selected.append({
            "product_id": best["product_id"],
            "name": best["name"],
            "price": best.get("price"),
            "ddr": config["ddr"],
            "capacity": config["capacity"],
            "label": config["label"],
        })
        log.info(f"  Benchmark {config['label']}: {best['name']} (ID: {best['product_id']}, €{best.get('price')})")

    return selected


# ─── Price History API ────────────────────────────────────────────────────────

def get_price_history(session, product_id):
    """
    Fetch historical price data for a product via Tweakers AJAX API.
    Returns list of (date_str, min_price, avg_price) tuples.
    """
    url = PRICE_HISTORY_URL.format(product_id=product_id)
    log.info(f"  Fetching price history for product {product_id}...")

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, headers={
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json, text/javascript, */*; q=0.01",
            })
            if resp.status_code == 429:
                wait = 5 * (2 ** attempt)
                log.warning(f"Rate limited on history API, waiting {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 404:
                log.warning(f"  Product {product_id} history not found (404)")
                return []
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                log.warning(f"  Retry {attempt + 1}: {e}")
                time.sleep(3)
            else:
                log.error(f"  Failed to fetch history for {product_id}: {e}")
                return []

    try:
        data = resp.json()
    except (json.JSONDecodeError, ValueError):
        log.error(f"  Invalid JSON response for product {product_id}")
        return []

    # Parse the dataset
    dataset = data.get("dataset", {})
    source = dataset.get("source", [])
    dimensions = dataset.get("dimensions", [])

    if not source:
        log.warning(f"  Empty price history for product {product_id}")
        return []

    # Determine column indices
    ts_idx = 0
    min_idx = 1
    avg_idx = 2 if len(dimensions) > 2 else 1

    history = []
    for row in source:
        if len(row) < 2:
            continue
        date_str = str(row[ts_idx])
        min_price = row[min_idx] if row[min_idx] is not None else None
        avg_price = row[avg_idx] if len(row) > avg_idx and row[avg_idx] is not None else min_price

        if min_price is not None:
            history.append((date_str, float(min_price), float(avg_price or min_price)))

    log.info(f"  Got {len(history)} data points for product {product_id}")
    return history


# ─── Aggregation ──────────────────────────────────────────────────────────────

def aggregate_monthly(history, capacity):
    """
    Aggregate daily price data to monthly data points.
    Takes the last data point of each month.
    Returns list of dicts: {date, pricePerGB, kitPrice, capacity}
    """
    by_month = defaultdict(list)

    for date_str, min_price, avg_price in history:
        try:
            # Handle different date formats
            if "T" in date_str:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            else:
                dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
            month_key = dt.strftime("%Y-%m")
            by_month[month_key].append({
                "date": dt,
                "min_price": min_price,
                "avg_price": avg_price,
            })
        except (ValueError, TypeError):
            continue

    monthly = []
    for month_key in sorted(by_month.keys()):
        points = by_month[month_key]
        # Use the last data point of the month (most recent)
        last_point = sorted(points, key=lambda p: p["date"])[-1]
        price = last_point["min_price"]
        price_per_gb = round(price / capacity, 2) if capacity > 0 else 0

        monthly.append({
            "date": month_key,
            "pricePerGB": price_per_gb,
            "kitPrice": round(price),
            "capacity": capacity,
        })

    return monthly


# ─── CSV Output ───────────────────────────────────────────────────────────────

def write_csv(output_path, all_series):
    """
    Write aggregated data to CSV in StatForge format.
    all_series: list of (ddr_type, monthly_data) tuples
    """
    rows = []
    for ddr_type, monthly in all_series:
        for point in monthly:
            rows.append({
                "date": point["date"],
                "type": ddr_type,
                "pricePerGB": f"{point['pricePerGB']:.2f}",
                "kitPrice": str(point["kitPrice"]),
                "capacity": str(point["capacity"]),
            })

    # Sort by date, then type
    rows.sort(key=lambda r: (r["date"], r["type"]))

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "type", "pricePerGB", "kitPrice", "capacity"])
        writer.writeheader()
        writer.writerows(rows)

    log.info(f"Wrote {len(rows)} rows to {output_path}")
    return len(rows)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scrape DRAM prices from Tweakers.net")
    parser.add_argument("--output", "-o", default=None, help="Output CSV path (default: dram_prices.csv next to this script)")
    parser.add_argument("--max-pages", type=int, default=3, help="Max category pages to browse (default: 3, ~120 products)")
    parser.add_argument("--dry-run", action="store_true", help="Discover products but don't fetch price history")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)

    # Default output path: next to this script
    if args.output is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        args.output = os.path.join(script_dir, "dram_prices.csv")

    log.info("=" * 60)
    log.info("StatForge — Tweakers.net DRAM Price Scraper")
    log.info("=" * 60)

    # 1. Initialize session
    session = init_session()

    # 2. Browse products
    log.info(f"\n[1/4] Browsing {CATEGORY_SLUG} (max {args.max_pages} pages)...")
    products = browse_products(session, max_pages=args.max_pages)

    if not products:
        log.error("No products found! Tweakers page structure may have changed.")
        log.info("Attempting fallback: using search API...")
        products = _search_fallback(session)

    if not products:
        log.error("No products found via any method. Exiting.")
        sys.exit(1)

    log.info(f"Discovered {len(products)} products total")

    # 3. Classify & select benchmarks
    log.info("\n[2/4] Selecting benchmark products...")
    benchmarks = select_benchmarks(products)

    if not benchmarks:
        log.error("No benchmark products could be selected. Exiting.")
        sys.exit(1)

    log.info(f"Selected {len(benchmarks)} benchmark products")

    if args.dry_run:
        log.info("\n[DRY RUN] Skipping price history fetch")
        for b in benchmarks:
            log.info(f"  Would fetch: {b['label']} — {b['name']} (ID: {b['product_id']})")
        return

    # 4. Fetch price history for each benchmark
    log.info("\n[3/4] Fetching price history...")
    all_series = []

    for benchmark in benchmarks:
        time.sleep(API_DELAY)
        history = get_price_history(session, benchmark["product_id"])

        if not history:
            log.warning(f"  No history for {benchmark['label']}, skipping")
            continue

        monthly = aggregate_monthly(history, benchmark["capacity"])
        all_series.append((benchmark["ddr"], monthly))
        log.info(f"  {benchmark['label']}: {len(monthly)} monthly data points")

    if not all_series:
        log.error("No price history retrieved for any benchmark. Exiting.")
        sys.exit(1)

    # 5. Merge series of the same DDR type (pick best capacity)
    # For the same DDR type, merge by taking the more commonly tracked capacity
    merged = _merge_series(all_series)

    # 6. Write CSV
    log.info(f"\n[4/4] Writing CSV to {args.output}...")
    count = write_csv(args.output, merged)

    log.info(f"\nDone! {count} data points written to {args.output}")
    log.info(f"Data spans: {merged[0][1][0]['date'] if merged and merged[0][1] else '?'} → {merged[0][1][-1]['date'] if merged and merged[0][1] else '?'}")


def _merge_series(all_series):
    """
    If multiple series exist for the same DDR type, pick the one with the most
    data points for a clean, consistent price line. No mixing of capacities.
    """
    # Group by (ddr_type, capacity)
    by_key = defaultdict(list)
    for ddr_type, monthly in all_series:
        cap = monthly[0]["capacity"] if monthly else 0
        by_key[(ddr_type, cap)] = monthly

    # For each DDR type, pick the series with the most data points
    best_by_type = {}
    for (ddr_type, cap), monthly in by_key.items():
        if ddr_type not in best_by_type or len(monthly) > len(best_by_type[ddr_type]):
            best_by_type[ddr_type] = monthly

    result = []
    for ddr_type in sorted(best_by_type.keys()):
        monthly = sorted(best_by_type[ddr_type], key=lambda p: p["date"])
        result.append((ddr_type, monthly))

    return result


def _search_fallback(session):
    """Fallback: use Tweakers search API to find DRAM products."""
    products = []
    keywords = ["DDR4 16GB", "DDR4 32GB", "DDR5 32GB", "DDR5 64GB"]

    for keyword in keywords:
        url = f"{BASE_URL}/ajax/zoeken/pricewatch/?keyword={keyword}"
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, headers={
                "X-Requested-With": "XMLHttpRequest",
            })
            if resp.ok:
                data = resp.json()
                for item in data if isinstance(data, list) else data.get("results", []):
                    pid = item.get("id") or item.get("product_id")
                    name = item.get("name") or item.get("label") or ""
                    price = item.get("price") or item.get("min_price")
                    if pid:
                        products.append({
                            "product_id": int(pid),
                            "name": name,
                            "price": float(price) if price else None,
                            "url": "",
                        })
            time.sleep(API_DELAY)
        except Exception as e:
            log.warning(f"Search fallback failed for '{keyword}': {e}")

    return products


if __name__ == "__main__":
    main()
