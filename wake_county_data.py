#!/usr/bin/env python3
"""
Wake County Parcel Intelligence System
Downloads, stores, and serves Wake County real estate data locally.

Usage:
  python wake_county_data.py refresh        # Download latest data
  python wake_county_data.py serve          # Start local API server (port 7474)
  python wake_county_data.py query <PIN>    # Query a parcel by PIN
  python wake_county_data.py start          # Refresh + serve (recommended)

Requirements:
  pip install requests pandas openpyxl flask flask-cors --break-system-packages
"""

import sys
import os
import json
import sqlite3
import hashlib
import datetime
import time
import re
import argparse
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────────
DATA_DIR   = Path.home() / ".wake_county_data"
DB_PATH    = DATA_DIR / "parcels.db"
LOG_PATH   = DATA_DIR / "refresh.log"
API_PORT   = 7474

# Wake County data sources
SOURCES = {
    "realestate": {
        "url_template": "https://services.wake.gov/realestate/RealEstData{date}.xlsx",
        "fallback_url": "https://services.wake.gov/realestate/",
        "description": "Daily parcel / ownership / valuation data"
    },
    "sales": {
        "url_template": "https://services.wake.gov/realestate/RealEstDataQualifiedSales.xlsx",
        "description": "Qualified sales last 24 months"
    }
}

# Column mappings from Wake County data to our schema
COLUMN_MAP = {
    # Real estate file columns (Wake County names → our names)
    "REID":          "reid",
    "PIN_NUM":       "pin",
    "ADDR1":         "address",
    "CITY_NAME":     "city",
    "ZIP_CODE":      "zip",
    "OWNER":         "owner",
    "DEED_ACRES":    "acres",
    "BILLING_CLASS": "billing_class",
    "LAND_CLASS":    "land_class",
    "LAND_VALUE":    "land_value",
    "BLDG_VALUE":    "building_value",
    "TOTAL_VALUE":   "total_value",
    "PREV_TOTAL":    "prev_total_value",
    "TOT_SALE_PRICE":"last_sale_price",
    "SALE_DATE":     "last_sale_date",
    "ZONING":        "zoning",
    "TOWNSHIP":      "township",
    "PLANNING_JURIS":"planning_juris",
    "EXEMPT_STATUS": "exempt_status",
    "DEED_BOOK":     "deed_book",
    "DEED_PAGE":     "deed_page",
    "YEAR_BUILT":    "year_built",
    "TYPE_AND_USE":  "type_and_use",
    "HEATED_AREA":   "heated_area",
    "PHYSICAL_CITY": "physical_city",
}

DATA_DIR.mkdir(parents=True, exist_ok=True)


# ── Database ─────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS parcels (
            pin           TEXT PRIMARY KEY,
            reid          TEXT,
            address       TEXT,
            city          TEXT,
            zip           TEXT,
            owner         TEXT,
            acres         REAL,
            billing_class TEXT,
            land_class    TEXT,
            land_value    REAL,
            building_value REAL,
            total_value   REAL,
            prev_total_value REAL,
            last_sale_price REAL,
            last_sale_date  TEXT,
            zoning        TEXT,
            township      TEXT,
            planning_juris TEXT,
            exempt_status TEXT,
            deed_book     TEXT,
            deed_page     TEXT,
            year_built    TEXT,
            type_and_use  TEXT,
            heated_area   REAL,
            physical_city TEXT,
            red_flags     TEXT,
            last_updated  TEXT,
            data_hash     TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_pin    ON parcels(pin);
        CREATE INDEX IF NOT EXISTS idx_reid   ON parcels(reid);
        CREATE INDEX IF NOT EXISTS idx_addr   ON parcels(address);
        CREATE INDEX IF NOT EXISTS idx_owner  ON parcels(owner);

        CREATE TABLE IF NOT EXISTS changes (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            pin         TEXT,
            field       TEXT,
            old_value   TEXT,
            new_value   TEXT,
            changed_at  TEXT
        );

        CREATE TABLE IF NOT EXISTS sales (
            pin           TEXT,
            sale_date     TEXT,
            sale_price    REAL,
            buyer         TEXT,
            seller        TEXT,
            deed_book     TEXT,
            deed_page     TEXT,
            valid_sale    TEXT,
            PRIMARY KEY (pin, sale_date, sale_price)
        );

        CREATE TABLE IF NOT EXISTS refresh_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at      TEXT,
            parcels_total   INTEGER,
            parcels_new     INTEGER,
            parcels_changed INTEGER,
            source_file TEXT,
            status      TEXT,
            notes       TEXT
        );
    """)
    conn.commit()
    conn.close()


# ── Red Flag Analysis ─────────────────────────────────────────────────────────
def analyze_red_flags(row: dict) -> list:
    flags = []

    # Tax exempt / government owned
    if row.get("exempt_status") and str(row["exempt_status"]).strip():
        flags.append({"type": "exempt", "severity": "info",
                      "msg": f"Exempt parcel: {row['exempt_status']} — may not be privately purchasable"})

    # LLC ownership (harder to negotiate, may signal assemblage)
    owner = str(row.get("owner", ""))
    if any(x in owner.upper() for x in ["LLC", "LP ", "LLP", "INC", "CORP", "TRUST"]):
        flags.append({"type": "entity_owner", "severity": "info",
                      "msg": f"Entity ownership ({owner}) — may need extra research on decision maker"})

    # Very small parcel
    acres = float(row.get("acres") or 0)
    if 0 < acres < 0.5:
        flags.append({"type": "small_parcel", "severity": "warning",
                      "msg": f"Small parcel ({acres:.2f} ac) — may not meet minimum lot size requirements"})

    # High price appreciation
    total = float(row.get("total_value") or 0)
    prev  = float(row.get("prev_total_value") or 0)
    if prev > 0 and total > 0:
        pct = (total - prev) / prev * 100
        if pct > 30:
            flags.append({"type": "rapid_appreciation", "severity": "warning",
                          "msg": f"Value jumped {pct:.0f}% since last assessment — verify with recent comps"})

    # Last sale price vs assessed value gap
    sale = float(row.get("last_sale_price") or 0)
    if sale > 0 and total > 0:
        ratio = sale / total
        if ratio < 0.5:
            flags.append({"type": "below_assessed", "severity": "info",
                          "msg": f"Last sale (${sale:,.0f}) was {ratio:.0%} of assessed value — may indicate distressed sale or intra-family transfer"})
        elif ratio > 2.0:
            flags.append({"type": "above_assessed", "severity": "info",
                          "msg": f"Last sale (${sale:,.0f}) was {ratio:.0%} of assessed value — strong market demand"})

    # Watershed keywords in township
    township = str(row.get("township", "")).upper()
    if any(x in township for x in ["SWIFT CREEK", "FALLS LAKE", "JORDAN LAKE", "NEUSE"]):
        flags.append({"type": "watershed", "severity": "warning",
                      "msg": f"Watershed area ({township}) — density/impervious restrictions likely apply"})

    # Land class
    land_class = str(row.get("land_class", "")).upper()
    if "VACANT" in land_class:
        flags.append({"type": "vacant", "severity": "ok",
                      "msg": "Vacant land — no demolition cost or tenant displacement concerns"})

    return flags


# ── Download ──────────────────────────────────────────────────────────────────
def get_today_filename():
    """Wake County names files RealEstDataMMDDYYYY.xlsx"""
    d = datetime.date.today()
    return f"RealEstData{d.strftime('%m%d%Y')}.xlsx"


def download_realestate_data():
    try:
        import requests
    except ImportError:
        print("ERROR: Install requests: pip install requests --break-system-packages")
        sys.exit(1)

    # Check if any recent local file already exists (use most recent)
    for days_back in range(8):
        d = datetime.date.today() - datetime.timedelta(days=days_back)
        fname = f"RealEstData{d.strftime('%m%d%Y')}.xlsx"
        local = DATA_DIR / fname
        if local.exists():
            print(f"  ✓ Using local file: {fname} ({days_back} days old)")
            return local

    # Try downloading up to 7 days back (handles weekends/holidays)
    tried = []
    for days_back in range(8):
        d = datetime.date.today() - datetime.timedelta(days=days_back)
        fname = f"RealEstData{d.strftime('%m%d%Y')}.xlsx"
        url = f"https://services.wake.gov/realestate/{fname}"
        local = DATA_DIR / fname
        tried.append(url)
        print(f"  → Trying {url}")
        try:
            resp = requests.get(url, timeout=120, stream=True)
            if resp.status_code == 200:
                with open(local, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                size_mb = local.stat().st_size / 1_000_000
                print(f"  ✓ Downloaded: {fname} ({size_mb:.1f} MB)")
                return local
        except Exception as e:
            print(f"  ⚠ Error: {e}")
            continue

    raise RuntimeError(
        f"Could not download Wake County data after trying 8 dates.\n"
        f"Last tried: {tried[-1]}\n"
        f"Check https://services.wake.gov/realestate/ manually for available files."
    )


def download_sales_data():
    try:
        import requests
    except ImportError:
        return None

    url = "https://services.wake.gov/realestate/RealEstDataQualifiedSales.xlsx"
    local_path = DATA_DIR / "QualifiedSales.xlsx"
    today_str = datetime.date.today().isoformat()

    # Re-download weekly
    if local_path.exists():
        age = datetime.date.today() - datetime.date.fromtimestamp(local_path.stat().st_mtime)
        if age.days < 7:
            print(f"  ✓ Sales data fresh ({age.days} days old)")
            return local_path

    print(f"  → Downloading qualified sales data...")
    try:
        resp = requests.get(url, timeout=60, stream=True)
        if resp.status_code == 200:
            with open(local_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"  ✓ Downloaded sales data")
            return local_path
    except Exception as e:
        print(f"  ⚠ Sales data unavailable: {e}")
    return None


# ── Import ────────────────────────────────────────────────────────────────────
def import_parcels(filepath: Path):
    try:
        import pandas as pd
    except ImportError:
        print("ERROR: Install pandas: pip install pandas openpyxl --break-system-packages")
        sys.exit(1)

    print(f"  → Reading {filepath.name}...")
    df = pd.read_excel(filepath, dtype=str)
    df = df.fillna("")

    # Normalize column names (strip spaces, uppercase)
    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]

    # Map known columns
    mapped = {}
    for src, dst in COLUMN_MAP.items():
        if src in df.columns:
            mapped[dst] = df[src]

    if "pin" not in mapped:
        # Try to find PIN column
        for col in df.columns:
            if "PIN" in col and "NUM" in col:
                mapped["pin"] = df[col]
                break

    if "pin" not in mapped:
        raise ValueError(f"Could not find PIN column. Available: {list(df.columns)[:20]}")

    data_df = pd.DataFrame(mapped)
    data_df = data_df[data_df["pin"].str.strip() != ""]

    print(f"  → Processing {len(data_df):,} parcels...")

    conn = get_db()
    new_count = 0
    changed_count = 0
    now = datetime.datetime.now().isoformat()

    for _, row in data_df.iterrows():
        pin = str(row.get("pin", "")).strip()
        if not pin:
            continue

        r = {k: str(v).strip() if v is not None else "" for k, v in row.items()}

        # Numeric cleanup
        for num_field in ["acres", "land_value", "building_value", "total_value",
                           "prev_total_value", "last_sale_price", "heated_area"]:
            raw = r.get(num_field, "")
            try:
                r[num_field] = float(raw.replace(",", "").replace("$", "")) if raw else None
            except ValueError:
                r[num_field] = None

        # Red flags
        flags = analyze_red_flags(r)
        r["red_flags"] = json.dumps(flags)
        r["last_updated"] = now

        # Hash for change detection
        hash_src = json.dumps({k: r.get(k) for k in [
            "owner", "total_value", "land_value", "zoning", "last_sale_price"
        ]}, sort_keys=True)
        r["data_hash"] = hashlib.md5(hash_src.encode()).hexdigest()

        # Check existing
        existing = conn.execute("SELECT * FROM parcels WHERE pin=?", (pin,)).fetchone()

        if existing is None:
            # New parcel
            cols = [c for c in r if c in [
                "pin","reid","address","city","zip","owner","acres","billing_class",
                "land_class","land_value","building_value","total_value","prev_total_value",
                "last_sale_price","last_sale_date","zoning","township","planning_juris",
                "exempt_status","deed_book","deed_page","year_built","type_and_use",
                "heated_area","physical_city","red_flags","last_updated","data_hash"
            ]]
            vals = [r.get(c) for c in cols]
            placeholders = ",".join("?" * len(cols))
            conn.execute(f"INSERT INTO parcels ({','.join(cols)}) VALUES ({placeholders})", vals)
            new_count += 1
        elif existing["data_hash"] != r["data_hash"]:
            # Changed — log what changed
            for field in ["owner", "total_value", "land_value", "zoning", "last_sale_price"]:
                old_val = existing[field]
                new_val = r.get(field)
                if str(old_val) != str(new_val):
                    conn.execute(
                        "INSERT INTO changes (pin,field,old_value,new_value,changed_at) VALUES (?,?,?,?,?)",
                        (pin, field, old_val, new_val, now)
                    )
            conn.execute("""
                UPDATE parcels SET
                  reid=?,address=?,city=?,zip=?,owner=?,acres=?,billing_class=?,
                  land_class=?,land_value=?,building_value=?,total_value=?,
                  prev_total_value=?,last_sale_price=?,last_sale_date=?,zoning=?,
                  township=?,planning_juris=?,exempt_status=?,deed_book=?,deed_page=?,
                  year_built=?,type_and_use=?,heated_area=?,physical_city=?,
                  red_flags=?,last_updated=?,data_hash=?
                WHERE pin=?
            """, (
                r.get("reid"), r.get("address"), r.get("city"), r.get("zip"),
                r.get("owner"), r.get("acres"), r.get("billing_class"), r.get("land_class"),
                r.get("land_value"), r.get("building_value"), r.get("total_value"),
                r.get("prev_total_value"), r.get("last_sale_price"), r.get("last_sale_date"),
                r.get("zoning"), r.get("township"), r.get("planning_juris"), r.get("exempt_status"),
                r.get("deed_book"), r.get("deed_page"), r.get("year_built"), r.get("type_and_use"),
                r.get("heated_area"), r.get("physical_city"), r.get("red_flags"),
                now, r.get("data_hash"), pin
            ))
            changed_count += 1

    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM parcels").fetchone()[0]
    conn.execute("""
        INSERT INTO refresh_log (run_at, parcels_total, parcels_new, parcels_changed, source_file, status)
        VALUES (?,?,?,?,?,?)
    """, (now, total, new_count, changed_count, filepath.name, "ok"))
    conn.commit()
    conn.close()

    print(f"  ✓ Import complete: {total:,} total | {new_count:,} new | {changed_count:,} changed")
    return total, new_count, changed_count


def import_sales(filepath: Path):
    if not filepath or not filepath.exists():
        return

    try:
        import pandas as pd
        df = pd.read_excel(filepath, dtype=str).fillna("")
        df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]

        conn = get_db()
        count = 0
        for _, row in df.iterrows():
            pin = str(row.get("PIN_NUM", row.get("PIN", ""))).strip()
            if not pin:
                continue
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO sales
                    (pin, sale_date, sale_price, buyer, seller, deed_book, deed_page, valid_sale)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (
                    pin,
                    str(row.get("SALE_DATE", "")),
                    float(str(row.get("SALE_PRICE", row.get("TOT_SALE_PRICE", "0"))).replace(",","").replace("$","") or 0),
                    str(row.get("BUYER", row.get("OWNER", ""))),
                    str(row.get("SELLER", "")),
                    str(row.get("DEED_BOOK", "")),
                    str(row.get("DEED_PAGE", "")),
                    str(row.get("VALID_SALE", row.get("QUALIFIED", ""))),
                ))
                count += 1
            except Exception:
                pass
        conn.commit()
        conn.close()
        print(f"  ✓ Imported {count:,} sales records")
    except Exception as e:
        print(f"  ⚠ Sales import failed: {e}")


# ── Query ─────────────────────────────────────────────────────────────────────
def query_parcel(pin_or_address: str) -> dict | None:
    conn = get_db()
    term = pin_or_address.strip().upper()

    # Try PIN (exact or partial)
    row = conn.execute("SELECT * FROM parcels WHERE pin=?", (term,)).fetchone()
    if not row:
        row = conn.execute("SELECT * FROM parcels WHERE pin LIKE ?", (f"%{term}%",)).fetchone()
    # Try address
    if not row:
        row = conn.execute(
            "SELECT * FROM parcels WHERE UPPER(address) LIKE ?",
            (f"%{term}%",)
        ).fetchone()
    # Try REID
    if not row:
        row = conn.execute("SELECT * FROM parcels WHERE reid=?", (term,)).fetchone()

    if not row:
        conn.close()
        return None

    result = dict(row)

    # Add sales history
    sales = conn.execute(
        "SELECT * FROM sales WHERE pin=? ORDER BY sale_date DESC LIMIT 10",
        (result["pin"],)
    ).fetchall()
    result["sales_history"] = [dict(s) for s in sales]

    # Parse red flags
    try:
        result["red_flags"] = json.loads(result.get("red_flags") or "[]")
    except Exception:
        result["red_flags"] = []

    # Add portal links
    pin = result["pin"]
    reid = result.get("reid", "")
    result["links"] = {
        "imaps": f"https://imaps.wake.gov/iMAPS/?pin={pin}",
        "tax_portal": f"https://services.wake.gov/TaxPortal/Property/MainSearch?searchBy=pin&searchTerm={pin}",
        "permits": f"https://energov.wakegov.com/energovprod/selfservice#/search?module=1&keyword={pin}",
        "register_of_deeds": f"https://rodweb.wake.gov/rod/web/",
    }

    # Computed fields
    lv = result.get("land_value") or 0
    tv = result.get("total_value") or 0
    sp = result.get("last_sale_price") or 0
    acres = result.get("acres") or 0

    result["computed"] = {
        "land_pct_of_total": round(lv / tv * 100, 1) if tv else None,
        "price_per_acre": round(sp / acres, 0) if acres and sp else None,
        "assessed_per_acre": round(tv / acres, 0) if acres and tv else None,
        "sale_to_assessed_ratio": round(sp / tv, 2) if tv and sp else None,
    }

    conn.close()
    return result


def get_recent_changes(days=7):
    conn = get_db()
    cutoff = (datetime.datetime.now() - datetime.timedelta(days=days)).isoformat()
    rows = conn.execute("""
        SELECT c.*, p.address, p.owner FROM changes c
        LEFT JOIN parcels p ON p.pin=c.pin
        WHERE c.changed_at > ? ORDER BY c.changed_at DESC LIMIT 200
    """, (cutoff,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_recent_sales(days=90, min_acres=1.0):
    conn = get_db()
    cutoff = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()
    rows = conn.execute("""
        SELECT s.*, p.address, p.acres, p.zoning, p.township, p.planning_juris
        FROM sales s
        LEFT JOIN parcels p ON p.pin=s.pin
        WHERE s.sale_date > ? AND s.sale_price > 0 AND (p.acres IS NULL OR p.acres >= ?)
        ORDER BY s.sale_date DESC LIMIT 100
    """, (cutoff, min_acres)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_db_stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM parcels").fetchone()[0]
    last_refresh = conn.execute(
        "SELECT run_at, parcels_new, parcels_changed, source_file FROM refresh_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    changes_7d = conn.execute(
        "SELECT COUNT(*) FROM changes WHERE changed_at > ?",
        ((datetime.datetime.now() - datetime.timedelta(days=7)).isoformat(),)
    ).fetchone()[0]
    conn.close()
    return {
        "total_parcels": total,
        "last_refresh": dict(last_refresh) if last_refresh else None,
        "changes_last_7_days": changes_7d,
        "db_size_mb": round(DB_PATH.stat().st_size / 1_000_000, 1) if DB_PATH.exists() else 0
    }


# ── REST API ──────────────────────────────────────────────────────────────────
def run_server():
    try:
        from flask import Flask, jsonify, request
        from flask_cors import CORS
    except ImportError:
        print("ERROR: Install flask: pip install flask flask-cors --break-system-packages")
        sys.exit(1)

    app = Flask(__name__)
    CORS(app)  # Allow LandWorks HTML to call this from file:// or any origin

    @app.route("/")
    def index():
        return jsonify({
            "service": "Wake County Parcel Intelligence",
            "version": "1.0",
            "endpoints": {
                "/parcel/<pin_or_address>": "Look up a parcel by PIN or address",
                "/search?q=<query>":        "Search parcels",
                "/changes?days=7":          "Recent data changes",
                "/sales?days=90&min_acres=1": "Recent land sales",
                "/stats":                   "Database statistics",
                "/refresh":                 "Trigger data refresh (POST)"
            }
        })

    @app.route("/parcel/<path:query>")
    def get_parcel(query):
        result = query_parcel(query)
        if result:
            return jsonify({"ok": True, "parcel": result})
        return jsonify({"ok": False, "error": f"No parcel found for: {query}"}), 404

    @app.route("/search")
    def search():
        q = request.args.get("q", "").strip()
        if len(q) < 3:
            return jsonify({"ok": False, "error": "Query too short"}), 400
        conn = get_db()
        rows = conn.execute("""
            SELECT pin, reid, address, city, owner, acres, total_value, zoning, planning_juris
            FROM parcels
            WHERE UPPER(pin) LIKE ? OR UPPER(address) LIKE ? OR UPPER(owner) LIKE ?
            LIMIT 20
        """, (f"%{q.upper()}%", f"%{q.upper()}%", f"%{q.upper()}%")).fetchall()
        conn.close()
        return jsonify({"ok": True, "results": [dict(r) for r in rows]})

    @app.route("/changes")
    def changes():
        days = int(request.args.get("days", 7))
        return jsonify({"ok": True, "changes": get_recent_changes(days)})

    @app.route("/sales")
    def sales():
        days = int(request.args.get("days", 90))
        min_acres = float(request.args.get("min_acres", 1.0))
        return jsonify({"ok": True, "sales": get_recent_sales(days, min_acres)})

    @app.route("/stats")
    def stats():
        return jsonify({"ok": True, "stats": get_db_stats()})

    @app.route("/refresh", methods=["POST"])
    def refresh_endpoint():
        try:
            print("  [API] Refresh triggered via HTTP")
            parcel_file = download_realestate_data()
            total, new_c, changed_c = import_parcels(parcel_file)
            sales_file = download_sales_data()
            if sales_file:
                import_sales(sales_file)
            return jsonify({
                "ok": True,
                "result": {
                    "total": total,
                    "new": new_c,
                    "changed": changed_c,
                    "timestamp": datetime.datetime.now().isoformat()
                }
            })
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    print(f"""
╔════════════════════════════════════════════════╗
║   Wake County Parcel Intelligence API          ║
║   Running at http://127.0.0.1:{API_PORT}            ║
╠════════════════════════════════════════════════╣
║  GET  /parcel/<PIN>      Look up any parcel   ║
║  GET  /search?q=<query>  Search parcels       ║
║  GET  /sales             Recent land sales    ║
║  GET  /changes           Data changes         ║
║  GET  /stats             DB statistics        ║
║  POST /refresh           Download new data    ║
╚════════════════════════════════════════════════╝
    """)

    app.run(host="127.0.0.1", port=API_PORT, debug=False)


# ── CLI ───────────────────────────────────────────────────────────────────────
def cmd_refresh():
    print("\n🔄 Wake County Data Refresh")
    print(f"   Data directory: {DATA_DIR}")
    print()
    init_db()
    parcel_file = download_realestate_data()
    import_parcels(parcel_file)
    sales_file = download_sales_data()
    if sales_file:
        import_sales(sales_file)
    stats = get_db_stats()
    print(f"\n✅ Done. Database: {stats['total_parcels']:,} parcels | {stats['db_size_mb']} MB")


def cmd_query(term):
    init_db()
    result = query_parcel(term)
    if result:
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"No parcel found for: {term}")


def cmd_serve():
    init_db()
    if not DB_PATH.exists() or get_db_stats()["total_parcels"] == 0:
        print("⚠  Database is empty. Running initial refresh first...")
        cmd_refresh()
    run_server()


def cmd_start():
    """Refresh data then start server — the recommended way to run."""
    init_db()
    print("\n🔄 Checking for updated Wake County data...")
    stats = get_db_stats()
    needs_refresh = True

    if stats["last_refresh"]:
        last_run = datetime.datetime.fromisoformat(stats["last_refresh"]["run_at"])
        age_hours = (datetime.datetime.now() - last_run).total_seconds() / 3600
        if age_hours < 20:
            print(f"  ✓ Data is {age_hours:.1f}h old — skipping download (refresh < 20h ago)")
            needs_refresh = False
        else:
            print(f"  → Data is {age_hours:.1f}h old — downloading fresh data")

    if needs_refresh:
        try:
            parcel_file = download_realestate_data()
            import_parcels(parcel_file)
            sales_file = download_sales_data()
            if sales_file:
                import_sales(sales_file)
        except Exception as e:
            print(f"  ⚠ Refresh failed: {e}")
            if stats["total_parcels"] == 0:
                print("  ✗ No local data. Check your internet connection and try again.")
                sys.exit(1)
            print("  → Continuing with existing data.")

    run_server()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Wake County Parcel Intelligence")
    parser.add_argument("command", choices=["refresh","serve","query","start"],
                        help="refresh=download data, serve=start API, query=look up a PIN, start=refresh+serve")
    parser.add_argument("term", nargs="?", help="PIN or address for query command")
    args = parser.parse_args()

    if args.command == "refresh":
        cmd_refresh()
    elif args.command == "serve":
        cmd_serve()
    elif args.command == "query":
        if not args.term:
            print("Usage: python wake_county_data.py query <PIN_or_address>")
            sys.exit(1)
        cmd_query(args.term)
    elif args.command == "start":
        cmd_start()
