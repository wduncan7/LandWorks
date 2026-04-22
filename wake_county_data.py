#!/usr/bin/env python3
"""
Wake County Parcel Intelligence System
Queries Wake County ArcGIS Feature Service by PIN, caches locally, serves REST API.

Usage:
  python wake_county_data.py serve          # Start local API server (port 7474)
  python wake_county_data.py query <PIN>    # Query a parcel by PIN
  python wake_county_data.py start          # Start server (same as serve)

Requirements:
  pip install requests flask flask-cors --break-system-packages
"""

import sys
import os
import json
import sqlite3
import datetime
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
DATA_DIR = Path.home() / ".wake_county_data"
DB_PATH  = DATA_DIR / "parcels.db"
API_PORT = 7474

# Wake County ArcGIS Feature Service
PARCEL_SERVICE_URL = (
    "https://maps.wake.gov/arcgis/rest/services/Property/Parcels/MapServer/0/query"
)
CACHE_TTL_HOURS = 24  # Re-fetch from ArcGIS after this many hours

DATA_DIR.mkdir(parents=True, exist_ok=True)


# ── Database ──────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS parcels (
            pin              TEXT PRIMARY KEY,
            reid             TEXT,
            address          TEXT,
            city             TEXT,
            zip              TEXT,
            owner            TEXT,
            acres            REAL,
            billing_class    TEXT,
            land_class       TEXT,
            land_value       REAL,
            building_value   REAL,
            total_value      REAL,
            last_sale_price  REAL,
            last_sale_date   TEXT,
            zoning           TEXT,
            township         TEXT,
            planning_juris   TEXT,
            exempt_status    TEXT,
            deed_book        TEXT,
            deed_page        TEXT,
            year_built       TEXT,
            type_and_use     TEXT,
            heated_area      REAL,
            propdesc         TEXT,
            units            TEXT,
            red_flags        TEXT,
            raw_json         TEXT,
            last_updated     TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_pin   ON parcels(pin);
        CREATE INDEX IF NOT EXISTS idx_owner ON parcels(owner);
        CREATE INDEX IF NOT EXISTS idx_addr  ON parcels(address);
    """)
    conn.commit()
    conn.close()


# ── Red Flag Analysis ─────────────────────────────────────────────────────────
def analyze_red_flags(row: dict) -> list:
    flags = []

    if row.get("exempt_status") and str(row["exempt_status"]).strip():
        flags.append({"type": "exempt", "severity": "info",
                      "msg": f"Exempt parcel: {row['exempt_status']} — may not be privately purchasable"})

    owner = str(row.get("owner", ""))
    if any(x in owner.upper() for x in ["LLC", "LP ", "LLP", "INC", "CORP", "TRUST"]):
        flags.append({"type": "entity_owner", "severity": "info",
                      "msg": f"Entity ownership ({owner}) — may need extra research on decision maker"})

    acres = float(row.get("acres") or 0)
    if 0 < acres < 0.5:
        flags.append({"type": "small_parcel", "severity": "warning",
                      "msg": f"Small parcel ({acres:.2f} ac) — may not meet minimum lot size requirements"})

    land_class = str(row.get("land_class", "")).upper()
    if "VACANT" in land_class:
        flags.append({"type": "vacant", "severity": "ok",
                      "msg": "Vacant land — no demolition cost or tenant displacement concerns"})

    township = str(row.get("township", "")).upper()
    if any(x in township for x in ["SWIFT CREEK", "FALLS LAKE", "JORDAN LAKE", "NEUSE"]):
        flags.append({"type": "watershed", "severity": "warning",
                      "msg": f"Watershed area ({township}) — density/impervious restrictions likely apply"})

    return flags


# ── ArcGIS Query ──────────────────────────────────────────────────────────────
def fetch_parcel_from_arcgis(pin: str) -> dict | None:
    try:
        import requests
    except ImportError:
        print("ERROR: pip install requests --break-system-packages")
        return None

    params = {
        "where": f"PIN_NUM='{pin}'",
        "outFields": "*",
        "f": "json",
        "returnGeometry": "false",
    }
    headers = {"User-Agent": "Mozilla/5.0 LandWorks/1.0"}

    try:
        resp = requests.get(PARCEL_SERVICE_URL, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ✗ ArcGIS request failed: {e}")
        return None

    features = data.get("features", [])
    if not features:
        return None

    attrs = features[0].get("attributes", {})

    # Map ArcGIS field names → our schema
    row = {
        "pin":            str(attrs.get("PIN_NUM", pin)).strip(),
        "reid":           attrs.get("REID", ""),
        "address":        attrs.get("SITE_ADDRESS", ""),
        "city":           attrs.get("CITY_DECODE", attrs.get("CITY", "")),
        "zip":            str(attrs.get("ZIPNUM", "")),
        "owner":          attrs.get("OWNER", ""),
        "acres":          _float(attrs.get("DEED_ACRES")),
        "billing_class":  attrs.get("BILLING_CLASS_DECODE", attrs.get("BILLCLASS", "")),
        "land_class":     attrs.get("LAND_CLASS_DECODE", attrs.get("LAND_CLASS", "")),
        "land_value":     _float(attrs.get("LAND_VAL")),
        "building_value": _float(attrs.get("BLDG_VAL")),
        "total_value":    _float(attrs.get("TOTAL_VALUE_ASSD")),
        "last_sale_price":_float(attrs.get("TOTSALPRICE")),
        "last_sale_date": _epoch_to_date(attrs.get("SALE_DATE")),
        "zoning":         attrs.get("PROPDESC", ""),
        "township":       attrs.get("TOWNSHIP_DECODE", attrs.get("TOWNSHIP", "")),
        "planning_juris": attrs.get("PLANNING_JURISDICTION", ""),
        "exempt_status":  attrs.get("EXEMPTDESC", attrs.get("EXEMPTSTAT", "")),
        "deed_book":      attrs.get("DEED_BOOK", ""),
        "deed_page":      attrs.get("DEED_PAGE", ""),
        "year_built":     str(attrs.get("YEAR_BUILT", "")),
        "type_and_use":   attrs.get("TYPE_USE_DECODE", attrs.get("TYPE_AND_USE", "")),
        "heated_area":    _float(attrs.get("HEATEDAREA")),
        "propdesc":       attrs.get("PROPDESC", ""),
        "units":          str(attrs.get("UNITS", "")),
        "raw_json":       json.dumps(attrs),
    }

    row["red_flags"] = json.dumps(analyze_red_flags(row))
    return row


def _float(val):
    try:
        return float(val) if val not in (None, "", "null") else None
    except (ValueError, TypeError):
        return None


def _epoch_to_date(val):
    """Convert ArcGIS epoch milliseconds to YYYY-MM-DD string."""
    if not val:
        return None
    try:
        return datetime.datetime.fromtimestamp(int(val) / 1000).strftime("%Y-%m-%d")
    except Exception:
        return str(val)


# ── Cache Logic ───────────────────────────────────────────────────────────────
def get_cached_parcel(pin: str) -> dict | None:
    conn = get_db()
    row = conn.execute("SELECT * FROM parcels WHERE pin=?", (pin,)).fetchone()
    conn.close()
    if not row:
        return None
    # Check freshness
    last = row["last_updated"]
    if last:
        age = datetime.datetime.utcnow() - datetime.datetime.fromisoformat(last)
        if age.total_seconds() < CACHE_TTL_HOURS * 3600:
            return dict(row)
    return None  # Stale — re-fetch


def cache_parcel(row: dict):
    conn = get_db()
    now = datetime.datetime.utcnow().isoformat()
    conn.execute("""
        INSERT OR REPLACE INTO parcels
        (pin,reid,address,city,zip,owner,acres,billing_class,land_class,
         land_value,building_value,total_value,last_sale_price,last_sale_date,
         zoning,township,planning_juris,exempt_status,deed_book,deed_page,
         year_built,type_and_use,heated_area,propdesc,units,red_flags,raw_json,last_updated)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        row.get("pin"), row.get("reid"), row.get("address"), row.get("city"),
        row.get("zip"), row.get("owner"), row.get("acres"), row.get("billing_class"),
        row.get("land_class"), row.get("land_value"), row.get("building_value"),
        row.get("total_value"), row.get("last_sale_price"), row.get("last_sale_date"),
        row.get("zoning"), row.get("township"), row.get("planning_juris"),
        row.get("exempt_status"), row.get("deed_book"), row.get("deed_page"),
        row.get("year_built"), row.get("type_and_use"), row.get("heated_area"),
        row.get("propdesc"), row.get("units"), row.get("red_flags"),
        row.get("raw_json"), now
    ))
    conn.commit()
    conn.close()


def lookup_parcel(pin: str) -> dict | None:
    """Return parcel data — from cache if fresh, else live from ArcGIS."""
    pin = pin.strip()
    cached = get_cached_parcel(pin)
    if cached:
        print(f"  ✓ Cache hit for PIN {pin}")
        return cached
    print(f"  → Fetching PIN {pin} from Wake County ArcGIS...")
    row = fetch_parcel_from_arcgis(pin)
    if row:
        cache_parcel(row)
        print(f"  ✓ Fetched and cached: {row.get('address')} — {row.get('owner')}")
    return row


# ── Flask API ─────────────────────────────────────────────────────────────────
def serve():
    try:
        from flask import Flask, jsonify, request
        from flask_cors import CORS
    except ImportError:
        print("ERROR: pip install flask flask-cors --break-system-packages")
        sys.exit(1)

    app = Flask(__name__)
    CORS(app)

    @app.route("/ping")
    def ping():
        return jsonify({"status": "ok", "service": "wake_county_data", "port": API_PORT})

    @app.route("/parcel/<pin>")
    def get_parcel(pin):
        force = request.args.get("force", "false").lower() == "true"
        if force:
            # Clear cache entry so we re-fetch
            conn = get_db()
            conn.execute("DELETE FROM parcels WHERE pin=?", (pin,))
            conn.commit()
            conn.close()
        row = lookup_parcel(pin)
        if not row:
            return jsonify({"error": f"PIN {pin} not found in Wake County"}), 404
        result = dict(row)
        # Parse JSON fields
        for f in ("red_flags", "raw_json"):
            if isinstance(result.get(f), str):
                try:
                    result[f] = json.loads(result[f])
                except Exception:
                    pass
        return jsonify(result)

    @app.route("/search")
    def search():
        q = request.args.get("q", "").strip()
        field = request.args.get("field", "address").lower()
        limit = min(int(request.args.get("limit", 20)), 100)
        if not q:
            return jsonify({"error": "q parameter required"}), 400
        conn = get_db()
        col_map = {"address": "address", "owner": "owner", "pin": "pin"}
        col = col_map.get(field, "address")
        rows = conn.execute(
            f"SELECT * FROM parcels WHERE {col} LIKE ? LIMIT ?",
            (f"%{q.upper()}%", limit)
        ).fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])

    @app.route("/stats")
    def stats():
        conn = get_db()
        total = conn.execute("SELECT COUNT(*) FROM parcels").fetchone()[0]
        recent = conn.execute(
            "SELECT COUNT(*) FROM parcels WHERE last_updated > datetime('now','-1 day')"
        ).fetchone()[0]
        conn.close()
        return jsonify({"cached_parcels": total, "updated_last_24h": recent})

    print(f"🏗  Wake County Parcel API running on http://localhost:{API_PORT}")
    print(f"   GET /parcel/<PIN>  — lookup by PIN (cached {CACHE_TTL_HOURS}h)")
    print(f"   GET /ping          — health check")
    print(f"   GET /stats         — cache stats")
    app.run(host="0.0.0.0", port=API_PORT, debug=False)


# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    init_db()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "serve"

    if cmd in ("serve", "start"):
        serve()

    elif cmd == "query":
        if len(sys.argv) < 3:
            print("Usage: python wake_county_data.py query <PIN>")
            sys.exit(1)
        pin = sys.argv[2]
        row = lookup_parcel(pin)
        if row:
            for k, v in row.items():
                if k not in ("raw_json",):
                    print(f"  {k:20s}: {v}")
        else:
            print(f"  PIN {pin} not found.")

    elif cmd == "clear-cache":
        conn = get_db()
        conn.execute("DELETE FROM parcels")
        conn.commit()
        conn.close()
        print("  ✓ Cache cleared.")

    else:
        print(f"Unknown command: {cmd}")
        print("Usage: python wake_county_data.py [serve|start|query <PIN>|clear-cache]")
        sys.exit(1)


if __name__ == "__main__":
    main()
