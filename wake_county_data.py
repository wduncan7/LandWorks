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
from typing import Optional

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
def fetch_parcel_from_arcgis(pin: str) -> Optional[dict]:
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
        "zoning":         attrs.get("ZONING_CLASS") or None,
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
def get_cached_parcel(pin: str) -> Optional[dict]:
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


def lookup_parcel(pin: str) -> Optional[dict]:
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

    @app.route("/geometry/<pin>")
    def get_geometry(pin):
        """Fetch parcel boundary polygon from Wake County ArcGIS in WGS84 (lat/lon)."""
        try:
            import requests as _req
        except ImportError:
            return jsonify({"error": "requests not installed"}), 500
        params = {
            "where":          f"PIN_NUM='{pin}'",
            "outFields":      "PIN_NUM,DEED_ACRES,SITE_ADDRESS,CITY_DECODE",
            "returnGeometry": "true",
            "outSR":          "4326",   # WGS84 lat/lon for Leaflet
            "f":              "json",
        }
        try:
            resp = _req.get(PARCEL_SERVICE_URL, params=params,
                            headers={"User-Agent": "Mozilla/5.0 LandWorks/1.0"}, timeout=20)
            data = resp.json()
            features = data.get("features", [])
            if not features:
                return jsonify({"error": f"PIN {pin} not found"}), 404
            feat = features[0]
            rings = feat.get("geometry", {}).get("rings", [])
            # Convert Esri rings [lon, lat] → GeoJSON Polygon
            geojson = {
                "type": "Polygon",
                "coordinates": rings
            }
            return jsonify({
                "pin":        pin,
                "geojson":    geojson,
                "attributes": feat.get("attributes", {}),
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

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

    # ── Auto-fetched cases DB ─────────────────────────────────────────────────
    CASES_DB_PATH = DATA_DIR / "cases.db"

    @app.route("/cases")
    def get_cases():
        """Return all auto-fetched cases from lw_auto_fetch.py's database."""
        if not CASES_DB_PATH.exists():
            return jsonify({"cases": [], "total": 0, "note": "Run lw_auto_fetch.py to populate"})
        try:
            conn = sqlite3.connect(CASES_DB_PATH)
            conn.row_factory = sqlite3.Row
            city  = request.args.get("city")
            limit = min(int(request.args.get("limit", 500)), 2000)
            if city:
                rows = conn.execute("SELECT * FROM cases WHERE city=? ORDER BY meeting_date DESC LIMIT ?",
                                    (city, limit)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM cases ORDER BY meeting_date DESC LIMIT ?",
                                    (limit,)).fetchall()
            conn.close()
            cases = [dict(r) for r in rows]
            # Convert int booleans back to bool for JS
            for c in cases:
                for f in ("adjacent_sf","traffic_study","affordable_housing","transition_buffer"):
                    if c.get(f) is not None:
                        c[f] = bool(c[f])
            return jsonify({"cases": cases, "total": len(cases)})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/cases/stats")
    def case_stats():
        """Summary stats on the cases database."""
        if not CASES_DB_PATH.exists():
            return jsonify({"total": 0})
        conn = sqlite3.connect(CASES_DB_PATH)
        total   = conn.execute("SELECT COUNT(*) FROM cases").fetchone()[0]
        by_city = conn.execute("SELECT city, COUNT(*) as n FROM cases GROUP BY city").fetchall()
        by_out  = conn.execute("SELECT outcome, COUNT(*) as n FROM cases GROUP BY outcome").fetchall()
        conn.close()
        return jsonify({
            "total":     total,
            "by_city":   {r[0]: r[1] for r in by_city},
            "by_outcome":{r[0]: r[1] for r in by_out},
        })

    @app.route("/council")
    def get_council():
        """
        Return council member profiles built from the council_intelligence table.
        Each member gets a pro_dev_score (0-10) derived from their vote history,
        aggregated concerns, and most recent quotes.
        GET /council?city=Raleigh   — filter by city
        GET /council                — all cities
        """
        if not CASES_DB_PATH.exists():
            return jsonify({"members": [], "total": 0})
        try:
            conn = sqlite3.connect(CASES_DB_PATH)
            conn.row_factory = sqlite3.Row
            city = request.args.get("city")

            # Check if table exists
            tbl = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='council_intelligence'"
            ).fetchone()
            if not tbl:
                conn.close()
                return jsonify({"members": [], "total": 0, "note": "No council data yet — run video scraper"})

            if city:
                rows = conn.execute(
                    "SELECT * FROM council_intelligence WHERE city=? ORDER BY extracted_at DESC",
                    (city,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM council_intelligence ORDER BY city, member_name, extracted_at DESC"
                ).fetchall()
            conn.close()

            import json as _json

            # Aggregate by city + member_name
            members = {}
            for r in rows:
                key = (r["city"], r["member_name"])
                if key not in members:
                    members[key] = {
                        "city":            r["city"],
                        "name":            r["member_name"],
                        "role":            r["role"] or "council_member",
                        "votes":           [],
                        "yes_votes":       0,
                        "no_votes":        0,
                        "abstain_votes":   0,
                        "concerns_tally":  {},
                        "key_quotes":      [],
                    }
                m = members[key]

                # Tally vote
                vote = (r["vote"] or "").lower()
                if vote == "yes":   m["yes_votes"] += 1
                elif vote == "no":  m["no_votes"] += 1
                elif vote == "abstain": m["abstain_votes"] += 1

                # Add vote record
                concerns = []
                conditions = []
                try:
                    concerns   = _json.loads(r["stated_concerns"] or "[]")
                    conditions = _json.loads(r["conditions_requested"] or "[]")
                except Exception:
                    pass

                m["votes"].append({
                    "case_number":        r["case_number"],
                    "meeting_date":       r["meeting_date"],
                    "vote":               r["vote"],
                    "sentiment":          r["sentiment"],
                    "key_quote":          r["key_quote"],
                    "stated_concerns":    concerns,
                    "conditions_requested": conditions,
                })

                # Tally concerns
                for c in concerns:
                    label = c.strip().lower()[:60]
                    m["concerns_tally"][label] = m["concerns_tally"].get(label, 0) + 1

                # Keep recent quotes
                if r["key_quote"] and len(m["key_quotes"]) < 3:
                    m["key_quotes"].append(r["key_quote"])

            # Calculate pro_dev_score for each member
            result = []
            for m in members.values():
                total_votes = m["yes_votes"] + m["no_votes"] + m["abstain_votes"]
                if total_votes > 0:
                    raw = (m["yes_votes"] / (m["yes_votes"] + m["no_votes"])) * 10 if (m["yes_votes"] + m["no_votes"]) > 0 else 5
                    m["pro_dev_score"] = round(raw, 1)
                else:
                    m["pro_dev_score"] = None

                m["total_votes"]     = total_votes
                m["top_concerns"]    = sorted(m["concerns_tally"].items(), key=lambda x: -x[1])[:5]
                m["latest_quote"]    = m["key_quotes"][0] if m["key_quotes"] else None
                result.append(m)

            # Sort: city, then by most votes (most data first)
            result.sort(key=lambda x: (x["city"], -(x["total_votes"])))
            return jsonify({"members": result, "total": len(result)})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    # ── Zoning lookup (bypasses browser CORS for Planning/Zoning layer) ──
    @app.route("/zoning/<pin>")
    def get_zoning(pin):
        """
        Fetch zoning district for a parcel PIN.
        Strategy 1: Property/Property/MapServer ZONING_CLASS attribute query
        Strategy 2: Spatial point-in-polygon on Planning/Zoning/MapServer
        """
        try:
            import requests as _req
        except ImportError:
            return jsonify({"error": "requests not installed"}), 500

        pin_clean = pin.strip().replace("-", "")
        headers = {"User-Agent": "Mozilla/5.0 LandWorks/1.0"}

        # ── Strategy 1: ZONING_CLASS from Property/Property layer ──────────────
        prop_endpoints = [
            "https://maps.wake.gov/arcgis/rest/services/Property/Property/MapServer/0/query",
            "https://maps.wakegov.com/arcgis/rest/services/Property/Property/MapServer/0/query",
            "https://maps.wake.gov/arcgis/rest/services/Property/Parcels/MapServer/0/query",
        ]
        for ep in prop_endpoints:
            try:
                resp = _req.get(ep, params={
                    "where": f"PIN_NUM='{pin_clean}'",
                    "outFields": "ZONING_CLASS,ZONING_DESC,ZONE_CLASS",
                    "returnGeometry": "false",
                    "f": "json",
                }, headers=headers, timeout=15)
                data = resp.json()
                feats = data.get("features", [])
                if feats:
                    attrs = feats[0].get("attributes", {})
                    z = (attrs.get("ZONING_CLASS") or attrs.get("ZONING_DESC") or
                         attrs.get("ZONE_CLASS") or "")
                    if z and str(z).strip() and str(z).lower() not in ("null", "none", ""):
                        return jsonify({"zoning": str(z).strip(), "source": "property_layer"})
            except Exception:
                continue

        # ── Strategy 2: Spatial query on Planning/Zoning layer ─────────────────
        # First get parcel centroid from geometry endpoint
        try:
            # Inline geometry fetch to get centroid
            geom_resp = _req.get(
                "https://maps.wake.gov/arcgis/rest/services/Property/Parcels/MapServer/0/query",
                params={
                    "where": f"PIN_NUM='{pin_clean}'",
                    "outFields": "PIN_NUM",
                    "returnGeometry": "true",
                    "outSR": "4326",
                    "f": "json",
                }, headers=headers, timeout=15
            )
            geom_data = geom_resp.json()
            geom_feats = geom_data.get("features", [])
            if geom_feats:
                rings = (geom_feats[0].get("geometry") or {}).get("rings", [])
                if rings and rings[0]:
                    pts = rings[0]
                    cx = sum(p[0] for p in pts) / len(pts)
                    cy = sum(p[1] for p in pts) / len(pts)

                    # Spatial query on Planning/Zoning layers
                    zone_endpoints = [
                        "https://maps.wake.gov/arcgis/rest/services/Planning/Zoning/MapServer/0/query",
                        "https://maps.wakegov.com/arcgis/rest/services/Planning/Zoning/MapServer/0/query",
                        "https://maps.wake.gov/arcgis/rest/services/Planning/Zoning/FeatureServer/0/query",
                    ]
                    for ep in zone_endpoints:
                        try:
                            z_resp = _req.get(ep, params={
                                "geometry": f"{cx:.6f},{cy:.6f}",
                                "geometryType": "esriGeometryPoint",
                                "inSR": "4326",
                                "spatialRel": "esriSpatialRelIntersects",
                                "outFields": "ZONING_CLASS,ZONING_DESC,ZONE_CLASS,ZONING,ZONE_DESC",
                                "returnGeometry": "false",
                                "f": "json",
                            }, headers=headers, timeout=15)
                            z_data = z_resp.json()
                            z_feats = z_data.get("features", [])
                            if z_feats:
                                a = z_feats[0].get("attributes", {})
                                z = (a.get("ZONING_CLASS") or a.get("ZONING_DESC") or
                                     a.get("ZONE_CLASS") or a.get("ZONING") or
                                     a.get("ZONE_DESC") or "")
                                if z and str(z).strip() and str(z).lower() not in ("null", "none", ""):
                                    return jsonify({"zoning": str(z).strip(), "source": "planning_zoning_layer"})
                        except Exception:
                            continue
        except Exception:
            pass

        return jsonify({"zoning": None, "source": None, "note": "Not found in any Wake County GIS layer"}), 200

    # ── Legistar API proxy (bypasses CORS block on browser direct calls) ──
    @app.route("/legistar/<client_id>/<path:endpoint>")
    def legistar_proxy(client_id, endpoint):
        """
        Proxy requests to the Legistar public REST API.
        Usage: GET /legistar/raleigh/matters?$top=50&$filter=...
        """
        try:
            import requests as _req
            from flask import request as freq
        except ImportError:
            return jsonify({"error": "requests not installed"}), 500

        legistar_url = f"https://webapi.legistar.com/v1/{client_id}/{endpoint}"
        params = dict(freq.args)

        try:
            resp = _req.get(
                legistar_url,
                params=params,
                headers={"User-Agent": "Mozilla/5.0 LandWorks/1.0"},
                timeout=30
            )
            return (resp.content, resp.status_code, {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    print(f"🏗  Wake County Parcel API running on http://localhost:{API_PORT}")
    print(f"   GET /parcel/<PIN>              — lookup by PIN (cached {CACHE_TTL_HOURS}h)")
    print(f"   GET /geometry/<PIN>            — parcel boundary polygon")
    print(f"   GET /zoning/<PIN>              — zoning district (Property + Planning/Zoning layers)")
    print(f"   GET /legistar/<city>/<endpoint> — Legistar API proxy (e.g. /legistar/raleigh/matters)")
    print(f"   GET /ping                      — health check")
    print(f"   GET /stats                     — cache stats")
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
