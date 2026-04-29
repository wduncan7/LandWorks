#!/usr/bin/env python3
"""
LandWorks Auto-Fetch — Automated data pipeline for LandWorks case database.

What it does:
  1. Pulls rezoning cases from Legistar (Raleigh, and others as they're added)
  2. Classifies each case with Claude Haiku (cheap + fast)
  3. Writes structured cases to ~/.wake_county_data/cases.db (SQLite)
  4. wake_county_data.py serves these via GET /cases so LandWorks loads them automatically

Usage:
  python3 lw_auto_fetch.py                    # Run once (all cities)
  python3 lw_auto_fetch.py --city Raleigh     # Run for one city
  python3 lw_auto_fetch.py --dry-run          # Fetch + classify but don't save

Cron (nightly at 2am):
  0 2 * * * python3 ~/LandWorks/lw_auto_fetch.py >> ~/LandWorks/fetch.log 2>&1

Requirements:
  pip install anthropic requests --break-system-packages
"""

import sys
import os
import json
import sqlite3
import datetime
import argparse
import time
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

DATA_DIR  = Path.home() / ".wake_county_data"
CASES_DB  = DATA_DIR / "cases.db"
LOG_FILE  = DATA_DIR / "fetch.log"

# Your Anthropic API key — set here OR as env var ANTHROPIC_API_KEY
API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# Legistar city clients — add more as they become available
LEGISTAR_CLIENTS = {
    "Raleigh":    "raleigh",
    # "Cary":     "cary",       # not on Legistar yet — add when available
    # "Durham":   "durham",
    # "Apex":     "apex",
}

LEGISTAR_BASE = "https://webapi.legistar.com/v1"
HAIKU_MODEL   = "claude-haiku-4-5-20251001"
MAX_MATTERS   = 60   # How many recent matters to pull per city per run

DATA_DIR.mkdir(parents=True, exist_ok=True)


# ── Logging ───────────────────────────────────────────────────────────────────

def log(msg: str):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


# ── Database ──────────────────────────────────────────────────────────────────

def init_cases_db():
    conn = sqlite3.connect(CASES_DB)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS cases (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            case_number             TEXT,
            city                    TEXT,
            meeting_date            TEXT,
            title                   TEXT,
            change_requested        TEXT,
            acreage                 REAL,
            outcome                 TEXT,
            dev_type                TEXT,
            height_stories          INTEGER,
            adjacent_sf             INTEGER DEFAULT 0,
            traffic_study           INTEGER DEFAULT 0,
            affordable_housing      INTEGER DEFAULT 0,
            transition_buffer       INTEGER DEFAULT 0,
            staff_recommendation    TEXT,
            vote_yes                INTEGER,
            vote_no                 INTEGER,
            public_oppose           INTEGER,
            public_support          INTEGER,
            notes                   TEXT,
            source                  TEXT DEFAULT 'legistar',
            fetched_at              TEXT,
            UNIQUE(city, case_number)
        );
        CREATE INDEX IF NOT EXISTS idx_cases_city    ON cases(city);
        CREATE INDEX IF NOT EXISTS idx_cases_outcome ON cases(outcome);
        CREATE INDEX IF NOT EXISTS idx_cases_date    ON cases(meeting_date);
    """)
    conn.commit()
    conn.close()


def get_existing_keys(city: str) -> set:
    conn = sqlite3.connect(CASES_DB)
    rows = conn.execute("SELECT case_number FROM cases WHERE city=?", (city,)).fetchall()
    conn.close()
    return {r[0] for r in rows}


def insert_cases(cases: list):
    if not cases:
        return 0
    conn = sqlite3.connect(CASES_DB)
    added = 0
    for c in cases:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO cases
                (case_number, city, meeting_date, title, change_requested,
                 acreage, outcome, dev_type, height_stories, adjacent_sf,
                 traffic_study, affordable_housing, transition_buffer,
                 staff_recommendation, vote_yes, vote_no, public_oppose,
                 public_support, notes, source, fetched_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                c.get("case_number"), c.get("city"), c.get("meeting_date"),
                c.get("title"), c.get("change_requested"),
                c.get("acreage"), c.get("outcome"), c.get("dev_type"),
                c.get("height_stories"), int(bool(c.get("adjacent_sf"))),
                int(bool(c.get("traffic_study"))), int(bool(c.get("affordable_housing"))),
                int(bool(c.get("transition_buffer"))),
                c.get("staff_recommendation"),
                c.get("vote_yes"), c.get("vote_no"),
                c.get("public_oppose"), c.get("public_support"),
                c.get("notes"), c.get("source", "legistar"),
                datetime.datetime.utcnow().isoformat()
            ))
            if conn.execute("SELECT changes()").fetchone()[0]:
                added += 1
        except Exception as e:
            log(f"  ✗ Insert error for {c.get('case_number')}: {e}")
    conn.commit()
    conn.close()
    return added


# ── Legistar Fetch ────────────────────────────────────────────────────────────

def fetch_legistar(city: str, client_id: str) -> list:
    """Pull raw matters from Legistar API."""
    try:
        import requests
    except ImportError:
        log("ERROR: pip install requests --break-system-packages")
        return []

    headers = {"User-Agent": "LandWorks/1.0"}
    matters = []

    try:
        # Get matter types to find rezoning ID
        tr = requests.get(f"{LEGISTAR_BASE}/{client_id}/mattertypes",
                         headers=headers, timeout=20)
        tr.raise_for_status()
        types = tr.json()
        rez_type = next((t for t in types if any(
            x in t.get("MatterTypeName","").lower()
            for x in ["rezoning","zoning","rezoning petition"]
        )), None)

        # Fetch recent matters
        url = f"{LEGISTAR_BASE}/{client_id}/matters?$top={MAX_MATTERS}&$orderby=MatterLastModifiedUtc desc"
        if rez_type:
            url += f"&$filter=MatterTypeId eq {rez_type['MatterTypeId']}"

        mr = requests.get(url, headers=headers, timeout=30)
        mr.raise_for_status()
        matters = mr.json()
        log(f"  Legistar: {len(matters)} matters fetched for {city}")

    except Exception as e:
        log(f"  ✗ Legistar fetch failed for {city}: {e}")

    return matters


def get_vote_counts(client_id: str, matter_id: int) -> tuple:
    """Try to get vote tallies from matter history. Returns (yes, no)."""
    try:
        import requests
        hr = requests.get(
            f"{LEGISTAR_BASE}/{client_id}/matters/{matter_id}/histories",
            headers={"User-Agent": "LandWorks/1.0"}, timeout=10
        )
        if hr.ok:
            for h in hr.json():
                if isinstance(h.get("MatterHistoryPassedCount"), (int, float)):
                    return int(h["MatterHistoryPassedCount"]), int(h.get("MatterHistoryFailedCount", 0))
    except Exception:
        pass
    return None, None


# ── Claude Classification ─────────────────────────────────────────────────────

def classify_matters_with_claude(city: str, matters: list) -> list:
    """
    Send a batch of raw Legistar matters to Claude Haiku for classification.
    Returns a list of structured case dicts.
    """
    if not API_KEY:
        log("ERROR: Set ANTHROPIC_API_KEY env var or set API_KEY in this script")
        return []

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=API_KEY)
    except ImportError:
        log("ERROR: pip install anthropic --break-system-packages")
        return []

    # Build compact summary of each matter to send to Claude
    matter_summaries = []
    for m in matters:
        matter_summaries.append({
            "id":     m.get("MatterId"),
            "file":   m.get("MatterFile", ""),
            "name":   m.get("MatterName", ""),
            "title":  m.get("MatterTitle", ""),
            "status": m.get("MatterStatusName", ""),
            "intro":  (m.get("MatterIntroDate") or "")[:10],
        })

    prompt = f"""You are a land development data analyst. Below are {len(matter_summaries)} rezoning matters from {city}'s Legistar system.

For each matter, extract and classify the following fields. Return a JSON array with one object per matter:

{{
  "case_number": "the MatterFile value (e.g. Z-12-25)",
  "title": "short plain-English title of what is being rezoned",
  "change_requested": "1-sentence description of the zoning change",
  "acreage": <number or null>,
  "outcome": "approved" | "denied" | "withdrawn" | "continued" | "pending" | "unknown",
  "dev_type": "residential" | "commercial" | "mixed_use" | "industrial" | "office" | "hotel" | "senior_living" | "storage" | "municipal" | "unknown",
  "height_stories": <integer or null>,
  "adjacent_sf": <true if adjacent to single family neighborhoods, false if not, null if unknown>,
  "traffic_study": <true | false | null>,
  "affordable_housing": <true | false | null>,
  "staff_recommendation": "approve" | "deny" | "unknown",
  "notes": "any important detail in 1 sentence or null"
}}

Rules:
- Infer outcome from status: "passed"/"approved" → "approved", "failed"/"denied" → "denied", "withdrawn" → "withdrawn", "in committee"/"tabled" → "continued", "introduced" → "pending"
- Infer dev_type from title/name keywords
- If a field can't be determined, use null
- Return ONLY the JSON array, no other text

MATTERS:
{json.dumps(matter_summaries, indent=2)}"""

    try:
        log(f"  Sending {len(matters)} matters to Claude Haiku for classification...")
        resp = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}]
        )
        text = resp.content[0].text.strip()

        # Parse JSON
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        classified = json.loads(text.strip())
        log(f"  ✓ Claude classified {len(classified)} cases")
        return classified

    except json.JSONDecodeError as e:
        log(f"  ✗ JSON parse error from Claude: {e}")
        log(f"  Raw response: {text[:300]}")
        return []
    except Exception as e:
        log(f"  ✗ Claude API error: {e}")
        return []


# ── Main Pipeline ─────────────────────────────────────────────────────────────

def run_city(city: str, client_id: str, dry_run: bool = False):
    log(f"\n{'='*50}")
    log(f"Processing: {city} (Legistar: {client_id})")

    # 1. Get existing case numbers so we don't re-add
    existing = get_existing_keys(city)
    log(f"  Existing cases in DB: {len(existing)}")

    # 2. Fetch from Legistar
    matters = fetch_legistar(city, client_id)
    if not matters:
        log(f"  No matters returned — skipping {city}")
        return

    # 3. Filter to only new matters
    new_matters = [m for m in matters
                   if (m.get("MatterFile") or f"LEG-{m.get('MatterId')}") not in existing]
    log(f"  New matters to classify: {len(new_matters)}")

    if not new_matters:
        log(f"  Nothing new for {city}")
        return

    # 4. Classify with Claude (in batches of 20 to stay within token limits)
    all_classified = []
    batch_size = 20
    for i in range(0, len(new_matters), batch_size):
        batch = new_matters[i:i+batch_size]
        classified = classify_matters_with_claude(city, batch)
        all_classified.extend(classified)
        if len(new_matters) > batch_size:
            time.sleep(1)  # Brief pause between batches

    # 5. Enrich with vote counts and merge city/date
    enriched = []
    for c in all_classified:
        case_num = c.get("case_number", "")
        # Find matching raw matter for date + vote data
        raw = next((m for m in new_matters
                    if m.get("MatterFile","") == case_num), None)
        meeting_date = None
        vote_yes = vote_no = None
        if raw:
            meeting_date = (raw.get("MatterIntroDate") or "")[:10] or None
            vote_yes, vote_no = get_vote_counts(client_id, raw.get("MatterId", 0))

        enriched.append({
            **c,
            "city":         city,
            "meeting_date": meeting_date,
            "vote_yes":     vote_yes,
            "vote_no":      vote_no,
            "source":       "legistar_auto",
        })

    if dry_run:
        log(f"  DRY RUN — would have saved {len(enriched)} cases:")
        for c in enriched[:5]:
            log(f"    {c.get('case_number')} | {c.get('outcome')} | {c.get('title','')[:60]}")
        return

    # 6. Write to database
    added = insert_cases(enriched)
    log(f"  ✓ Saved {added} new cases for {city}")


def main():
    parser = argparse.ArgumentParser(description="LandWorks auto data fetch")
    parser.add_argument("--city", help="Run for specific city only (e.g. Raleigh)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch + classify but don't save")
    parser.add_argument("--api-key", help="Anthropic API key (or set ANTHROPIC_API_KEY env var)")
    args = parser.parse_args()

    global API_KEY
    if args.api_key:
        API_KEY = args.api_key

    if not API_KEY:
        print("ERROR: Set ANTHROPIC_API_KEY environment variable or pass --api-key")
        print("  export ANTHROPIC_API_KEY=sk-ant-api03-...")
        sys.exit(1)

    log(f"\n{'='*60}")
    log("LandWorks Auto-Fetch starting")
    log(f"Database: {CASES_DB}")
    log(f"Dry run: {args.dry_run}")

    init_cases_db()

    cities = LEGISTAR_CLIENTS
    if args.city:
        if args.city not in LEGISTAR_CLIENTS:
            log(f"ERROR: '{args.city}' not in LEGISTAR_CLIENTS. Available: {list(LEGISTAR_CLIENTS.keys())}")
            sys.exit(1)
        cities = {args.city: LEGISTAR_CLIENTS[args.city]}

    for city, client_id in cities.items():
        try:
            run_city(city, client_id, dry_run=args.dry_run)
        except Exception as e:
            log(f"  ✗ Unhandled error for {city}: {e}")

    log(f"\n✅ Auto-fetch complete")


if __name__ == "__main__":
    main()
