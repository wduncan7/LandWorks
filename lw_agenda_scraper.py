#!/usr/bin/env python3
"""
LandWorks Agenda Scraper — PDF-based city pipeline.

Handles Wake County municipalities that are NOT on Legistar (Cary, Apex,
Wake Forest, Holly Springs, Fuquay-Varina, Garner, Morrisville, etc.)

What it does:
  1. Fetches planning/zoning board agenda PDFs from each city's website
  2. Extracts text from the PDFs
  3. Sends batches to Claude Haiku to extract rezoning/development cases
  4. Writes structured cases to ~/.wake_county_data/cases.db
     (same DB as lw_auto_fetch.py — LandWorks loads both together)

Usage:
  python3 lw_agenda_scraper.py                   # All cities
  python3 lw_agenda_scraper.py --city Cary       # One city
  python3 lw_agenda_scraper.py --dry-run         # Extract but don't save
  python3 lw_agenda_scraper.py --pdf /path/to.pdf --city Cary  # Manual PDF

Cron (nightly, 30min after lw_auto_fetch.py):
  30 2 * * * ANTHROPIC_API_KEY=sk-... python3 ~/LandWorks/lw_agenda_scraper.py >> ~/LandWorks/agenda.log 2>&1

Requirements:
  pip install anthropic requests pdfplumber --break-system-packages
"""

import sys
import os
import re
import json
import sqlite3
import datetime
import argparse
import hashlib
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

# ── Config ─────────────────────────────────────────────────────────────────────

DATA_DIR  = Path.home() / ".wake_county_data"
CASES_DB  = DATA_DIR / "cases.db"
PDF_CACHE = DATA_DIR / "agenda_pdfs"
LOG_FILE  = DATA_DIR / "agenda.log"

API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
HAIKU   = "claude-haiku-4-5-20251001"

DATA_DIR.mkdir(parents=True, exist_ok=True)
PDF_CACHE.mkdir(parents=True, exist_ok=True)

# ── City Agenda Sources ────────────────────────────────────────────────────────
# Each entry defines how to find agenda PDFs for that city.
# "agenda_index" = page that lists recent agendas/minutes
# "pdf_pattern"  = regex to match PDF links on that page
# "base_url"     = prepend to relative links
# "board"        = which board handles rezonings (for extraction prompt context)

CITY_SOURCES = {
    "Cary": {
        "agenda_index": "https://www.cary.nc.gov/government/town-council/council-meetings/agendas-minutes",
        "pdf_pattern":  r'href=["\']([^"\']*(?:agenda|minutes)[^"\']*\.pdf)["\']',
        "base_url":     "https://www.cary.nc.gov",
        "board":        "Cary Town Council",
        "zoning_board_index": "https://www.cary.nc.gov/government/boards-committees/planning-and-zoning-board/agendas-minutes",
    },
    "Apex": {
        "agenda_index": "https://www.apexnc.org/339/Meeting-Agendas-Minutes",
        "pdf_pattern":  r'href=["\']([^"\']*\.pdf)["\']',
        "base_url":     "https://www.apexnc.org",
        "board":        "Apex Town Council / Planning Board",
        "zoning_board_index": "https://www.apexnc.org/339/Meeting-Agendas-Minutes",
    },
    "Wake Forest": {
        "agenda_index": "https://www.wakeforestnc.gov/town-government/boards-and-committees/planning-board",
        "pdf_pattern":  r'href=["\']([^"\']*\.pdf)["\']',
        "base_url":     "https://www.wakeforestnc.gov",
        "board":        "Wake Forest Planning Board / Town Council",
    },
    "Holly Springs": {
        "agenda_index": "https://www.hollyspringsnc.gov/government/town-council/agendas-minutes",
        "pdf_pattern":  r'href=["\']([^"\']*\.pdf)["\']',
        "base_url":     "https://www.hollyspringsnc.gov",
        "board":        "Holly Springs Town Council",
    },
    "Fuquay-Varina": {
        "agenda_index": "https://www.fuquay-varina.org/313/Agendas-Minutes",
        "pdf_pattern":  r'href=["\']([^"\']*\.pdf)["\']',
        "base_url":     "https://www.fuquay-varina.org",
        "board":        "Fuquay-Varina Town Council / Planning Board",
    },
    "Garner": {
        "agenda_index": "https://www.garnernc.gov/government/town-council/agendas-minutes",
        "pdf_pattern":  r'href=["\']([^"\']*\.pdf)["\']',
        "base_url":     "https://www.garnernc.gov",
        "board":        "Garner Town Council / Planning Board",
    },
    "Morrisville": {
        "agenda_index": "https://www.morrisvillenc.gov/government/town-council/agendas-and-minutes",
        "pdf_pattern":  r'href=["\']([^"\']*\.pdf)["\']',
        "base_url":     "https://www.morrisvillenc.gov",
        "board":        "Morrisville Town Council / Planning Board",
    },
    "Knightdale": {
        "agenda_index": "https://www.knightdalenc.gov/government/town-council",
        "pdf_pattern":  r'href=["\']([^"\']*\.pdf)["\']',
        "base_url":     "https://www.knightdalenc.gov",
        "board":        "Knightdale Town Council",
    },
    "Wendell": {
        "agenda_index": "https://www.wendellnc.gov/252/Agendas-Minutes",
        "pdf_pattern":  r'href=["\']([^"\']*\.pdf)["\']',
        "base_url":     "https://www.wendellnc.gov",
        "board":        "Wendell Town Council / Planning Board",
    },
    "Zebulon": {
        "agenda_index": "https://www.zebulonnc.gov/government/agendas-minutes",
        "pdf_pattern":  r'href=["\']([^"\']*\.pdf)["\']',
        "base_url":     "https://www.zebulonnc.gov",
        "board":        "Zebulon Town Council",
    },
}

# How many recent PDFs to check per city per run (avoid re-processing old ones)
MAX_PDFS_PER_CITY = 4

# Only process PDFs from the last N days (avoid full archive re-scan)
MAX_AGE_DAYS = 90


# ── Logging ────────────────────────────────────────────────────────────────────

def log(msg: str):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


# ── PDF Cache ──────────────────────────────────────────────────────────────────

def pdf_cache_path(url: str) -> Path:
    """Return local cache path for a PDF URL."""
    h = hashlib.md5(url.encode()).hexdigest()[:12]
    return PDF_CACHE / f"{h}.pdf"


def already_processed(url: str) -> bool:
    """Check if we've already extracted cases from this PDF."""
    marker = PDF_CACHE / f"{hashlib.md5(url.encode()).hexdigest()[:12]}.done"
    return marker.exists()


def mark_processed(url: str):
    marker = PDF_CACHE / f"{hashlib.md5(url.encode()).hexdigest()[:12]}.done"
    marker.touch()


# ── Web Fetch ──────────────────────────────────────────────────────────────────

def fetch_url(url: str, timeout: int = 20) -> Optional[bytes]:
    try:
        import requests
        r = requests.get(url, timeout=timeout,
                        headers={"User-Agent": "LandWorks/1.0 (land development research)"})
        r.raise_for_status()
        return r.content
    except Exception as e:
        log(f"  ✗ Fetch failed for {url[:80]}: {e}")
        return None


def find_pdf_links(html: str, base_url: str, pattern: str) -> list:
    """Extract PDF links from HTML using regex pattern."""
    links = re.findall(pattern, html, re.IGNORECASE)
    result = []
    for link in links:
        # Make absolute URL
        if link.startswith("http"):
            full = link
        elif link.startswith("/"):
            # Extract base domain
            from urllib.parse import urlparse
            parsed = urlparse(base_url)
            full = f"{parsed.scheme}://{parsed.netloc}{link}"
        else:
            full = urljoin(base_url, link)
        if full not in result:
            result.append(full)
    return result


def is_recent_pdf(url: str) -> bool:
    """Try to determine if a PDF is recent based on URL date patterns."""
    # Look for year/month patterns in URL
    now = datetime.datetime.now()
    cutoff = now - datetime.timedelta(days=MAX_AGE_DAYS)

    # Common patterns: 2026-04, 2026/04, 04-2026, April-2026, etc.
    year_matches = re.findall(r'202[0-9]', url)
    if year_matches:
        year = int(year_matches[-1])
        if year < cutoff.year:
            return False
        if year == cutoff.year:
            month_matches = re.findall(r'(?:^|[-/_])([01]?\d)(?:[-/_]|$)', url)
            if month_matches:
                month = int(month_matches[0])
                if month < cutoff.month:
                    return False
    return True


# ── PDF Text Extraction ────────────────────────────────────────────────────────

def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        import pdfplumber
        import io
        text_parts = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages[:20]:  # Cap at 20 pages
                t = page.extract_text()
                if t:
                    text_parts.append(t)
        return "\n".join(text_parts)
    except ImportError:
        log("  pdfplumber not installed — trying PyPDF2 fallback")
        return extract_pdf_text_pypdf2(pdf_bytes)
    except Exception as e:
        log(f"  ✗ PDF extraction error: {e}")
        return ""


def extract_pdf_text_pypdf2(pdf_bytes: bytes) -> str:
    """Fallback: extract text using PyPDF2."""
    try:
        import PyPDF2
        import io
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        parts = []
        for page in reader.pages[:20]:
            t = page.extract_text()
            if t:
                parts.append(t)
        return "\n".join(parts)
    except Exception as e:
        log(f"  ✗ PyPDF2 also failed: {e}")
        return ""


# ── Claude Extraction ──────────────────────────────────────────────────────────

EXTRACTION_PROMPT = """You are a land development data analyst reviewing a {city} planning/zoning board agenda or meeting minutes.

Extract ALL rezoning, conditional use permit, subdivision, annexation, or special use permit cases you can find.
For each case return a JSON object with these fields:

{{
  "case_number": "the case/file number (e.g. PZ-24-001, RZ-2025-04, Z-12-25, or 'UNKNOWN' if not found)",
  "title": "short plain-English description of what is being proposed",
  "change_requested": "one sentence: what zoning change or approval is being requested",
  "acreage": <number or null>,
  "location": "street address or general location description",
  "applicant": "applicant or owner name if mentioned",
  "outcome": "approved" | "denied" | "withdrawn" | "continued" | "pending" | "tabled" | "unknown",
  "dev_type": "residential" | "commercial" | "mixed_use" | "industrial" | "office" | "hotel" | "senior_living" | "storage" | "municipal" | "subdivision" | "annexation" | "unknown",
  "height_stories": <integer or null>,
  "adjacent_sf": <true if text mentions adjacent single family, else false>,
  "traffic_study": <true if traffic study mentioned, else false>,
  "affordable_housing": <true if affordable housing mentioned, else false>,
  "transition_buffer": <true if buffer or transition mentioned, else false>,
  "staff_recommendation": "approve" | "deny" | "unknown",
  "vote_yes": <integer or null>,
  "vote_no": <integer or null>,
  "meeting_date": "YYYY-MM-DD if determinable from document, else null",
  "notes": "any important conditions, stipulations, or context in 1-2 sentences"
}}

Rules:
- Only include actual case items — skip procedural votes, consent agendas, budget items, etc.
- If you find no rezoning/development cases, return an empty array []
- Return ONLY a valid JSON array, no other text

CITY: {city}
BOARD: {board}

AGENDA TEXT (first 12000 characters):
{text}"""


def extract_cases_from_text(city: str, board: str, text: str, pdf_url: str) -> list:
    """Send agenda text to Claude Haiku and extract structured case data."""
    if not API_KEY:
        log("  ERROR: ANTHROPIC_API_KEY not set")
        return []
    if not text.strip():
        log("  No text extracted from PDF — skipping")
        return []

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=API_KEY)

        # Truncate text to keep within token limits
        truncated = text[:12000]

        prompt = EXTRACTION_PROMPT.format(
            city=city, board=board,
            text=truncated
        )

        resp = client.messages.create(
            model=HAIKU,
            max_tokens=3000,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = resp.content[0].text.strip()

        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = re.sub(r'^```[a-z]*\n?', '', raw)
            raw = re.sub(r'\n?```$', '', raw)

        cases = json.loads(raw.strip())
        if not isinstance(cases, list):
            cases = []

        # Tag each case with source info
        for c in cases:
            c["city"]   = city
            c["source"] = "agenda_pdf"
            c["_pdf_url"] = pdf_url

        log(f"  Claude extracted {len(cases)} cases from {city} PDF")
        return cases

    except json.JSONDecodeError as e:
        log(f"  ✗ JSON parse error: {e} — raw: {raw[:200]}")
        return []
    except Exception as e:
        log(f"  ✗ Claude API error: {e}")
        return []


# ── Database ───────────────────────────────────────────────────────────────────

def init_db():
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
            source                  TEXT DEFAULT 'agenda_pdf',
            fetched_at              TEXT,
            UNIQUE(city, case_number)
        );
        CREATE INDEX IF NOT EXISTS idx_cases_city    ON cases(city);
        CREATE INDEX IF NOT EXISTS idx_cases_outcome ON cases(outcome);
        CREATE INDEX IF NOT EXISTS idx_cases_date    ON cases(meeting_date);

        -- Track which PDFs we've already processed
        CREATE TABLE IF NOT EXISTS processed_pdfs (
            url        TEXT PRIMARY KEY,
            city       TEXT,
            processed_at TEXT,
            cases_found INTEGER DEFAULT 0
        );
    """)
    conn.commit()
    conn.close()


def is_pdf_processed(url: str) -> bool:
    conn = sqlite3.connect(CASES_DB)
    row = conn.execute("SELECT 1 FROM processed_pdfs WHERE url=?", (url,)).fetchone()
    conn.close()
    return row is not None


def mark_pdf_processed(url: str, city: str, cases_found: int):
    conn = sqlite3.connect(CASES_DB)
    conn.execute("""
        INSERT OR REPLACE INTO processed_pdfs (url, city, processed_at, cases_found)
        VALUES (?, ?, ?, ?)
    """, (url, city, datetime.datetime.utcnow().isoformat(), cases_found))
    conn.commit()
    conn.close()


def insert_cases(cases: list) -> int:
    if not cases:
        return 0
    conn = sqlite3.connect(CASES_DB)
    added = 0
    for c in cases:
        case_num = c.get("case_number") or "UNKNOWN"
        # Skip pure unknowns with no title
        if case_num == "UNKNOWN" and not c.get("title"):
            continue
        # Make case_number unique per city+date if UNKNOWN
        if case_num == "UNKNOWN":
            case_num = f"PDF-{c.get('city','?')[:3].upper()}-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}-{added}"
        try:
            conn.execute("""
                INSERT OR IGNORE INTO cases
                (case_number, city, meeting_date, title, change_requested,
                 acreage, outcome, dev_type, height_stories, adjacent_sf,
                 traffic_study, affordable_housing, transition_buffer,
                 staff_recommendation, vote_yes, vote_no,
                 notes, source, fetched_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                case_num,
                c.get("city"), c.get("meeting_date"),
                c.get("title"), c.get("change_requested"),
                c.get("acreage"), c.get("outcome"), c.get("dev_type"),
                c.get("height_stories"),
                int(bool(c.get("adjacent_sf"))),
                int(bool(c.get("traffic_study"))),
                int(bool(c.get("affordable_housing"))),
                int(bool(c.get("transition_buffer"))),
                c.get("staff_recommendation"),
                c.get("vote_yes"), c.get("vote_no"),
                c.get("notes"),
                c.get("source", "agenda_pdf"),
                datetime.datetime.utcnow().isoformat()
            ))
            if conn.execute("SELECT changes()").fetchone()[0]:
                added += 1
        except Exception as e:
            log(f"  ✗ Insert error for {case_num}: {e}")
    conn.commit()
    conn.close()
    return added


# ── Manual PDF mode ────────────────────────────────────────────────────────────

def process_manual_pdf(pdf_path: str, city: str, dry_run: bool = False):
    """Process a single PDF file that Tyler downloaded manually."""
    log(f"\nManual PDF: {pdf_path} | City: {city}")
    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()

    text = extract_pdf_text(pdf_bytes)
    if not text:
        log("  Could not extract text from PDF")
        return

    src = CITY_SOURCES.get(city, {})
    board = src.get("board", f"{city} Planning Board")
    cases = extract_cases_from_text(city, board, text, f"file://{pdf_path}")

    if dry_run:
        log(f"  DRY RUN — found {len(cases)} cases:")
        for c in cases:
            log(f"    {c.get('case_number')} | {c.get('outcome')} | {c.get('title','')[:70]}")
        return

    added = insert_cases(cases)
    log(f"  ✓ Saved {added} new cases from manual PDF")


# ── Auto City Pipeline ─────────────────────────────────────────────────────────

def run_city(city: str, src: dict, dry_run: bool = False):
    log(f"\n{'='*50}")
    log(f"Processing: {city}")

    try:
        import requests
    except ImportError:
        log("ERROR: pip install requests --break-system-packages")
        return

    board = src.get("board", f"{city} Planning Board")
    total_added = 0

    # Check both main council and zoning board index pages
    index_urls = [src["agenda_index"]]
    if src.get("zoning_board_index") and src["zoning_board_index"] != src["agenda_index"]:
        index_urls.append(src["zoning_board_index"])

    all_pdf_urls = []
    for index_url in index_urls:
        html_bytes = fetch_url(index_url)
        if not html_bytes:
            continue
        html = html_bytes.decode("utf-8", errors="replace")
        pdf_urls = find_pdf_links(html, src["base_url"], src["pdf_pattern"])

        # Filter to recent PDFs and limit count
        recent = [u for u in pdf_urls if is_recent_pdf(u)]
        log(f"  Found {len(pdf_urls)} PDF links, {len(recent)} appear recent")
        all_pdf_urls.extend(recent)

    # Deduplicate
    seen = set()
    unique_pdfs = []
    for u in all_pdf_urls:
        if u not in seen:
            seen.add(u)
            unique_pdfs.append(u)

    # Process up to MAX_PDFS_PER_CITY new ones
    processed = 0
    for pdf_url in unique_pdfs:
        if processed >= MAX_PDFS_PER_CITY:
            break
        if is_pdf_processed(pdf_url):
            log(f"  ↷ Already processed: {pdf_url[-60:]}")
            continue

        log(f"  → Fetching PDF: {pdf_url[-70:]}")
        pdf_bytes = fetch_url(pdf_url, timeout=30)
        if not pdf_bytes:
            continue

        text = extract_pdf_text(pdf_bytes)
        if len(text) < 200:
            log(f"  ↷ Too little text extracted ({len(text)} chars) — skipping")
            mark_pdf_processed(pdf_url, city, 0)
            processed += 1
            continue

        cases = extract_cases_from_text(city, board, text, pdf_url)
        mark_pdf_processed(pdf_url, city, len(cases))
        processed += 1

        if dry_run:
            log(f"  DRY RUN — {len(cases)} cases found:")
            for c in cases[:5]:
                log(f"    {c.get('case_number')} | {c.get('outcome')} | {c.get('title','')[:60]}")
            continue

        added = insert_cases(cases)
        total_added += added
        log(f"  ✓ +{added} new cases from this PDF")

        time.sleep(1)  # Brief pause between PDFs

    if not dry_run:
        log(f"  City total: +{total_added} new cases for {city}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="LandWorks agenda PDF scraper")
    parser.add_argument("--city",    help="Run for specific city only")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--pdf",     help="Process a specific local PDF file")
    parser.add_argument("--api-key", help="Anthropic API key")
    args = parser.parse_args()

    global API_KEY
    if args.api_key:
        API_KEY = args.api_key
    if not API_KEY:
        print("ERROR: Set ANTHROPIC_API_KEY env var or pass --api-key")
        sys.exit(1)

    log(f"\n{'='*60}")
    log("LandWorks Agenda Scraper starting")
    log(f"Database: {CASES_DB}")
    log(f"Dry run: {args.dry_run}")

    init_db()

    # Manual PDF mode
    if args.pdf:
        if not args.city:
            print("ERROR: --pdf requires --city")
            sys.exit(1)
        process_manual_pdf(args.pdf, args.city, dry_run=args.dry_run)
        return

    # Auto mode — scrape city websites
    cities = CITY_SOURCES
    if args.city:
        if args.city not in CITY_SOURCES:
            log(f"ERROR: '{args.city}' not configured. Available: {list(CITY_SOURCES.keys())}")
            sys.exit(1)
        cities = {args.city: CITY_SOURCES[args.city]}

    for city, src in cities.items():
        try:
            run_city(city, src, dry_run=args.dry_run)
        except Exception as e:
            log(f"  ✗ Unhandled error for {city}: {e}")

    log(f"\n✅ Agenda scraper complete")


if __name__ == "__main__":
    main()
