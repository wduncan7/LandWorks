#!/usr/bin/env python3
"""
LandWorks Video Scraper — Council meeting video → case + council intelligence data.

Two modes (fastest first):
  1. CAPTIONS mode: grabs YouTube auto-captions via yt-dlp (no audio download needed)
  2. WHISPER mode: downloads audio + transcribes locally with Whisper (offline/private streams)

Then sends transcript to Claude Haiku to extract:
  - Rezoning/development case outcomes + votes
  - Council member statements, concerns, sentiment per case
  - Conditions requested by each member

Writes to:
  - ~/.wake_county_data/cases.db  (same DB as lw_auto_fetch + lw_agenda_scraper)
  - council_intelligence table    (member vote history + concern patterns)

Usage:
  python3 lw_video_scraper.py --url "https://www.youtube.com/watch?v=XYZ" --city Apex
  python3 lw_video_scraper.py --channel "https://www.youtube.com/c/TownofApexGov" --city Apex
  python3 lw_video_scraper.py --channel "https://www.youtube.com/c/TownofApexGov" --city Apex --count 3
  python3 lw_video_scraper.py --audio /path/to/meeting.mp3 --city Apex   # Local file
  python3 lw_video_scraper.py --dry-run --url "..." --city Apex           # Don't save

Cron (weekly, Sunday 3am — after nightly scripts):
  0 3 * * 0 ANTHROPIC_API_KEY=sk-... python3 ~/LandWorks/lw_video_scraper.py \\
    --channel "https://www.youtube.com/c/TownofApexGov" --city Apex --count 2 \\
    >> ~/LandWorks/video.log 2>&1

Requirements:
  pip install yt-dlp anthropic --break-system-packages
  pip install openai-whisper --break-system-packages  # Only for WHISPER mode
"""

import sys, os, json, re, sqlite3, datetime, argparse, subprocess, time, hashlib, tempfile
from pathlib import Path

DATA_DIR   = Path.home() / ".wake_county_data"
CASES_DB   = DATA_DIR / "cases.db"
MEDIA_DIR  = DATA_DIR / "meeting_media"
LOG_FILE   = DATA_DIR / "video.log"

API_KEY    = os.environ.get("ANTHROPIC_API_KEY", "")
HAIKU      = "claude-haiku-4-5-20251001"
SONNET     = "claude-sonnet-4-6"


def _parse_json_array(raw: str) -> list:
    """Robustly extract the first JSON array from a Claude response."""
    # Strip markdown fences
    raw = re.sub(r'^```[a-z]*\n?', '', raw.strip())
    raw = re.sub(r'\n?```.*$', '', raw, flags=re.DOTALL)
    raw = raw.strip()
    if not raw:
        return []
    # Find the first [ ... ] block
    start = raw.find('[')
    if start == -1:
        return []
    depth = 0
    for i, ch in enumerate(raw[start:], start):
        if ch == '[':
            depth += 1
        elif ch == ']':
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(raw[start:i+1])
                except json.JSONDecodeError:
                    return []
    return []

DATA_DIR.mkdir(parents=True, exist_ok=True)
MEDIA_DIR.mkdir(parents=True, exist_ok=True)

# ── City YouTube channels ──────────────────────────────────────────────────────
# Verified channels marked ✓ | Unverified marked with ? (may need updating)
CITY_CHANNELS = {
    # Wake County municipalities
    "Apex":         "https://www.youtube.com/c/TownofApexGov",           # ✓ confirmed
    "Raleigh":      "https://www.youtube.com/@CityofRaleighGov",         # ✓
    "Cary":         "https://www.youtube.com/@TownofCaryNC",             # ? verify
    "Wake Forest":  "https://www.youtube.com/@TownofWakeForestNC",       # ? verify
    "Holly Springs":"https://www.youtube.com/@TownofHollySpringsNC",     # ? verify
    "Fuquay-Varina":"https://www.youtube.com/@TownofFuquayVarinaNC",     # ? verify
    "Garner":       "https://www.youtube.com/@TownofGarnerNC",           # ? verify
    "Morrisville":  "https://www.youtube.com/@TownofMorrisvilleNC",      # ? verify
    "Knightdale":   "https://www.youtube.com/@TownofKnightdaleNC",       # ? verify
    "Wendell":      "https://www.youtube.com/@TownofWendellNC",          # ? verify
    "Zebulon":      "https://www.youtube.com/@TownofZebulonNC",          # ? verify
    "Rolesville":   "https://www.youtube.com/@TownofRolesvilleNC",       # ? verify
    # County board — handles rezonings in unincorporated Wake County
    "Wake County":  "https://www.youtube.com/@WakeCountyGovNC",          # ? verify
}

# Keywords to identify planning/rezoning meeting videos
MEETING_KEYWORDS = [
    "council", "planning", "zoning", "rezoning", "board",
    "regular meeting", "public hearing", "town meeting", "city council"
]


# ── Logging ────────────────────────────────────────────────────────────────────
def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


# ── Database ───────────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(CASES_DB)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS cases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            case_number TEXT, city TEXT, meeting_date TEXT, title TEXT,
            change_requested TEXT, acreage REAL, outcome TEXT, dev_type TEXT,
            height_stories INTEGER, adjacent_sf INTEGER DEFAULT 0,
            traffic_study INTEGER DEFAULT 0, affordable_housing INTEGER DEFAULT 0,
            transition_buffer INTEGER DEFAULT 0, staff_recommendation TEXT,
            vote_yes INTEGER, vote_no INTEGER, public_oppose INTEGER, public_support INTEGER,
            notes TEXT, source TEXT DEFAULT 'video', fetched_at TEXT,
            UNIQUE(city, case_number)
        );
        CREATE INDEX IF NOT EXISTS idx_cases_city ON cases(city);

        CREATE TABLE IF NOT EXISTS council_intelligence (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL,
            member_name TEXT NOT NULL,
            role TEXT,
            case_number TEXT,
            meeting_date TEXT,
            vote TEXT,
            sentiment TEXT,
            key_quote TEXT,
            stated_concerns TEXT,
            conditions_requested TEXT,
            video_url TEXT,
            extracted_at TEXT,
            UNIQUE(city, member_name, case_number)
        );
        CREATE INDEX IF NOT EXISTS idx_ci_city   ON council_intelligence(city);
        CREATE INDEX IF NOT EXISTS idx_ci_member ON council_intelligence(member_name);

        CREATE TABLE IF NOT EXISTS processed_videos (
            video_id TEXT PRIMARY KEY,
            city TEXT,
            title TEXT,
            processed_at TEXT,
            cases_found INTEGER DEFAULT 0,
            members_updated INTEGER DEFAULT 0
        );
    """)
    conn.commit()
    conn.close()


def is_video_processed(video_id):
    conn = sqlite3.connect(CASES_DB)
    r = conn.execute("SELECT 1 FROM processed_videos WHERE video_id=?", (video_id,)).fetchone()
    conn.close()
    return r is not None


def mark_video_processed(video_id, city, title, cases_found, members_updated):
    conn = sqlite3.connect(CASES_DB)
    conn.execute("""INSERT OR REPLACE INTO processed_videos
        (video_id, city, title, processed_at, cases_found, members_updated)
        VALUES (?,?,?,?,?,?)""",
        (video_id, city, title, datetime.datetime.utcnow().isoformat(), cases_found, members_updated))
    conn.commit()
    conn.close()


# ── yt-dlp helpers ─────────────────────────────────────────────────────────────
def get_recent_videos(channel_url, city, count=3):
    """Get recent meeting videos from a YouTube channel."""
    log(f"  Scanning channel for recent {city} meeting videos...")
    try:
        result = subprocess.run([
            "yt-dlp", "--flat-playlist", "--playlist-end", str(count * 5),
            "--print", "%(id)s|||%(title)s|||%(upload_date)s",
            channel_url
        ], capture_output=True, text=True, timeout=60)

        videos = []
        for line in result.stdout.strip().split("\n"):
            if "|||" not in line:
                continue
            parts = line.split("|||")
            if len(parts) < 2:
                continue
            vid_id, title = parts[0].strip(), parts[1].strip()
            upload_date = parts[2].strip() if len(parts) > 2 else ""

            # Filter for meeting-related videos
            title_lower = title.lower()
            if any(kw in title_lower for kw in MEETING_KEYWORDS):
                videos.append({
                    "id": vid_id,
                    "url": f"https://www.youtube.com/watch?v={vid_id}",
                    "title": title,
                    "upload_date": upload_date,
                })
                if len(videos) >= count:
                    break

        log(f"  Found {len(videos)} meeting videos")
        return videos
    except Exception as e:
        log(f"  ✗ Channel scan failed: {e}")
        return []


def get_captions(video_url):
    """
    Extract auto-generated captions via yt-dlp (no audio download).
    Returns plain text transcript or None if unavailable.
    """
    log(f"  Fetching captions: {video_url[-50:]}")
    try:
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run([
                "yt-dlp",
                "--write-auto-sub", "--sub-lang", "en",
                "--sub-format", "vtt",
                "--skip-download",
                "--output", f"{tmp}/%(id)s",
                video_url
            ], capture_output=True, text=True, timeout=60)

            vtt_files = list(Path(tmp).glob("*.vtt"))
            if not vtt_files:
                log("  No auto-captions available")
                return None

            raw = vtt_files[0].read_text(encoding="utf-8", errors="replace")
            # Strip VTT formatting → clean text
            text = re.sub(r'WEBVTT.*?\n\n', '', raw, flags=re.DOTALL)
            text = re.sub(r'\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3}[^\n]*\n', '', text)
            text = re.sub(r'<[^>]+>', '', text)
            text = re.sub(r'\n+', '\n', text).strip()
            # Deduplicate repeated lines (VTT often has overlapping segments)
            lines = text.split('\n')
            deduped = []
            prev = ''
            for line in lines:
                line = line.strip()
                if line and line != prev and not line.startswith('align:'):
                    deduped.append(line)
                    prev = line
            clean = '\n'.join(deduped)
            log(f"  ✓ Got {len(clean):,} chars of captions")
            return clean
    except Exception as e:
        log(f"  ✗ Caption fetch failed: {e}")
        return None


def transcribe_with_whisper(audio_path):
    """Transcribe audio file using local Whisper model."""
    log(f"  Transcribing with Whisper: {audio_path}")
    try:
        import whisper
        model = whisper.load_model("base")  # Use 'small' for better accuracy
        result = model.transcribe(str(audio_path))
        text = result["text"]
        log(f"  ✓ Whisper: {len(text):,} chars transcribed")
        return text
    except ImportError:
        log("  Whisper not installed — run: pip install openai-whisper --break-system-packages")
        return None
    except Exception as e:
        log(f"  ✗ Whisper error: {e}")
        return None


def download_audio(video_url):
    """Download audio from video URL using yt-dlp."""
    log(f"  Downloading audio...")
    try:
        out_path = MEDIA_DIR / f"{hashlib.md5(video_url.encode()).hexdigest()[:10]}.mp3"
        if out_path.exists():
            log(f"  Audio already cached: {out_path}")
            return out_path
        subprocess.run([
            "yt-dlp", "-x", "--audio-format", "mp3",
            "--output", str(out_path.with_suffix("")),
            video_url
        ], check=True, capture_output=True, timeout=300)
        log(f"  ✓ Audio saved: {out_path}")
        return out_path
    except Exception as e:
        log(f"  ✗ Audio download failed: {e}")
        return None


# ── Claude Extraction ──────────────────────────────────────────────────────────
CASE_PROMPT = """You are analyzing a {city} town/city council meeting transcript.

Extract ALL rezoning, annexation, conditional use, subdivision, or special use cases discussed.
For each case return a JSON object:
{{
  "case_number": "case file number or 'UNKNOWN'",
  "title": "plain-English description",
  "change_requested": "one sentence summary of what is being requested",
  "acreage": <number or null>,
  "outcome": "approved" | "denied" | "withdrawn" | "continued" | "tabled" | "pending" | "unknown",
  "dev_type": "residential" | "commercial" | "mixed_use" | "industrial" | "office" | "hotel" | "senior_living" | "storage" | "municipal" | "subdivision" | "annexation" | "unknown",
  "vote_yes": <integer or null>,
  "vote_no": <integer or null>,
  "staff_recommendation": "approve" | "deny" | "unknown",
  "adjacent_sf": <true/false/null>,
  "traffic_study": <true/false/null>,
  "affordable_housing": <true/false/null>,
  "transition_buffer": <true/false/null>,
  "notes": "key conditions or context, 1-2 sentences"
}}

Return ONLY a JSON array. Empty array [] if no cases found.

CITY: {city}
TRANSCRIPT (first 15000 chars):
{text}"""


COUNCIL_PROMPT = """Analyze this {city} council meeting transcript for individual council member behavior.

For EACH council member who spoke about a development/rezoning case, return:
{{
  "member_name": "full name as spoken",
  "role": "Mayor" | "Mayor Pro Tem" | "Council Member" | "unknown",
  "case_number": "case number they discussed",
  "vote": "yes" | "no" | "abstain" | "unknown",
  "sentiment": "strong_support" | "support" | "neutral" | "oppose" | "strong_oppose",
  "key_quote": "their most revealing quote, under 30 words",
  "stated_concerns": ["concern1", "concern2"],
  "conditions_requested": ["condition1", "condition2"]
}}

Return ONLY a JSON array. Focus on substantive statements, not procedural remarks.

CITY: {city}
KNOWN COUNCIL MEMBERS (if any): {members}
TRANSCRIPT:
{text}"""


def extract_with_claude(city, transcript, video_url, use_sonnet_for_council=False):
    """Send transcript to Claude for case + council member extraction."""
    if not API_KEY:
        log("  ERROR: ANTHROPIC_API_KEY not set")
        return [], []
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=API_KEY)

        # --- Case extraction (Haiku — cheap) ---
        log("  Extracting cases with Claude Haiku...")
        resp = client.messages.create(
            model=HAIKU, max_tokens=3000,
            messages=[{"role":"user","content":
                CASE_PROMPT.format(city=city, text=transcript[:15000])
            }]
        )
        raw = resp.content[0].text.strip()
        cases = _parse_json_array(raw)
        for c in cases:
            c.update({"city": city, "source": "video", "_video_url": video_url})
        log(f"  ✓ {len(cases)} cases extracted")

        # --- Council member extraction (Haiku or Sonnet) ---
        log("  Extracting council member data...")
        council_model = SONNET if use_sonnet_for_council else HAIKU

        # Get known members from DB for context
        try:
            conn = sqlite3.connect(CASES_DB)
            known = conn.execute(
                "SELECT DISTINCT member_name FROM council_intelligence WHERE city=?", (city,)
            ).fetchall()
            conn.close()
            members_str = ", ".join(r[0] for r in known) if known else "unknown — extract from transcript"
        except Exception:
            members_str = "unknown — extract from transcript"

        resp2 = client.messages.create(
            model=council_model, max_tokens=4000,
            messages=[{"role":"user","content":
                COUNCIL_PROMPT.format(city=city, members=members_str, text=transcript[:15000])
            }]
        )
        raw2 = resp2.content[0].text.strip()
        members = _parse_json_array(raw2)
        for m in members:
            m["video_url"] = video_url
        log(f"  ✓ {len(members)} council member records extracted")

        return cases, members

    except json.JSONDecodeError as e:
        log(f"  ✗ JSON parse error: {e}")
        return [], []
    except Exception as e:
        log(f"  ✗ Claude error: {e}")
        return [], []


# ── DB Writers ─────────────────────────────────────────────────────────────────
def save_cases(cases, dry_run=False):
    if not cases: return 0
    if dry_run:
        for c in cases:
            log(f"  [DRY] Case: {c.get('case_number')} | {c.get('outcome')} | {c.get('title','')[:60]}")
        return len(cases)
    conn = sqlite3.connect(CASES_DB)
    added = 0
    for c in cases:
        case_num = c.get("case_number") or "UNKNOWN"
        if case_num == "UNKNOWN":
            case_num = f"VID-{c.get('city','?')[:3].upper()}-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}-{added}"
        try:
            conn.execute("""INSERT OR IGNORE INTO cases
                (case_number,city,meeting_date,title,change_requested,acreage,outcome,dev_type,
                 height_stories,adjacent_sf,traffic_study,affordable_housing,transition_buffer,
                 staff_recommendation,vote_yes,vote_no,notes,source,fetched_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (case_num, c.get("city"), c.get("meeting_date"),
                 c.get("title"), c.get("change_requested"), c.get("acreage"),
                 c.get("outcome"), c.get("dev_type"), c.get("height_stories"),
                 int(bool(c.get("adjacent_sf"))), int(bool(c.get("traffic_study"))),
                 int(bool(c.get("affordable_housing"))), int(bool(c.get("transition_buffer"))),
                 c.get("staff_recommendation"), c.get("vote_yes"), c.get("vote_no"),
                 c.get("notes"), c.get("source","video"), datetime.datetime.utcnow().isoformat()))
            if conn.execute("SELECT changes()").fetchone()[0]: added += 1
        except Exception as e:
            log(f"  ✗ Case insert error: {e}")
    conn.commit(); conn.close()
    return added


def save_council_intel(members, meeting_date, dry_run=False):
    if not members: return 0
    if dry_run:
        for m in members:
            log(f"  [DRY] Member: {m.get('member_name')} | {m.get('vote')} | Case: {m.get('case_number')}")
        return len(members)
    conn = sqlite3.connect(CASES_DB)
    saved = 0
    for m in members:
        try:
            conn.execute("""INSERT OR REPLACE INTO council_intelligence
                (city,member_name,role,case_number,meeting_date,vote,sentiment,
                 key_quote,stated_concerns,conditions_requested,video_url,extracted_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (m.get("city"), m.get("member_name"), m.get("role"),
                 m.get("case_number"), meeting_date, m.get("vote"), m.get("sentiment"),
                 m.get("key_quote"),
                 json.dumps(m.get("stated_concerns",[])),
                 json.dumps(m.get("conditions_requested",[])),
                 m.get("video_url"), datetime.datetime.utcnow().isoformat()))
            saved += 1
        except Exception as e:
            log(f"  ✗ Council insert error: {e}")
    conn.commit(); conn.close()
    return saved


# ── Main Pipeline ──────────────────────────────────────────────────────────────
def process_video(video_url, city, video_id=None, video_title="", dry_run=False, use_whisper=False):
    log(f"\n  Video: {video_title or video_url[-50:]}")

    if video_id and is_video_processed(video_id):
        log(f"  ↷ Already processed")
        return 0, 0

    # Step 1: Get transcript
    transcript = None
    if use_whisper:
        audio = download_audio(video_url)
        if audio:
            transcript = transcribe_with_whisper(audio)

    if not transcript:
        transcript = get_captions(video_url)

    if not transcript or len(transcript) < 500:
        log(f"  ✗ No usable transcript (got {len(transcript or '')} chars)")
        if video_id:
            mark_video_processed(video_id, city, video_title, 0, 0)
        return 0, 0

    # Try to extract meeting date from title
    meeting_date = None
    date_match = re.search(r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+20\d{2}',
                            video_title.lower())
    if date_match:
        try:
            meeting_date = datetime.datetime.strptime(date_match.group(), "%B %d, %Y").strftime("%Y-%m-%d")
        except: pass

    # Step 2: Claude extraction
    cases, members = extract_with_claude(city, transcript, video_url)

    # Add meeting date to cases
    for c in cases:
        if not c.get("meeting_date"):
            c["meeting_date"] = meeting_date

    # Step 3: Save
    cases_added = save_cases(cases, dry_run)
    members_saved = save_council_intel(members, meeting_date, dry_run)

    if video_id and not dry_run:
        mark_video_processed(video_id, city, video_title, cases_added, members_saved)

    log(f"  ✓ Cases: +{cases_added} | Council records: +{members_saved}")
    return cases_added, members_saved


def main():
    parser = argparse.ArgumentParser(description="LandWorks council meeting video scraper")
    parser.add_argument("--url",      help="Single YouTube video URL")
    parser.add_argument("--channel",  help="YouTube channel URL (auto-find recent meetings)")
    parser.add_argument("--city",     required=True, help="City name (e.g. Apex, Raleigh)")
    parser.add_argument("--count",    type=int, default=2, help="Videos to process from channel (default: 2)")
    parser.add_argument("--audio",    help="Local audio/video file to transcribe with Whisper")
    parser.add_argument("--whisper",  action="store_true", help="Use Whisper instead of YouTube captions")
    parser.add_argument("--dry-run",  action="store_true")
    parser.add_argument("--api-key",  help="Anthropic API key")
    args = parser.parse_args()

    global API_KEY
    if args.api_key: API_KEY = args.api_key
    if not API_KEY:
        print("ERROR: Set ANTHROPIC_API_KEY or pass --api-key")
        sys.exit(1)

    log(f"\n{'='*60}")
    log(f"LandWorks Video Scraper | City: {args.city} | Dry run: {args.dry_run}")

    init_db()
    total_cases = total_members = 0

    if args.audio:
        # Local audio file mode
        transcript = transcribe_with_whisper(args.audio)
        if transcript:
            cases, members = extract_with_claude(args.city, transcript, args.audio)
            total_cases += save_cases(cases, args.dry_run)
            total_members += save_council_intel(members, None, args.dry_run)

    elif args.url:
        # Single video
        vid_id = re.search(r'v=([^&]+)', args.url)
        vid_id = vid_id.group(1) if vid_id else args.url[-11:]
        c, m = process_video(args.url, args.city, vid_id, "", args.dry_run, args.whisper)
        total_cases += c; total_members += m

    elif args.channel:
        # Channel scan
        channel_url = args.channel or CITY_CHANNELS.get(args.city)
        if not channel_url:
            log(f"ERROR: No channel URL for {args.city}. Pass --channel URL or add to CITY_CHANNELS.")
            sys.exit(1)
        videos = get_recent_videos(channel_url, args.city, args.count)
        for v in videos:
            c, m = process_video(v["url"], args.city, v["id"], v["title"], args.dry_run, args.whisper)
            total_cases += c; total_members += m
            time.sleep(2)
    else:
        # Auto mode — use configured channel
        channel_url = CITY_CHANNELS.get(args.city)
        if not channel_url:
            log(f"ERROR: No channel configured for {args.city}. Pass --url or --channel.")
            sys.exit(1)
        videos = get_recent_videos(channel_url, args.city, args.count)
        for v in videos:
            c, m = process_video(v["url"], args.city, v["id"], v["title"], args.dry_run, args.whisper)
            total_cases += c; total_members += m
            time.sleep(2)

    log(f"\n✅ Done — Cases: +{total_cases} | Council records: +{total_members}")


if __name__ == "__main__":
    main()
