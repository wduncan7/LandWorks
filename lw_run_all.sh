#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# LandWorks Full Data Pipeline — runs all scrapers in sequence
#
# Usage:
#   bash ~/LandWorks/lw_run_all.sh           # Full run
#   bash ~/LandWorks/lw_run_all.sh --dry-run # Test without saving
#
# Cron (nightly 2am):
#   0 2 * * * bash /home/tylerduncan/LandWorks/lw_run_all.sh >> /home/tylerduncan/LandWorks/run_all.log 2>&1
# ─────────────────────────────────────────────────────────────────────────────

set -e
LW_DIR="$HOME/LandWorks"
LOG="$LW_DIR/run_all.log"
DRY=""
if [[ "$1" == "--dry-run" ]]; then DRY="--dry-run"; fi

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG"; }

log "═══════════════════════════════════════"
log "LandWorks Full Pipeline Starting"
log "═══════════════════════════════════════"

# ── 1. Legistar (Raleigh + future cities) ─────────────────────────────────────
log "▶ Step 1: Legistar auto-fetch (Raleigh)"
python3 "$LW_DIR/lw_auto_fetch.py" $DRY || log "  ⚠ Legistar fetch had errors"

# ── 2. Video scraper — all configured cities ──────────────────────────────────
log "▶ Step 2: Video scraper (YouTube captions → cases + council intel)"

CITIES=(
    "Apex"
    "Raleigh"
    "Cary"
    "Wake Forest"
    "Holly Springs"
    "Fuquay-Varina"
    "Garner"
    "Morrisville"
    "Knightdale"
    "Wendell"
    "Zebulon"
    "Wake County"
)

for CITY in "${CITIES[@]}"; do
    log "  → $CITY"
    python3 "$LW_DIR/lw_video_scraper.py" \
        --city "$CITY" \
        --count 2 \
        $DRY || log "  ⚠ Video scraper error for $CITY"
    sleep 3  # Be polite to YouTube
done

# ── 3. Summary ────────────────────────────────────────────────────────────────
log "▶ Step 3: Database summary"
python3 - << 'PYEOF'
import sqlite3
from pathlib import Path

db = Path.home() / ".wake_county_data" / "cases.db"
if db.exists():
    conn = sqlite3.connect(db)
    total = conn.execute("SELECT COUNT(*) FROM cases").fetchone()[0]
    by_city = conn.execute(
        "SELECT city, COUNT(*) as n FROM cases GROUP BY city ORDER BY n DESC"
    ).fetchall()
    ci_total = 0
    try:
        ci_total = conn.execute("SELECT COUNT(*) FROM council_intelligence").fetchone()[0]
    except: pass
    conn.close()
    print(f"  Total cases: {total}")
    print(f"  Council intel records: {ci_total}")
    for city, n in by_city:
        print(f"    {city}: {n} cases")
else:
    print("  No database found")
PYEOF

log "═══════════════════════════════════════"
log "LandWorks Pipeline Complete"
log "═══════════════════════════════════════"
