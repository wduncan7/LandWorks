# LandWorks Project Roadmap
*Last updated: 2026-04-29*

---

## ⚠️ Pending: Name & Trademark
- Current name: **LandWorks** (replaced "LandSport" which was trademarked by an aluminum co.)
- "LandWorks" is also common in landscape/engineering industry — needs USPTO.gov search
- Tyler to decide on final brand name before any public launch
- TODO: Run trademark search, consider variations (LandWorks Pro, WakeDev, GroundWork, PlatForm, etc.)

---

## ✅ Completed This Week (2026-04-29)
- Legistar auto-fetch pipeline (`lw_auto_fetch.py`) — nightly cron, 60 Raleigh cases live
- PDF agenda scraper (`lw_agenda_scraper.py`) — Cary, Apex, Wake Forest + 7 more cities
- Collapsible card panels in LandWorks
- 7 new development types (Senior Living, Commercial, Office, Hotel, Industrial, Storage, Municipal)
- Trial Run per-product breakdown
- Local proxy server fixes (CORS, /cases endpoint, Legistar proxy route)

---

## 🌊 Priority: Stormwater Calculation Tool

### Tyler's current workflow (to be automated/assisted):
1. **Drainage area delineation** — AutoCAD parcels used to calculate drainage areas
   to the storm network and to each SCM
2. **Curve Number calculation** — spreadsheet using NRCS TR-55 method,
   soil type + land cover → weighted CN for each drainage area
3. **SCM sizing** — spreadsheet calculating size requirements/design for each
   Stormwater Control Measure (bioretention, wet pond, etc.)
4. **Storm network sizing** — spreadsheet calculating pipe sizes for the storm
   drainage network using rational method
5. **SCM grading** — Civil 3D grading features to grade the SCM footprint
6. **Pond routing** — Hydrology Studio routes ponds to verify SCMs can handle
   required stormwater events (10-yr, 25-yr, 100-yr storms)

### Regulatory outputs required:
- **Wake County SNAP tool** — must be filled out for all submittals
  (Stormwater and Non-point Source Assessment Program)
- **Wake County Stormwater Tool** — submittal requirement
- NC DEQ compliance (Jordan Lake / Neuse River Basin rules)

### Build plan:
- Phase 1: CN calculator (input: drainage area polygons + soil type → output: weighted CN)
- Phase 2: SCM sizing calculator (bioretention, wet pond) per NCDEQ BMP Design Manual
- Phase 3: Rational method pipe sizing
- Phase 4: Simple pond routing (replace Hydrology Studio for basic cases)
- Phase 5: Auto-populate Wake County SNAP form from calculated values
- Phase 6: Civil 3D grading automation via Python COM API (Caddy agent)

---

## 🔗 Priority: Wire LandWorks ↔ OpenClaw Agents

### How the integration should work:
LandWorks (browser app) talks to OpenClaw (local AI gateway) via a local REST bridge.

**Architecture:**
```
LandWorks.html (Chrome)
    │
    ▼
http://localhost:7474  ← wake_county_data.py (Flask, parcel data)
    │
http://localhost:11434 ← OpenClaw gateway (AI routing)
    │
    ├── Alma  (Land Developer lead)
    ├── Sport (Civil PE)
    ├── Caddy (Civil 3D tech)
    ├── Arch  (Architect)
    ├── Rex   (Construction Manager)
    └── MEP   (MEP Engineer)
```

**What needs to be built:**
1. Sport AI sidebar in LandWorks needs to POST to OpenClaw's chat endpoint instead of (or in addition to) direct Anthropic API
2. LandWorks should pass project context (PIN, zoning, acreage, jurisdiction, proforma data) as system context to every OpenClaw call
3. OpenClaw routes to the right agent based on question type (Alma for deal questions, Sport for permits, etc.)
4. Agents should be able to READ the current LandWorks project state (pass as JSON in every request)

**OpenClaw chat endpoint (once working):**
- POST http://localhost:11434/v1/chat/completions  (OpenAI-compatible)
- Or OpenClaw's native endpoint — need to confirm exact URL from openclaw docs/logs

**LandWorks changes needed:**
- Replace/augment Sport AI fetch() calls to hit OpenClaw instead of direct Anthropic
- Add agent selector dropdown (Alma / Sport / Caddy / Arch / Rex / MEP)
- Pass full project JSON as system message context on every call

---

## 🧠 Priority 2: Agent Training Strategy

### What "training" means here (these are prompt-engineered agents, not fine-tuned models):

**Layer 1 — Base agent definitions (already built)**
- ALMA.md, SPORT.md, CADDY.md, ARCH.md, REX.md, MEP.md
- Each has: identity, expertise, Wake County specifics, sample prompts, handoff protocols

**Layer 2 — Project context injection (needs building)**
- Every chat call sends current LandWorks project as JSON:
  ```json
  { "pin": "0123456", "acres": 12.4, "zoning": "R-40W", 
    "jurisdiction": "Wake County", "phase": "Due Diligence",
    "proforma": { "acquisition": 850000, "units": 32 } }
  ```
- Agents respond IN CONTEXT of the actual deal on screen

**Layer 3 — Memory / knowledge files (OpenClaw workspace)**
- Feed agents real Wake County documents: UDO, stormwater regs, fee schedules, standard details
- Format: plain text or markdown files in ~/.openclaw/workspace/knowledge/
- Example files to add:
  - WakeCountyUDO_excerpts.md (zoning table, use matrix)
  - NeuseStewardship_rules.md (buffer rules, density limits)
  - WakeCountyFeeSchedule_2026.md
  - NCDOTAccess_standards.md

**Layer 4 — Real project examples (few-shot)**
- Add 2-3 completed Wake County deals as example project files
- Agents use these as reference for "what a typical deal looks like"

**Layer 5 — Feedback loop (future)**
- When Tyler corrects an agent answer, log the correction
- Periodically review and update agent .md files with refined guidance

---

## 🗺️ Priority 3: Setup Checklist (before agents are usable)

- [ ] Fix OpenClaw config (leading space in API key — rewrite full JSON)
- [ ] Clear sessions.json so OpenClaw stops defaulting to Amazon Bedrock
- [ ] Confirm `ping` works in OpenClaw chat
- [ ] Install agent files: cp openclaw_agents/*.md ~/.openclaw/workspace/agents/
- [ ] Run wake_county_data.py: `python3 ~/LandWorks/wake_county_data.py start`
- [ ] Wire LandWorks Sport AI sidebar to OpenClaw endpoint
- [ ] Test: open LandWorks, enter a PIN, ask Sport a question

---

## 🌍 Priority 4: Expansion (Future)

### Other counties/states — the playbook:
1. Each county gets its own data script (like wake_county_data.py) 
   - Different parcel data URLs, different field names, different fee schedules
2. Agent .md files get county-specific knowledge layers added
3. LandWorks gets a "jurisdiction selector" that loads the right data + agent context
4. Eventually: SaaS model where each subscriber gets their county's agent stack

**Counties to tackle next (NC):**
- Johnston County (adjacent to Wake, high growth)
- Durham County (different UDO, different stormwater authority)
- Chatham County (Jordan Lake watershed rules)

**States to consider:**
- SC (similar market, different agency structure)
- GA (Atlanta suburbs — massive market)
- TX (high volume, public parcel data widely available)

---

## 📁 File Locations (current session outputs)
- `LandWorks.html` — main app
- `wake_county_data.py` — parcel data server (port 7474)
- `openclaw_agents/` — all 6 agent .md files + AGENTS.md + INSTALL.md
- `GetLandWorks.html` — downloader page for getting LandWorks.html into Chrome

