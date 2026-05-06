## NetWatch NG Project Handoff Summary

### 1) Project Purpose
Build a Nigeria-focused telecom monitoring platform that:
- maps cell towers
- ingests network telemetry
- visualizes network quality
- ranks providers by location
- stores historical data for outage/prediction work

Current architecture is web-first (no required dedicated mobile app yet).

---

### 2) Stack + Structure
- Frontend: single-file dashboard at `public/index.html`
- Backend: Node.js + Express
  - `server/index.js`
  - `server/routes.js`
  - `server/db.js`
- DB: PostgreSQL
- Data import: `scripts/importTowers.js`
- IODA sync: `scripts/syncIODA.js`

Core folders:
- `data/621.csv` (OpenCelliD Nigeria raw CSV, no headers)
- `public/`
- `server/`
- `scripts/`

---

### 3) Database State and Schema
Tables in active design:
1. `towers`
2. `readings`
3. `speed_tests`
4. `outages` (IODA integration groundwork)

`bootstrap()` in `server/db.js` auto-creates all tables + indexes on server start.

Important:
- `towers` has unique `(cell_id, lac, mnc)`
- `readings.tower_id` references `towers.id`
- `outages` has dedupe unique index on source/entity/operator/signal/time tuple

---

### 4) Data Sources in Use
1. OpenCelliD static tower base (`621.csv`)
2. Simulated telemetry (legacy fallback mode)
3. Real phone uploads (now active path via MacroDroid + HTTP Shortcuts)
4. IODA outage feed integration code exists but external connectivity to IODA endpoint has been unreliable from user environment

---

### 5) Key Backend Features Implemented

#### A) Reading ingestion hardening
- `POST /api/readings` (bulk)
- `POST /api/readings/device` (single device-friendly)
- Validation + normalization:
  - `signal_dbm` required numeric
  - bounds/clamps applied to numeric fields
- Optional ingest auth:
  - `INGEST_API_KEY`
  - header: `x-netwatch-key`
- `cell_id -> tower_id` auto-resolution when possible

#### B) Existing analytics APIs retained
- `/api/health`
- `/api/towers`
- `/api/towers/stats`
- `/api/readings/latest`
- `/api/readings/history`
- `/api/readings/summary`
- `/api/speedtests` (GET/POST)

#### C) Recommendation API
- `GET /api/recommendation?lat=...&lon=...&radius_km=...&hours=...`
- ranks provider using weighted score:
  - signal, latency, dl speed, freshness, sample count
- returns confidence + best provider

#### D) Outage APIs (IODA groundwork)
- `GET /api/outages`
- `GET /api/outages/stats`
- `GET /api/outages/correlate`
- `POST /api/outages/sync`
- `server/index.js` has 6-hour `setInterval` auto sync hook to run IODA script

#### E) Real speed test APIs
- `GET /api/speedtest/ping`
- `GET /api/speedtest/download`
- `POST /api/speedtest/upload`
Used by frontend speed test for measured throughput/latency.

---

### 6) Frontend Features Implemented

#### Tabs
- Map
- Analytics
- Data Log
- Patterns
- Outages
- Speed Test

#### Map upgrades
- base layer toggle:
  - standard
  - earth/satellite-like
  - roads
- place search (Nigeria-focused, Nominatim)
- destination-based provider recommendation panel
- hover tooltip on tower markers:
  - operator/cell/network/signal/latency/DL/UL
- click tower details panel
- coverage radial circle on click added (UX behavior still needs refinement based on user feedback)

#### Speed test changes
- switched from pure simulated profile values to real HTTP measurement flow
- server/provider selector removed per user request
- now “current connection” style (fast.com-like UX)

#### Outages tab
- summary cards
- timeline chart
- outage table
- pre-outage correlation panel
- sync-now button

---

### 7) Mobile Data Collection Status
User has MacroDroid + HTTP Shortcuts posting to deployed backend and ingestion is confirmed.

Observed `/api/readings/latest` sample:
- one device row with placeholder values:
  - `tower_id: null`
  - `cell_id: "123456789"`
  - `operator: "Unknown"`
  - `signal_dbm: -85`
This confirms pipeline works but values are still placeholders, not yet true telephony variables.

Meaning:
- transport/auth/path are good
- telemetry quality tuning still pending (real cell/operator/signal extraction on device side)

---

### 8) Deployment Status
- User moved away from Railway and deployed on Render.
- User confirmed deployment “working fine”.
- Main app reachable and operating.
- Remaining operational work is data quality + model/prediction evolution.

---

### 9) Major Issues Encountered (and outcomes)
1. Git not installed initially -> resolved.
2. Git identity not configured -> resolved.
3. Push/refspec/remote mismatch issues -> resolved progressively.
4. Large push/network reset concerns noted (recommend `.gitignore` hygiene).
5. Supabase DNS/connectivity issues from local environment appeared during alternative host exploration.
6. IODA fetch currently failing from user environment (`fetch failed`, timeout) despite code integration.
7. Coverage radial UX not yet satisfactory to user (known pending UI refinement).
8. MacroDroid variable confusion:
   - user ended up with working uploads but placeholder payload values.

---

### 10) Current Functional Truth
- Core platform is operational on Render.
- Real ingestion endpoint works in production.
- Dashboard updates from backend.
- Speed test now measures real network transfer against server endpoints.
- Outage framework integrated in code + DB, but external IODA data sync depends on network reachability to CAIDA API.
- Prediction modeling is not yet built; groundwork is present (outages + correlate endpoint).

---

### 11) What’s Pending (highest priority)
1. Replace placeholder mobile payload fields with real values:
   - `cell_id`
   - `operator`
   - `signal_dbm`
2. Refine tower coverage circle UX behavior as requested.
3. Verify and stabilize IODA connectivity path (or run sync from environment with reachable egress).
4. Build first outage-risk predictor pipeline from:
   - confirmed outages
   - pre-outage reading windows

---

### 12) Important API/Auth Details for next model
Device upload:
- Endpoint: `POST /api/readings/device`
- Header: `x-netwatch-key: <INGEST_API_KEY>`
- `signal_dbm` must be numeric or request fails
- inserted into `readings` table
- unresolved towers remain `tower_id = null`

---

### 13) Data Integrity Notes
- Headerless `621.csv` is expected and correctly handled by import script (position-based parsing).
- “Only 1000 rows” confusion likely UI pagination, not true table size; should always verify with SQL `count(*)`.

---

### 14) Suggested Next Action for another AI
If continuing this project, start by:
1. validating live payload quality in `readings` (not just transport)
2. implementing robust mobile-side fallback mapping rules
3. adding diagnostics endpoint/dashboard card for “ingestion quality”:
   - percent rows with non-null `tower_id`
   - percent non-Unknown operator
   - percent dynamic signal values
4. then proceed to outage prediction baseline model.

