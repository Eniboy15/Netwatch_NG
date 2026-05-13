## NetWatch NG Project Handoff Summary

### 1) Project Purpose
Build a Nigeria-focused telecom monitoring platform that:
- maps cell towers
- ingests live mobile network telemetry
- visualizes network quality
- ranks providers by location
- stores historical and synthetic training data for outage prediction

Current architecture is web-first. No dedicated mobile app is required yet.

---

### 2) Stack and Structure
- Frontend: single-file dashboard at `public/index.html`
- Backend: Node.js + Express
  - `server/index.js`
  - `server/routes.js`
  - `server/db.js`
- Database: PostgreSQL on Supabase
- Deployment: Render
- Data import scripts: `scripts/`

Core folders:
- `data/`
- `public/`
- `server/`
- `scripts/`

Important local data files are intentionally ignored by Git:
- `.env`
- `data/*.csv`
- `node_modules/`
- `*.log`

The dataset CSVs should live locally in `data/` and the imported rows should live in Supabase, not GitHub.

---

### 3) Main Database Tables
`bootstrap()` in `server/db.js` auto-creates tables and indexes on server start.

Core app tables:
1. `towers`
2. `readings`
3. `speed_tests`
4. `outages`

Synthetic ElectricSheep Africa / Amon Din tables added:
1. `qos_metrics`
2. `coverage_data`
3. `network_events`
4. `hardware_sensors`
5. `uptime_logs`
6. `energy_consumption`
7. `maintenance_orders`
8. `latency_logs`
9. `handover_records`
10. `dropped_calls`
11. `mobility_traces`
12. `network_penetration`
13. `technician_logs`

Important:
- `towers` has unique `(cell_id, lac, mnc)`
- `readings.tower_id` references `towers.id`
- `outages` has a unique dedupe index on source/entity/operator/signal/time tuple
- Synthetic tower IDs are stored as `source_tower_id` because they are labels like `CAL-6586`, not numeric OpenCelliD IDs
- All synthetic datasets must be labeled as synthetic training data in any UI that surfaces them

---

### 4) Data Sources
1. OpenCelliD Nigeria tower CSV: `data/621.csv`
2. Live phone uploads through MacroDroid + HTTP Shortcuts
3. Legacy simulated telemetry fallback
4. IODA outage feed groundwork
5. ElectricSheep Africa / Amon Din synthetic telecom datasets from Hugging Face

The ElectricSheep datasets are synthetic and generated to model Nigerian telecom behavior. They are useful for baseline analytics and model training, but they are not real measured network data.

---

### 5) Import Scripts and Commands
Existing:
- `npm run import` -> `scripts/importTowers.js`
- `npm run sync:ioda` -> `scripts/syncIODA.js`

Synthetic dataset imports:
- `npm run import:qos`
- `npm run import:coverage`
- `npm run import:events`
- `npm run import:hardware`
- `npm run import:uptime`
- `npm run import:energy`
- `npm run import:maintenance`
- `npm run import:latency`
- `npm run import:handovers`
- `npm run import:dropped`
- `npm run import:mobility`
- `npm run import:penetration`
- `npm run import:technicians`

Combined remaining Tier 2 import:
- `npm run import:remaining`

`import:remaining` runs:
1. dropped calls
2. mobility traces
3. network penetration
4. technician logs

All import scripts:
- read CSV files from `data/`
- batch insert in chunks of 100
- use parameterized SQL
- use `ON CONFLICT DO NOTHING`
- print progress and summary output

---

### 6) Expected CSV Filenames
These filenames are expected in `data/`:

- `quality_of_service_metrics.csv`
- `coverage_maps_and_signal_strength_data.csv`
- `network_event_logs.csv`
- `hardware_sensor_data.csv`
- `base_station_uptime_logs.csv`
- `tower_energy_consumption_records.csv`
- `maintenance_work_orders.csv`
- `ultra_low_latency_logs.csv`
- `cell_tower_handover_data.csv`
- `dropped_call_records.csv`
- `mobility_trace_datasets.csv`
- `fourth_generation_fifth_generation_penetration_datasets.csv`
- `technician_activity_logs.csv`

Do not commit these CSVs to GitHub.

---

### 7) Backend APIs
Core APIs retained:
- `GET /api/health`
- `GET /api/towers`
- `GET /api/towers/stats`
- `POST /api/readings`
- `POST /api/readings/device`
- `GET /api/readings/latest`
- `GET /api/readings/history`
- `GET /api/readings/summary`
- `GET /api/speedtests`
- `POST /api/speedtests`
- `GET /api/speedtest/ping`
- `GET /api/speedtest/download`
- `POST /api/speedtest/upload`
- `GET /api/recommendation`
- `GET /api/outages`
- `GET /api/outages/stats`
- `GET /api/outages/correlate`
- `POST /api/outages/sync`

Synthetic dataset APIs added:
- `GET /api/qos`
- `GET /api/coverage`
- `GET /api/events`
- `GET /api/hardware`
- `GET /api/uptime`
- `GET /api/energy`
- `GET /api/maintenance`
- `GET /api/latency`
- `GET /api/handovers`
- `GET /api/dropped-calls`
- `GET /api/mobility`
- `GET /api/penetration`
- `GET /api/technicians`

Recommendation endpoint behavior:
- Uses live readings first
- Falls back to synthetic `coverage_data` when no recent live readings exist nearby

Outage correlation behavior:
- `/api/outages/correlate` now includes pre-outage signals from:
  - live readings
  - network events
  - hardware sensors
  - uptime logs
  - energy records
  - maintenance orders
  - latency logs
  - handover records
  - dropped calls
  - technician logs

---

### 8) Frontend State
Current dashboard tabs:
- Map
- Analytics
- Data Log
- Patterns
- Outages
- Speed Test

Implemented frontend features:
- Map layer toggle
- Nigeria-focused place search
- destination-based provider recommendation panel
- tower hover tooltip
- tower detail panel
- coverage radial circle on click
- real HTTP speed test
- outage summary and correlation panel
- user location detection with automatic map centering
- location marker with coordinates display

Frontend still needs updates to fully visualize the newly imported synthetic datasets.

Recommended next frontend work:
1. Add synthetic source labels wherever synthetic data appears
2. Add dashboard cards for QoS, coverage, energy, maintenance, dropped calls, and technician activity
3. Add a Predictions tab
4. Add outage-risk markers on map

---

### 9) Mobile Data Collection Status
User has MacroDroid + HTTP Shortcuts posting to deployed backend and ingestion is confirmed.

Observed issue:
- uploads worked but sample values were placeholders:
  - `tower_id: null`
  - `cell_id: "123456789"`
  - `operator: "Unknown"`
  - `signal_dbm: -85`

Meaning:
- transport/auth/API path are good
- phone-side telemetry extraction still needs tuning

High-priority mobile work:
1. replace placeholder `cell_id`
2. replace placeholder `operator`
3. capture dynamic real `signal_dbm`
4. improve tower resolution rate

---

### 10) Deployment Status
- App is deployed on Render
- Supabase is the production database
- GitHub repo is the deployment source
- Render auto-redeploys after pushes to GitHub

After backend changes:
1. commit code
2. push to GitHub
3. wait for Render deploy
4. test API endpoint

Example:
```powershell
git add package.json server/db.js server/routes.js scripts/importDroppedCalls.js scripts/importMobilityTraces.js scripts/importNetworkPenetration.js scripts/importTechnicianLogs.js
git commit -m "Add remaining Tier 2 dataset imports and APIs"
git push origin main
```

---

### 11) Known Issues and Notes
1. IODA sync can fail due to CAIDA/IODA connectivity or egress timeout.
2. Local Supabase connectivity can occasionally fail due to DNS/network issues.
3. Windows can lock CSV files during import.
   - Example seen during mobility import:
     - `EBUSY: resource busy or locked, read`
     - import had already inserted about 445,500 mobility rows before failing
   - Likely causes:
     - CSV open in Excel/Notepad/VS Code
     - File Explorer preview pane
     - OneDrive sync
     - Windows Defender scan
     - file still copying/downloading
   - Safe fix:
     - close anything using the file
     - wait 10 seconds
     - rerun the importer
   - Re-running is safe because imports use `ON CONFLICT DO NOTHING`.
4. `.env` must not be committed.
5. CSV dataset files must not be committed.
6. Coverage radial UX still needs refinement.

---

### 12) Current Import Status
Completed as of May 13, 2026:
- QoS import worked
- Coverage import worked
- All remaining imports completed successfully:
  - Dropped calls
  - Mobility traces
  - Network penetration
  - Technician logs
- Code changes pushed to GitHub
- New API endpoints verified on Render:
  - `/api/dropped-calls?limit=5`
  - `/api/mobility?limit=5`
  - `/api/penetration?limit=5`
  - `/api/technicians?limit=5`

All synthetic datasets are now imported and accessible via API.

---

### 13) Suggested Next Action
Best next step:
- Start building the first outage-risk scoring endpoint and Predictions dashboard tab

First prediction baseline should use:
- low signal / high latency from readings and QoS
- outage labels from uptime logs
- event spikes from network events
- hardware alerts and bad health
- energy anomalies
- recent maintenance and technician activity
- handover failures
- dropped-call spikes
