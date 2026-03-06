# OIL LIBYA ETHIOPIA - Implementation Summary

## Project Overview

This is a full-stack fuel monitoring platform built as a monorepo:

- `backend/` - Python Flask API + SQLite storage + anomaly/transfer logic
- `frontend/` - React + Vite responsive dashboard

Primary use case:

- Ingest fuel sensor readings from **Generator Tank** and **External Tank**
- Detect important events (`normal_usage`, `abnormal_drop`, `refill`)
- Automatically trigger software-based refill transfer from External Tank to Generator Tank when Generator Tank falls below threshold
- Display live status in a mobile-friendly dashboard (auto refresh every 5 seconds)

---

## Backend (Flask + SQLite)

### Implemented API Endpoints

- `GET /api/health`
  - Returns service health status
- `GET /api/metrics`
  - Returns latest generator metrics and current tank levels
  - Includes: generator fuel level, external fuel level, consumption rate, last update time, minimum level, tank status
- `GET /api/events`
  - Returns last 50 events with timestamps and details
- `POST /api/ingest`
  - Accepts JSON sensor payload and stores it in SQLite

### Data Storage

SQLite database in backend:

- `sensor_data` table
  - stores tank snapshots over time
- `events` table
  - stores derived events and rule details

### Rules and Logic Implemented

1. **Anomaly rules (within 10 minutes):**
   - `abnormal_drop` if fuel decreases by more than 10%
   - `refill` if fuel increases by more than 8%
   - otherwise `normal_usage`

2. **Two-tank model:**
   - `Generator Tank`
   - `External Tank`

3. **Auto-refill transfer logic (software simulation of solenoid behavior):**
   - If Generator Tank level < `GENERATOR_MIN_LEVEL` (25%), backend auto-transfers from External Tank
   - Transfer targets `GENERATOR_REFILL_TARGET` (60%) or until External Tank is depleted
   - Creates event records for both tanks with `auto_transfer` metadata

4. **Fuel percentage constraints:**
   - Levels are clamped to `0% – 100%`

5. **Structured logging:**
   - JSON logs to console for ingest, refill, and runtime events

---

## Frontend (React + Vite)

### Dashboard Features Implemented

- Professional top bar branding: **OIL LIBYA ETHIOPIA**
- Hero section with architecture and stack visibility
- KPI cards for:
  - Generator Tank level
  - External Tank level
  - Generator consumption rate
  - Last update time
  - Tank status
- Fuel Level Over Time chart (Generator Tank trend)
- Events timeline list (latest first)
- Alert banner when latest event is `abnormal_drop`
- Live VPS link indicator (`Online / Degraded / Offline / Checking`)
- ESP32/VPS/React architecture presentation for client clarity
- Responsive layout for mobile/tablet/desktop

### Client-facing Architecture Display Added

Frontend now explicitly shows:

- **ESP32 SENSOR NODE** → sends fuel payloads
- **VPS SERVER (Flask + SQLite)** → receives data, applies rules
- **WEB DASHBOARD** → displays real-time monitoring

---

## Dev Setup and Runtime

### Monorepo Structure

- `backend/`
- `frontend/`

### One-command startup

- `./start-all.sh`
  - Starts backend and frontend together
  - Seeds sample data
  - Cleans stale dev processes first
  - Uses fixed frontend port `5173`

### Manual startup (supported)

- Backend in one terminal
- Frontend in another terminal

### Seed script

- `backend/seed.py`
- Generates sample historical data/events for both tanks

---

## Key Technical Notes

1. **Backend auto-refill currently simulates solenoid action in software logic** (event-driven transfer in DB).
2. **Physical GPIO solenoid control is not yet wired to real ESP32 firmware** in this repository.
3. The dashboard is already suitable for phone viewing and periodic live updates over VPS-hosted APIs.

### Next Step: Real Solenoid Command Integration Contract

If needed, the next implementation phase can connect VPS decisions to a real ESP32-controlled solenoid.

#### Option A — VPS to ESP32 Device Endpoint (HTTP)

- VPS sends command to ESP32 REST endpoint when refill condition is met.
- Suggested endpoint on ESP32:
  - `POST /api/solenoid/command`
- Payload example:

```json
{
  "command": "OPEN",
  "reason": "generator_below_minimum",
  "generator_level": 22.4,
  "external_level": 68.1,
  "target_level": 60.0,
  "request_id": "evt_20260304_001",
  "issued_at": "2026-03-04T18:20:00Z"
}
```

- ESP32 response example:

```json
{
  "status": "accepted",
  "pin": 26,
  "relay_state": "OPEN",
  "request_id": "evt_20260304_001"
}
```

#### Option B — MQTT Topic Contract (Recommended for IoT reliability)

- VPS publishes control commands to broker topic.
- ESP32 subscribes and actuates relay.

Suggested topics:

- Command topic: `oil/libya-ethiopia/solenoid/cmd`
- Ack topic: `oil/libya-ethiopia/solenoid/ack`
- Telemetry topic: `oil/libya-ethiopia/tank/telemetry`

Suggested command payload:

```json
{
  "device_id": "esp32-generator-01",
  "command": "OPEN",
  "duration_sec": 20,
  "reason": "generator_below_minimum",
  "request_id": "evt_20260304_001",
  "issued_at": "2026-03-04T18:20:00Z"
}
```

#### Safety/Control Requirements for Production

- Add command authentication (token or signed message).
- Add command timeout + auto-close fallback.
- Add idempotency using `request_id`.
- Log every command/ack in VPS database for audit.
- Add manual override endpoint (`OPEN`/`CLOSE`) with role-based access.
- Add watchdog rules to prevent overfilling.

---

## Files Added/Updated During Implementation

### Backend

- `backend/app.py`
- `backend/db.py`
- `backend/seed.py`
- `backend/requirements.txt`

### Frontend

- `frontend/package.json`
- `frontend/vite.config.js`
- `frontend/index.html`
- `frontend/src/main.jsx`
- `frontend/src/App.jsx`
- `frontend/src/styles.css`

### Root/Project

- `start-all.sh`
- `.gitignore`
- `README.md`
- `.vscode/settings.json`
- `IMPLEMENTATION_SUMMARY.md`

---

## Final Outcome

The solution now provides a production-oriented Flask + React full-stack platform that:

- Accepts and stores fuel sensor data
- Detects abnormal changes and refill behavior
- Models Generator/External tank transfer behavior using thresholds
- Shows live operational visibility on a responsive dashboard suitable for client phone access
- Runs locally on Ubuntu with clear startup commands
