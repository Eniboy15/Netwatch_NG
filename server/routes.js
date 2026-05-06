// server/routes.js — all REST API endpoints
const express = require('express');
const router  = express.Router();
const { pool } = require('./db');
const { runSyncIODA } = require('../scripts/syncIODA');
const MAX_BULK_READINGS = 1000;
const INGEST_API_KEY = process.env.INGEST_API_KEY || '';

function toNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function toInt(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = parseInt(value, 10);
  return Number.isFinite(n) ? n : null;
}

function clamp(value, min, max) {
  if (value === null || value === undefined) return null;
  return Math.min(max, Math.max(min, value));
}

function cleanText(value, fallback, maxLen) {
  const v = (value === null || value === undefined) ? '' : String(value).trim();
  const out = v || fallback;
  return out.slice(0, maxLen);
}

function normalizeNetwork(value) {
  const raw = cleanText(value, '4G', 10).toUpperCase();
  if (raw === 'NR' || raw === '5G') return '5G';
  if (raw === 'LTE' || raw === '4G') return '4G';
  if (raw === 'UMTS' || raw === 'HSPA' || raw === '3G') return '3G';
  if (raw === 'GSM' || raw === 'EDGE' || raw === '2G') return '2G';
  return '4G';
}

function normalizeReading(raw) {
  const source = raw || {};
  const signal = clamp(toNumber(source.signal_dbm), -140, -20);
  const when = source.recorded_at ? new Date(source.recorded_at) : new Date();

  if (signal === null) {
    throw new Error('signal_dbm is required and must be numeric');
  }
  if (Number.isNaN(when.getTime())) {
    throw new Error('recorded_at must be a valid date/time');
  }

  return {
    tower_id: toInt(source.tower_id),
    cell_id: toInt(source.cell_id),
    city: cleanText(source.city, 'Unknown', 60),
    operator: cleanText(source.operator, 'Unknown', 20),
    network: normalizeNetwork(source.network),
    signal_dbm: signal,
    latency_ms: clamp(toInt(source.latency_ms), 0, 5000),
    dl_mbps: clamp(toNumber(source.dl_mbps), 0, 10000),
    ul_mbps: clamp(toNumber(source.ul_mbps), 0, 10000),
    rsrp: clamp(toNumber(source.rsrp), -160, -20),
    rsrq: clamp(toNumber(source.rsrq), -40, 5),
    recorded_at: when.toISOString(),
  };
}

async function resolveTowerIds(readings) {
  const pendingCellIds = [];
  const unique = new Set();
  for (const reading of readings) {
    if (!reading.tower_id && reading.cell_id) {
      const key = String(reading.cell_id);
      if (!unique.has(key)) {
        unique.add(key);
        pendingCellIds.push(reading.cell_id);
      }
    }
  }
  if (!pendingCellIds.length) return;

  const { rows } = await pool.query(
    `SELECT DISTINCT ON (cell_id) id, cell_id
     FROM towers
     WHERE cell_id = ANY($1::bigint[])
     ORDER BY cell_id, samples DESC`,
    [pendingCellIds]
  );
  const byCellId = new Map(rows.map((r) => [String(r.cell_id), r.id]));
  for (const reading of readings) {
    if (!reading.tower_id && reading.cell_id) {
      reading.tower_id = byCellId.get(String(reading.cell_id)) || null;
    }
  }
}

async function insertReadings(readings) {
  if (!readings.length) return 0;
  await resolveTowerIds(readings);

  const values = [];
  const params = [];
  let p = 1;

  for (const r of readings) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    params.push(
      r.tower_id, r.cell_id, r.city, r.operator, r.network,
      r.signal_dbm, r.latency_ms, r.dl_mbps, r.ul_mbps,
      r.rsrp, r.rsrq, r.recorded_at
    );
  }

  await pool.query(
    `INSERT INTO readings
       (tower_id,cell_id,city,operator,network,signal_dbm,latency_ms,
        dl_mbps,ul_mbps,rsrp,rsrq,recorded_at)
     VALUES ${values.join(',')}`,
    params
  );
  return readings.length;
}

function requireIngestKey(req, res, next) {
  if (!INGEST_API_KEY) return next();
  const headerKey = req.header('x-netwatch-key');
  const bodyKey = req.body && req.body.api_key;
  const key = headerKey || bodyKey;
  if (key !== INGEST_API_KEY) {
    return res.status(401).json({ error: 'Unauthorized ingest key' });
  }
  return next();
}

// ─── GET /api/towers ──────────────────────────────────────────────
// Returns towers with optional filters: operator, network, city, bbox
// bbox=minLat,minLon,maxLat,maxLon  e.g. ?bbox=4,2,14,15
router.get('/towers', async (req, res) => {
  try {
    const { operator, network, city, bbox, limit = 5000 } = req.query;
    const conds = [], params = [];
    let p = 1;

    if (operator && operator !== 'all') {
      conds.push(`operator = $${p++}`); params.push(operator);
    }
    if (network && network !== 'all') {
      conds.push(`network = $${p++}`); params.push(network);
    }
    if (city && city !== 'all') {
      conds.push(`city = $${p++}`); params.push(city);
    }
    if (bbox) {
      const [minLat, minLon, maxLat, maxLon] = bbox.split(',').map(Number);
      conds.push(`lat BETWEEN $${p++} AND $${p++}`); params.push(minLat, maxLat);
      conds.push(`lon BETWEEN $${p++} AND $${p++}`); params.push(minLon, maxLon);
    }

    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
    const { rows } = await pool.query(
      `SELECT id, cell_id, radio, operator, network, city,
              lat, lon, range_m, samples, average_signal
       FROM towers ${where}
       ORDER BY samples DESC
       LIMIT $${p}`,
      [...params, parseInt(limit)]
    );
    res.json({ count: rows.length, towers: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── GET /api/towers/stats ────────────────────────────────────────
// Summary counts grouped by operator / network / city
router.get('/towers/stats', async (req, res) => {
  try {
    const [byOp, byNet, byCity] = await Promise.all([
      pool.query(`SELECT operator, COUNT(*) AS count FROM towers GROUP BY operator ORDER BY count DESC`),
      pool.query(`SELECT network, COUNT(*) AS count FROM towers GROUP BY network ORDER BY count DESC`),
      pool.query(`SELECT city, COUNT(*) AS count FROM towers GROUP BY city ORDER BY count DESC LIMIT 20`),
    ]);
    res.json({
      by_operator: byOp.rows,
      by_network:  byNet.rows,
      by_city:     byCity.rows,
      total:       byOp.rows.reduce((s, r) => s + parseInt(r.count), 0),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── POST /api/readings ───────────────────────────────────────────
// Bulk-insert simulated/real readings from the dashboard collector
router.post('/readings', async (req, res) => {
  try {
    const payload = Array.isArray(req.body?.readings) ? req.body.readings : [];
    if (!payload.length) return res.json({ inserted: 0 });
    if (payload.length > MAX_BULK_READINGS) {
      return res.status(400).json({ error: `Too many readings in one request. Max ${MAX_BULK_READINGS}.` });
    }

    const normalized = payload.map((item) => normalizeReading(item));
    const inserted = await insertReadings(normalized);
    res.json({ inserted });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

// Device-friendly single-reading ingest endpoint.
// Accepts one reading object and can be protected via INGEST_API_KEY.
router.post('/readings/device', requireIngestKey, async (req, res) => {
  try {
    const normalized = normalizeReading(req.body || {});
    const inserted = await insertReadings([normalized]);
    res.status(201).json({ inserted });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

// ─── GET /api/readings/latest ─────────────────────────────────────
// Most recent reading per tower (for map colouring on page load)
router.get('/readings/latest', async (req, res) => {
  try {
    const limit = Math.min(Math.max(parseInt(req.query.limit || '10000', 10) || 10000, 1), 20000);
    const { rows } = await pool.query(`
      SELECT DISTINCT ON (COALESCE(tower_id::bigint, -cell_id, -id::bigint))
             tower_id, cell_id, city, operator, network,
             signal_dbm, latency_ms, dl_mbps, ul_mbps, rsrp, rsrq, recorded_at
      FROM readings
      ORDER BY COALESCE(tower_id::bigint, -cell_id, -id::bigint), recorded_at DESC
      LIMIT $1
    `, [limit]);
    res.json({ count: rows.length, readings: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── GET /api/readings/history ────────────────────────────────────
// Time-series for analytics charts. ?hours=1  ?city=Lagos  ?operator=MTN
router.get('/readings/history', async (req, res) => {
  try {
    const { hours = 1, city, operator } = req.query;
    const conds = [`recorded_at > NOW() - INTERVAL '${parseInt(hours)} hours'`];
    const params = [];
    let p = 1;
    if (city)     { conds.push(`city = $${p++}`);     params.push(city); }
    if (operator) { conds.push(`operator = $${p++}`); params.push(operator); }

    const { rows } = await pool.query(
      `SELECT operator, network, city,
              AVG(signal_dbm) AS avg_signal,
              AVG(latency_ms) AS avg_latency,
              AVG(dl_mbps)    AS avg_dl,
              AVG(ul_mbps)    AS avg_ul,
              COUNT(*)        AS count,
              DATE_TRUNC('minute', recorded_at) AS bucket
       FROM readings
       WHERE ${conds.join(' AND ')}
       GROUP BY operator, network, city, bucket
       ORDER BY bucket ASC`,
      params
    );
    res.json({ rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── GET /api/readings/summary ────────────────────────────────────
// Aggregated stats for Patterns tab
router.get('/readings/summary', async (req, res) => {
  try {
    const [overall, byOp, byCity, byNet] = await Promise.all([
      pool.query(`
        SELECT COUNT(*) AS total,
               ROUND(AVG(signal_dbm)::numeric,1) AS avg_signal,
               ROUND(AVG(latency_ms)::numeric,0) AS avg_latency,
               ROUND(AVG(dl_mbps)::numeric,1)    AS avg_dl
        FROM readings WHERE recorded_at > NOW() - INTERVAL '1 hour'
      `),
      pool.query(`
        SELECT operator,
               ROUND(AVG(signal_dbm)::numeric,1) AS avg_signal,
               ROUND(AVG(dl_mbps)::numeric,1)    AS avg_dl,
               ROUND(AVG(latency_ms)::numeric,0) AS avg_latency,
               COUNT(*) AS readings
        FROM readings WHERE recorded_at > NOW() - INTERVAL '1 hour'
        GROUP BY operator ORDER BY avg_signal DESC
      `),
      pool.query(`
        SELECT city,
               ROUND(AVG(signal_dbm)::numeric,1) AS avg_signal,
               ROUND(AVG(latency_ms)::numeric,0) AS avg_latency,
               COUNT(*) AS readings
        FROM readings WHERE recorded_at > NOW() - INTERVAL '1 hour'
        GROUP BY city ORDER BY avg_signal DESC LIMIT 14
      `),
      pool.query(`
        SELECT network, COUNT(*) AS count
        FROM readings WHERE recorded_at > NOW() - INTERVAL '1 hour'
        GROUP BY network
      `),
    ]);
    res.json({
      overall:     overall.rows[0],
      by_operator: byOp.rows,
      by_city:     byCity.rows,
      by_network:  byNet.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/qos?city=Lagos&operator=MTN&network=4G&limit=200
// Synthetic ElectricSheep Africa QoS baseline metrics.
router.get('/qos', async (req, res) => {
  try {
    const {
      city,
      operator,
      network,
      tower_id,
      from,
      until,
      limit = 200,
    } = req.query;
    const conds = [];
    const params = [];
    let p = 1;

    if (city && city !== 'all') {
      conds.push(`city = $${p++}`);
      params.push(city);
    }
    if (operator && operator !== 'all') {
      conds.push(`operator = $${p++}`);
      params.push(operator);
    }
    if (network && network !== 'all') {
      conds.push(`network = $${p++}`);
      params.push(normalizeNetwork(network));
    }
    if (tower_id) {
      conds.push(`source_tower_id = $${p++}`);
      params.push(String(tower_id).slice(0, 40));
    }
    if (from) {
      const fromDate = new Date(from);
      if (Number.isNaN(fromDate.getTime())) {
        return res.status(400).json({ error: 'from must be a valid date/time' });
      }
      conds.push(`measured_at >= $${p++}`);
      params.push(fromDate.toISOString());
    }
    if (until) {
      const untilDate = new Date(until);
      if (Number.isNaN(untilDate.getTime())) {
        return res.status(400).json({ error: 'until must be a valid date/time' });
      }
      conds.push(`measured_at <= $${p++}`);
      params.push(untilDate.toISOString());
    }

    const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 200, 1), 1000);
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
    params.push(safeLimit);

    const { rows } = await pool.query(
      `SELECT metric_id, measured_at, source_tower_id, city, operator, network,
              latency_ms, jitter_ms, throughput_mbps, packet_loss_rate,
              error_rate, signal_strength_dbm, active_users, source
       FROM qos_metrics
       ${where}
       ORDER BY measured_at DESC
       LIMIT $${p}`,
      params
    );

    res.json({
      count: rows.length,
      source_note: 'Synthetic training data from ElectricSheep Africa / Amon Din, not live measured data.',
      metrics: rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/coverage?city=Kano&operator=MTN&network=4G&limit=200
// Synthetic ElectricSheep Africa coverage and signal-strength data.
router.get('/coverage', async (req, res) => {
  try {
    const { city, operator, network, bbox, limit = 200 } = req.query;
    const conds = [];
    const params = [];
    let p = 1;

    if (city && city !== 'all') {
      conds.push(`city = $${p++}`);
      params.push(city);
    }
    if (operator && operator !== 'all') {
      conds.push(`operator = $${p++}`);
      params.push(operator);
    }
    if (network && network !== 'all') {
      conds.push(`network = $${p++}`);
      params.push(normalizeNetwork(network));
    }
    if (bbox) {
      const [minLat, minLon, maxLat, maxLon] = bbox.split(',').map(Number);
      if (![minLat, minLon, maxLat, maxLon].every(Number.isFinite)) {
        return res.status(400).json({ error: 'bbox must be minLat,minLon,maxLat,maxLon' });
      }
      conds.push(`lat BETWEEN $${p++} AND $${p++}`);
      params.push(minLat, maxLat);
      conds.push(`lon BETWEEN $${p++} AND $${p++}`);
      params.push(minLon, maxLon);
    }

    const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 200, 1), 2000);
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
    params.push(safeLimit);

    const { rows } = await pool.query(
      `SELECT measurement_id, lat, lon, city, operator, network,
              signal_strength_dbm, coverage_quality, download_speed_mbps,
              upload_speed_mbps, latency_ms, source_tower_id,
              distance_to_tower_km, indoor_outdoor, source
       FROM coverage_data
       ${where}
       ORDER BY signal_strength_dbm DESC NULLS LAST
       LIMIT $${p}`,
      params
    );

    res.json({
      count: rows.length,
      source_note: 'Synthetic training data from ElectricSheep Africa / Amon Din, not live measured data.',
      coverage: rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events?operator=MTN&city=Lagos&type=congestion&severity=high&limit=200
// Synthetic ElectricSheep Africa network event logs.
router.get('/events', async (req, res) => {
  try {
    const {
      operator,
      city,
      type,
      severity,
      network,
      from,
      until,
      limit = 200,
    } = req.query;
    const conds = [];
    const params = [];
    let p = 1;

    if (operator && operator !== 'all') {
      conds.push(`operator = $${p++}`);
      params.push(operator);
    }
    if (city && city !== 'all') {
      conds.push(`city = $${p++}`);
      params.push(city);
    }
    if (type && type !== 'all') {
      conds.push(`event_type = $${p++}`);
      params.push(String(type).slice(0, 60));
    }
    if (severity && severity !== 'all') {
      conds.push(`severity = $${p++}`);
      params.push(String(severity).slice(0, 20));
    }
    if (network && network !== 'all') {
      conds.push(`network = $${p++}`);
      params.push(normalizeNetwork(network));
    }
    if (from) {
      const fromDate = new Date(from);
      if (Number.isNaN(fromDate.getTime())) {
        return res.status(400).json({ error: 'from must be a valid date/time' });
      }
      conds.push(`event_time >= $${p++}`);
      params.push(fromDate.toISOString());
    }
    if (until) {
      const untilDate = new Date(until);
      if (Number.isNaN(untilDate.getTime())) {
        return res.status(400).json({ error: 'until must be a valid date/time' });
      }
      conds.push(`event_time <= $${p++}`);
      params.push(untilDate.toISOString());
    }

    const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 200, 1), 1000);
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
    params.push(safeLimit);

    const { rows } = await pool.query(
      `SELECT event_id, event_time, source_tower_id, city, event_type, severity,
              affected_users, packet_loss_percent, network, operator, source
       FROM network_events
       ${where}
       ORDER BY event_time DESC
       LIMIT $${p}`,
      params
    );

    res.json({
      count: rows.length,
      source_note: 'Synthetic training data from ElectricSheep Africa / Amon Din, not live measured data.',
      events: rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/hardware?city=Lagos&tower_id=LAG-1234&alert=true&limit=200
// Synthetic ElectricSheep Africa tower hardware sensor data.
router.get('/hardware', async (req, res) => {
  try {
    const {
      city,
      tower_id,
      equipment_type,
      health_status,
      alert,
      from,
      until,
      limit = 200,
    } = req.query;
    const conds = [];
    const params = [];
    let p = 1;

    if (city && city !== 'all') {
      conds.push(`city = $${p++}`);
      params.push(city);
    }
    if (tower_id) {
      conds.push(`source_tower_id = $${p++}`);
      params.push(String(tower_id).slice(0, 40));
    }
    if (equipment_type && equipment_type !== 'all') {
      conds.push(`equipment_type = $${p++}`);
      params.push(String(equipment_type).slice(0, 40));
    }
    if (health_status && health_status !== 'all') {
      conds.push(`health_status = $${p++}`);
      params.push(String(health_status).slice(0, 20));
    }
    if (alert !== undefined && alert !== 'all') {
      if (!['true', 'false'].includes(String(alert).toLowerCase())) {
        return res.status(400).json({ error: 'alert must be true or false' });
      }
      conds.push(`alert_triggered = $${p++}`);
      params.push(String(alert).toLowerCase() === 'true');
    }
    if (from) {
      const fromDate = new Date(from);
      if (Number.isNaN(fromDate.getTime())) {
        return res.status(400).json({ error: 'from must be a valid date/time' });
      }
      conds.push(`sensor_time >= $${p++}`);
      params.push(fromDate.toISOString());
    }
    if (until) {
      const untilDate = new Date(until);
      if (Number.isNaN(untilDate.getTime())) {
        return res.status(400).json({ error: 'until must be a valid date/time' });
      }
      conds.push(`sensor_time <= $${p++}`);
      params.push(untilDate.toISOString());
    }

    const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 200, 1), 1000);
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
    params.push(safeLimit);

    const { rows } = await pool.query(
      `SELECT sensor_id, sensor_time, source_tower_id, city, equipment_type,
              temperature_celsius, power_draw_watts, voltage_v, humidity_percent,
              vibration_level, health_status, alert_triggered, source
       FROM hardware_sensors
       ${where}
       ORDER BY sensor_time DESC
       LIMIT $${p}`,
      params
    );

    res.json({
      count: rows.length,
      source_note: 'Synthetic training data from ElectricSheep Africa / Amon Din, not live measured data.',
      sensors: rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── POST /api/speedtests ─────────────────────────────────────────
router.post('/speedtests', async (req, res) => {
  try {
    const { server_city, operator, dl_mbps, ul_mbps, ping_ms, jitter_ms, rating } = req.body;
    const { rows } = await pool.query(
      `INSERT INTO speed_tests (server_city,operator,dl_mbps,ul_mbps,ping_ms,jitter_ms,rating)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
      [server_city, operator, dl_mbps, ul_mbps, ping_ms, jitter_ms, rating]
    );
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── GET /api/speedtests ─────────────────────────────────────────
router.get('/speedtests', async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT * FROM speed_tests ORDER BY tested_at DESC LIMIT 50`
    );
    res.json({ tests: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── GET /api/health ─────────────────────────────────────────────
router.get('/health', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT COUNT(*) FROM towers');
    res.json({ status: 'ok', towers_in_db: parseInt(rows[0].count) });
  } catch (err) {
    res.status(500).json({ status: 'error', error: err.message });
  }
});

// GET /api/recommendation?lat=6.52&lon=3.37&radius_km=5&hours=24
// Returns ranked provider recommendations near a destination.
router.get('/recommendation', async (req, res) => {
  try {
    const lat = Number(req.query.lat);
    const lon = Number(req.query.lon);
    const radiusKm = Math.min(Math.max(Number(req.query.radius_km || 5), 1), 50);
    const hours = Math.min(Math.max(parseInt(req.query.hours || '24', 10) || 24, 1), 168);

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      return res.status(400).json({ error: 'lat and lon are required numeric query params' });
    }

    const { rows } = await pool.query(
      `
      WITH nearby AS (
        SELECT
          r.operator,
          r.signal_dbm,
          r.latency_ms,
          r.dl_mbps,
          r.ul_mbps,
          r.recorded_at,
          (6371 * acos(
            cos(radians($1)) * cos(radians(t.lat)) * cos(radians(t.lon) - radians($2)) +
            sin(radians($1)) * sin(radians(t.lat))
          )) AS distance_km
        FROM readings r
        JOIN towers t ON t.id = r.tower_id
        WHERE r.recorded_at > NOW() - ($4::text || ' hours')::interval
      )
      SELECT
        operator,
        COUNT(*)::int AS sample_count,
        ROUND(AVG(signal_dbm)::numeric, 1) AS avg_signal,
        ROUND(AVG(latency_ms)::numeric, 0) AS avg_latency,
        ROUND(AVG(dl_mbps)::numeric, 1) AS avg_dl,
        ROUND(AVG(ul_mbps)::numeric, 1) AS avg_ul,
        ROUND(MIN(distance_km)::numeric, 2) AS nearest_km,
        ROUND(MAX(EXTRACT(EPOCH FROM (NOW() - recorded_at)))::numeric, 0) AS max_age_sec
      FROM nearby
      WHERE distance_km <= $3
      GROUP BY operator
      ORDER BY sample_count DESC
      `,
      [lat, lon, radiusKm, hours]
    );

    if (!rows.length) {
      const { rows: coverageRows } = await pool.query(
        `
        WITH nearby AS (
          SELECT
            operator,
            signal_strength_dbm,
            latency_ms,
            download_speed_mbps,
            upload_speed_mbps,
            (6371 * acos(
              LEAST(1, GREATEST(-1,
                cos(radians($1)) * cos(radians(lat)) * cos(radians(lon) - radians($2)) +
                sin(radians($1)) * sin(radians(lat))
              ))
            )) AS distance_km
          FROM coverage_data
        )
        SELECT
          operator,
          COUNT(*)::int AS sample_count,
          ROUND(AVG(signal_strength_dbm)::numeric, 1) AS avg_signal,
          ROUND(AVG(latency_ms)::numeric, 0) AS avg_latency,
          ROUND(AVG(download_speed_mbps)::numeric, 1) AS avg_dl,
          ROUND(AVG(upload_speed_mbps)::numeric, 1) AS avg_ul,
          ROUND(MIN(distance_km)::numeric, 2) AS nearest_km
        FROM nearby
        WHERE distance_km <= $3
        GROUP BY operator
        ORDER BY sample_count DESC
        `,
        [lat, lon, radiusKm]
      );

      if (!coverageRows.length) {
        return res.json({
          ok: true,
          destination: { lat, lon, radius_km: radiusKm, hours },
          confidence: 'low',
          message: 'No recent readings or synthetic coverage samples found near this destination.',
          providers: [],
        });
      }

      const coverageRanked = coverageRows.map((r) => {
        const sigScore = Math.max(0, Math.min(100, ((Number(r.avg_signal) + 110) / 60) * 100));
        const latScore = Math.max(0, Math.min(100, 100 - (Number(r.avg_latency) / 200) * 100));
        const dlScore = Math.max(0, Math.min(100, (Number(r.avg_dl) / 50) * 100));
        const sampleScore = Math.max(20, Math.min(100, (Number(r.sample_count) / 80) * 100));
        const totalScore = (sigScore * 0.45) + (latScore * 0.2) + (dlScore * 0.25) + (sampleScore * 0.1);

        return {
          operator: r.operator || 'Unknown',
          score: Math.round(totalScore * 10) / 10,
          sample_count: Number(r.sample_count),
          avg_signal: Number(r.avg_signal),
          avg_latency: Number(r.avg_latency),
          avg_dl: Number(r.avg_dl),
          avg_ul: Number(r.avg_ul),
          nearest_km: Number(r.nearest_km),
          source: 'synthetic_coverage',
        };
      }).sort((a, b) => b.score - a.score);

      return res.json({
        ok: true,
        destination: { lat, lon, radius_km: radiusKm, hours },
        confidence: 'medium',
        source_note: 'Recommendation uses synthetic ElectricSheep Africa coverage data because no recent live readings were found nearby.',
        providers: coverageRanked,
        best_provider: coverageRanked[0] || null,
      });
    }

    const ranked = rows.map((r) => {
      const sigScore = Math.max(0, Math.min(100, ((Number(r.avg_signal) + 110) / 60) * 100));
      const latScore = Math.max(0, Math.min(100, 100 - (Number(r.avg_latency) / 200) * 100));
      const dlScore = Math.max(0, Math.min(100, (Number(r.avg_dl) / 50) * 100));
      const freshnessScore = Number(r.max_age_sec) <= 3600 ? 100 : Number(r.max_age_sec) <= 21600 ? 75 : 50;
      const sampleScore = Math.max(20, Math.min(100, (Number(r.sample_count) / 80) * 100));
      const totalScore = (sigScore * 0.4) + (latScore * 0.25) + (dlScore * 0.2) + (freshnessScore * 0.1) + (sampleScore * 0.05);

      return {
        operator: r.operator || 'Unknown',
        score: Math.round(totalScore * 10) / 10,
        sample_count: Number(r.sample_count),
        avg_signal: Number(r.avg_signal),
        avg_latency: Number(r.avg_latency),
        avg_dl: Number(r.avg_dl),
        avg_ul: Number(r.avg_ul),
        nearest_km: Number(r.nearest_km),
      };
    }).sort((a, b) => b.score - a.score);

    const totalSamples = ranked.reduce((sum, p) => sum + p.sample_count, 0);
    const confidence = totalSamples >= 120 ? 'high' : totalSamples >= 40 ? 'medium' : 'low';

    res.json({
      ok: true,
      destination: { lat, lon, radius_km: radiusKm, hours },
      confidence,
      providers: ranked,
      best_provider: ranked[0] || null,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/outages?operator=MTN&from=...&until=...&min_severity=20
router.get('/outages', async (req, res) => {
  try {
    const operator = req.query.operator;
    const minSeverity = Number(req.query.min_severity || 0);
    const limit = Math.min(Math.max(parseInt(req.query.limit || '200', 10) || 200, 1), 500);
    const from = req.query.from ? new Date(Number(req.query.from) * 1000) : new Date(Date.now() - (7 * 24 * 3600 * 1000));
    const until = req.query.until ? new Date(Number(req.query.until) * 1000) : new Date();

    const conds = ['started_at BETWEEN $1 AND $2'];
    const params = [from.toISOString(), until.toISOString()];
    let p = 3;
    if (operator && operator !== 'all') {
      conds.push(`operator = $${p++}`);
      params.push(operator);
    }
    if (Number.isFinite(minSeverity) && minSeverity > 0) {
      conds.push(`severity >= $${p++}`);
      params.push(minSeverity);
    }

    params.push(limit);
    const where = conds.join(' AND ');
    const { rows } = await pool.query(
      `SELECT id, source, entity_type, entity_code, operator, severity, signal_type, started_at, ended_at, duration_min, raw_data
       FROM outages
       WHERE ${where}
       ORDER BY started_at DESC
       LIMIT $${p}`,
      params
    );
    res.json({ count: rows.length, outages: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/outages/stats
router.get('/outages/stats', async (req, res) => {
  try {
    const [byOperator, total30] = await Promise.all([
      pool.query(`
        SELECT
          operator,
          COUNT(*)::int AS total_outages,
          ROUND(AVG(severity)::numeric, 1) AS avg_severity,
          ROUND(AVG(duration_min)::numeric, 1) AS avg_duration_min,
          MAX(started_at) AS most_recent_outage
        FROM outages
        GROUP BY operator
        ORDER BY total_outages DESC
      `),
      pool.query(`
        SELECT COUNT(*)::int AS total_outages_30d
        FROM outages
        WHERE started_at > NOW() - INTERVAL '30 days'
      `),
    ]);

    res.json({
      by_operator: byOperator.rows,
      total_outages_30d: Number(total30.rows[0]?.total_outages_30d || 0),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/outages/correlate
// For each outage, compute operator readings in the 2h pre-outage window.
router.get('/outages/correlate', async (req, res) => {
  try {
    const limit = Math.min(Math.max(parseInt(req.query.limit || '100', 10) || 100, 1), 500);
    const { rows } = await pool.query(
      `
      WITH limited_outages AS (
        SELECT *
        FROM outages
        ORDER BY started_at DESC
        LIMIT $1
      ),
      reading_stats AS (
        SELECT
          o.id AS outage_id,
          COUNT(r.*)::int AS sample_count,
          ROUND(AVG(r.signal_dbm)::numeric, 1) AS pre_avg_signal,
          ROUND(AVG(r.latency_ms)::numeric, 1) AS pre_avg_latency,
          ROUND(AVG(r.dl_mbps)::numeric, 1) AS pre_avg_dl
        FROM limited_outages o
        LEFT JOIN readings r
          ON r.operator = o.operator
         AND r.recorded_at >= o.started_at - INTERVAL '2 hours'
         AND r.recorded_at < o.started_at
        GROUP BY o.id
      ),
      event_stats AS (
        SELECT
          o.id AS outage_id,
          COUNT(ne.*)::int AS pre_event_count,
          ROUND(AVG(ne.packet_loss_percent)::numeric, 2) AS pre_avg_packet_loss,
          COALESCE(
            jsonb_object_agg(ne.event_type, event_counts.total)
              FILTER (WHERE ne.event_type IS NOT NULL),
            '{}'::jsonb
          ) AS pre_event_types
        FROM limited_outages o
        LEFT JOIN network_events ne
          ON ne.operator = o.operator
         AND ne.event_time >= o.started_at - INTERVAL '2 hours'
         AND ne.event_time < o.started_at
        LEFT JOIN LATERAL (
          SELECT COUNT(*)::int AS total
          FROM network_events ne2
          WHERE ne2.operator = o.operator
            AND ne2.event_time >= o.started_at - INTERVAL '2 hours'
            AND ne2.event_time < o.started_at
            AND ne2.event_type = ne.event_type
        ) event_counts ON true
        GROUP BY o.id
      ),
      hardware_stats AS (
        SELECT
          o.id AS outage_id,
          COUNT(hs.*)::int AS pre_hardware_sample_count,
          COUNT(*) FILTER (WHERE hs.alert_triggered = true)::int AS pre_hardware_alert_count,
          COUNT(*) FILTER (
            WHERE LOWER(COALESCE(hs.health_status, '')) IN ('warning', 'critical', 'fault', 'faulty', 'degraded')
          )::int AS pre_hardware_bad_health_count,
          ROUND(AVG(hs.temperature_celsius)::numeric, 1) AS pre_avg_temperature_celsius,
          ROUND(AVG(hs.power_draw_watts)::numeric, 1) AS pre_avg_power_draw_watts
        FROM limited_outages o
        LEFT JOIN hardware_sensors hs
          ON hs.sensor_time >= o.started_at - INTERVAL '2 hours'
         AND hs.sensor_time < o.started_at
        GROUP BY o.id
      )
      SELECT
        o.id AS outage_id,
        o.operator,
        o.signal_type,
        o.severity,
        o.started_at,
        o.ended_at,
        o.duration_min,
        COALESCE(rs.sample_count, 0) AS sample_count,
        rs.pre_avg_signal,
        rs.pre_avg_latency,
        rs.pre_avg_dl,
        COALESCE(es.pre_event_count, 0) AS pre_event_count,
        es.pre_avg_packet_loss,
        COALESCE(es.pre_event_types, '{}'::jsonb) AS pre_event_types,
        COALESCE(hws.pre_hardware_sample_count, 0) AS pre_hardware_sample_count,
        COALESCE(hws.pre_hardware_alert_count, 0) AS pre_hardware_alert_count,
        COALESCE(hws.pre_hardware_bad_health_count, 0) AS pre_hardware_bad_health_count,
        hws.pre_avg_temperature_celsius,
        hws.pre_avg_power_draw_watts
      FROM limited_outages o
      LEFT JOIN reading_stats rs ON rs.outage_id = o.id
      LEFT JOIN event_stats es ON es.outage_id = o.id
      LEFT JOIN hardware_stats hws ON hws.outage_id = o.id
      ORDER BY o.started_at DESC
      `,
      [limit]
    );
    res.json({ count: rows.length, correlations: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/outages/sync
router.post('/outages/sync', async (req, res) => {
  try {
    const from = req.body?.from ? Number(req.body.from) : undefined;
    const until = req.body?.until ? Number(req.body.until) : undefined;
    const result = await runSyncIODA({ from, until });
    res.json({ ok: true, ...result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/speedtest/ping', async (req, res) => {
  res.set('Cache-Control', 'no-store');
  res.json({ ok: true, ts: Date.now() });
});

router.get('/speedtest/download', async (req, res) => {
  const sizeMb = Math.min(Math.max(parseInt(req.query.size_mb || '8', 10) || 8, 1), 30);
  const bytes = sizeMb * 1024 * 1024;
  const buffer = Buffer.alloc(bytes, 97);
  res.set('Content-Type', 'application/octet-stream');
  res.set('Content-Length', String(bytes));
  res.set('Cache-Control', 'no-store');
  res.send(buffer);
});

router.post('/speedtest/upload', express.raw({ type: '*/*', limit: '35mb' }), async (req, res) => {
  const size = req.body ? req.body.length : 0;
  res.set('Cache-Control', 'no-store');
  res.json({ ok: true, received_bytes: size, ts: Date.now() });
});

module.exports = router;
