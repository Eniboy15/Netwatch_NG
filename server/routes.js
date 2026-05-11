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

function scoreRange(value, low, high, maxPoints, invert = false) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  const raw = invert ? (high - n) / (high - low) : (n - low) / (high - low);
  return clamp(raw, 0, 1) * maxPoints;
}

function scoreBelow(value, ok, bad, maxPoints) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return clamp((ok - n) / (ok - bad), 0, 1) * maxPoints;
}

function riskBand(score) {
  if (score >= 70) return 'critical';
  if (score >= 45) return 'high';
  if (score >= 25) return 'medium';
  return 'low';
}

function addRisk(contributors, label, points, evidence, source = 'live_or_imported') {
  const value = Number(points);
  if (!Number.isFinite(value) || value <= 0) return;
  contributors.push({
    label,
    points: Math.round(value * 10) / 10,
    evidence,
    source,
  });
}

function buildOutageRisk(row) {
  const contributors = [];

  addRisk(
    contributors,
    'Weak recent signal',
    scoreBelow(row.live_avg_signal, -85, -110, 18),
    row.live_avg_signal === null ? null : `${row.live_avg_signal} dBm`,
    'live_readings'
  );
  addRisk(
    contributors,
    'High live latency',
    scoreRange(row.live_avg_latency, 80, 260, 12),
    row.live_avg_latency === null ? null : `${row.live_avg_latency} ms`,
    'live_readings'
  );
  addRisk(
    contributors,
    'Low live download speed',
    scoreBelow(row.live_avg_dl, 12, 3, 8),
    row.live_avg_dl === null ? null : `${row.live_avg_dl} Mbps`,
    'live_readings'
  );
  addRisk(
    contributors,
    'Synthetic QoS degradation',
    scoreBelow(row.qos_avg_signal, -88, -112, 10)
      + scoreRange(row.qos_avg_latency, 90, 260, 6)
      + scoreRange(row.qos_avg_packet_loss, 1, 8, 6)
      + scoreRange(row.qos_avg_error_rate, 0.5, 5, 4),
    `${row.qos_sample_count || 0} QoS samples`,
    'synthetic_training'
  );
  addRisk(
    contributors,
    'Network event spike',
    scoreRange(row.event_count, 50, 1500, 12) + scoreRange(row.event_avg_packet_loss, 1, 10, 5),
    `${row.event_count || 0} events`,
    'synthetic_training'
  );
  addRisk(
    contributors,
    'Uptime outage labels',
    scoreRange(row.uptime_downtime_minutes, 10, 240, 10)
      + scoreRange(row.uptime_outage_count, 1000, 20000, 8)
      + scoreBelow(row.uptime_avg_percentage, 99.2, 95, 8),
    `${row.uptime_outage_count || 0} synthetic outages`,
    'synthetic_training'
  );
  addRisk(
    contributors,
    'Infrastructure hardware alerts',
    scoreRange(row.hardware_alert_count, 20, 500, 7) + scoreRange(row.hardware_bad_health_count, 20, 500, 7),
    `${row.hardware_alert_count || 0} alerts`,
    'synthetic_training'
  );
  addRisk(
    contributors,
    'Energy pressure',
    scoreBelow(row.energy_avg_grid_hours, 18, 6, 5) + scoreRange(row.energy_avg_generator_hours, 3, 16, 5),
    row.energy_avg_grid_hours === null ? null : `${row.energy_avg_grid_hours}h grid avg`,
    'synthetic_training'
  );
  addRisk(
    contributors,
    'Maintenance backlog',
    scoreRange(row.maintenance_high_priority_count, 1, 12, 8) + scoreRange(row.maintenance_downtime_minutes, 30, 360, 7),
    `${row.maintenance_high_priority_count || 0} high-priority orders`,
    'synthetic_training'
  );
  addRisk(
    contributors,
    'Latency SLA misses',
    scoreRange(row.latency_sla_miss_count, 2, 30, 8) + scoreRange(row.latency_avg_packet_loss, 1, 8, 5),
    `${row.latency_sla_miss_count || 0} SLA misses`,
    'synthetic_training'
  );
  addRisk(
    contributors,
    'Handover failures',
    scoreRange(row.handover_failure_count, 2, 35, 8),
    `${row.handover_failure_count || 0} failures`,
    'synthetic_training'
  );
  addRisk(
    contributors,
    'Dropped-call spike',
    scoreRange(row.dropped_call_count, 20, 500, 10)
      + scoreBelow(row.dropped_avg_signal, -88, -112, 5),
    `${row.dropped_call_count || 0} dropped calls`,
    'synthetic_training'
  );
  addRisk(
    contributors,
    'Unresolved technician activity',
    scoreRange(row.technician_unresolved_count, 1, 15, 8),
    `${row.technician_unresolved_count || 0} unresolved jobs`,
    'synthetic_training'
  );

  const rawScore = contributors.reduce((sum, item) => sum + item.points, 0);
  const score = Math.round(Math.min(100, rawScore) * 10) / 10;
  const evidenceCount =
    Number(row.live_sample_count || 0)
    + Number(row.qos_sample_count || 0)
    + Number(row.event_count || 0)
    + Number(row.uptime_log_count || 0)
    + Number(row.maintenance_order_count || 0)
    + Number(row.latency_log_count || 0)
    + Number(row.handover_count || 0)
    + Number(row.dropped_call_count || 0)
    + Number(row.technician_activity_count || 0);
  const confidence = evidenceCount >= 300 ? 'high' : evidenceCount >= 60 ? 'medium' : 'low';

  return {
    operator: row.operator,
    score,
    band: riskBand(score),
    confidence,
    evidence_count: evidenceCount,
    live: {
      sample_count: Number(row.live_sample_count || 0),
      avg_signal: row.live_avg_signal === null ? null : Number(row.live_avg_signal),
      avg_latency: row.live_avg_latency === null ? null : Number(row.live_avg_latency),
      avg_dl: row.live_avg_dl === null ? null : Number(row.live_avg_dl),
    },
    synthetic: {
      qos_samples: Number(row.qos_sample_count || 0),
      network_events: Number(row.event_count || 0),
      uptime_outages: Number(row.uptime_outage_count || 0),
      hardware_alerts: Number(row.hardware_alert_count || 0),
      maintenance_orders: Number(row.maintenance_order_count || 0),
      latency_sla_misses: Number(row.latency_sla_miss_count || 0),
      handover_failures: Number(row.handover_failure_count || 0),
      dropped_calls: Number(row.dropped_call_count || 0),
      unresolved_technician_jobs: Number(row.technician_unresolved_count || 0),
    },
    contributors: contributors.sort((a, b) => b.points - a.points).slice(0, 6),
  };
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

// GET /api/uptime?operator=MTN&city=Lagos&reason=power_failure&limit=200
// Synthetic ElectricSheep Africa base-station uptime and downtime labels.
router.get('/uptime', async (req, res) => {
  try {
    const {
      operator,
      city,
      state,
      reason,
      network,
      tower_id,
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
    if (state && state !== 'all') {
      conds.push(`state = $${p++}`);
      params.push(state);
    }
    if (reason && reason !== 'all') {
      conds.push(`outage_reason = $${p++}`);
      params.push(String(reason).slice(0, 40));
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
      conds.push(`log_date >= $${p++}`);
      params.push(fromDate.toISOString().slice(0, 10));
    }
    if (until) {
      const untilDate = new Date(until);
      if (Number.isNaN(untilDate.getTime())) {
        return res.status(400).json({ error: 'until must be a valid date/time' });
      }
      conds.push(`log_date <= $${p++}`);
      params.push(untilDate.toISOString().slice(0, 10));
    }

    const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 200, 1), 1000);
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
    params.push(safeLimit);

    const { rows } = await pool.query(
      `SELECT source_tower_id, log_date, operator, city, state, uptime_percentage,
              downtime_minutes, outage_count, outage_reason, network,
              avg_users_affected, source
       FROM uptime_logs
       ${where}
       ORDER BY log_date DESC, downtime_minutes DESC NULLS LAST
       LIMIT $${p}`,
      params
    );

    res.json({
      count: rows.length,
      source_note: 'Synthetic training data from ElectricSheep Africa / Amon Din, not live measured data.',
      uptime_logs: rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/energy?city=Lagos&tower_id=LAG-1234&power_source=generator&limit=200
// Synthetic ElectricSheep Africa tower energy consumption records.
router.get('/energy', async (req, res) => {
  try {
    const {
      city,
      tower_id,
      power_source,
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
    if (power_source && power_source !== 'all') {
      conds.push(`power_source = $${p++}`);
      params.push(String(power_source).slice(0, 40));
    }
    if (from) {
      const fromDate = new Date(from);
      if (Number.isNaN(fromDate.getTime())) {
        return res.status(400).json({ error: 'from must be a valid date/time' });
      }
      conds.push(`usage_date >= $${p++}`);
      params.push(fromDate.toISOString().slice(0, 10));
    }
    if (until) {
      const untilDate = new Date(until);
      if (Number.isNaN(untilDate.getTime())) {
        return res.status(400).json({ error: 'until must be a valid date/time' });
      }
      conds.push(`usage_date <= $${p++}`);
      params.push(untilDate.toISOString().slice(0, 10));
    }

    const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 200, 1), 1000);
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
    params.push(safeLimit);

    const { rows } = await pool.query(
      `SELECT source_tower_id, usage_date, city, power_source,
              daily_consumption_kwh, cost_per_kwh_ngn, total_cost_ngn,
              grid_availability_hours, generator_runtime_hours,
              solar_generation_kwh, fuel_consumed_liters,
              carbon_emissions_kg, source
       FROM energy_consumption
       ${where}
       ORDER BY usage_date DESC, daily_consumption_kwh DESC NULLS LAST
       LIMIT $${p}`,
      params
    );

    res.json({
      count: rows.length,
      source_note: 'Synthetic training data from ElectricSheep Africa / Amon Din, not live measured data.',
      energy: rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/maintenance?operator=MTN&city=Lagos&status=completed&limit=200
// Synthetic ElectricSheep Africa maintenance work orders.
router.get('/maintenance', async (req, res) => {
  try {
    const {
      operator,
      city,
      tower_id,
      type,
      issue,
      priority,
      status,
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
    if (tower_id) {
      conds.push(`source_tower_id = $${p++}`);
      params.push(String(tower_id).slice(0, 40));
    }
    if (type && type !== 'all') {
      conds.push(`maintenance_type = $${p++}`);
      params.push(String(type).slice(0, 40));
    }
    if (issue && issue !== 'all') {
      conds.push(`issue_category = $${p++}`);
      params.push(String(issue).slice(0, 60));
    }
    if (priority && priority !== 'all') {
      conds.push(`priority = $${p++}`);
      params.push(String(priority).slice(0, 20));
    }
    if (status && status !== 'all') {
      conds.push(`status = $${p++}`);
      params.push(String(status).slice(0, 30));
    }
    if (from) {
      const fromDate = new Date(from);
      if (Number.isNaN(fromDate.getTime())) {
        return res.status(400).json({ error: 'from must be a valid date/time' });
      }
      conds.push(`scheduled_date >= $${p++}`);
      params.push(fromDate.toISOString().slice(0, 10));
    }
    if (until) {
      const untilDate = new Date(until);
      if (Number.isNaN(untilDate.getTime())) {
        return res.status(400).json({ error: 'until must be a valid date/time' });
      }
      conds.push(`scheduled_date <= $${p++}`);
      params.push(untilDate.toISOString().slice(0, 10));
    }

    const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 200, 1), 1000);
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
    params.push(safeLimit);

    const { rows } = await pool.query(
      `SELECT work_order_id, source_tower_id, city, operator, maintenance_type,
              issue_category, priority, status, scheduled_date, completed_date,
              technician_id, repair_duration_hours, parts_replaced, cost_ngn,
              downtime_minutes, source
       FROM maintenance_orders
       ${where}
       ORDER BY scheduled_date DESC NULLS LAST, priority
       LIMIT $${p}`,
      params
    );

    res.json({
      count: rows.length,
      source_note: 'Synthetic training data from ElectricSheep Africa / Amon Din, not live measured data.',
      maintenance: rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/latency?operator=MTN&city=Lagos&network=5G&limit=200
// Synthetic ElectricSheep Africa ultra-low-latency logs.
router.get('/latency', async (req, res) => {
  try {
    const {
      operator,
      city,
      network,
      tower_id,
      application_type,
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
    if (network && network !== 'all') {
      conds.push(`network = $${p++}`);
      params.push(normalizeNetwork(network));
    }
    if (tower_id) {
      conds.push(`source_tower_id = $${p++}`);
      params.push(String(tower_id).slice(0, 40));
    }
    if (application_type && application_type !== 'all') {
      conds.push(`application_type = $${p++}`);
      params.push(String(application_type).slice(0, 60));
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
      `SELECT log_id, measured_at, source_tower_id, city, operator, network,
              application_type, latency_ms, jitter_ms, packet_loss_percent,
              throughput_mbps, edge_server_id, edge_distance_km, sla_target_ms,
              sla_met, reliability_percent, active_connections, source
       FROM latency_logs
       ${where}
       ORDER BY measured_at DESC
       LIMIT $${p}`,
      params
    );

    res.json({
      count: rows.length,
      source_note: 'Synthetic training data from ElectricSheep Africa / Amon Din, not live measured data.',
      latency_logs: rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/handovers?operator=MTN&city=Lagos&success=false&limit=200
// Synthetic ElectricSheep Africa cell-tower handover records.
router.get('/handovers', async (req, res) => {
  try {
    const {
      operator,
      city,
      network,
      tower_id,
      success,
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
      conds.push(`(source_city = $${p} OR target_city = $${p})`);
      params.push(city);
      p++;
    }
    if (network && network !== 'all') {
      conds.push(`network = $${p++}`);
      params.push(normalizeNetwork(network));
    }
    if (tower_id) {
      conds.push(`(source_tower_id = $${p} OR target_tower_id = $${p})`);
      params.push(String(tower_id).slice(0, 40));
      p++;
    }
    if (success !== undefined && success !== 'all') {
      if (!['true', 'false'].includes(String(success).toLowerCase())) {
        return res.status(400).json({ error: 'success must be true or false' });
      }
      conds.push(`success = $${p++}`);
      params.push(String(success).toLowerCase() === 'true');
    }
    if (from) {
      const fromDate = new Date(from);
      if (Number.isNaN(fromDate.getTime())) {
        return res.status(400).json({ error: 'from must be a valid date/time' });
      }
      conds.push(`handover_time >= $${p++}`);
      params.push(fromDate.toISOString());
    }
    if (until) {
      const untilDate = new Date(until);
      if (Number.isNaN(untilDate.getTime())) {
        return res.status(400).json({ error: 'until must be a valid date/time' });
      }
      conds.push(`handover_time <= $${p++}`);
      params.push(untilDate.toISOString());
    }

    const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 200, 1), 1000);
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
    params.push(safeLimit);

    const { rows } = await pool.query(
      `SELECT handover_id, handover_time, source_tower_id, target_tower_id,
              source_city, target_city, operator, network, handover_type,
              success, duration_ms, source_signal_dbm, target_signal_dbm,
              failure_reason, active_call, active_data_session,
              device_speed_kmh, source
       FROM handover_records
       ${where}
       ORDER BY handover_time DESC
       LIMIT $${p}`,
      params
    );

    res.json({
      count: rows.length,
      source_note: 'Synthetic training data from ElectricSheep Africa / Amon Din, not live measured data.',
      handovers: rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/dropped-calls', async (req, res) => {
  try {
    const { operator, city, network, reason, tower_id, from, until, limit = 200 } = req.query;
    const conds = [];
    const params = [];
    let p = 1;

    if (operator && operator !== 'all') { conds.push(`operator = $${p++}`); params.push(operator); }
    if (city && city !== 'all') { conds.push(`city = $${p++}`); params.push(city); }
    if (network && network !== 'all') { conds.push(`network = $${p++}`); params.push(normalizeNetwork(network)); }
    if (reason && reason !== 'all') { conds.push(`drop_reason = $${p++}`); params.push(String(reason).slice(0, 60)); }
    if (tower_id) { conds.push(`source_tower_id = $${p++}`); params.push(String(tower_id).slice(0, 40)); }
    if (from) {
      const d = new Date(from);
      if (Number.isNaN(d.getTime())) return res.status(400).json({ error: 'from must be a valid date/time' });
      conds.push(`dropped_at >= $${p++}`); params.push(d.toISOString());
    }
    if (until) {
      const d = new Date(until);
      if (Number.isNaN(d.getTime())) return res.status(400).json({ error: 'until must be a valid date/time' });
      conds.push(`dropped_at <= $${p++}`); params.push(d.toISOString());
    }

    const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 200, 1), 1000);
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
    params.push(safeLimit);
    const { rows } = await pool.query(
      `SELECT drop_id, dropped_at, operator, calling_number, called_number,
              source_tower_id, city, call_duration_before_drop, drop_reason,
              signal_strength_dbm, network, source
       FROM dropped_calls
       ${where}
       ORDER BY dropped_at DESC
       LIMIT $${p}`,
      params
    );
    res.json({ count: rows.length, source_note: 'Synthetic training data from ElectricSheep Africa / Amon Din, not live measured data.', dropped_calls: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/mobility', async (req, res) => {
  try {
    const { city, tower_id, density, location_type, bbox, limit = 200 } = req.query;
    const conds = [];
    const params = [];
    let p = 1;

    if (city && city !== 'all') { conds.push(`city = $${p++}`); params.push(city); }
    if (tower_id) { conds.push(`source_tower_id = $${p++}`); params.push(String(tower_id).slice(0, 40)); }
    if (density && density !== 'all') { conds.push(`user_density = $${p++}`); params.push(String(density).slice(0, 20)); }
    if (location_type && location_type !== 'all') { conds.push(`location_type = $${p++}`); params.push(String(location_type).slice(0, 40)); }
    if (bbox) {
      const [minLat, minLon, maxLat, maxLon] = bbox.split(',').map(Number);
      if (![minLat, minLon, maxLat, maxLon].every(Number.isFinite)) {
        return res.status(400).json({ error: 'bbox must be minLat,minLon,maxLat,maxLon' });
      }
      conds.push(`lat BETWEEN $${p++} AND $${p++}`); params.push(minLat, maxLat);
      conds.push(`lon BETWEEN $${p++} AND $${p++}`); params.push(minLon, maxLon);
    }

    const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 200, 1), 1000);
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
    params.push(safeLimit);
    const { rows } = await pool.query(
      `SELECT trace_id, trace_time, customer_id, lat, lon, city, source_tower_id,
              movement_speed_kmh, direction_degrees, user_density, time_of_day,
              day_of_week, location_type, source
       FROM mobility_traces
       ${where}
       ORDER BY trace_time DESC
       LIMIT $${p}`,
      params
    );
    res.json({ count: rows.length, source_note: 'Synthetic training data from ElectricSheep Africa / Amon Din, not live measured data.', traces: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/penetration', async (req, res) => {
  try {
    const { operator, city, state, from, until, limit = 200 } = req.query;
    const conds = [];
    const params = [];
    let p = 1;

    if (operator && operator !== 'all') { conds.push(`operator = $${p++}`); params.push(operator); }
    if (city && city !== 'all') { conds.push(`city = $${p++}`); params.push(city); }
    if (state && state !== 'all') { conds.push(`state = $${p++}`); params.push(state); }
    if (from) {
      const d = new Date(from);
      if (Number.isNaN(d.getTime())) return res.status(400).json({ error: 'from must be a valid date/time' });
      conds.push(`month >= $${p++}`); params.push(d.toISOString().slice(0, 10));
    }
    if (until) {
      const d = new Date(until);
      if (Number.isNaN(d.getTime())) return res.status(400).json({ error: 'until must be a valid date/time' });
      conds.push(`month <= $${p++}`); params.push(d.toISOString().slice(0, 10));
    }

    const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 200, 1), 1000);
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
    params.push(safeLimit);
    const { rows } = await pool.query(
      `SELECT city, state, operator, month, total_users, users_2g, users_3g,
              users_4g, users_5g, penetration_4g_percent, penetration_5g_percent,
              growth_rate_4g_percent, growth_rate_5g_percent, source
       FROM network_penetration
       ${where}
       ORDER BY month DESC, city, operator
       LIMIT $${p}`,
      params
    );
    res.json({ count: rows.length, source_note: 'Synthetic training data from ElectricSheep Africa / Amon Din, not live measured data.', penetration: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/technicians', async (req, res) => {
  try {
    const { operator, city, tower_id, technician_id, status, type, from, until, limit = 200 } = req.query;
    const conds = [];
    const params = [];
    let p = 1;

    if (operator && operator !== 'all') { conds.push(`operator = $${p++}`); params.push(operator); }
    if (city && city !== 'all') { conds.push(`city = $${p++}`); params.push(city); }
    if (tower_id) { conds.push(`source_tower_id = $${p++}`); params.push(String(tower_id).slice(0, 40)); }
    if (technician_id) { conds.push(`technician_id = $${p++}`); params.push(String(technician_id).slice(0, 40)); }
    if (status && status !== 'all') { conds.push(`status = $${p++}`); params.push(String(status).slice(0, 30)); }
    if (type && type !== 'all') { conds.push(`activity_type = $${p++}`); params.push(String(type).slice(0, 60)); }
    if (from) {
      const d = new Date(from);
      if (Number.isNaN(d.getTime())) return res.status(400).json({ error: 'from must be a valid date/time' });
      conds.push(`started_at >= $${p++}`); params.push(d.toISOString());
    }
    if (until) {
      const d = new Date(until);
      if (Number.isNaN(d.getTime())) return res.status(400).json({ error: 'until must be a valid date/time' });
      conds.push(`started_at <= $${p++}`); params.push(d.toISOString());
    }

    const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 200, 1), 1000);
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
    params.push(safeLimit);
    const { rows } = await pool.query(
      `SELECT activity_id, technician_id, source_tower_id, city, operator,
              activity_type, priority, status, started_at, ended_at,
              duration_min, issue_resolved, travel_km, materials_used, notes, source
       FROM technician_logs
       ${where}
       ORDER BY started_at DESC NULLS LAST
       LIMIT $${p}`,
      params
    );
    res.json({ count: rows.length, source_note: 'Synthetic training data from ElectricSheep Africa / Amon Din, not live measured data.', technician_logs: rows });
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
      ),
      uptime_stats AS (
        SELECT
          o.id AS outage_id,
          COUNT(ul.*)::int AS uptime_log_count,
          ROUND(AVG(ul.uptime_percentage)::numeric, 2) AS avg_uptime_percentage,
          ROUND(SUM(ul.downtime_minutes)::numeric, 1) AS total_downtime_minutes,
          SUM(ul.outage_count)::int AS total_synthetic_outages,
          COALESCE(
            jsonb_object_agg(ul.outage_reason, reason_counts.total)
              FILTER (WHERE ul.outage_reason IS NOT NULL),
            '{}'::jsonb
          ) AS outage_reason_counts
        FROM limited_outages o
        LEFT JOIN uptime_logs ul
          ON ul.operator = o.operator
         AND ul.log_date BETWEEN (o.started_at::date - INTERVAL '1 day') AND o.started_at::date
        LEFT JOIN LATERAL (
          SELECT COUNT(*)::int AS total
          FROM uptime_logs ul2
          WHERE ul2.operator = o.operator
            AND ul2.log_date BETWEEN (o.started_at::date - INTERVAL '1 day') AND o.started_at::date
            AND ul2.outage_reason = ul.outage_reason
        ) reason_counts ON true
        GROUP BY o.id
      ),
      energy_stats AS (
        SELECT
          o.id AS outage_id,
          COUNT(ec.*)::int AS energy_record_count,
          ROUND(AVG(ec.daily_consumption_kwh)::numeric, 2) AS avg_daily_consumption_kwh,
          ROUND(AVG(ec.grid_availability_hours)::numeric, 1) AS avg_grid_availability_hours,
          ROUND(AVG(ec.generator_runtime_hours)::numeric, 1) AS avg_generator_runtime_hours,
          ROUND(SUM(ec.fuel_consumed_liters)::numeric, 1) AS total_fuel_consumed_liters,
          ROUND(SUM(ec.carbon_emissions_kg)::numeric, 1) AS total_carbon_emissions_kg
        FROM limited_outages o
        LEFT JOIN energy_consumption ec
          ON ec.usage_date BETWEEN (o.started_at::date - INTERVAL '1 day') AND o.started_at::date
        GROUP BY o.id
      ),
      maintenance_stats AS (
        SELECT
          o.id AS outage_id,
          COUNT(mo.*)::int AS maintenance_order_count,
          COUNT(*) FILTER (
            WHERE LOWER(COALESCE(mo.priority, '')) IN ('high', 'critical', 'urgent')
          )::int AS high_priority_maintenance_count,
          ROUND(AVG(mo.repair_duration_hours)::numeric, 1) AS avg_repair_duration_hours,
          ROUND(SUM(mo.downtime_minutes)::numeric, 1) AS maintenance_downtime_minutes,
          COALESCE(
            jsonb_object_agg(mo.issue_category, issue_counts.total)
              FILTER (WHERE mo.issue_category IS NOT NULL),
            '{}'::jsonb
          ) AS maintenance_issue_counts
        FROM limited_outages o
        LEFT JOIN maintenance_orders mo
          ON mo.operator = o.operator
         AND mo.scheduled_date BETWEEN (o.started_at::date - INTERVAL '7 days') AND o.started_at::date
        LEFT JOIN LATERAL (
          SELECT COUNT(*)::int AS total
          FROM maintenance_orders mo2
          WHERE mo2.operator = o.operator
            AND mo2.scheduled_date BETWEEN (o.started_at::date - INTERVAL '7 days') AND o.started_at::date
            AND mo2.issue_category = mo.issue_category
        ) issue_counts ON true
        GROUP BY o.id
      ),
      latency_stats AS (
        SELECT
          o.id AS outage_id,
          COUNT(ll.*)::int AS latency_log_count,
          ROUND(AVG(ll.latency_ms)::numeric, 2) AS avg_ultra_latency_ms,
          ROUND(AVG(ll.jitter_ms)::numeric, 2) AS avg_ultra_jitter_ms,
          ROUND(AVG(ll.packet_loss_percent)::numeric, 2) AS avg_ultra_packet_loss_percent,
          COUNT(*) FILTER (WHERE ll.sla_met = false)::int AS latency_sla_miss_count
        FROM limited_outages o
        LEFT JOIN latency_logs ll
          ON ll.operator = o.operator
         AND ll.measured_at >= o.started_at - INTERVAL '2 hours'
         AND ll.measured_at < o.started_at
        GROUP BY o.id
      ),
      handover_stats AS (
        SELECT
          o.id AS outage_id,
          COUNT(hr.*)::int AS handover_count,
          COUNT(*) FILTER (WHERE hr.success = false)::int AS handover_failure_count,
          ROUND(AVG(hr.duration_ms)::numeric, 1) AS avg_handover_duration_ms,
          ROUND(AVG(hr.source_signal_dbm)::numeric, 1) AS avg_handover_source_signal_dbm,
          ROUND(AVG(hr.target_signal_dbm)::numeric, 1) AS avg_handover_target_signal_dbm
        FROM limited_outages o
        LEFT JOIN handover_records hr
          ON hr.operator = o.operator
         AND hr.handover_time >= o.started_at - INTERVAL '2 hours'
         AND hr.handover_time < o.started_at
        GROUP BY o.id
      ),
      dropped_call_stats AS (
        SELECT
          o.id AS outage_id,
          COUNT(dc.*)::int AS dropped_call_count,
          ROUND(AVG(dc.signal_strength_dbm)::numeric, 1) AS avg_dropped_call_signal_dbm,
          COALESCE(
            jsonb_object_agg(dc.drop_reason, drop_counts.total)
              FILTER (WHERE dc.drop_reason IS NOT NULL),
            '{}'::jsonb
          ) AS dropped_call_reason_counts
        FROM limited_outages o
        LEFT JOIN dropped_calls dc
          ON dc.operator = o.operator
         AND dc.dropped_at >= o.started_at - INTERVAL '2 hours'
         AND dc.dropped_at < o.started_at
        LEFT JOIN LATERAL (
          SELECT COUNT(*)::int AS total
          FROM dropped_calls dc2
          WHERE dc2.operator = o.operator
            AND dc2.dropped_at >= o.started_at - INTERVAL '2 hours'
            AND dc2.dropped_at < o.started_at
            AND dc2.drop_reason = dc.drop_reason
        ) drop_counts ON true
        GROUP BY o.id
      ),
      technician_stats AS (
        SELECT
          o.id AS outage_id,
          COUNT(tl.*)::int AS technician_activity_count,
          COUNT(*) FILTER (WHERE tl.issue_resolved = false)::int AS unresolved_technician_activity_count,
          ROUND(AVG(tl.duration_min)::numeric, 1) AS avg_technician_duration_min,
          ROUND(SUM(tl.travel_km)::numeric, 1) AS total_technician_travel_km
        FROM limited_outages o
        LEFT JOIN technician_logs tl
          ON tl.operator = o.operator
         AND tl.started_at >= o.started_at - INTERVAL '7 days'
         AND tl.started_at < o.started_at
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
        hws.pre_avg_power_draw_watts,
        COALESCE(us.uptime_log_count, 0) AS uptime_log_count,
        us.avg_uptime_percentage,
        us.total_downtime_minutes,
        COALESCE(us.total_synthetic_outages, 0) AS total_synthetic_outages,
        COALESCE(us.outage_reason_counts, '{}'::jsonb) AS outage_reason_counts,
        COALESCE(ens.energy_record_count, 0) AS energy_record_count,
        ens.avg_daily_consumption_kwh,
        ens.avg_grid_availability_hours,
        ens.avg_generator_runtime_hours,
        ens.total_fuel_consumed_liters,
        ens.total_carbon_emissions_kg,
        COALESCE(ms.maintenance_order_count, 0) AS maintenance_order_count,
        COALESCE(ms.high_priority_maintenance_count, 0) AS high_priority_maintenance_count,
        ms.avg_repair_duration_hours,
        ms.maintenance_downtime_minutes,
        COALESCE(ms.maintenance_issue_counts, '{}'::jsonb) AS maintenance_issue_counts,
        COALESCE(ls.latency_log_count, 0) AS latency_log_count,
        ls.avg_ultra_latency_ms,
        ls.avg_ultra_jitter_ms,
        ls.avg_ultra_packet_loss_percent,
        COALESCE(ls.latency_sla_miss_count, 0) AS latency_sla_miss_count,
        COALESCE(hds.handover_count, 0) AS handover_count,
        COALESCE(hds.handover_failure_count, 0) AS handover_failure_count,
        hds.avg_handover_duration_ms,
        hds.avg_handover_source_signal_dbm,
        hds.avg_handover_target_signal_dbm,
        COALESCE(dcs.dropped_call_count, 0) AS dropped_call_count,
        dcs.avg_dropped_call_signal_dbm,
        COALESCE(dcs.dropped_call_reason_counts, '{}'::jsonb) AS dropped_call_reason_counts,
        COALESCE(ts.technician_activity_count, 0) AS technician_activity_count,
        COALESCE(ts.unresolved_technician_activity_count, 0) AS unresolved_technician_activity_count,
        ts.avg_technician_duration_min,
        ts.total_technician_travel_km
      FROM limited_outages o
      LEFT JOIN reading_stats rs ON rs.outage_id = o.id
      LEFT JOIN event_stats es ON es.outage_id = o.id
      LEFT JOIN hardware_stats hws ON hws.outage_id = o.id
      LEFT JOIN uptime_stats us ON us.outage_id = o.id
      LEFT JOIN energy_stats ens ON ens.outage_id = o.id
      LEFT JOIN maintenance_stats ms ON ms.outage_id = o.id
      LEFT JOIN latency_stats ls ON ls.outage_id = o.id
      LEFT JOIN handover_stats hds ON hds.outage_id = o.id
      LEFT JOIN dropped_call_stats dcs ON dcs.outage_id = o.id
      LEFT JOIN technician_stats ts ON ts.outage_id = o.id
      ORDER BY o.started_at DESC
      `,
      [limit]
    );
    res.json({ count: rows.length, correlations: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/outages/risk
// Baseline outage-risk score from live readings plus synthetic training signals.
router.get('/outages/risk', async (req, res) => {
  try {
    const requestedHours = parseInt(req.query.hours || '6', 10) || 6;
    const hours = Math.min(Math.max(requestedHours, 1), 72);
    const operators = ['MTN', 'Airtel', 'Glo', '9mobile'];

    const { rows } = await pool.query(
      `
      WITH operators AS (
        SELECT unnest($1::text[]) AS operator
      ),
      latest_qos AS (SELECT MAX(measured_at) AS ts FROM qos_metrics),
      latest_events AS (SELECT MAX(event_time) AS ts FROM network_events),
      latest_uptime AS (SELECT MAX(log_date) AS dt FROM uptime_logs),
      latest_hardware AS (SELECT MAX(sensor_time) AS ts FROM hardware_sensors),
      latest_energy AS (SELECT MAX(usage_date) AS dt FROM energy_consumption),
      latest_maintenance AS (SELECT MAX(scheduled_date) AS dt FROM maintenance_orders),
      latest_latency AS (SELECT MAX(measured_at) AS ts FROM latency_logs),
      latest_handover AS (SELECT MAX(handover_time) AS ts FROM handover_records),
      latest_dropped AS (SELECT MAX(dropped_at) AS ts FROM dropped_calls),
      latest_technician AS (SELECT MAX(started_at) AS ts FROM technician_logs),
      live_stats AS (
        SELECT
          operator,
          COUNT(*)::int AS live_sample_count,
          ROUND(AVG(signal_dbm)::numeric, 1) AS live_avg_signal,
          ROUND(AVG(latency_ms)::numeric, 1) AS live_avg_latency,
          ROUND(AVG(dl_mbps)::numeric, 1) AS live_avg_dl
        FROM readings
        WHERE recorded_at >= NOW() - ($2::int * INTERVAL '1 hour')
          AND operator = ANY($1::text[])
        GROUP BY operator
      ),
      qos_stats AS (
        SELECT
          qm.operator,
          COUNT(*)::int AS qos_sample_count,
          ROUND(AVG(qm.signal_strength_dbm)::numeric, 1) AS qos_avg_signal,
          ROUND(AVG(qm.latency_ms)::numeric, 1) AS qos_avg_latency,
          ROUND(AVG(qm.packet_loss_rate)::numeric, 2) AS qos_avg_packet_loss,
          ROUND(AVG(qm.error_rate)::numeric, 2) AS qos_avg_error_rate
        FROM qos_metrics qm, latest_qos lq
        WHERE lq.ts IS NOT NULL
          AND qm.measured_at >= lq.ts - ($2::int * INTERVAL '1 hour')
          AND qm.operator = ANY($1::text[])
        GROUP BY qm.operator
      ),
      event_stats AS (
        SELECT
          ne.operator,
          COUNT(*)::int AS event_count,
          ROUND(AVG(ne.packet_loss_percent)::numeric, 2) AS event_avg_packet_loss
        FROM network_events ne, latest_events le
        WHERE le.ts IS NOT NULL
          AND ne.event_time >= le.ts - ($2::int * INTERVAL '1 hour')
          AND ne.operator = ANY($1::text[])
        GROUP BY ne.operator
      ),
      uptime_stats AS (
        SELECT
          ul.operator,
          COUNT(*)::int AS uptime_log_count,
          ROUND(AVG(ul.uptime_percentage)::numeric, 2) AS uptime_avg_percentage,
          ROUND(SUM(ul.downtime_minutes)::numeric, 1) AS uptime_downtime_minutes,
          COALESCE(SUM(ul.outage_count), 0)::int AS uptime_outage_count
        FROM uptime_logs ul, latest_uptime lu
        WHERE lu.dt IS NOT NULL
          AND ul.log_date >= lu.dt - INTERVAL '7 days'
          AND ul.operator = ANY($1::text[])
        GROUP BY ul.operator
      ),
      hardware_stats AS (
        SELECT
          COUNT(*) FILTER (WHERE hs.alert_triggered = true)::int AS hardware_alert_count,
          COUNT(*) FILTER (
            WHERE LOWER(COALESCE(hs.health_status, '')) IN ('warning', 'critical', 'fault', 'faulty', 'degraded')
          )::int AS hardware_bad_health_count
        FROM hardware_sensors hs, latest_hardware lh
        WHERE lh.ts IS NOT NULL
          AND hs.sensor_time >= lh.ts - ($2::int * INTERVAL '1 hour')
      ),
      energy_stats AS (
        SELECT
          ROUND(AVG(ec.grid_availability_hours)::numeric, 1) AS energy_avg_grid_hours,
          ROUND(AVG(ec.generator_runtime_hours)::numeric, 1) AS energy_avg_generator_hours
        FROM energy_consumption ec, latest_energy le
        WHERE le.dt IS NOT NULL
          AND ec.usage_date >= le.dt - INTERVAL '2 days'
      ),
      maintenance_stats AS (
        SELECT
          mo.operator,
          COUNT(*)::int AS maintenance_order_count,
          COUNT(*) FILTER (
            WHERE LOWER(COALESCE(mo.priority, '')) IN ('high', 'critical', 'urgent')
          )::int AS maintenance_high_priority_count,
          ROUND(SUM(mo.downtime_minutes)::numeric, 1) AS maintenance_downtime_minutes
        FROM maintenance_orders mo, latest_maintenance lm
        WHERE lm.dt IS NOT NULL
          AND mo.scheduled_date >= lm.dt - INTERVAL '14 days'
          AND mo.operator = ANY($1::text[])
        GROUP BY mo.operator
      ),
      latency_stats AS (
        SELECT
          ll.operator,
          COUNT(*)::int AS latency_log_count,
          ROUND(AVG(ll.packet_loss_percent)::numeric, 2) AS latency_avg_packet_loss,
          COUNT(*) FILTER (WHERE ll.sla_met = false)::int AS latency_sla_miss_count
        FROM latency_logs ll, latest_latency la
        WHERE la.ts IS NOT NULL
          AND ll.measured_at >= la.ts - ($2::int * INTERVAL '1 hour')
          AND ll.operator = ANY($1::text[])
        GROUP BY ll.operator
      ),
      handover_stats AS (
        SELECT
          hr.operator,
          COUNT(*)::int AS handover_count,
          COUNT(*) FILTER (WHERE hr.success = false)::int AS handover_failure_count
        FROM handover_records hr, latest_handover lh
        WHERE lh.ts IS NOT NULL
          AND hr.handover_time >= lh.ts - ($2::int * INTERVAL '1 hour')
          AND hr.operator = ANY($1::text[])
        GROUP BY hr.operator
      ),
      dropped_stats AS (
        SELECT
          dc.operator,
          COUNT(*)::int AS dropped_call_count,
          ROUND(AVG(dc.signal_strength_dbm)::numeric, 1) AS dropped_avg_signal
        FROM dropped_calls dc, latest_dropped ld
        WHERE ld.ts IS NOT NULL
          AND dc.dropped_at >= ld.ts - ($2::int * INTERVAL '1 hour')
          AND dc.operator = ANY($1::text[])
        GROUP BY dc.operator
      ),
      technician_stats AS (
        SELECT
          tl.operator,
          COUNT(*)::int AS technician_activity_count,
          COUNT(*) FILTER (WHERE tl.issue_resolved = false)::int AS technician_unresolved_count
        FROM technician_logs tl, latest_technician lt
        WHERE lt.ts IS NOT NULL
          AND tl.started_at >= lt.ts - INTERVAL '7 days'
          AND tl.operator = ANY($1::text[])
        GROUP BY tl.operator
      )
      SELECT
        o.operator,
        COALESCE(ls.live_sample_count, 0) AS live_sample_count,
        ls.live_avg_signal,
        ls.live_avg_latency,
        ls.live_avg_dl,
        COALESCE(qs.qos_sample_count, 0) AS qos_sample_count,
        qs.qos_avg_signal,
        qs.qos_avg_latency,
        qs.qos_avg_packet_loss,
        qs.qos_avg_error_rate,
        COALESCE(es.event_count, 0) AS event_count,
        es.event_avg_packet_loss,
        COALESCE(us.uptime_log_count, 0) AS uptime_log_count,
        us.uptime_avg_percentage,
        us.uptime_downtime_minutes,
        COALESCE(us.uptime_outage_count, 0) AS uptime_outage_count,
        COALESCE(hws.hardware_alert_count, 0) AS hardware_alert_count,
        COALESCE(hws.hardware_bad_health_count, 0) AS hardware_bad_health_count,
        ens.energy_avg_grid_hours,
        ens.energy_avg_generator_hours,
        COALESCE(ms.maintenance_order_count, 0) AS maintenance_order_count,
        COALESCE(ms.maintenance_high_priority_count, 0) AS maintenance_high_priority_count,
        ms.maintenance_downtime_minutes,
        COALESCE(lts.latency_log_count, 0) AS latency_log_count,
        lts.latency_avg_packet_loss,
        COALESCE(lts.latency_sla_miss_count, 0) AS latency_sla_miss_count,
        COALESCE(hds.handover_count, 0) AS handover_count,
        COALESCE(hds.handover_failure_count, 0) AS handover_failure_count,
        COALESCE(ds.dropped_call_count, 0) AS dropped_call_count,
        ds.dropped_avg_signal,
        COALESCE(ts.technician_activity_count, 0) AS technician_activity_count,
        COALESCE(ts.technician_unresolved_count, 0) AS technician_unresolved_count
      FROM operators o
      LEFT JOIN live_stats ls ON ls.operator = o.operator
      LEFT JOIN qos_stats qs ON qs.operator = o.operator
      LEFT JOIN event_stats es ON es.operator = o.operator
      LEFT JOIN uptime_stats us ON us.operator = o.operator
      CROSS JOIN hardware_stats hws
      CROSS JOIN energy_stats ens
      LEFT JOIN maintenance_stats ms ON ms.operator = o.operator
      LEFT JOIN latency_stats lts ON lts.operator = o.operator
      LEFT JOIN handover_stats hds ON hds.operator = o.operator
      LEFT JOIN dropped_stats ds ON ds.operator = o.operator
      LEFT JOIN technician_stats ts ON ts.operator = o.operator
      ORDER BY o.operator
      `,
      [operators, hours]
    );

    const risks = rows.map(buildOutageRisk).sort((a, b) => b.score - a.score);
    const highest = risks[0] || null;

    res.json({
      ok: true,
      generated_at: new Date().toISOString(),
      model: 'baseline_weighted_v1',
      window_hours: hours,
      source_note: 'Risk scores combine live readings with ElectricSheep Africa / Amon Din synthetic training datasets. Synthetic signals are baseline indicators, not confirmed live outages.',
      highest_risk: highest,
      risks,
    });
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
