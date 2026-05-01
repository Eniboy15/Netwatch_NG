#!/usr/bin/env node
require('dotenv').config();
const { pool, bootstrap } = require('../server/db');

const IODA_BASE = 'https://api.ioda.caida.org/v2';
const DEFAULT_DAYS = 30;
const SIGNAL_TYPES = ['bgp', 'ping', 'ibr'];

const ENTITIES = [
  { operator: 'MTN', entity_type: 'asn', entity_code: '29465' },
  { operator: 'Airtel', entity_type: 'asn', entity_code: '36873' },
  { operator: 'Glo', entity_type: 'asn', entity_code: '37148' },
  { operator: '9mobile', entity_type: 'asn', entity_code: '37076' },
  { operator: 'Nigeria', entity_type: 'country', entity_code: 'NG' },
];

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (token === '--from') args.from = Number(argv[i + 1]);
    if (token === '--until') args.until = Number(argv[i + 1]);
  }
  return args;
}

function toTimestamp(value) {
  if (value === null || value === undefined || value === '') return null;
  if (value instanceof Date) return Number.isFinite(value.getTime()) ? value : null;
  const n = Number(value);
  if (Number.isFinite(n)) {
    const ms = n > 1e12 ? n : n * 1000;
    const dt = new Date(ms);
    return Number.isFinite(dt.getTime()) ? dt : null;
  }
  const dt = new Date(value);
  return Number.isFinite(dt.getTime()) ? dt : null;
}

function minutesBetween(startedAt, endedAt) {
  if (!startedAt || !endedAt) return null;
  const diff = Math.max(0, endedAt.getTime() - startedAt.getTime());
  return Math.round(diff / 60000);
}

async function safeFetchJson(url) {
  const res = await fetch(url, { headers: { Accept: 'application/json' } });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`HTTP ${res.status} for ${url} :: ${body.slice(0, 220)}`);
  }
  return res.json();
}

async function fetchEntityOutages(entity, fromTs, untilTs) {
  const urls = [
    `${IODA_BASE}/outages?entity_type=${entity.entity_type}&entity_code=${entity.entity_code}&from=${fromTs}&until=${untilTs}`,
    `${IODA_BASE}/outages?entityType=${entity.entity_type}&entityCode=${entity.entity_code}&from=${fromTs}&until=${untilTs}`,
  ];

  let data;
  let lastErr;
  for (const url of urls) {
    try {
      data = await safeFetchJson(url);
      break;
    } catch (err) {
      lastErr = err;
    }
  }
  if (!data) throw lastErr || new Error('IODA fetch failed with unknown error');

  const rows = Array.isArray(data) ? data : (data.outages || data.events || data.results || []);
  if (!Array.isArray(rows)) return [];

  const normalized = [];
  for (const row of rows) {
    const startedAt = toTimestamp(row.started_at || row.start || row.start_ts || row.from || row.time_start);
    if (!startedAt) continue;
    const endedAt = toTimestamp(row.ended_at || row.end || row.end_ts || row.until || row.time_end);
    const severity = Number(row.severity ?? row.score ?? row.value ?? row.level ?? 0);

    const signals = Array.isArray(row.signal_types) && row.signal_types.length
      ? row.signal_types
      : [row.signal_type || row.signal || 'unknown'];

    for (const signal of signals) {
      normalized.push({
        source: 'IODA',
        entity_type: entity.entity_type,
        entity_code: entity.entity_code,
        operator: entity.operator,
        severity: Number.isFinite(severity) ? severity : 0,
        signal_type: String(signal || 'unknown').toLowerCase(),
        started_at: startedAt,
        ended_at: endedAt,
        duration_min: minutesBetween(startedAt, endedAt),
        raw_data: row,
      });
    }
  }
  return normalized;
}

async function insertOutages(items) {
  if (!items.length) return 0;
  const values = [];
  const params = [];
  let p = 1;
  for (const item of items) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    params.push(
      item.source, item.entity_type, item.entity_code, item.operator, item.severity,
      item.signal_type, item.started_at.toISOString(), item.ended_at ? item.ended_at.toISOString() : null,
      item.duration_min, JSON.stringify(item.raw_data)
    );
  }

  const sql = `
    INSERT INTO outages
      (source, entity_type, entity_code, operator, severity, signal_type, started_at, ended_at, duration_min, raw_data)
    VALUES ${values.join(',')}
    ON CONFLICT (source, entity_type, entity_code, operator, signal_type, started_at, ended_at)
    DO NOTHING
  `;
  const result = await pool.query(sql, params);
  return result.rowCount || 0;
}

async function runSyncIODA(options = {}) {
  const nowSec = Math.floor(Date.now() / 1000);
  const untilTs = Number.isFinite(options.until) ? options.until : nowSec;
  const fromTs = Number.isFinite(options.from) ? options.from : (untilTs - (DEFAULT_DAYS * 24 * 3600));

  const summary = {};
  let attempted = 0;
  let inserted = 0;

  for (const entity of ENTITIES) {
    try {
      const events = await fetchEntityOutages(entity, fromTs, untilTs);
      attempted += events.length;
      const added = await insertOutages(events);
      inserted += added;
      summary[entity.operator] = { found: events.length, inserted: added };
      console.log(`[IODA] ${entity.operator}: found=${events.length}, inserted=${added}`);
    } catch (err) {
      summary[entity.operator] = { found: 0, inserted: 0, error: err.message };
      console.error(`[IODA] ${entity.operator}: error=${err.message}`);
    }
  }

  return {
    from: fromTs,
    until: untilTs,
    attempted,
    inserted,
    summary,
    signals: SIGNAL_TYPES,
  };
}

async function runCli() {
  const args = parseArgs(process.argv.slice(2));
  try {
    await bootstrap();
    const result = await runSyncIODA(args);
    console.log('');
    console.log('IODA sync complete');
    console.log(`Range: ${new Date(result.from * 1000).toISOString()} -> ${new Date(result.until * 1000).toISOString()}`);
    console.log(`Attempted: ${result.attempted} | Inserted: ${result.inserted}`);
    for (const [op, info] of Object.entries(result.summary)) {
      if (info.error) {
        console.log(`- ${op}: ERROR (${info.error})`);
      } else {
        console.log(`- ${op}: found ${info.found}, inserted ${info.inserted}`);
      }
    }
  } catch (err) {
    console.error(`IODA sync failed: ${err.message}`);
    process.exitCode = 1;
  } finally {
    await pool.end().catch(() => {});
  }
}

if (require.main === module) {
  runCli();
}

module.exports = { runSyncIODA };
