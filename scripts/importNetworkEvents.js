// scripts/importNetworkEvents.js
// Run once: node scripts/importNetworkEvents.js  (or: npm run import:events)
//
// Imports ElectricSheep Africa synthetic network event logs.

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { pool, bootstrap } = require('../server/db');

const CSV_PATH = process.env.NETWORK_EVENTS_CSV_PATH
  || path.join(__dirname, '../data/network_event_logs.csv');

const BATCH_SIZE = 100;

function toNumber(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function toInt(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = parseInt(value, 10);
  return Number.isFinite(n) ? n : null;
}

function cleanText(value, fallback, maxLen) {
  const raw = value === null || value === undefined ? '' : String(value).trim();
  return (raw || fallback).slice(0, maxLen);
}

function normalizeNetwork(value) {
  const raw = cleanText(value, '4G', 10).toUpperCase();
  if (raw === 'NR' || raw === '5G') return '5G';
  if (raw === 'LTE' || raw === '4G') return '4G';
  if (raw === 'UMTS' || raw === 'HSPA' || raw === '3G') return '3G';
  if (raw === 'GSM' || raw === 'EDGE' || raw === '2G') return '2G';
  return raw.slice(0, 10);
}

function normalizeRow(row) {
  const eventTime = new Date(row.timestamp);
  if (!row.event_id || Number.isNaN(eventTime.getTime())) return null;

  return {
    event_id: cleanText(row.event_id, '', 40),
    event_time: eventTime.toISOString(),
    source_tower_id: cleanText(row.tower_id, 'Unknown', 40),
    city: cleanText(row.city, 'Unknown', 60),
    event_type: cleanText(row.event_type, 'Unknown', 60),
    severity: cleanText(row.severity, 'Unknown', 20),
    affected_users: toInt(row.affected_users),
    packet_loss_percent: toNumber(row.packet_loss_percent),
    network: normalizeNetwork(row.network_type),
    operator: cleanText(row.operator, 'Unknown', 20),
  };
}

async function flushBatch(rows) {
  const values = [];
  const params = [];
  let p = 1;

  for (const r of rows) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    params.push(
      r.event_id,
      r.event_time,
      r.source_tower_id,
      r.city,
      r.event_type,
      r.severity,
      r.affected_users,
      r.packet_loss_percent,
      r.network,
      r.operator
    );
  }

  await pool.query(
    `INSERT INTO network_events
       (event_id, event_time, source_tower_id, city, event_type, severity,
        affected_users, packet_loss_percent, network, operator)
     VALUES ${values.join(',')}
     ON CONFLICT (event_id) DO NOTHING`,
    params
  );
}

async function importNetworkEvents() {
  await bootstrap();

  if (!fs.existsSync(CSV_PATH)) {
    console.error(`CSV not found at: ${CSV_PATH}`);
    console.error('Download it into data/network_event_logs.csv or set NETWORK_EVENTS_CSV_PATH.');
    process.exit(1);
  }

  console.log(`Reading ${CSV_PATH} ...`);

  let batch = [];
  let parsed = 0;
  let inserted = 0;
  let skipped = 0;

  const parser = fs
    .createReadStream(CSV_PATH)
    .pipe(parse({
      columns: true,
      skip_empty_lines: true,
      trim: true,
    }));

  for await (const row of parser) {
    parsed++;
    const normalized = normalizeRow(row);

    if (!normalized) {
      skipped++;
      continue;
    }

    batch.push(normalized);

    if (batch.length >= BATCH_SIZE) {
      await flushBatch(batch);
      inserted += batch.length;
      process.stdout.write(`\rInserted ${inserted.toLocaleString()} network events...`);
      batch = [];
    }
  }

  if (batch.length) {
    await flushBatch(batch);
    inserted += batch.length;
  }

  const { rows: summary } = await pool.query(`
    SELECT operator, event_type, COUNT(*)::int AS rows
    FROM network_events
    GROUP BY operator, event_type
    ORDER BY operator, event_type
  `);

  console.log(`\nImport complete: ${inserted.toLocaleString()} parsed rows sent, ${skipped.toLocaleString()} skipped.`);
  console.log(`CSV rows parsed: ${parsed.toLocaleString()}`);
  console.table(summary);

  await pool.end();
}

importNetworkEvents().catch((err) => {
  console.error('Import failed:', err.stack || err.message || err);
  process.exit(1);
});
