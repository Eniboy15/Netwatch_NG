// scripts/importQoS.js
// Run once: node scripts/importQoS.js  (or: npm run import:qos)
//
// Imports ElectricSheep Africa synthetic quality-of-service metrics.

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { pool, bootstrap } = require('../server/db');

const CSV_PATH = process.env.QOS_CSV_PATH
  || path.join(__dirname, '../data/quality_of_service_metrics.csv');

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
  const measuredAt = new Date(row.timestamp);
  if (!row.metric_id || Number.isNaN(measuredAt.getTime())) return null;

  return {
    metric_id: cleanText(row.metric_id, '', 40),
    measured_at: measuredAt.toISOString(),
    source_tower_id: cleanText(row.tower_id, 'Unknown', 40),
    city: cleanText(row.city, 'Unknown', 60),
    operator: cleanText(row.operator, 'Unknown', 20),
    network: normalizeNetwork(row.network_type),
    latency_ms: toNumber(row.latency_ms),
    jitter_ms: toNumber(row.jitter_ms),
    throughput_mbps: toNumber(row.throughput_mbps),
    packet_loss_rate: toNumber(row.packet_loss_rate),
    error_rate: toNumber(row.error_rate),
    signal_strength_dbm: toNumber(row.signal_strength_dbm),
    active_users: toInt(row.active_users),
  };
}

async function flushBatch(rows) {
  const values = [];
  const params = [];
  let p = 1;

  for (const r of rows) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    params.push(
      r.metric_id,
      r.measured_at,
      r.source_tower_id,
      r.city,
      r.operator,
      r.network,
      r.latency_ms,
      r.jitter_ms,
      r.throughput_mbps,
      r.packet_loss_rate,
      r.error_rate,
      r.signal_strength_dbm,
      r.active_users
    );
  }

  await pool.query(
    `INSERT INTO qos_metrics
       (metric_id, measured_at, source_tower_id, city, operator, network,
        latency_ms, jitter_ms, throughput_mbps, packet_loss_rate, error_rate,
        signal_strength_dbm, active_users)
     VALUES ${values.join(',')}
     ON CONFLICT (metric_id) DO NOTHING`,
    params
  );
}

async function importQoS() {
  await bootstrap();

  if (!fs.existsSync(CSV_PATH)) {
    console.error(`CSV not found at: ${CSV_PATH}`);
    console.error('Download the QoS CSV into data/quality_of_service_metrics.csv or set QOS_CSV_PATH.');
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
      process.stdout.write(`\rInserted ${inserted.toLocaleString()} QoS rows...`);
      batch = [];
    }
  }

  if (batch.length) {
    await flushBatch(batch);
    inserted += batch.length;
  }

  const { rows: summary } = await pool.query(`
    SELECT operator, network, COUNT(*)::int AS rows
    FROM qos_metrics
    GROUP BY operator, network
    ORDER BY operator, network
  `);

  console.log(`\nImport complete: ${inserted.toLocaleString()} parsed rows sent, ${skipped.toLocaleString()} skipped.`);
  console.log(`CSV rows parsed: ${parsed.toLocaleString()}`);
  console.table(summary);

  await pool.end();
}

importQoS().catch((err) => {
  console.error('Import failed:', err.stack || err.message || err);
  process.exit(1);
});
