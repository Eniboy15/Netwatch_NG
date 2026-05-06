// scripts/importLatencyLogs.js
// Run once: node scripts/importLatencyLogs.js  (or: npm run import:latency)
//
// Imports ElectricSheep Africa synthetic ultra-low-latency logs.

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { pool, bootstrap } = require('../server/db');

const CSV_PATH = process.env.LATENCY_LOGS_CSV_PATH
  || path.join(__dirname, '../data/ultra_low_latency_logs.csv');

const BATCH_SIZE = 100;

function first(row, names) {
  for (const name of names) {
    if (row[name] !== undefined && row[name] !== '') return row[name];
  }
  return null;
}

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

function toBoolean(value) {
  if (value === null || value === undefined || value === '') return null;
  const raw = String(value).trim().toLowerCase();
  if (['true', 'yes', 'y', '1', 'met', 'pass', 'passed'].includes(raw)) return true;
  if (['false', 'no', 'n', '0', 'missed', 'fail', 'failed'].includes(raw)) return false;
  return null;
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

function normalizeRow(row, rowNumber) {
  const measuredAt = new Date(first(row, ['timestamp', 'measured_at', 'recorded_at', 'log_time']));
  if (Number.isNaN(measuredAt.getTime())) return null;

  return {
    log_id: cleanText(first(row, ['log_id', 'latency_id', 'record_id', 'id']), `LAT-${rowNumber}`, 40),
    measured_at: measuredAt.toISOString(),
    source_tower_id: cleanText(first(row, ['tower_id', 'source_tower_id']), 'Unknown', 40),
    city: cleanText(first(row, ['city']), 'Unknown', 60),
    operator: cleanText(first(row, ['operator', 'provider']), 'Unknown', 20),
    network: normalizeNetwork(first(row, ['network_type', 'network'])),
    application_type: cleanText(first(row, ['application_type', 'service_type', 'use_case', 'app_type']), 'Unknown', 60),
    latency_ms: toNumber(first(row, ['latency_ms', 'round_trip_latency_ms', 'rtt_ms', 'e2e_latency_ms'])),
    jitter_ms: toNumber(first(row, ['jitter_ms'])),
    packet_loss_percent: toNumber(first(row, ['packet_loss_percent', 'packet_loss_rate'])),
    throughput_mbps: toNumber(first(row, ['throughput_mbps', 'bandwidth_mbps'])),
    edge_server_id: cleanText(first(row, ['edge_server_id', 'server_id']), 'Unknown', 40),
    edge_distance_km: toNumber(first(row, ['edge_distance_km', 'edge_server_distance_km', 'distance_to_edge_km'])),
    sla_target_ms: toNumber(first(row, ['sla_target_ms', 'sla_requirement_ms', 'latency_requirement_ms'])),
    sla_met: toBoolean(first(row, ['sla_met', 'sla_compliance', 'requirement_met'])),
    reliability_percent: toNumber(first(row, ['reliability_percent', 'availability_percent'])),
    active_connections: toInt(first(row, ['active_connections', 'connected_users', 'active_users'])),
  };
}

async function flushBatch(rows) {
  const values = [];
  const params = [];
  let p = 1;

  for (const r of rows) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    params.push(
      r.log_id,
      r.measured_at,
      r.source_tower_id,
      r.city,
      r.operator,
      r.network,
      r.application_type,
      r.latency_ms,
      r.jitter_ms,
      r.packet_loss_percent,
      r.throughput_mbps,
      r.edge_server_id,
      r.edge_distance_km,
      r.sla_target_ms,
      r.sla_met,
      r.reliability_percent,
      r.active_connections
    );
  }

  await pool.query(
    `INSERT INTO latency_logs
       (log_id, measured_at, source_tower_id, city, operator, network,
        application_type, latency_ms, jitter_ms, packet_loss_percent,
        throughput_mbps, edge_server_id, edge_distance_km, sla_target_ms,
        sla_met, reliability_percent, active_connections)
     VALUES ${values.join(',')}
     ON CONFLICT (log_id) DO NOTHING`,
    params
  );
}

async function importLatencyLogs() {
  await bootstrap();

  if (!fs.existsSync(CSV_PATH)) {
    console.error(`CSV not found at: ${CSV_PATH}`);
    console.error('Download it into data/ultra_low_latency_logs.csv or set LATENCY_LOGS_CSV_PATH.');
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
    const normalized = normalizeRow(row, parsed);

    if (!normalized) {
      skipped++;
      continue;
    }

    batch.push(normalized);

    if (batch.length >= BATCH_SIZE) {
      await flushBatch(batch);
      inserted += batch.length;
      process.stdout.write(`\rInserted ${inserted.toLocaleString()} latency logs...`);
      batch = [];
    }
  }

  if (batch.length) {
    await flushBatch(batch);
    inserted += batch.length;
  }

  const { rows: summary } = await pool.query(`
    SELECT operator, network, COUNT(*)::int AS rows,
           ROUND(AVG(latency_ms)::numeric, 2) AS avg_latency_ms
    FROM latency_logs
    GROUP BY operator, network
    ORDER BY operator, network
  `);

  console.log(`\nImport complete: ${inserted.toLocaleString()} parsed rows sent, ${skipped.toLocaleString()} skipped.`);
  console.log(`CSV rows parsed: ${parsed.toLocaleString()}`);
  console.table(summary);

  await pool.end();
}

importLatencyLogs().catch((err) => {
  console.error('Import failed:', err.stack || err.message || err);
  process.exit(1);
});
