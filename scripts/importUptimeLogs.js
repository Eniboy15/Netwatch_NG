// scripts/importUptimeLogs.js
// Run once: node scripts/importUptimeLogs.js  (or: npm run import:uptime)
//
// Imports ElectricSheep Africa synthetic base-station uptime logs.

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { pool, bootstrap } = require('../server/db');

const CSV_PATH = process.env.UPTIME_LOGS_CSV_PATH
  || path.join(__dirname, '../data/base_station_uptime_logs.csv');

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
  const logDate = new Date(row.date);
  if (!row.tower_id || Number.isNaN(logDate.getTime())) return null;

  return {
    source_tower_id: cleanText(row.tower_id, 'Unknown', 40),
    log_date: logDate.toISOString().slice(0, 10),
    operator: cleanText(row.operator, 'Unknown', 20),
    city: cleanText(row.city, 'Unknown', 60),
    state: cleanText(row.state, 'Unknown', 60),
    uptime_percentage: toNumber(row.uptime_percentage),
    downtime_minutes: toNumber(row.downtime_minutes),
    outage_count: toInt(row.outage_count),
    outage_reason: cleanText(row.outage_reason, 'none', 40),
    network: normalizeNetwork(row.network_type),
    avg_users_affected: toInt(row.avg_users_affected),
  };
}

async function flushBatch(rows) {
  const values = [];
  const params = [];
  let p = 1;

  for (const r of rows) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    params.push(
      r.source_tower_id,
      r.log_date,
      r.operator,
      r.city,
      r.state,
      r.uptime_percentage,
      r.downtime_minutes,
      r.outage_count,
      r.outage_reason,
      r.network,
      r.avg_users_affected
    );
  }

  await pool.query(
    `INSERT INTO uptime_logs
       (source_tower_id, log_date, operator, city, state, uptime_percentage,
        downtime_minutes, outage_count, outage_reason, network, avg_users_affected)
     VALUES ${values.join(',')}
     ON CONFLICT (source_tower_id, log_date, operator, network) DO NOTHING`,
    params
  );
}

async function importUptimeLogs() {
  await bootstrap();

  if (!fs.existsSync(CSV_PATH)) {
    console.error(`CSV not found at: ${CSV_PATH}`);
    console.error('Download it into data/base_station_uptime_logs.csv or set UPTIME_LOGS_CSV_PATH.');
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
      process.stdout.write(`\rInserted ${inserted.toLocaleString()} uptime rows...`);
      batch = [];
    }
  }

  if (batch.length) {
    await flushBatch(batch);
    inserted += batch.length;
  }

  const { rows: summary } = await pool.query(`
    SELECT outage_reason, COUNT(*)::int AS rows
    FROM uptime_logs
    GROUP BY outage_reason
    ORDER BY rows DESC
  `);

  console.log(`\nImport complete: ${inserted.toLocaleString()} parsed rows sent, ${skipped.toLocaleString()} skipped.`);
  console.log(`CSV rows parsed: ${parsed.toLocaleString()}`);
  console.table(summary);

  await pool.end();
}

importUptimeLogs().catch((err) => {
  console.error('Import failed:', err.stack || err.message || err);
  process.exit(1);
});
