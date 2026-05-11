require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { pool, bootstrap } = require('../server/db');

const CSV_PATH = process.env.MOBILITY_TRACES_CSV_PATH || path.join(__dirname, '../data/mobility_trace_datasets.csv');
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

function normalizeRow(row) {
  const traceTime = new Date(row.timestamp);
  if (!row.trace_id || Number.isNaN(traceTime.getTime())) return null;
  return {
    trace_id: cleanText(row.trace_id, '', 40),
    trace_time: traceTime.toISOString(),
    customer_id: cleanText(row.customer_id, 'Unknown', 40),
    lat: toNumber(row.latitude),
    lon: toNumber(row.longitude),
    city: cleanText(row.city, 'Unknown', 60),
    source_tower_id: cleanText(row.tower_id, 'Unknown', 40),
    movement_speed_kmh: toNumber(row.movement_speed_kmh),
    direction_degrees: toInt(row.direction_degrees),
    user_density: cleanText(row.user_density, 'Unknown', 20),
    time_of_day: cleanText(row.time_of_day, 'Unknown', 20),
    day_of_week: cleanText(row.day_of_week, 'Unknown', 20),
    location_type: cleanText(row.location_type, 'Unknown', 40),
  };
}

async function flushBatch(rows) {
  const values = [];
  const params = [];
  let p = 1;
  for (const r of rows) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    params.push(r.trace_id, r.trace_time, r.customer_id, r.lat, r.lon, r.city, r.source_tower_id, r.movement_speed_kmh, r.direction_degrees, r.user_density, r.time_of_day, r.day_of_week, r.location_type);
  }
  await pool.query(
    `INSERT INTO mobility_traces
       (trace_id, trace_time, customer_id, lat, lon, city, source_tower_id,
        movement_speed_kmh, direction_degrees, user_density, time_of_day,
        day_of_week, location_type)
     VALUES ${values.join(',')}
     ON CONFLICT (trace_id) DO NOTHING`,
    params
  );
}

async function importMobilityTraces() {
  await bootstrap();
  if (!fs.existsSync(CSV_PATH)) {
    console.error(`CSV not found at: ${CSV_PATH}`);
    process.exit(1);
  }
  let batch = [], parsed = 0, inserted = 0, skipped = 0;
  const parser = fs.createReadStream(CSV_PATH).pipe(parse({ columns: true, skip_empty_lines: true, trim: true }));
  for await (const row of parser) {
    parsed++;
    const normalized = normalizeRow(row);
    if (!normalized) { skipped++; continue; }
    batch.push(normalized);
    if (batch.length >= BATCH_SIZE) {
      await flushBatch(batch);
      inserted += batch.length;
      process.stdout.write(`\rInserted ${inserted.toLocaleString()} mobility traces...`);
      batch = [];
    }
  }
  if (batch.length) { await flushBatch(batch); inserted += batch.length; }
  console.log(`\nImport complete: ${inserted.toLocaleString()} rows sent, ${skipped.toLocaleString()} skipped, ${parsed.toLocaleString()} parsed.`);
  await pool.end();
}

importMobilityTraces().catch((err) => {
  console.error('Import failed:', err.stack || err.message || err);
  process.exit(1);
});
