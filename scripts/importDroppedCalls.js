require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { pool, bootstrap } = require('../server/db');

const CSV_PATH = process.env.DROPPED_CALLS_CSV_PATH || path.join(__dirname, '../data/dropped_call_records.csv');
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
  const droppedAt = new Date(row.timestamp);
  if (!row.drop_id || Number.isNaN(droppedAt.getTime())) return null;
  return {
    drop_id: cleanText(row.drop_id, '', 40),
    dropped_at: droppedAt.toISOString(),
    operator: cleanText(row.operator, 'Unknown', 20),
    calling_number: cleanText(row.calling_number, 'Unknown', 30),
    called_number: cleanText(row.called_number, 'Unknown', 30),
    source_tower_id: cleanText(row.tower_id, 'Unknown', 40),
    city: cleanText(row.city, 'Unknown', 60),
    call_duration_before_drop: toInt(row.call_duration_before_drop),
    drop_reason: cleanText(row.drop_reason, 'Unknown', 60),
    signal_strength_dbm: toNumber(row.signal_strength_dbm),
    network: normalizeNetwork(row.network_type),
  };
}

async function flushBatch(rows) {
  const values = [];
  const params = [];
  let p = 1;
  for (const r of rows) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    params.push(r.drop_id, r.dropped_at, r.operator, r.calling_number, r.called_number, r.source_tower_id, r.city, r.call_duration_before_drop, r.drop_reason, r.signal_strength_dbm, r.network);
  }
  await pool.query(
    `INSERT INTO dropped_calls
       (drop_id, dropped_at, operator, calling_number, called_number, source_tower_id,
        city, call_duration_before_drop, drop_reason, signal_strength_dbm, network)
     VALUES ${values.join(',')}
     ON CONFLICT (drop_id) DO NOTHING`,
    params
  );
}

async function importDroppedCalls() {
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
      process.stdout.write(`\rInserted ${inserted.toLocaleString()} dropped calls...`);
      batch = [];
    }
  }
  if (batch.length) { await flushBatch(batch); inserted += batch.length; }
  console.log(`\nImport complete: ${inserted.toLocaleString()} rows sent, ${skipped.toLocaleString()} skipped, ${parsed.toLocaleString()} parsed.`);
  await pool.end();
}

importDroppedCalls().catch((err) => {
  console.error('Import failed:', err.stack || err.message || err);
  process.exit(1);
});
