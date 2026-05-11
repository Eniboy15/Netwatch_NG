require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { pool, bootstrap } = require('../server/db');

const CSV_PATH = process.env.TECHNICIAN_LOGS_CSV_PATH || path.join(__dirname, '../data/technician_activity_logs.csv');
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
  if (['true', 'yes', 'y', '1', 'resolved', 'completed', 'success'].includes(raw)) return true;
  if (['false', 'no', 'n', '0', 'unresolved', 'failed'].includes(raw)) return false;
  return null;
}

function toDate(value) {
  if (value === null || value === undefined || value === '') return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function cleanText(value, fallback, maxLen) {
  const raw = value === null || value === undefined ? '' : String(value).trim();
  return (raw || fallback).slice(0, maxLen);
}

function normalizeRow(row, rowNumber) {
  return {
    activity_id: cleanText(first(row, ['activity_id', 'log_id', 'ticket_id', 'work_order_id', 'id']), `TECH-${rowNumber}`, 40),
    technician_id: cleanText(first(row, ['technician_id', 'tech_id', 'field_technician_id']), 'Unknown', 40),
    source_tower_id: cleanText(first(row, ['tower_id', 'source_tower_id', 'site_id']), 'Unknown', 40),
    city: cleanText(first(row, ['city', 'location_city']), 'Unknown', 60),
    operator: cleanText(first(row, ['operator', 'provider']), 'Unknown', 20),
    activity_type: cleanText(first(row, ['activity_type', 'job_type', 'task_type']), 'Unknown', 60),
    priority: cleanText(first(row, ['priority']), 'Unknown', 20),
    status: cleanText(first(row, ['status', 'outcome']), 'Unknown', 30),
    started_at: toDate(first(row, ['started_at', 'start_time', 'timestamp', 'dispatch_time'])),
    ended_at: toDate(first(row, ['ended_at', 'end_time', 'completed_at', 'completion_time'])),
    duration_min: toInt(first(row, ['duration_min', 'duration_minutes', 'activity_duration_min'])),
    issue_resolved: toBoolean(first(row, ['issue_resolved', 'resolved', 'outcome'])),
    travel_km: toNumber(first(row, ['travel_km', 'distance_km'])),
    materials_used: cleanText(first(row, ['materials_used', 'parts_used']), 'None', 120),
    notes: cleanText(first(row, ['notes', 'description', 'remarks']), '', 1000),
  };
}

async function flushBatch(rows) {
  const values = [];
  const params = [];
  let p = 1;
  for (const r of rows) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    params.push(r.activity_id, r.technician_id, r.source_tower_id, r.city, r.operator, r.activity_type, r.priority, r.status, r.started_at, r.ended_at, r.duration_min, r.issue_resolved, r.travel_km, r.materials_used, r.notes);
  }
  await pool.query(
    `INSERT INTO technician_logs
       (activity_id, technician_id, source_tower_id, city, operator, activity_type,
        priority, status, started_at, ended_at, duration_min, issue_resolved,
        travel_km, materials_used, notes)
     VALUES ${values.join(',')}
     ON CONFLICT (activity_id) DO NOTHING`,
    params
  );
}

async function importTechnicianLogs() {
  await bootstrap();
  if (!fs.existsSync(CSV_PATH)) {
    console.error(`CSV not found at: ${CSV_PATH}`);
    process.exit(1);
  }
  let batch = [], parsed = 0, inserted = 0, skipped = 0;
  const parser = fs.createReadStream(CSV_PATH).pipe(parse({ columns: true, skip_empty_lines: true, trim: true }));
  for await (const row of parser) {
    parsed++;
    const normalized = normalizeRow(row, parsed);
    if (!normalized.activity_id) { skipped++; continue; }
    batch.push(normalized);
    if (batch.length >= BATCH_SIZE) {
      await flushBatch(batch);
      inserted += batch.length;
      process.stdout.write(`\rInserted ${inserted.toLocaleString()} technician logs...`);
      batch = [];
    }
  }
  if (batch.length) { await flushBatch(batch); inserted += batch.length; }
  console.log(`\nImport complete: ${inserted.toLocaleString()} rows sent, ${skipped.toLocaleString()} skipped, ${parsed.toLocaleString()} parsed.`);
  await pool.end();
}

importTechnicianLogs().catch((err) => {
  console.error('Import failed:', err.stack || err.message || err);
  process.exit(1);
});
