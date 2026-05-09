// scripts/importHandovers.js
// Run once: node scripts/importHandovers.js  (or: npm run import:handovers)
//
// Imports ElectricSheep Africa synthetic cell-tower handover data.

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { pool, bootstrap } = require('../server/db');

const CSV_PATH = process.env.HANDOVER_RECORDS_CSV_PATH
  || path.join(__dirname, '../data/cell_tower_handover_data.csv');

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

function toBoolean(value) {
  if (value === null || value === undefined || value === '') return null;
  const raw = String(value).trim().toLowerCase();
  if (['true', 'yes', 'y', '1', 'success', 'successful', 'completed'].includes(raw)) return true;
  if (['false', 'no', 'n', '0', 'failure', 'failed', 'dropped'].includes(raw)) return false;
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
  const handoverTime = new Date(first(row, ['timestamp', 'handover_time', 'recorded_at']));
  if (Number.isNaN(handoverTime.getTime())) return null;

  return {
    handover_id: cleanText(first(row, ['handover_id', 'record_id', 'event_id', 'id']), `HND-${rowNumber}`, 40),
    handover_time: handoverTime.toISOString(),
    source_tower_id: cleanText(first(row, ['source_tower_id', 'from_tower_id', 'tower_id_from', 'origin_tower_id']), 'Unknown', 40),
    target_tower_id: cleanText(first(row, ['target_tower_id', 'to_tower_id', 'tower_id_to', 'destination_tower_id']), 'Unknown', 40),
    source_city: cleanText(first(row, ['source_city', 'from_city', 'origin_city', 'city_from']), 'Unknown', 60),
    target_city: cleanText(first(row, ['target_city', 'to_city', 'destination_city', 'city_to']), 'Unknown', 60),
    operator: cleanText(first(row, ['operator', 'provider']), 'Unknown', 20),
    network: normalizeNetwork(first(row, ['network_type', 'network'])),
    handover_type: cleanText(first(row, ['handover_type', 'type']), 'Unknown', 40),
    success: toBoolean(first(row, ['success', 'handover_success', 'successful', 'status'])),
    duration_ms: toNumber(first(row, ['duration_ms', 'handover_duration_ms'])),
    source_signal_dbm: toNumber(first(row, ['source_signal_dbm', 'from_signal_dbm', 'signal_before_dbm'])),
    target_signal_dbm: toNumber(first(row, ['target_signal_dbm', 'to_signal_dbm', 'signal_after_dbm'])),
    failure_reason: cleanText(first(row, ['failure_reason', 'drop_reason', 'reason']), 'None', 80),
    active_call: toBoolean(first(row, ['active_call', 'call_active', 'voice_call_active'])),
    active_data_session: toBoolean(first(row, ['active_data_session', 'data_session_active', 'active_data'])),
    device_speed_kmh: toNumber(first(row, ['device_speed_kmh', 'speed_kmh', 'user_speed_kmh'])),
  };
}

async function flushBatch(rows) {
  const values = [];
  const params = [];
  let p = 1;

  for (const r of rows) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    params.push(
      r.handover_id,
      r.handover_time,
      r.source_tower_id,
      r.target_tower_id,
      r.source_city,
      r.target_city,
      r.operator,
      r.network,
      r.handover_type,
      r.success,
      r.duration_ms,
      r.source_signal_dbm,
      r.target_signal_dbm,
      r.failure_reason,
      r.active_call,
      r.active_data_session,
      r.device_speed_kmh
    );
  }

  await pool.query(
    `INSERT INTO handover_records
       (handover_id, handover_time, source_tower_id, target_tower_id,
        source_city, target_city, operator, network, handover_type, success,
        duration_ms, source_signal_dbm, target_signal_dbm, failure_reason,
        active_call, active_data_session, device_speed_kmh)
     VALUES ${values.join(',')}
     ON CONFLICT (handover_id) DO NOTHING`,
    params
  );
}

async function importHandovers() {
  await bootstrap();

  if (!fs.existsSync(CSV_PATH)) {
    console.error(`CSV not found at: ${CSV_PATH}`);
    console.error('Download it into data/cell_tower_handover_data.csv or set HANDOVER_RECORDS_CSV_PATH.');
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
      process.stdout.write(`\rInserted ${inserted.toLocaleString()} handover records...`);
      batch = [];
    }
  }

  if (batch.length) {
    await flushBatch(batch);
    inserted += batch.length;
  }

  const { rows: summary } = await pool.query(`
    SELECT operator, network, success, COUNT(*)::int AS rows
    FROM handover_records
    GROUP BY operator, network, success
    ORDER BY operator, network, success
  `);

  console.log(`\nImport complete: ${inserted.toLocaleString()} parsed rows sent, ${skipped.toLocaleString()} skipped.`);
  console.log(`CSV rows parsed: ${parsed.toLocaleString()}`);
  console.table(summary);

  await pool.end();
}

importHandovers().catch((err) => {
  console.error('Import failed:', err.stack || err.message || err);
  process.exit(1);
});
