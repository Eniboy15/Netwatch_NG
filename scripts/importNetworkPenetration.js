require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { pool, bootstrap } = require('../server/db');

const CSV_PATH = process.env.NETWORK_PENETRATION_CSV_PATH || path.join(__dirname, '../data/fourth_generation_fifth_generation_penetration_datasets.csv');
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

function normalizeMonth(value) {
  const raw = cleanText(value, '', 20);
  const d = new Date(raw.length === 7 ? `${raw}-01` : raw);
  return Number.isNaN(d.getTime()) ? null : d.toISOString().slice(0, 10);
}

function normalizeRow(row) {
  const month = normalizeMonth(row.month);
  if (!month || !row.city || !row.state || !row.operator) return null;
  return {
    city: cleanText(row.city, 'Unknown', 60),
    state: cleanText(row.state, 'Unknown', 60),
    operator: cleanText(row.operator, 'Unknown', 20),
    month,
    total_users: toInt(row.total_users),
    users_2g: toInt(row.users_2g),
    users_3g: toInt(row.users_3g),
    users_4g: toInt(row.users_4g),
    users_5g: toInt(row.users_5g),
    penetration_4g_percent: toNumber(row.penetration_4g_percent),
    penetration_5g_percent: toNumber(row.penetration_5g_percent),
    growth_rate_4g_percent: toNumber(row.growth_rate_4g_percent),
    growth_rate_5g_percent: toNumber(row.growth_rate_5g_percent),
  };
}

async function flushBatch(rows) {
  const values = [];
  const params = [];
  let p = 1;
  for (const r of rows) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    params.push(r.city, r.state, r.operator, r.month, r.total_users, r.users_2g, r.users_3g, r.users_4g, r.users_5g, r.penetration_4g_percent, r.penetration_5g_percent, r.growth_rate_4g_percent, r.growth_rate_5g_percent);
  }
  await pool.query(
    `INSERT INTO network_penetration
       (city, state, operator, month, total_users, users_2g, users_3g,
        users_4g, users_5g, penetration_4g_percent, penetration_5g_percent,
        growth_rate_4g_percent, growth_rate_5g_percent)
     VALUES ${values.join(',')}
     ON CONFLICT (city, state, operator, month) DO NOTHING`,
    params
  );
}

async function importNetworkPenetration() {
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
      process.stdout.write(`\rInserted ${inserted.toLocaleString()} penetration rows...`);
      batch = [];
    }
  }
  if (batch.length) { await flushBatch(batch); inserted += batch.length; }
  console.log(`\nImport complete: ${inserted.toLocaleString()} rows sent, ${skipped.toLocaleString()} skipped, ${parsed.toLocaleString()} parsed.`);
  await pool.end();
}

importNetworkPenetration().catch((err) => {
  console.error('Import failed:', err.stack || err.message || err);
  process.exit(1);
});
