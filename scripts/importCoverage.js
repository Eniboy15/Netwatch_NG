// scripts/importCoverage.js
// Run once: node scripts/importCoverage.js  (or: npm run import:coverage)
//
// Imports ElectricSheep Africa synthetic coverage and signal-strength data.

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { pool, bootstrap } = require('../server/db');

const CSV_PATH = process.env.COVERAGE_CSV_PATH
  || path.join(__dirname, '../data/coverage_maps_and_signal_strength_data.csv');

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
  const lat = toNumber(first(row, ['latitude', 'lat']));
  const lon = toNumber(first(row, ['longitude', 'lon', 'lng']));
  if (lat === null || lon === null) return null;

  const measurementId = cleanText(
    first(row, ['measurement_id', 'coverage_id', 'record_id', 'id']),
    `COV-${rowNumber}`,
    40
  );

  return {
    measurement_id: measurementId,
    lat,
    lon,
    city: cleanText(first(row, ['city']), 'Unknown', 60),
    operator: cleanText(first(row, ['operator', 'provider']), 'Unknown', 20),
    network: normalizeNetwork(first(row, ['network_type', 'network'])),
    signal_strength_dbm: toNumber(first(row, ['signal_strength_dbm', 'signal_dbm'])),
    coverage_quality: cleanText(first(row, ['coverage_quality', 'quality']), 'Unknown', 20),
    download_speed_mbps: toNumber(first(row, ['download_speed_mbps', 'dl_mbps'])),
    upload_speed_mbps: toNumber(first(row, ['upload_speed_mbps', 'ul_mbps'])),
    latency_ms: toNumber(first(row, ['latency_ms'])),
    source_tower_id: cleanText(first(row, ['tower_id', 'source_tower_id']), 'Unknown', 40),
    distance_to_tower_km: toNumber(first(row, ['distance_to_tower_km', 'distance_km'])),
    indoor_outdoor: cleanText(first(row, ['indoor_outdoor', 'environment']), 'Unknown', 20),
  };
}

async function flushBatch(rows) {
  const values = [];
  const params = [];
  let p = 1;

  for (const r of rows) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    params.push(
      r.measurement_id,
      r.lat,
      r.lon,
      r.city,
      r.operator,
      r.network,
      r.signal_strength_dbm,
      r.coverage_quality,
      r.download_speed_mbps,
      r.upload_speed_mbps,
      r.latency_ms,
      r.source_tower_id,
      r.distance_to_tower_km,
      r.indoor_outdoor
    );
  }

  await pool.query(
    `INSERT INTO coverage_data
       (measurement_id, lat, lon, city, operator, network, signal_strength_dbm,
        coverage_quality, download_speed_mbps, upload_speed_mbps, latency_ms,
        source_tower_id, distance_to_tower_km, indoor_outdoor)
     VALUES ${values.join(',')}
     ON CONFLICT (measurement_id) DO NOTHING`,
    params
  );
}

async function importCoverage() {
  await bootstrap();

  if (!fs.existsSync(CSV_PATH)) {
    console.error(`CSV not found at: ${CSV_PATH}`);
    console.error('Download it into data/coverage_maps_and_signal_strength_data.csv or set COVERAGE_CSV_PATH.');
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
      process.stdout.write(`\rInserted ${inserted.toLocaleString()} coverage rows...`);
      batch = [];
    }
  }

  if (batch.length) {
    await flushBatch(batch);
    inserted += batch.length;
  }

  const { rows: summary } = await pool.query(`
    SELECT operator, network, COUNT(*)::int AS rows
    FROM coverage_data
    GROUP BY operator, network
    ORDER BY operator, network
  `);

  console.log(`\nImport complete: ${inserted.toLocaleString()} parsed rows sent, ${skipped.toLocaleString()} skipped.`);
  console.log(`CSV rows parsed: ${parsed.toLocaleString()}`);
  console.table(summary);

  await pool.end();
}

importCoverage().catch((err) => {
  console.error('Import failed:', err.stack || err.message || err);
  process.exit(1);
});
