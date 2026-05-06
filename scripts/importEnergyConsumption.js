// scripts/importEnergyConsumption.js
// Run once: node scripts/importEnergyConsumption.js  (or: npm run import:energy)
//
// Imports ElectricSheep Africa synthetic tower energy consumption records.

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { pool, bootstrap } = require('../server/db');

const CSV_PATH = process.env.ENERGY_CONSUMPTION_CSV_PATH
  || path.join(__dirname, '../data/tower_energy_consumption_records.csv');

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

function cleanText(value, fallback, maxLen) {
  const raw = value === null || value === undefined ? '' : String(value).trim();
  return (raw || fallback).slice(0, maxLen);
}

function normalizeRow(row) {
  const usageDate = new Date(first(row, ['date', 'usage_date', 'record_date']));
  const sourceTowerId = cleanText(first(row, ['tower_id', 'source_tower_id']), '', 40);
  if (!sourceTowerId || Number.isNaN(usageDate.getTime())) return null;

  return {
    source_tower_id: sourceTowerId,
    usage_date: usageDate.toISOString().slice(0, 10),
    city: cleanText(first(row, ['city']), 'Unknown', 60),
    power_source: cleanText(first(row, ['power_source', 'primary_power_source']), 'Unknown', 40),
    daily_consumption_kwh: toNumber(first(row, ['daily_consumption_kwh', 'energy_consumption_kwh', 'consumption_kwh'])),
    cost_per_kwh_ngn: toNumber(first(row, ['cost_per_kwh_ngn', 'cost_per_kwh'])),
    total_cost_ngn: toNumber(first(row, ['total_cost_ngn', 'daily_cost_ngn', 'cost_ngn'])),
    grid_availability_hours: toInt(first(row, ['grid_availability_hours', 'grid_hours'])),
    generator_runtime_hours: toInt(first(row, ['generator_runtime_hours', 'generator_hours'])),
    solar_generation_kwh: toNumber(first(row, ['solar_generation_kwh', 'solar_kwh'])),
    fuel_consumed_liters: toNumber(first(row, ['fuel_consumed_liters', 'fuel_liters'])),
    carbon_emissions_kg: toNumber(first(row, ['carbon_emissions_kg', 'co2_emissions_kg'])),
  };
}

async function flushBatch(rows) {
  const values = [];
  const params = [];
  let p = 1;

  for (const r of rows) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    params.push(
      r.source_tower_id,
      r.usage_date,
      r.city,
      r.power_source,
      r.daily_consumption_kwh,
      r.cost_per_kwh_ngn,
      r.total_cost_ngn,
      r.grid_availability_hours,
      r.generator_runtime_hours,
      r.solar_generation_kwh,
      r.fuel_consumed_liters,
      r.carbon_emissions_kg
    );
  }

  await pool.query(
    `INSERT INTO energy_consumption
       (source_tower_id, usage_date, city, power_source, daily_consumption_kwh,
        cost_per_kwh_ngn, total_cost_ngn, grid_availability_hours,
        generator_runtime_hours, solar_generation_kwh, fuel_consumed_liters,
        carbon_emissions_kg)
     VALUES ${values.join(',')}
     ON CONFLICT (source_tower_id, usage_date, power_source) DO NOTHING`,
    params
  );
}

async function importEnergyConsumption() {
  await bootstrap();

  if (!fs.existsSync(CSV_PATH)) {
    console.error(`CSV not found at: ${CSV_PATH}`);
    console.error('Download it into data/tower_energy_consumption_records.csv or set ENERGY_CONSUMPTION_CSV_PATH.');
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
      process.stdout.write(`\rInserted ${inserted.toLocaleString()} energy rows...`);
      batch = [];
    }
  }

  if (batch.length) {
    await flushBatch(batch);
    inserted += batch.length;
  }

  const { rows: summary } = await pool.query(`
    SELECT power_source, COUNT(*)::int AS rows,
           ROUND(AVG(daily_consumption_kwh)::numeric, 2) AS avg_kwh
    FROM energy_consumption
    GROUP BY power_source
    ORDER BY rows DESC
  `);

  console.log(`\nImport complete: ${inserted.toLocaleString()} parsed rows sent, ${skipped.toLocaleString()} skipped.`);
  console.log(`CSV rows parsed: ${parsed.toLocaleString()}`);
  console.table(summary);

  await pool.end();
}

importEnergyConsumption().catch((err) => {
  console.error('Import failed:', err.stack || err.message || err);
  process.exit(1);
});
